/*-------------------------------------------------------------------------
 *
 * nodeHashgroupjoin.c
 *	  Routines to handle a hash join fused with the GROUP BY above it.
 *
 * dbblue-specific.  See dbblue_groupjoin.md.
 *
 * A HashGroupJoin performs a hash join and its immediately-following grouped
 * aggregation in a single pass over a single hash table.  Each build-side
 * tuple in the join's hash table carries that group's aggregate transition
 * states in extra space allocated after the tuple (HJTUPLE_EXTRA), so probe
 * tuples advance the accumulators in place instead of being emitted.  No
 * second hash table is built and the join's output is never materialized.
 *
 * This is legal only because the planner has proven that the grouping key is
 * the join's hash key (or the probe side's own copy of it -- see
 * groupjoin_keys_match() in planner.c) and that the key uniquely determines a
 * build-side row (see try_add_hashgroupjoin_path()).  One build tuple
 * therefore *is* one group.
 *
 * A genuine (uncommuted) LEFT join is also supported, but only when the
 * planner's probe_side_provably_total() has proven that a NULL probe key is
 * the *only* way such a join can leave a probe row unmatched (an enforced
 * foreign key rules out any other kind of miss).  Under that proof, exactly
 * one extra group can exist beyond the hash table's own entries: the rows
 * whose key came up NULL.  That group is not a hash bucket -- there is no
 * build row for it to attach to -- so it gets one reserved accumulator,
 * hgj_NullKeyPergroup, folded into by the probe phase and emitted once, at
 * the very end, if it ever received a row.
 *
 * The aggregate machinery itself is nodeAgg.c's.  We hold a synthetic Agg
 * plan node and a real AggState built by ExecInitAggMachinery(), and drive it
 * per group: the AggState owns the compiled transition expressions, the final
 * projection (its targetlist is ours, containing the Aggrefs) and the HAVING
 * qual.  Reimplementing transition-function semantics here would be a good way
 * to get pass-by-reference states and strictness subtly wrong.
 *
 * Phases:
 *	  build	 -- MultiExecHash fills the hash table, then we walk it once to
 *				initialize each entry's transition states
 *	  probe	 -- for each outer tuple, advance the transition states of every
 *				matching bucket entry; emit nothing
 *	  emit	 -- walk every bucket entry (not just unmatched ones, as a plain
 *				hash join does), finalize, apply HAVING, project.  This is
 *				what makes an unmatched build row still produce its group with
 *				COUNT returning 0.
 *
 * LIMITATION (v1): single batch only.  The planner refuses to build this path
 * when it estimates more than one batch, and we re-check at runtime.  Groups
 * never span batches (batch number is a pure function of the hash value, and
 * the hash key is the group key), so multi-batch is a natural extension --
 * process each batch independently and emit its groups at batch end -- but it
 * needs the outer-side batch spooling that nodeHashjoin.c does, which is not
 * written yet.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHashgroupjoin.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeAgg.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashgroupjoin.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

/* phases of the fused operator */
#define HGJ_BUILD		1
#define HGJ_PROBE		2
#define HGJ_EMIT		3
#define HGJ_DONE		4

static void ExecHashGroupJoinBuild(HashGroupJoinState *node);
static void ExecHashGroupJoinProbeOne(HashGroupJoinState *node,
									  TupleTableSlot *outerslot);
static TupleTableSlot *ExecHashGroupJoinEmit(HashGroupJoinState *node);
static void ExecHashGroupJoinPopulateEchoSlot(HashGroupJoinState *node,
											  ExprContext *econtext,
											  bool has_probe_match);

/*
 * pergroup_for_tuple
 *		The transition-state array living in a hash entry's extra space.
 */
static inline AggStatePerGroup
pergroup_for_tuple(HashJoinTuple hashTuple)
{
	return (AggStatePerGroup) HJTUPLE_EXTRA(hashTuple);
}

/* ----------------------------------------------------------------
 *		ExecHashGroupJoinBuild
 *
 *		Build the hash table and initialize one group's transition states
 *		per build tuple.
 * ----------------------------------------------------------------
 */
static void
ExecHashGroupJoinBuild(HashGroupJoinState *node)
{
	HashState  *hashNode = (HashState *) innerPlanState(node);
	HashJoinTable hashtable;
	AggState   *aggstate = node->hgj_AggState;
	MemoryContext oldcxt;
	int			i;

	hashtable = ExecHashTableCreate(hashNode);

	/*
	 * Every hash entry must carry its group's transition states.  This has to
	 * be set before a single tuple is inserted.
	 */
	hashtable->extraTupleSpace = node->hgj_PergroupSize;

	/*
	 * v1 is single-batch (see the file header).  Growing the number of
	 * batches mid-build would silently split the input across batch files
	 * that the probe loop below does not know how to read back, so switch
	 * growth off and verify.
	 */
	hashtable->growEnabled = false;
	if (hashtable->nbatch > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("HashGroupJoin does not support multiple batches yet"),
				 errhint("Increase work_mem, or set dbblue_enable_groupjoin to off.")));

	node->hgj_HashTable = hashtable;
	hashNode->hashtable = hashtable;

	(void) MultiExecProcNode((PlanState *) hashNode);

	/*
	 * Initialize each group's transition states.  These must be created in
	 * the aggregate context: pass-by-reference initial values are palloc'd
	 * there, and they have to outlive the per-tuple context.
	 *
	 * Note there is deliberately no "build side is empty, nothing to do"
	 * early exit here (there was one prior to LEFT-join support).  For a
	 * proven-safe LEFT join (see the file header), an empty build side does
	 * not mean zero groups: probe_side_provably_total()'s guarantee runs the
	 * other way too -- if the build side has no rows at all, the foreign key
	 * it relies on means *every* probe row's key must be NULL (a non-NULL
	 * key would require a referenced row to exist), so every probe row
	 * belongs to the reserved group below.  ExecHashTableCreate() always
	 * allocates hashtable->buckets regardless of totalTuples, so the loop
	 * over nbuckets remains safe (and simply does nothing) when it is empty.
	 */
	oldcxt = MemoryContextSwitchTo(aggstate->aggcontexts[0]->ecxt_per_tuple_memory);

	if (node->js.jointype == JOIN_LEFT)
	{
		ExecAggInitPergroup(aggstate, node->hgj_NullKeyPergroup);
		node->hgj_NullKeyMatched = false;
		node->hgj_NullKeyEmitted = false;
	}

	for (i = 0; i < hashtable->nbuckets; i++)
	{
		HashJoinTuple hashTuple = hashtable->buckets.unshared[i];

		while (hashTuple != NULL)
		{
			ExecAggInitPergroup(aggstate, pergroup_for_tuple(hashTuple));
			HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));
			hashTuple = hashTuple->next.unshared;
		}
	}

	MemoryContextSwitchTo(oldcxt);
}

/* ----------------------------------------------------------------
 *		ExecHashGroupJoinProbeOne
 *
 *		Fold one outer tuple into every matching group.  Emits nothing.
 * ----------------------------------------------------------------
 */
static void
ExecHashGroupJoinProbeOne(HashGroupJoinState *node, TupleTableSlot *outerslot)
{
	HashJoinTable hashtable = node->hgj_HashTable;
	AggState   *aggstate = node->hgj_AggState;
	ExprContext *econtext = node->js.ps.ps_ExprContext;
	ExprContext *aggcontext = aggstate->tmpcontext;
	uint32		hashvalue;
	int			bucketno;
	int			batchno;
	HashJoinTuple hashTuple;
	bool		isnull;
	Datum		hashdatum;

	/*
	 * Compute the outer tuple's hash value.  The hash expression reads the
	 * outer tuple from the node's own econtext.
	 */
	econtext->ecxt_outertuple = outerslot;
	ResetExprContext(econtext);

	hashdatum = ExecEvalExprSwitchContext(node->hgj_OuterHash, econtext,
										  &isnull);
	if (isnull)
	{
		/*
		 * A NULL key never matches any bucket.  For a proven-safe LEFT join,
		 * this is the ONE way such a join can produce an unmatched row (see
		 * the file header and probe_side_provably_total() in planner.c), and
		 * it is exactly what the reserved accumulator exists for.  For
		 * INNER/RIGHT there is no such reservation -- an unmatched-by-NULL
		 * probe row simply contributes nothing, same as an ordinary hash
		 * join, and this is not a new case: the plain hash key match below
		 * would have found nothing for it anyway.
		 */
		if (node->js.jointype == JOIN_LEFT)
		{
			aggcontext->ecxt_outertuple = outerslot;
			aggcontext->ecxt_innertuple = node->hgj_NullInnerTupleSlot;
			ExecAggAdvance(aggstate, node->hgj_NullKeyPergroup);
			ResetExprContext(aggcontext);
			node->hgj_NullKeyMatched = true;
		}
		return;
	}

	hashvalue = DatumGetUInt32(hashdatum);
	ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno, &batchno);
	Assert(batchno == 0);		/* single batch, enforced at build time */

	for (hashTuple = hashtable->buckets.unshared[bucketno];
		 hashTuple != NULL;
		 hashTuple = hashTuple->next.unshared)
	{
		if (hashTuple->hashvalue != hashvalue)
			continue;

		ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
							  node->hgj_HashTupleSlot,
							  false);

		econtext->ecxt_innertuple = node->hgj_HashTupleSlot;

		/* Recheck the hash clauses, then any other join quals */
		if (!ExecQualAndReset(node->hashclauses, econtext))
			continue;

		econtext->ecxt_outertuple = outerslot;
		econtext->ecxt_innertuple = node->hgj_HashTupleSlot;

		if (node->js.joinqual != NULL &&
			!ExecQual(node->js.joinqual, econtext))
			continue;

		/*
		 * Quals that must be applied after the join ("otherquals") filter the
		 * joined row before it reaches a transition function.  They do NOT
		 * suppress the group -- an outer join's unmatched build row still
		 * produces one.
		 */
		if (node->js.ps.qual != NULL && !ExecQual(node->js.ps.qual, econtext))
			continue;

		HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(hashTuple));

		/*
		 * Fold the joined row into this group.  The transition expressions
		 * read both sides from the aggregate's own per-tuple context.
		 */
		aggcontext->ecxt_outertuple = outerslot;
		aggcontext->ecxt_innertuple = node->hgj_HashTupleSlot;

		ExecAggAdvance(aggstate, pergroup_for_tuple(hashTuple));

		ResetExprContext(aggcontext);
	}
}

/* ----------------------------------------------------------------
 *		ExecHashGroupJoinPopulateEchoSlot
 *
 *		Fill in the stand-in for the probe side used only when the final
 *		projection bears a bare reference to the probe's own echo of a hash
 *		key (see HashGroupJoin.buildEchoKeys, plannodes.h).
 *
 * If has_probe_match is true, this group was folded from at least one real
 * probe row, so every hash key's echoed value equals the build side's own
 * key value (that equality is exactly what makes fusion legal at all -- see
 * groupjoin_keys_match(), planner.c) -- evaluate buildEchoKeys against
 * econtext's current innertuple (the real build tuple) and use that.
 *
 * If has_probe_match is false, there was no real contributing probe row for
 * this output row at all: either it is a RIGHT join's build entry that
 * nothing ever matched, or it is the reserved NULL-key group.  In ordinary
 * SQL, a row with no real match on the probe side has every probe-side
 * column genuinely NULL, including the echoed one -- not "whatever the
 * build key happens to be".  So this leaves the whole slot NULL rather than
 * evaluating buildEchoKeys at all: evaluating it would read the *build*
 * side's real key (for a RIGHT join's unmatched build entry, that build
 * tuple does exist), which is a real value, not NULL, and would be wrong.
 * ----------------------------------------------------------------
 */
static void
ExecHashGroupJoinPopulateEchoSlot(HashGroupJoinState *node, ExprContext *econtext,
								  bool has_probe_match)
{
	TupleTableSlot *slot = node->hgj_EchoOuterTupleSlot;
	int			natts = slot->tts_tupleDescriptor->natts;
	int			i;

	ExecClearTuple(slot);
	for (i = 0; i < natts; i++)
		slot->tts_isnull[i] = true;

	if (has_probe_match)
	{
		ListCell   *lc1,
				   *lc2;

		forboth(lc1, node->hgj_EchoOuterVars, lc2, node->hgj_EchoBuildExprs)
		{
			Var		   *ovar = lfirst_node(Var, lc1);
			ExprState  *estate = (ExprState *) lfirst(lc2);
			bool		isnull;
			Datum		val;

			val = ExecEvalExpr(estate, econtext, &isnull);
			slot->tts_values[ovar->varattno - 1] = val;
			slot->tts_isnull[ovar->varattno - 1] = isnull;
		}
	}

	ExecStoreVirtualTuple(slot);
}

/* ----------------------------------------------------------------
 *		ExecHashGroupJoinEmit
 *
 *		Walk the hash table and return one row per surviving group.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecHashGroupJoinEmit(HashGroupJoinState *node)
{
	HashJoinTable hashtable = node->hgj_HashTable;
	AggState   *aggstate = node->hgj_AggState;
	ExprContext *aggecontext = aggstate->ss.ps.ps_ExprContext;

	for (;;)
	{
		HashJoinTuple hashTuple = node->hgj_EmitTuple;
		bool		emit_null_key_group = false;
		bool		has_probe_match;

		/* advance to the next non-empty bucket if needed */
		while (hashTuple == NULL)
		{
			if (node->hgj_EmitBucket >= hashtable->nbuckets)
			{
				/*
				 * All ordinary hash-table entries are done.  A proven-safe
				 * LEFT join (see the file header) can have exactly one more
				 * group beyond them: the reserved accumulator for probe rows
				 * whose key came up NULL.  Emit it exactly once, and only if
				 * it actually received a row -- same as any other GROUP BY,
				 * a group nothing ever folded into does not exist.
				 */
				if (node->js.jointype == JOIN_LEFT && !node->hgj_NullKeyEmitted)
				{
					node->hgj_NullKeyEmitted = true;
					if (node->hgj_NullKeyMatched)
					{
						emit_null_key_group = true;
						break;
					}
				}
				return NULL;	/* all groups emitted */
			}
			hashTuple = hashtable->buckets.unshared[node->hgj_EmitBucket];
			node->hgj_EmitBucket++;
		}

		has_probe_match = !emit_null_key_group;

		if (!emit_null_key_group)
		{
			node->hgj_EmitTuple = hashTuple->next.unshared;

			CHECK_FOR_INTERRUPTS();

			if (!HeapTupleHeaderHasMatch(HJTUPLE_MINTUPLE(hashTuple)))
			{
				ExprContext *econtext = node->js.ps.ps_ExprContext;

				/*
				 * dbblue: this build row was never matched by any real probe
				 * row, so if it is emitted at all (RIGHT preserves it -- see
				 * below), any probe-echo column in the output must show NULL,
				 * not the build key -- see
				 * ExecHashGroupJoinPopulateEchoSlot()'s comment.
				 */
				has_probe_match = false;

				/*
				 * For INNER, an unmatched build row yields no group, same as
				 * ever.  For a proven-safe LEFT join, likewise: LEFT
				 * preserves the *probe* side, not the build side, so a build
				 * row nothing ever referenced simply does not appear in the
				 * join's output at all -- it is RIGHT alone that preserves
				 * unmatched build rows.
				 */
				if (node->js.jointype == JOIN_INNER ||
					node->js.jointype == JOIN_LEFT)
					continue;

				/*
				 * The build side is preserved, so the join still produces exactly
				 * one row for this group: the build tuple NULL-extended on the
				 * probe side.  That row has to be folded into the aggregates --
				 * skipping it is not the same thing.  COUNT(*) must come out as 1
				 * here (one joined row) while COUNT(aml.id) comes out as 0 (that
				 * column is NULL), and only running the transition functions over
				 * the NULL-extended row gets both right.
				 *
				 * As in nodeHashjoin's HJ_FILL_INNER_TUPLES, only the otherquals
				 * apply to this synthesized row -- never the joinqual, which by
				 * definition failed for every outer tuple.  If the otherquals
				 * reject it the join emits nothing for this build row, so the
				 * group disappears entirely.
				 */
				ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
									  node->hgj_HashTupleSlot,
									  false);

				ResetExprContext(econtext);
				econtext->ecxt_innertuple = node->hgj_HashTupleSlot;
				econtext->ecxt_outertuple = node->hgj_NullOuterTupleSlot;

				if (node->js.ps.qual != NULL &&
					!ExecQual(node->js.ps.qual, econtext))
					continue;

				aggstate->tmpcontext->ecxt_innertuple = node->hgj_HashTupleSlot;
				aggstate->tmpcontext->ecxt_outertuple = node->hgj_NullOuterTupleSlot;
				ExecAggAdvance(aggstate, pergroup_for_tuple(hashTuple));
				ResetExprContext(aggstate->tmpcontext);
			}

			ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
								  node->hgj_HashTupleSlot,
								  false);
		}

		ResetExprContext(aggecontext);

		/*
		 * The grouping columns and the aggregate arguments both come from the
		 * build tuple's side.  The reserved NULL-key group has no build tuple
		 * at all -- there was never a match -- so hgj_NullInnerTupleSlot
		 * stands in for it, correctly all-NULLs.
		 */
		aggecontext->ecxt_innertuple = emit_null_key_group ?
			node->hgj_NullInnerTupleSlot : node->hgj_HashTupleSlot;

		/*
		 * There is no single probe tuple at emit time -- a group may have
		 * been folded from many, or from none.  The stand-in supplies the one
		 * thing the output can legitimately need from the probe side: its own
		 * echo of a hash key.  It must be built *after* ecxt_innertuple is
		 * set above, since that is what buildEchoKeys reads.
		 */
		ExecHashGroupJoinPopulateEchoSlot(node, aggecontext, has_probe_match);
		aggecontext->ecxt_outertuple = node->hgj_EchoOuterTupleSlot;

		ExecAggFinalize(aggstate, emit_null_key_group ?
					   node->hgj_NullKeyPergroup : pergroup_for_tuple(hashTuple));

		/* HAVING, which the AggState owns as its plan qual */
		if (aggstate->ss.ps.qual != NULL &&
			!ExecQual(aggstate->ss.ps.qual, aggecontext))
			continue;

		return ExecProject(aggstate->ss.ps.ps_ProjInfo);
	}
}

/* ----------------------------------------------------------------
 *		ExecHashGroupJoin
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecHashGroupJoin(PlanState *pstate)
{
	HashGroupJoinState *node = castNode(HashGroupJoinState, pstate);
	PlanState  *outerNode = outerPlanState(node);

	CHECK_FOR_INTERRUPTS();

	for (;;)
	{
		switch (node->hgj_Phase)
		{
			case HGJ_BUILD:
				ExecHashGroupJoinBuild(node);
				node->hgj_Phase = HGJ_PROBE;
				break;

			case HGJ_PROBE:
				{
					TupleTableSlot *outerslot;

					/*
					 * Nothing on the build side: for INNER/RIGHT, no groups
					 * are possible at all (RIGHT's own preserved-row entries
					 * live in the hash table, and there are none).  A
					 * proven-safe LEFT join is different -- see
					 * ExecHashGroupJoinBuild()'s comment -- so it must still
					 * probe, to accumulate into the reserved group, and go
					 * to EMIT rather than DONE.
					 */
					if (node->hgj_HashTable->totalTuples == 0 &&
						node->js.jointype != JOIN_LEFT)
					{
						node->hgj_Phase = HGJ_DONE;
						break;
					}

					outerslot = ExecProcNode(outerNode);
					if (TupIsNull(outerslot))
					{
						node->hgj_Phase = HGJ_EMIT;
						node->hgj_EmitBucket = 0;
						node->hgj_EmitTuple = NULL;
						break;
					}

					ExecHashGroupJoinProbeOne(node, outerslot);
					break;
				}

			case HGJ_EMIT:
				{
					TupleTableSlot *result = ExecHashGroupJoinEmit(node);

					if (!TupIsNull(result))
						return result;
					node->hgj_Phase = HGJ_DONE;
					break;
				}

			case HGJ_DONE:
				return NULL;

			default:
				elog(ERROR, "unrecognized hashgroupjoin phase: %d",
					 node->hgj_Phase);
		}
	}
}

/* ----------------------------------------------------------------
 *		ExecInitHashGroupJoin
 * ----------------------------------------------------------------
 */
HashGroupJoinState *
ExecInitHashGroupJoin(HashGroupJoin *node, EState *estate, int eflags)
{
	HashGroupJoinState *hgjstate;
	Plan	   *outerNode;
	Hash	   *hashNode;
	HashState  *hashstate;
	TupleDesc	outerDesc;
	Agg		   *aggnode;
	const TupleTableSlotOps *ops;
	Oid		   *outer_hashfuncid;
	Oid		   *inner_hashfuncid;
	bool	   *hash_strict;
	ListCell   *lc;
	int			nkeys;

	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	hgjstate = makeNode(HashGroupJoinState);
	hgjstate->js.ps.plan = (Plan *) node;
	hgjstate->js.ps.state = estate;
	hgjstate->js.ps.ExecProcNode = ExecHashGroupJoin;
	hgjstate->js.jointype = node->join.jointype;
	hgjstate->hgj_Phase = HGJ_BUILD;

	ExecAssignExprContext(estate, &hgjstate->js.ps);

	/*
	 * initialize child nodes
	 */
	outerNode = outerPlan(node);
	hashNode = (Hash *) innerPlan(node);

	outerPlanState(hgjstate) = ExecInitNode(outerNode, estate, eflags);
	outerDesc = ExecGetResultType(outerPlanState(hgjstate));
	innerPlanState(hgjstate) = ExecInitNode((Plan *) hashNode, estate, eflags);
	hashstate = (HashState *) innerPlanState(hgjstate);

	/*
	 * This node's own result slot describes the join's inputs only; the row
	 * we actually return is projected by the AggState, whose targetlist is
	 * this node's targetlist.  We still need a result tuple descriptor here
	 * because the hash expressions are built against it, exactly as
	 * ExecInitHashJoin does.
	 */
	ExecInitResultTupleSlotTL(&hgjstate->js.ps, &TTSOpsVirtual);

	ops = ExecGetResultSlotOps(outerPlanState(hgjstate), NULL);
	hgjstate->hgj_OuterTupleSlot = ExecInitExtraTupleSlot(estate, outerDesc,
														  ops);
	hgjstate->hgj_NullOuterTupleSlot =
		ExecInitNullTupleSlot(estate, outerDesc, ops);

	hgjstate->hgj_HashTupleSlot = hashstate->ps.ps_ResultTupleSlot;

	/*
	 * A proven-safe LEFT join (see the file header) needs an all-NULLs
	 * stand-in for the BUILD side too, for the reserved NULL-key group's
	 * emit step: that group has no real build tuple, since by construction
	 * nothing in the hash table ever matches it.
	 */
	if (node->join.jointype == JOIN_LEFT)
	{
		TupleDesc	innerDesc = ExecGetResultType(innerPlanState(hgjstate));
		const TupleTableSlotOps *innerOps =
			ExecGetResultSlotOps(innerPlanState(hgjstate), NULL);

		hgjstate->hgj_NullInnerTupleSlot =
			ExecInitNullTupleSlot(estate, innerDesc, innerOps);
	}

	/*
	 * Join-level expressions.  These reference the two join inputs, so they
	 * are evaluated against this node's econtext with both slots set.
	 */
	hgjstate->js.ps.qual =
		ExecInitQual(node->join.plan.qual, (PlanState *) hgjstate);
	hgjstate->js.joinqual =
		ExecInitQual(node->join.joinqual, (PlanState *) hgjstate);
	hgjstate->hashclauses =
		ExecInitQual(node->hashclauses, (PlanState *) hgjstate);

	/*
	 * Build the hash-value expression for the outer (probe) side, exactly as
	 * ExecInitHashJoin does.  The inner side's expression is set up by
	 * ExecInitHash on the Hash node itself.
	 */
	nkeys = list_length(node->hashoperators);
	outer_hashfuncid = palloc_array(Oid, nkeys);
	inner_hashfuncid = palloc_array(Oid, nkeys);
	hash_strict = palloc_array(bool, nkeys);

	foreach(lc, node->hashoperators)
	{
		Oid			hashop = lfirst_oid(lc);
		int			i = foreach_current_index(lc);

		if (!get_op_hash_functions(hashop,
								   &outer_hashfuncid[i],
								   &inner_hashfuncid[i]))
			elog(ERROR, "could not find hash function for hash operator %u",
				 hashop);
		hash_strict[i] = op_strict(hashop);
	}

	hgjstate->hgj_OuterHash =
		ExecBuildHash32Expr(hgjstate->js.ps.ps_ResultTupleDesc,
							hgjstate->js.ps.resultops,
							outer_hashfuncid,
							node->hashcollations,
							node->hashkeys,
							hash_strict,
							&hgjstate->js.ps,
							0);

	/*
	 * dbblue: the probe-echo stand-in (see the field comments in
	 * execnodes.h and HashGroupJoin.buildEchoKeys in plannodes.h).  Shaped
	 * like the outer side, since that is what a bare reference to the
	 * probe's own echo of a hash key expects to read from; compiled once,
	 * populated fresh by ExecHashGroupJoinPopulateEchoSlot() on every emit.
	 */
	hgjstate->hgj_EchoOuterTupleSlot = ExecInitExtraTupleSlot(estate, outerDesc,
															  ops);
	hgjstate->hgj_EchoOuterVars = node->hashkeys;
	hgjstate->hgj_EchoBuildExprs = ExecInitExprList(node->buildEchoKeys,
													(PlanState *) hgjstate);

	/*
	 * The Hash node's own expression, for the build side.  ExecInitHash does
	 * not build this -- the parent join does, because only the parent knows
	 * the join operators.
	 */
	hashstate->hash_expr =
		ExecBuildHash32Expr(hashstate->ps.ps_ResultTupleDesc,
							hashstate->ps.resultops,
							inner_hashfuncid,
							node->hashcollations,
							hashNode->hashkeys,
							hash_strict,
							&hashstate->ps,
							0);

	/*
	 * No build tuple can have a NULL hash key: try_add_hashgroupjoin_path()
	 * requires the grouping/hash columns to be NOT NULL.  That is what lets
	 * us skip the null-tuple store here -- tuples in it are not in any bucket
	 * and the emit phase would miss them -- and it is also what makes one
	 * hash entry equal one GROUP BY group, since GROUP BY folds all NULLs
	 * into a single group while a unique index does not.
	 */
	hashstate->keep_null_tuples = false;

	pfree(outer_hashfuncid);
	pfree(inner_hashfuncid);
	pfree(hash_strict);

	/*
	 * Set up the aggregate machinery.
	 *
	 * The synthetic Agg is never executed and has no child; it exists so that
	 * nodeAgg.c can build the pertrans state, the compiled transition
	 * expressions, the final projection (EEOP_AGGREF needs an AggState
	 * parent) and the HAVING qual.  AGG_PLAIN with numCols == 0 means nodeAgg
	 * manages exactly one implicit group and never builds a hash table of its
	 * own -- we supply the per-group state on every call.
	 */
	aggnode = makeNode(Agg);
	aggnode->plan.targetlist = node->join.plan.targetlist;
	aggnode->plan.qual = node->havingQual;
	aggnode->plan.lefttree = NULL;
	aggnode->plan.righttree = NULL;
	aggnode->aggstrategy = AGG_PLAIN;
	aggnode->aggsplit = AGGSPLIT_SIMPLE;
	aggnode->numCols = 0;
	aggnode->grpColIdx = NULL;
	aggnode->grpOperators = NULL;
	aggnode->grpCollations = NULL;
	aggnode->numGroups = node->numGroups;
	aggnode->transitionSpace = node->transitionSpace;
	aggnode->aggParams = node->aggParams;
	aggnode->groupingSets = NIL;
	aggnode->chain = NIL;

	/*
	 * Tell nodeAgg the exact slot types the transition expressions will see:
	 * probe tuples in the outer slot, hash-table tuples (always minimal) in
	 * the inner slot.  Leaving these unfixed costs real time -- the expression
	 * compiler falls back to generic slot access instead of emitting
	 * specialized deform steps for the hot per-probe-row path.
	 */
	hgjstate->hgj_AggState =
		ExecInitAggMachinery(aggnode, estate, eflags, outerDesc, ops,
							 ExecGetResultSlotOps(innerPlanState(hgjstate),
												  NULL),
							 outerPlanState(hgjstate),
							 innerPlanState(hgjstate));
	hgjstate->hgj_PergroupSize =
		MAXALIGN(ExecAggPergroupSize(hgjstate->hgj_AggState));

	/*
	 * The reserved accumulator for the NULL-key group (proven-safe LEFT
	 * joins only -- see the file header).  Allocated once, up front, in the
	 * query's long-lived per-query context (the default context here, since
	 * this runs during executor init, before any per-tuple context is even
	 * current); ExecHashGroupJoinBuild() initializes its actual transition
	 * values in the aggregate context on every (re)build, same as any hash
	 * entry's.
	 */
	if (node->join.jointype == JOIN_LEFT)
		hgjstate->hgj_NullKeyPergroup =
			(struct AggStatePerGroupData *) palloc0(hgjstate->hgj_PergroupSize);

	return hgjstate;
}

/* ----------------------------------------------------------------
 *		ExecEndHashGroupJoin
 * ----------------------------------------------------------------
 */
void
ExecEndHashGroupJoin(HashGroupJoinState *node)
{
	if (node->hgj_HashTable)
	{
		ExecHashTableDestroy(node->hgj_HashTable);
		node->hgj_HashTable = NULL;
	}

	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));
}
