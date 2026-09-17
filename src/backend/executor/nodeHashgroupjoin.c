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
 * Phases, cycling probe -> emit once per batch:
 *	  build	 -- MultiExecHash fills the hash table, then we walk it once to
 *				initialize each entry's transition states
 *	  probe	 -- for each outer tuple, advance the transition states of every
 *				matching bucket entry; emit nothing.  Rows whose key belongs
 *				to a batch not yet loaded are written to that batch's spill
 *				file instead.
 *	  emit	 -- walk every bucket entry (not just unmatched ones, as a plain
 *				hash join does), finalize, apply HAVING, project.  This is
 *				what makes an unmatched build row still produce its group with
 *				COUNT returning 0.  At the end, load the next batch and return
 *				to probe, or finish.
 *
 * Multiple batches are supported.  Fusing across them is sound because a
 * tuple's batch number is a pure function of its hash value, and that hash is
 * taken over the join key -- which the planner has proven is the grouping key.
 * Two rows of one group therefore always land in the same batch, so a batch's
 * groups are complete as soon as its probe rows are exhausted, and can be
 * finalized and emitted before the next batch is loaded.  Only one batch's
 * accumulators are resident at a time; ExecHashGroupJoinNextBatch() releases
 * the previous batch's before loading the next.
 *
 * The one piece of state that does not belong to any batch is the reserved
 * NULL-key group, since NULL keys hash nowhere.  It is complete after the
 * single pass over the outer plan (batch 0's probe sees every outer row), so
 * it is emitted at the end of batch 0 and its accumulator need not survive
 * into later batches.
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
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

/* phases of the fused operator */
#define HGJ_BUILD		1
#define HGJ_PROBE		2
#define HGJ_EMIT		3
#define HGJ_DONE		4

static void ExecHashGroupJoinBuild(HashGroupJoinState *node);
static void ExecHashGroupJoinInitPergroups(HashGroupJoinState *node);
static bool ExecHashGroupJoinNextBatch(HashGroupJoinState *node);
static TupleTableSlot *ExecHashGroupJoinNextOuter(HashGroupJoinState *node,
												  bool *have_hashvalue,
												  uint32 *hashvalue);
static void ExecHashGroupJoinProbeOne(HashGroupJoinState *node,
									  TupleTableSlot *outerslot,
									  bool have_hashvalue,
									  uint32 given_hashvalue);
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

	/*
	 * Every hash entry carries its group's transition states in extra space
	 * after the tuple.  Passing the size in means ExecChooseHashTableSize()
	 * picks nbatch against the real entry width, so the table starts out with
	 * the number of batches it will actually need.  (Getting this wrong was
	 * never a correctness problem -- spaceUsed counts the extra area, so the
	 * table would just split again at run time -- but that costs a wasted
	 * repartition pass over the whole build side.)
	 */
	hashtable = ExecHashTableCreate(hashNode, node->hgj_PergroupSize);

	node->hgj_HashTable = hashtable;
	hashNode->hashtable = hashtable;

	(void) MultiExecProcNode((PlanState *) hashNode);

	if (node->js.jointype == JOIN_LEFT)
	{
		oldcxt = MemoryContextSwitchTo(aggstate->aggcontexts[0]->ecxt_per_tuple_memory);
		ExecAggInitPergroup(aggstate, node->hgj_NullKeyPergroup);
		MemoryContextSwitchTo(oldcxt);
		node->hgj_NullKeyMatched = false;
		node->hgj_NullKeyEmitted = false;
	}

	ExecHashGroupJoinInitPergroups(node);
}

/* ----------------------------------------------------------------
 *		ExecHashGroupJoinInitPergroups
 *
 *		Give every build tuple now resident in the hash table a freshly
 *		initialized transition-state area.  Called once per batch, after all
 *		of that batch's build tuples are in place -- never before, since
 *		loading can relocate tuples to later batches.
 *
 * The states must be created in the aggregate context: pass-by-reference
 * initial values are palloc'd there, and they have to outlive the per-tuple
 * context.
 *
 * Note there is deliberately no "build side is empty, nothing to do" early
 * exit here (there was one prior to LEFT-join support).  For a proven-safe
 * LEFT join (see the file header), an empty build side does not mean zero
 * groups: probe_side_provably_total()'s guarantee runs the other way too --
 * if the build side has no rows at all, the foreign key it relies on means
 * *every* probe row's key must be NULL (a non-NULL key would require a
 * referenced row to exist), so every probe row belongs to the reserved
 * group.  ExecHashTableCreate() always allocates hashtable->buckets
 * regardless of totalTuples, so the loop below remains safe (and simply does
 * nothing) when it is empty.
 * ----------------------------------------------------------------
 */
static void
ExecHashGroupJoinInitPergroups(HashGroupJoinState *node)
{
	HashJoinTable hashtable = node->hgj_HashTable;
	AggState   *aggstate = node->hgj_AggState;
	MemoryContext oldcxt;
	int			i;

	oldcxt = MemoryContextSwitchTo(aggstate->aggcontexts[0]->ecxt_per_tuple_memory);

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
 *		ExecHashGroupJoinNextBatch
 *
 *		Retire the batch just emitted and load the next one.  Returns false
 *		when every batch has been processed.
 *
 * Fusing across batches is sound because a tuple's batch is a pure function
 * of its hash value, and the hash value is computed from the join key -- the
 * very columns the grouping is keyed on.  Two rows of the same group
 * therefore always land in the same batch, so a batch's groups are complete
 * the moment its probe rows run out, and can be finalized and emitted before
 * the next batch is loaded.  That is what keeps only one batch's worth of
 * accumulators in memory at a time.
 * ----------------------------------------------------------------
 */
static bool
ExecHashGroupJoinNextBatch(HashGroupJoinState *node)
{
	HashJoinTable hashtable = node->hgj_HashTable;
	AggState   *aggstate = node->hgj_AggState;
	int			nbatch = hashtable->nbatch;
	int			curbatch = hashtable->curbatch;
	TupleTableSlot *slot;
	uint32		hashvalue;
	BufFile    *innerFile;

	if (nbatch == 1)
		return false;

	curbatch++;

	/*
	 * Skip batches that cannot produce a group, mirroring
	 * ExecHashJoinNewBatch().  A batch with no build tuples has no groups at
	 * all for INNER and for a proven-safe LEFT (whose unmatched build rows
	 * are dropped anyway); RIGHT is the exception, since it preserves build
	 * rows nothing matched.  The nbatch_original / nbatch_outstart tests
	 * cover batches written before a split, which may hold tuples that now
	 * belong even later.
	 *
	 * There is no converse "outer file non-empty, inner missing" case to
	 * worry about: probe_side_provably_total() is what licensed a LEFT join
	 * here in the first place, so a probe row with a non-NULL key always has
	 * a build row somewhere, and NULL-keyed rows never reach a batch file.
	 */
	while (curbatch < nbatch &&
		   (hashtable->outerBatchFile[curbatch] == NULL ||
			hashtable->innerBatchFile[curbatch] == NULL))
	{
		if (hashtable->innerBatchFile[curbatch] &&
			node->js.jointype == JOIN_RIGHT)
			break;
		if (hashtable->innerBatchFile[curbatch] &&
			nbatch != hashtable->nbatch_original)
			break;
		if (hashtable->outerBatchFile[curbatch] &&
			nbatch != hashtable->nbatch_outstart)
			break;

		if (hashtable->innerBatchFile[curbatch])
			BufFileClose(hashtable->innerBatchFile[curbatch]);
		hashtable->innerBatchFile[curbatch] = NULL;
		if (hashtable->outerBatchFile[curbatch])
			BufFileClose(hashtable->outerBatchFile[curbatch]);
		hashtable->outerBatchFile[curbatch] = NULL;
		curbatch++;
	}

	if (curbatch >= nbatch)
		return false;

	hashtable->curbatch = curbatch;

	/*
	 * Discard the finished batch.  Its hash entries -- and the state areas
	 * embedded in them -- live in the table's batch context, which
	 * ExecHashTableReset() clears.  The transition *values* those states
	 * point at were palloc'd in the aggregate context instead, so they have
	 * to be released separately or every batch would strand another full set,
	 * which is precisely the memory this operator exists to avoid.  Rescan
	 * rather than plain reset, so a transfn's registered callback still runs.
	 */
	ExecHashTableReset(hashtable);
	ReScanExprContext(aggstate->aggcontexts[0]);

	/* Reload the hash table with the new inner batch (which could be empty) */
	innerFile = hashtable->innerBatchFile[curbatch];

	if (innerFile != NULL)
	{
		if (BufFileSeek(innerFile, 0, 0, SEEK_SET))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not rewind hash-join temporary file")));

		while ((slot = ExecHashJoinGetSavedTuple(innerFile,
												 &hashvalue,
												 node->hgj_HashTupleSlot)))
		{
			/*
			 * NOTE: some tuples may be sent to future batches.  Also, it is
			 * possible for hashtable->nbatch to be increased here!
			 */
			ExecHashTableInsert(hashtable, slot, hashvalue);
		}

		BufFileClose(innerFile);
		hashtable->innerBatchFile[curbatch] = NULL;
	}

	/* Only now is the batch's membership final, so states can be created. */
	ExecHashGroupJoinInitPergroups(node);

	/* Rewind the matching probe rows, if we spilled any. */
	if (hashtable->outerBatchFile[curbatch] != NULL &&
		BufFileSeek(hashtable->outerBatchFile[curbatch], 0, 0, SEEK_SET))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rewind hash-join temporary file")));

	node->hgj_EmitBucket = 0;
	node->hgj_EmitTuple = NULL;

	return true;
}

/* ----------------------------------------------------------------
 *		ExecHashGroupJoinNextOuter
 *
 *		Next probe row for the current batch: straight from the outer plan
 *		while batch 0 is current, and from that batch's spill file after
 *		that.  *hashvalue is set only in the latter case, where the value was
 *		recorded when the row was written and need not be recomputed.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecHashGroupJoinNextOuter(HashGroupJoinState *node, bool *have_hashvalue,
						   uint32 *hashvalue)
{
	HashJoinTable hashtable = node->hgj_HashTable;

	*have_hashvalue = false;

	if (hashtable->curbatch == 0)
		return ExecProcNode(outerPlanState(node));

	if (hashtable->outerBatchFile[hashtable->curbatch] == NULL)
		return NULL;

	*have_hashvalue = true;
	return ExecHashJoinGetSavedTuple(hashtable->outerBatchFile[hashtable->curbatch],
									 hashvalue,
									 node->hgj_OuterTupleSlot);
}

/* ----------------------------------------------------------------
 *		ExecHashGroupJoinProbeOne
 *
 *		Fold one outer tuple into every matching group.  Emits nothing.
 * ----------------------------------------------------------------
 */
static void
ExecHashGroupJoinProbeOne(HashGroupJoinState *node, TupleTableSlot *outerslot,
						  bool have_hashvalue, uint32 given_hashvalue)
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
	 *
	 * A row replayed from a spill file already carries the value computed
	 * when it was written, and cannot have a NULL key -- those are folded in
	 * below during the single pass over the outer plan and are never spilled.
	 */
	econtext->ecxt_outertuple = outerslot;
	ResetExprContext(econtext);

	if (have_hashvalue)
	{
		hashdatum = UInt32GetDatum(given_hashvalue);
		isnull = false;
	}
	else
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

	if (batchno != hashtable->curbatch)
	{
		bool		shouldFree;
		MinimalTuple mintuple = ExecFetchSlotMinimalTuple(outerslot,
														  &shouldFree);

		/*
		 * This row's group lives in a batch we have not loaded yet.  Park it
		 * in that batch's spill file; it will be folded in when we get there.
		 * Note batchno can be *later* than the file this row was read from,
		 * if the table split again while the current batch was loading.
		 */
		Assert(batchno > hashtable->curbatch);
		ExecHashJoinSaveTuple(mintuple, hashvalue,
							  &hashtable->outerBatchFile[batchno],
							  hashtable);

		if (shouldFree)
			heap_free_minimal_tuple(mintuple);
		return;
	}

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

	CHECK_FOR_INTERRUPTS();

	for (;;)
	{
		switch (node->hgj_Phase)
		{
			case HGJ_BUILD:
				ExecHashGroupJoinBuild(node);

				/*
				 * Remember whether nbatch grows once the outer scan is under
				 * way; ExecHashGroupJoinNextBatch() needs it to decide which
				 * spill files it may skip.
				 */
				node->hgj_HashTable->nbatch_outstart =
					node->hgj_HashTable->nbatch;
				node->hgj_Phase = HGJ_PROBE;
				break;

			case HGJ_PROBE:
				{
					TupleTableSlot *outerslot;
					bool		have_hashvalue;
					uint32		hashvalue = 0;

					/*
					 * Nothing on the build side: for INNER/RIGHT, no groups
					 * are possible at all (RIGHT's own preserved-row entries
					 * live in the hash table, and there are none).  A
					 * proven-safe LEFT join is different -- see
					 * ExecHashGroupJoinBuild()'s comment -- so it must still
					 * probe, to accumulate into the reserved group, and go
					 * to EMIT rather than DONE.
					 *
					 * totalTuples counts the whole build relation, not just
					 * the resident batch, so this stays correct once the join
					 * is split across batches.
					 */
					if (node->hgj_HashTable->totalTuples == 0 &&
						node->js.jointype != JOIN_LEFT)
					{
						node->hgj_Phase = HGJ_DONE;
						break;
					}

					outerslot = ExecHashGroupJoinNextOuter(node,
														   &have_hashvalue,
														   &hashvalue);
					if (TupIsNull(outerslot))
					{
						node->hgj_Phase = HGJ_EMIT;
						node->hgj_EmitBucket = 0;
						node->hgj_EmitTuple = NULL;
						break;
					}

					ExecHashGroupJoinProbeOne(node, outerslot,
											  have_hashvalue, hashvalue);
					break;
				}

			case HGJ_EMIT:
				{
					TupleTableSlot *result = ExecHashGroupJoinEmit(node);

					if (!TupIsNull(result))
						return result;

					/*
					 * This batch's groups are complete and emitted.  Retire
					 * it and pick up the next one, if any -- the accumulators
					 * for the batch just finished are released there, which
					 * is what bounds memory to a single batch.
					 */
					if (ExecHashGroupJoinNextBatch(node))
					{
						node->hgj_Phase = HGJ_PROBE;
						break;
					}

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

/* ----------------------------------------------------------------
 *		ExecReScanHashGroupJoin
 * ----------------------------------------------------------------
 */
void
ExecReScanHashGroupJoin(HashGroupJoinState *node)
{
	PlanState  *outerPlan = outerPlanState(node);
	PlanState  *innerPlan = innerPlanState(node);
	AggState   *aggstate = node->hgj_AggState;
	HashState  *hashNode;

	/*
	 * If we never got as far as building the table there is nothing to undo,
	 * and the next ExecHashGroupJoin() will start from HGJ_BUILD on its own.
	 */
	if (node->hgj_HashTable == NULL)
	{
		if (outerPlan->chgParam == NULL)
			ExecReScan(outerPlan);
		if (innerPlan->chgParam == NULL)
			ExecReScan(innerPlan);
		return;
	}

	/*
	 * Fast path: unlike a plain hash join, which has already handed its
	 * joined rows upwards and keeps only the build tuples, our hash table
	 * still holds the entire result -- one group per entry, with its
	 * transition values folded in.  So when nothing that feeds either side
	 * has changed, a rescan is just a replay: rewind the emit cursor and walk
	 * the buckets again, skipping both the build and the probe.
	 *
	 * This re-runs the finalfns over transition values that were already
	 * finalized once.  That is the same thing ExecReScanAgg() does on its own
	 * AGG_HASHED fast path, so the aggregates we accept are already required
	 * to tolerate it.
	 *
	 * We insist on our own chgParam being empty too.  A changed parameter can
	 * reach an aggregate argument or the HAVING qual without appearing in
	 * either child's chgParam, and unlike nodeAgg we have no aggParams set to
	 * test it against.
	 *
	 * This only works for a single-batch join.  Once the run has been split,
	 * the table holds just the batch that happened to be loaded last -- every
	 * earlier batch's groups were emitted and then discarded, and their spill
	 * files closed -- so there is nothing left to replay.  Multi-batch rescans
	 * go the hard way, as ExecReScanHashJoin() does for the same reason.
	 */
	if (node->hgj_HashTable->nbatch == 1 &&
		outerPlan->chgParam == NULL &&
		innerPlan->chgParam == NULL &&
		node->js.ps.chgParam == NULL)
	{
		node->hgj_Phase = HGJ_EMIT;
		node->hgj_EmitBucket = 0;
		node->hgj_EmitTuple = NULL;

		/*
		 * Let the reserved NULL-key group be emitted again, but leave
		 * hgj_NullKeyMatched alone -- whether that group ever received a row
		 * is part of the result we are replaying, not scan state.
		 */
		node->hgj_NullKeyEmitted = false;
		return;
	}

	/*
	 * Otherwise the groups, the transition values, or both are stale, and
	 * there is nothing incremental to salvage: throw the table away and
	 * rebuild from scratch.
	 */
	hashNode = castNode(HashState, innerPlan);
	Assert(hashNode->hashtable == node->hgj_HashTable);

	/* accumulate stats from the old table, if wanted (cf. ExecShutdownHash) */
	if (hashNode->ps.instrument && !hashNode->hinstrument)
		hashNode->hinstrument = palloc0_object(HashInstrumentation);
	if (hashNode->hinstrument)
		ExecHashAccumInstrumentation(hashNode->hinstrument,
									 hashNode->hashtable);

	/* for safety, be sure to clear the child plan node's pointer too */
	hashNode->hashtable = NULL;

	ExecHashTableDestroy(node->hgj_HashTable);
	node->hgj_HashTable = NULL;

	/*
	 * Release the groups' transition values.  Those are palloc'd in the
	 * aggregate context rather than inside the hash entries, so destroying
	 * the table above did not reclaim them; without this, every rescan would
	 * leak another full set.  Rescan rather than plain reset, so that any
	 * callback a transfn registered gets to run (cf. ExecReScanAgg).
	 *
	 * v1 plans a single grouping set, so context 0 is the only one in use.
	 * hgj_NullKeyPergroup itself survives -- it is allocated once at init in
	 * the per-query context, and ExecHashGroupJoinBuild() re-initializes it.
	 */
	ReScanExprContext(aggstate->aggcontexts[0]);

	node->hgj_Phase = HGJ_BUILD;
	node->hgj_EmitBucket = 0;
	node->hgj_EmitTuple = NULL;
	node->hgj_NullKeyMatched = false;
	node->hgj_NullKeyEmitted = false;

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (innerPlan->chgParam == NULL)
		ExecReScan(innerPlan);
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);
}
