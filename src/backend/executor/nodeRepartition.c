/*-------------------------------------------------------------------------
 *
 * nodeRepartition.c
 *	  Hash-exchange of partial aggregate rows between parallel participants.
 *
 * With stock parallel aggregation every worker emits one row per group it
 * saw, all of those rows travel through the Gather's tuple queues, and the
 * leader alone runs the Finalize aggregate over them.  When there are many
 * groups that serial merge dominates the runtime and does not shrink as
 * workers are added.
 *
 * This node sits between a Partial aggregate and a Finalize hash aggregate,
 * both of which run inside the parallel workers (below the Gather).  It
 * redistributes the partial rows so that all rows of a given group meet in a
 * single participant, which lets the Finalize step run in parallel too:
 *
 *	 1. WRITE phase.  Every participant drains its child, hashes each row on
 *		the grouping columns and appends it, as a MinimalTuple, to one of
 *		num_partitions SharedTuplestores.  Nothing is returned to the parent
 *		during this phase.
 *	 2. The participants meet at a barrier, so that every partition is
 *		complete before any of it is read.
 *	 3. READ phase.  Each participant repeatedly claims a whole, not yet
 *		claimed, partition from a shared counter and returns every row in it
 *		(from all participants' files).  The Finalize aggregate above sees
 *		the union of the partitions this participant claimed.
 *
 * Because a partition is read by exactly one participant, every group is
 * finalized exactly once, and the output of the participants is disjoint.
 * Participants can attach late (workers start at different times, and the
 * leader may be busy elsewhere): BarrierAttach() tells us the current phase,
 * and a participant that arrives after the WRITE phase has ended simply has
 * nothing to write and goes straight to claiming partitions.
 *
 * If there is no DSM segment (the plan is run without workers) the node is a
 * plain pass-through.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeRepartition.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "executor/nodeRepartition.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/barrier.h"
#include "storage/sharedfileset.h"
#include "storage/shm_toc.h"
#include "utils/sharedtuplestore.h"
#include "utils/wait_event.h"

/* Barrier phases */
#define REPARTITION_PHASE_WRITE		0
#define REPARTITION_PHASE_READ		1

/* RepartitionState->state */
#define RS_START		0		/* have not attached to the barrier yet */
#define RS_READING		1		/* attached, claiming and reading partitions */
#define RS_DONE			2		/* all done, detached */

/*
 * Initial value for the partitioning hash.  Aggregate hash tables seed their
 * hash with 0 or with a function of the worker number, so use something else
 * here: otherwise the rows of one partition would all share the low bits of
 * their hash, and then pile up in a few buckets of the aggregate's own hash
 * table.
 */
#define REPARTITION_HASH_IV		0x7F4A7C15

/* Shared state, lives in the parallel query's DSM segment. */
typedef struct SharedRepartitionState
{
	Barrier		barrier;		/* WRITE -> READ */
	pg_atomic_uint32 next_partition;	/* next partition to hand out */
	int			num_partitions;
	int			num_participants;
	SharedFileSet fileset;		/* where the partition files live */

	/* followed by num_partitions SharedTuplestores */
} SharedRepartitionState;

static TupleTableSlot *ExecRepartition(PlanState *pstate);
static void RepartitionWrite(RepartitionState *node);
static void repartition_setup_accessors(RepartitionState *node,
										SharedRepartitionState *shared,
										bool initialize);

static Size
repartition_shared_size(int num_partitions, int num_participants)
{
	return add_size(MAXALIGN(sizeof(SharedRepartitionState)),
					mul_size(num_partitions,
							 MAXALIGN(sts_estimate(num_participants))));
}

static SharedTuplestore *
repartition_store(SharedRepartitionState *shared, int partition)
{
	Size		one = MAXALIGN(sts_estimate(shared->num_participants));

	return (SharedTuplestore *)
		((char *) shared + MAXALIGN(sizeof(SharedRepartitionState)) +
		 (Size) partition * one);
}

/* ----------------------------------------------------------------
 *		ExecInitRepartition
 * ----------------------------------------------------------------
 */
RepartitionState *
ExecInitRepartition(Repartition *node, EState *estate, int eflags)
{
	RepartitionState *rs;
	PlanState  *outerps;
	Oid		   *eqfuncoids;
	FmgrInfo   *hashfuncs;
	const TupleTableSlotOps *ops;
	bool		isfixed;

	rs = makeNode(RepartitionState);
	rs->ps.plan = (Plan *) node;
	rs->ps.state = estate;
	rs->ps.ExecProcNode = ExecRepartition;

	rs->shared = NULL;
	rs->accessors = NULL;
	rs->participant = -1;
	rs->state = RS_START;
	rs->cur_partition = -1;

	/* needed to evaluate the partitioning hash */
	ExecAssignExprContext(estate, &rs->ps);

	/* the child need not support rewind, backward scan or mark/restore */
	eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);
	outerps = ExecInitNode(outerPlan(node), estate, eflags);
	outerPlanState(rs) = outerps;

	/*
	 * We return tuples read back from the tuplestores, which are minimal
	 * tuples; the result type is just the child's.  No projection.
	 */
	ExecInitResultTupleSlotTL(&rs->ps, &TTSOpsMinimalTuple);
	rs->ps.ps_ProjInfo = NULL;

	/* build the expression that hashes the key columns of the child's rows */
	execTuplesHashPrepare(node->numCols, node->keyOperators,
						  &eqfuncoids, &hashfuncs);
	ops = ExecGetResultSlotOps(outerps, &isfixed);
	rs->hashexpr = ExecBuildHash32FromAttrs(ExecGetResultType(outerps),
											isfixed ? ops : NULL,
											hashfuncs,
											node->keyCollations,
											node->numCols,
											node->keyColIdx,
											&rs->ps,
											REPARTITION_HASH_IV);

	return rs;
}

/* ----------------------------------------------------------------
 *		ExecEndRepartition
 * ----------------------------------------------------------------
 */
void
ExecEndRepartition(RepartitionState *node)
{
	/* leave the barrier if we are still attached, e.g. after a LIMIT */
	if (node->shared != NULL && node->state == RS_READING)
	{
		if (node->cur_partition >= 0)
			sts_end_parallel_scan(node->accessors[node->cur_partition]);
		BarrierDetach(&node->shared->barrier);
		node->state = RS_DONE;
	}

	ExecEndNode(outerPlanState(node));
}

/* ----------------------------------------------------------------
 *		ExecReScanRepartition
 *
 *		For a parallel plan the shared state has already been reset by
 *		ExecRepartitionReInitializeDSM() by the time we get here.
 * ----------------------------------------------------------------
 */
void
ExecReScanRepartition(RepartitionState *node)
{
	if (node->shared != NULL && node->cur_partition >= 0)
		sts_end_parallel_scan(node->accessors[node->cur_partition]);

	node->state = RS_START;
	node->cur_partition = -1;
	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}

/* ----------------------------------------------------------------
 *		RepartitionWrite
 *
 *		Drain the child and distribute its rows over the partitions.
 * ----------------------------------------------------------------
 */
static void
RepartitionWrite(RepartitionState *node)
{
	PlanState  *outerps = outerPlanState(node);
	ExprContext *econtext = node->ps.ps_ExprContext;
	SharedRepartitionState *shared = node->shared;
	uint32		nparts = (uint32) shared->num_partitions;

	for (;;)
	{
		TupleTableSlot *slot;
		MinimalTuple tuple;
		bool		shouldFree;
		bool		isnull;
		uint32		hashvalue;
		uint32		partition;

		slot = ExecProcNode(outerps);
		if (TupIsNull(slot))
			break;

		ResetExprContext(econtext);
		econtext->ecxt_innertuple = slot;
		hashvalue = DatumGetUInt32(ExecEvalExprSwitchContext(node->hashexpr,
															 econtext,
															 &isnull));

		/* spread the (possibly weak) hash, then map it onto [0, nparts) */
		partition = (uint32) (((uint64) murmurhash32(hashvalue) * nparts) >> 32);

		tuple = ExecFetchSlotMinimalTuple(slot, &shouldFree);
		sts_puttuple(node->accessors[partition], NULL, tuple);
		if (shouldFree)
			heap_free_minimal_tuple(tuple);

		CHECK_FOR_INTERRUPTS();
	}

	/* everything must be flushed before anybody can read it */
	for (int i = 0; i < shared->num_partitions; i++)
		sts_end_write(node->accessors[i]);
}

/* ----------------------------------------------------------------
 *		ExecRepartition
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecRepartition(PlanState *pstate)
{
	RepartitionState *node = castNode(RepartitionState, pstate);
	SharedRepartitionState *shared = node->shared;
	TupleTableSlot *slot = node->ps.ps_ResultTupleSlot;

	CHECK_FOR_INTERRUPTS();

	/* Not running in parallel: there is nobody to exchange rows with. */
	if (shared == NULL)
	{
		TupleTableSlot *outerslot = ExecProcNode(outerPlanState(node));

		if (TupIsNull(outerslot))
			return ExecClearTuple(slot);
		return ExecCopySlot(slot, outerslot);
	}

	if (node->state == RS_START)
	{
		int			phase = BarrierAttach(&shared->barrier);

		if (phase == REPARTITION_PHASE_WRITE)
		{
			RepartitionWrite(node);

			/* wait until every attached participant has written its rows */
			BarrierArriveAndWait(&shared->barrier,
								 WAIT_EVENT_REPARTITION_WRITE);
		}

		/*
		 * Otherwise the writers have all finished already and we arrived too
		 * late to have anything to contribute; our child was never run, which
		 * is fine as parallel scans below hand out work dynamically.
		 */
		node->state = RS_READING;
	}

	if (node->state == RS_DONE)
		return ExecClearTuple(slot);

	for (;;)
	{
		MinimalTuple tuple;

		if (node->cur_partition < 0)
		{
			uint32		next;

			next = pg_atomic_fetch_add_u32(&shared->next_partition, 1);
			if (next >= (uint32) shared->num_partitions)
			{
				/* nothing left to claim */
				node->state = RS_DONE;
				BarrierDetach(&shared->barrier);
				return ExecClearTuple(slot);
			}
			node->cur_partition = (int) next;
			sts_begin_parallel_scan(node->accessors[next]);
		}

		tuple = sts_parallel_scan_next(node->accessors[node->cur_partition],
									   NULL);
		if (tuple == NULL)
		{
			sts_end_parallel_scan(node->accessors[node->cur_partition]);
			node->cur_partition = -1;
			continue;
		}

		/* the tuple is only valid until the next call, hence shouldFree false */
		ExecForceStoreMinimalTuple(tuple, slot, false);
		return slot;
	}
}

/* ----------------------------------------------------------------
 *						Parallel Query Support
 * ----------------------------------------------------------------
 */

/*
 * Create or attach to the SharedTuplestore of every partition.  The leader
 * initializes them, workers attach to what the leader initialized.
 */
static void
repartition_setup_accessors(RepartitionState *node,
							SharedRepartitionState *shared, bool initialize)
{
	MemoryContext oldcxt;

	oldcxt = MemoryContextSwitchTo(node->ps.state->es_query_cxt);
	node->accessors = palloc_array(SharedTuplestoreAccessor *,
								   shared->num_partitions);
	for (int i = 0; i < shared->num_partitions; i++)
	{
		SharedTuplestore *sts = repartition_store(shared, i);

		if (initialize)
		{
			char		name[32];

			snprintf(name, sizeof(name), "repartition%d", i);
			node->accessors[i] = sts_initialize(sts,
												shared->num_participants,
												node->participant,
												0,	/* no per-tuple meta data */
												0,
												&shared->fileset,
												name);
		}
		else
			node->accessors[i] = sts_attach(sts, node->participant,
											&shared->fileset);
	}
	MemoryContextSwitchTo(oldcxt);
}

/*
 * ExecRepartitionEstimate
 *		Estimate space required to propagate the shared state to workers.
 */
void
ExecRepartitionEstimate(RepartitionState *node, ParallelContext *pcxt)
{
	Repartition *plan = (Repartition *) node->ps.plan;

	shm_toc_estimate_chunk(&pcxt->estimator,
						   repartition_shared_size(plan->num_partitions,
												   pcxt->nworkers + 1));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/*
 * ExecRepartitionInitializeDSM
 *		Set up the shared state in the leader.
 */
void
ExecRepartitionInitializeDSM(RepartitionState *node, ParallelContext *pcxt)
{
	Repartition *plan = (Repartition *) node->ps.plan;
	SharedRepartitionState *shared;

	/* Without a real DSM segment we cannot share files: pass rows through. */
	if (pcxt->seg == NULL)
		return;

	shared = shm_toc_allocate(pcxt->toc,
							  repartition_shared_size(plan->num_partitions,
													  pcxt->nworkers + 1));
	shm_toc_insert(pcxt->toc, plan->plan.plan_node_id, shared);

	shared->num_partitions = plan->num_partitions;
	shared->num_participants = pcxt->nworkers + 1;
	pg_atomic_init_u32(&shared->next_partition, 0);
	BarrierInit(&shared->barrier, 0);
	SharedFileSetInit(&shared->fileset, pcxt->seg);

	/* the leader is participant 0, worker N is participant N + 1 */
	node->participant = 0;
	node->shared = shared;
	repartition_setup_accessors(node, shared, true);
}

/*
 * ExecRepartitionReInitializeDSM
 *		Reset shared state before a rescan.
 */
void
ExecRepartitionReInitializeDSM(RepartitionState *node, ParallelContext *pcxt)
{
	SharedRepartitionState *shared = node->shared;

	if (shared == NULL)
		return;

	/* Throw away the previous round's partition files and start over. */
	SharedFileSetDeleteAll(&shared->fileset);
	pg_atomic_write_u32(&shared->next_partition, 0);
	BarrierInit(&shared->barrier, 0);
	repartition_setup_accessors(node, shared, true);

	node->state = RS_START;
	node->cur_partition = -1;
}

/*
 * ExecRepartitionInitializeWorker
 *		Attach to the shared state in a worker.
 */
void
ExecRepartitionInitializeWorker(RepartitionState *node,
								ParallelWorkerContext *pwcxt)
{
	Repartition *plan = (Repartition *) node->ps.plan;
	SharedRepartitionState *shared;

	shared = shm_toc_lookup(pwcxt->toc, plan->plan.plan_node_id, true);
	if (shared == NULL)
		return;

	SharedFileSetAttach(&shared->fileset, pwcxt->seg);

	node->participant = ParallelWorkerNumber + 1;
	node->shared = shared;
	repartition_setup_accessors(node, shared, false);
}
