/*-------------------------------------------------------------------------
 *
 * nodeLimit.c
 *	  Routines to handle limiting of query results where appropriate
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeLimit.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecLimit		- extract a limited range of tuples
 *		ExecInitLimit	- initialize node and subnodes..
 *		ExecEndLimit	- shutdown node and subnodes
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeLimit.h"
#include "miscadmin.h"

#include "access/genam.h"        /* index_beginscan, index_endscan */
#include "access/relscan.h"      /* IndexScanDescData full definition */
#include "access/tableam.h"      /* index_fetch_heap */
#include "access/visibilitymap.h" /* VM_ALL_VISIBLE */
#include "storage/bufmgr.h"      /* Buffer, ReleaseBuffer */
#include "executor/nodeIndexscan.h"      /* IndexScanState */
#include "executor/nodeIndexonlyscan.h"  /* IndexOnlyScanState */

static void recompute_limits(LimitState *node);
static int64 compute_tuples_needed(LimitState *node);
static int64 index_count_live_tuples(LimitState *node, Snapshot snapshot);
static TupleTableSlot *ExecBackwardScan(LimitState *node);
static TupleTableSlot *ExecBackwardScanNext(LimitState *node);


/* ----------------------------------------------------------------
 *		ExecLimit
 *
 *		This is a very simple node which just performs LIMIT/OFFSET
 *		filtering on the stream of tuples returned by a subplan.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecLimit(PlanState *pstate)
{
	LimitState *node = castNode(LimitState, pstate);
	ExprContext *econtext = node->ps.ps_ExprContext;
	ScanDirection direction;
	TupleTableSlot *slot;
	PlanState  *outerPlan;

	CHECK_FOR_INTERRUPTS();




   /*
    * BACKWARD_SCAN fast path.
    *
    * If scanBackward is set, we take a completely separate
    * execution path:
    *
    *   1. COUNT live rows (index-only, same snapshot)
    *   2. compute flipOffset
    *   3. collect rows into flipBuffer
    *   4. reverse flipBuffer
    *   5. return rows from flipBuffer one by one
    *
    * The existing forward logic below is completely untouched.
    * We never mix the two paths.
    */
	if (node->scanBackward)
		return ExecBackwardScan(node);    /* ← separate function */




	/*
	 * get information from the node
	 */
	direction = node->ps.state->es_direction;
	outerPlan = outerPlanState(node);

	/*
	 * The main logic is a simple state machine.
	 */
	switch (node->lstate)
	{
		case LIMIT_INITIAL:

			/*
			 * First call for this node, so compute limit/offset. (We can't do
			 * this any earlier, because parameters from upper nodes will not
			 * be set during ExecInitLimit.)  This also sets position = 0 and
			 * changes the state to LIMIT_RESCAN.
			 */
			recompute_limits(node);

			pg_fallthrough;

		case LIMIT_RESCAN:

			/*
			 * If backwards scan, just return NULL without changing state.
			 */
			if (!ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * Check for empty window; if so, treat like empty subplan.
			 */
			if (node->count <= 0 && !node->noCount)
			{
				node->lstate = LIMIT_EMPTY;
				return NULL;
			}

			/*
			 * Fetch rows from subplan until we reach position > offset.
			 */
			for (;;)
			{
				slot = ExecProcNode(outerPlan);
				if (TupIsNull(slot))
				{
					/*
					 * The subplan returns too few tuples for us to produce
					 * any output at all.
					 */
					node->lstate = LIMIT_EMPTY;
					return NULL;
				}

				/*
				 * Tuple at limit is needed for comparison in subsequent
				 * execution to detect ties.
				 */
				if (node->limitOption == LIMIT_OPTION_WITH_TIES &&
					node->position - node->offset == node->count - 1)
				{
					ExecCopySlot(node->last_slot, slot);
				}
				node->subSlot = slot;
				if (++node->position > node->offset)
					break;
			}

			/*
			 * Okay, we have the first tuple of the window.
			 */
			node->lstate = LIMIT_INWINDOW;
			break;

		case LIMIT_EMPTY:

			/*
			 * The subplan is known to return no tuples (or not more than
			 * OFFSET tuples, in general).  So we return no tuples.
			 */
			return NULL;

		case LIMIT_INWINDOW:
			if (ScanDirectionIsForward(direction))
			{
				/*
				 * Forwards scan, so check for stepping off end of window.  At
				 * the end of the window, the behavior depends on whether WITH
				 * TIES was specified: if so, we need to change the state
				 * machine to WINDOWEND_TIES, and fall through to the code for
				 * that case.  If not (nothing was specified, or ONLY was)
				 * return NULL without advancing the subplan or the position
				 * variable, but change the state machine to record having
				 * done so.
				 *
				 * Once at the end, ideally, we would shut down parallel
				 * resources; but that would destroy the parallel context
				 * which might be required for rescans.  To do that, we'll
				 * need to find a way to pass down more information about
				 * whether rescans are possible.
				 */
				if (!node->noCount &&
					node->position - node->offset >= node->count)
				{
					if (node->limitOption == LIMIT_OPTION_COUNT)
					{
						node->lstate = LIMIT_WINDOWEND;
						return NULL;
					}
					else
					{
						node->lstate = LIMIT_WINDOWEND_TIES;
						/* we'll fall through to the next case */
					}
				}
				else
				{
					/*
					 * Get next tuple from subplan, if any.
					 */
					slot = ExecProcNode(outerPlan);
					if (TupIsNull(slot))
					{
						node->lstate = LIMIT_SUBPLANEOF;
						return NULL;
					}

					/*
					 * If WITH TIES is active, and this is the last in-window
					 * tuple, save it to be used in subsequent WINDOWEND_TIES
					 * processing.
					 */
					if (node->limitOption == LIMIT_OPTION_WITH_TIES &&
						node->position - node->offset == node->count - 1)
					{
						ExecCopySlot(node->last_slot, slot);
					}
					node->subSlot = slot;
					node->position++;
					break;
				}
			}
			else
			{
				/*
				 * Backwards scan, so check for stepping off start of window.
				 * As above, only change state-machine status if so.
				 */
				if (node->position <= node->offset + 1)
				{
					node->lstate = LIMIT_WINDOWSTART;
					return NULL;
				}

				/*
				 * Get previous tuple from subplan; there should be one!
				 */
				slot = ExecProcNode(outerPlan);
				if (TupIsNull(slot))
					elog(ERROR, "LIMIT subplan failed to run backwards");
				node->subSlot = slot;
				node->position--;
				break;
			}

			Assert(node->lstate == LIMIT_WINDOWEND_TIES);
			pg_fallthrough;

		case LIMIT_WINDOWEND_TIES:
			if (ScanDirectionIsForward(direction))
			{
				/*
				 * Advance the subplan until we find the first row with
				 * different ORDER BY pathkeys.
				 */
				slot = ExecProcNode(outerPlan);
				if (TupIsNull(slot))
				{
					node->lstate = LIMIT_SUBPLANEOF;
					return NULL;
				}

				/*
				 * Test if the new tuple and the last tuple match. If so we
				 * return the tuple.
				 */
				econtext->ecxt_innertuple = slot;
				econtext->ecxt_outertuple = node->last_slot;
				if (ExecQualAndReset(node->eqfunction, econtext))
				{
					node->subSlot = slot;
					node->position++;
				}
				else
				{
					node->lstate = LIMIT_WINDOWEND;
					return NULL;
				}
			}
			else
			{
				/*
				 * Backwards scan, so check for stepping off start of window.
				 * Change only state-machine status if so.
				 */
				if (node->position <= node->offset + 1)
				{
					node->lstate = LIMIT_WINDOWSTART;
					return NULL;
				}

				/*
				 * Get previous tuple from subplan; there should be one! And
				 * change state-machine status.
				 */
				slot = ExecProcNode(outerPlan);
				if (TupIsNull(slot))
					elog(ERROR, "LIMIT subplan failed to run backwards");
				node->subSlot = slot;
				node->position--;
				node->lstate = LIMIT_INWINDOW;
			}
			break;

		case LIMIT_SUBPLANEOF:
			if (ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * Backing up from subplan EOF, so re-fetch previous tuple; there
			 * should be one!  Note previous tuple must be in window.
			 */
			slot = ExecProcNode(outerPlan);
			if (TupIsNull(slot))
				elog(ERROR, "LIMIT subplan failed to run backwards");
			node->subSlot = slot;
			node->lstate = LIMIT_INWINDOW;
			/* position does not change 'cause we didn't advance it before */
			break;

		case LIMIT_WINDOWEND:
			if (ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * We already past one position to detect ties so re-fetch
			 * previous tuple; there should be one!  Note previous tuple must
			 * be in window.
			 */
			if (node->limitOption == LIMIT_OPTION_WITH_TIES)
			{
				slot = ExecProcNode(outerPlan);
				if (TupIsNull(slot))
					elog(ERROR, "LIMIT subplan failed to run backwards");
				node->subSlot = slot;
				node->lstate = LIMIT_INWINDOW;
			}
			else
			{
				/*
				 * Backing up from window end: simply re-return the last tuple
				 * fetched from the subplan.
				 */
				slot = node->subSlot;
				node->lstate = LIMIT_INWINDOW;
				/* position does not change 'cause we didn't advance it before */
			}
			break;

		case LIMIT_WINDOWSTART:
			if (!ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * Advancing after having backed off window start: simply
			 * re-return the last tuple fetched from the subplan.
			 */
			slot = node->subSlot;
			node->lstate = LIMIT_INWINDOW;
			/* position does not change 'cause we didn't change it before */
			break;

		default:
			elog(ERROR, "impossible LIMIT state: %d",
				 (int) node->lstate);
			slot = NULL;		/* keep compiler quiet */
			break;
	}

	/* Return the current tuple */
	Assert(!TupIsNull(slot));

	return slot;
}





/*
 * ExecBackwardScanNext
 *
 * Returns the next row from flipBuffer.
 * flipBufCount tracks how many we have returned so far.
 * When exhausted, returns an empty slot.
 */
static TupleTableSlot *
ExecBackwardScanNext(LimitState *node)
{
    if (node->flipBufCount >= node->flipBufSize)
    {
        /*
         * Buffer exhausted.
         * Return NULL exactly like normal ExecLimit does
         * at end of scan — that is what the parent expects.
         */
        return NULL;
    }

    return node->flipBuffer[node->flipBufCount++];
}


/*
 * index_count_live_tuples
 *
 * Count live tuples using an index scan with the pinned snapshot.
 * Handles both IndexScanState and IndexOnlyScanState as the child plan.
 *
 * For pages that are all-visible in the visibility map, we count
 * without heap access. Otherwise we do a heap fetch to check MVCC.
 */
static int64
index_count_live_tuples(LimitState *node, Snapshot snapshot)
{
    PlanState      *child = outerPlanState(node);
    Relation        heapRel;
    Relation        indexRel;
    TupleTableSlot *heapSlot;
    IndexScanDesc   scandesc;
    int64           count = 0;
    bool            gotTuple;
    Buffer          vmbuffer = InvalidBuffer;
    bool            need_free_slot = false;

    /*
     * The child can be either IndexScanState or IndexOnlyScanState.
     * Extract the heap relation, index relation, and a tuple slot
     * from whichever type we actually have.
     */
    if (IsA(child, IndexScanState))
    {
        IndexScanState *iss = (IndexScanState *) child;
        heapRel  = iss->ss.ss_currentRelation;
        indexRel = iss->iss_RelationDesc;
        heapSlot = iss->ss.ss_ScanTupleSlot;
    }
    else if (IsA(child, IndexOnlyScanState))
    {
        IndexOnlyScanState *ioss = (IndexOnlyScanState *) child;
        heapRel  = ioss->ss.ss_currentRelation;
        indexRel = ioss->ioss_RelationDesc;

        /*
         * CRITICAL: IndexOnlyScan's ss_ScanTupleSlot has an index-shaped
         * descriptor (e.g. just "id").  index_fetch_heap stores a full
         * heap tuple, so we need a heap-shaped slot to avoid a crash.
         */
        heapSlot = MakeSingleTupleTableSlot(
            RelationGetDescr(heapRel),
            &TTSOpsBufferHeapTuple
        );
        need_free_slot = true;
    }
    else
    {
        /* Should not happen — planner checks ensured index scan */
        elog(ERROR, "backward scan child is neither IndexScan nor IndexOnlyScan");
        return 0;  /* keep compiler quiet */
    }

	scandesc = index_beginscan(
		heapRel,        /* heap relation */
		indexRel,       /* index relation */
		snapshot,       /* pinned snapshot */
		NULL,           /* IndexScanInstrumentation - NULL if not instrumenting */
		0,              /* nkeys */
		0,              /* norderbys */
		0               /* flags */
	);

    scandesc->xs_want_itup = true;
    index_rescan(scandesc, NULL, 0, NULL, 0);

    for (;;)
    {
        ItemPointer tid;

        CHECK_FOR_INTERRUPTS();

        gotTuple = index_getnext_tid(scandesc, ForwardScanDirection);
        if (!gotTuple)
            break;

        tid = &scandesc->xs_heaptid;

        /*
         * Check the visibility map: if the heap page is all-visible,
         * every tuple on it is live — count without heap access.
         * Otherwise, do a heap fetch to verify MVCC visibility.
         * This is the same pattern used by IndexOnlyScan.
         */
        if (VM_ALL_VISIBLE(scandesc->heapRelation,
                           ItemPointerGetBlockNumber(tid),
                           &vmbuffer))
        {
            count++;
        }
        else
        {
            if (index_fetch_heap(scandesc, heapSlot))
                count++;
        }
    }

    /* Release VM buffer pin if we acquired one */
    if (vmbuffer != InvalidBuffer)
        ReleaseBuffer(vmbuffer);

    index_endscan(scandesc);

    /* Free the temporary slot if we created one for IndexOnlyScan */
    if (need_free_slot)
        ExecDropSingleTupleTableSlot(heapSlot);

    return count;
}



static TupleTableSlot *
ExecBackwardScan(LimitState *node)
{
    TupleTableSlot *slot;
    TupleTableSlot *copy;
    TupleTableSlot *tmp;
    PlanState      *outerPlan;
    int64           exact_live_count;
    int64           flipOffset;
    int64           rows_available;
    int64           actual_count;
    int64           skipped;
    int64           collected;
    int             left;
    int             right;

    /*
     * If buffer already filled — just return next row.
     */
    if (node->flipBuffer != NULL)
        return ExecBackwardScanNext(node);

    /* Must compute offset/count before anything else */
    if (node->lstate == LIMIT_INITIAL)
        recompute_limits(node);

    outerPlan = outerPlanState(node);

    /* STEP A — Pin snapshot for consistent counting + fetching */
    node->backwardScanSnapshot = RegisterSnapshot(GetTransactionSnapshot());
    Assert(node->backwardScanSnapshot != InvalidSnapshot);

    /*
     * STEP B — Count live rows using pinned snapshot.
     * index_count_live_tuples performs an index-only scan
     * using the same snapshot as the fetch pass.
     */
    exact_live_count = index_count_live_tuples(
        node,
        node->backwardScanSnapshot
    );

    /* STEP C — Compute flip with actual available rows */
    rows_available = exact_live_count - node->offset;

    if (rows_available <= 0)
    {
        UnregisterSnapshot(node->backwardScanSnapshot);
        node->backwardScanSnapshot = InvalidSnapshot;
        node->flipBuffer   = (TupleTableSlot **) palloc(0);
        node->flipBufSize  = 0;
        node->flipBufCount = 0;
        return NULL;
    }

    actual_count = (node->count < rows_available) ? node->count : rows_available;
    flipOffset = exact_live_count - node->offset - actual_count;
    if (flipOffset < 0)
        flipOffset = 0;

    node->flipOffset = flipOffset;
    node->count = actual_count;

    /* STEP D — Allocate buffer and collect rows */
    node->flipBuffer = (TupleTableSlot **)
        palloc(node->count * sizeof(TupleTableSlot *));
    node->flipBufSize  = 0;
    node->flipBufCount = 0;

    /* Skip phase: advance past flipOffset rows */
    skipped = 0;
    while (skipped < node->flipOffset)
    {
        CHECK_FOR_INTERRUPTS();
        slot = ExecProcNode(outerPlan);

        if (slot == NULL || TupIsNull(slot))
        {
            UnregisterSnapshot(node->backwardScanSnapshot);
            node->backwardScanSnapshot = InvalidSnapshot;
            node->flipBuffer   = (TupleTableSlot **) palloc(0);
            node->flipBufSize  = 0;
            node->flipBufCount = 0;
            return NULL;
        }
        skipped++;
    }

    /* Collect phase: gather up to actual_count rows */
    collected = 0;
    while (collected < node->count)
    {
        CHECK_FOR_INTERRUPTS();
        slot = ExecProcNode(outerPlan);

        if (TupIsNull(slot))
            break;

        copy = MakeSingleTupleTableSlot(
            slot->tts_tupleDescriptor,
            slot->tts_ops
        );
        ExecCopySlot(copy, slot);
        node->flipBuffer[collected] = copy;
        collected++;
    }
    node->flipBufSize = collected;

    /* STEP E — Reverse buffer so rows come out in DESC order */
    left  = 0;
    right = (int) node->flipBufSize - 1;
    while (left < right)
    {
        tmp = node->flipBuffer[left];
        node->flipBuffer[left]  = node->flipBuffer[right];
        node->flipBuffer[right] = tmp;
        left++;
        right--;
    }

    return ExecBackwardScanNext(node);
}


/*
 * Evaluate the limit/offset expressions --- done at startup or rescan.
 *
 * This is also a handy place to reset the current-position state info.
 */
static void
recompute_limits(LimitState *node)
{
	ExprContext *econtext = node->ps.ps_ExprContext;
	Datum		val;
	bool		isNull;

	if (node->limitOffset)
	{
		val = ExecEvalExprSwitchContext(node->limitOffset,
										econtext,
										&isNull);
		/* Interpret NULL offset as no offset */
		if (isNull)
			node->offset = 0;
		else
		{
			node->offset = DatumGetInt64(val);
			if (node->offset < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE),
						 errmsg("OFFSET must not be negative")));
		}
	}
	else
	{
		/* No OFFSET supplied */
		node->offset = 0;
	}

	if (node->limitCount)
	{
		val = ExecEvalExprSwitchContext(node->limitCount,
										econtext,
										&isNull);
		/* Interpret NULL count as no count (LIMIT ALL) */
		if (isNull)
		{
			node->count = 0;
			node->noCount = true;
		}
		else
		{
			node->count = DatumGetInt64(val);
			if (node->count < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
						 errmsg("LIMIT must not be negative")));
			node->noCount = false;
		}
	}
	else
	{
		/* No COUNT supplied */
		node->count = 0;
		node->noCount = true;
	}

	/* Reset position to start-of-scan */
	node->position = 0;
	node->subSlot = NULL;

	/* Set state-machine state */
	node->lstate = LIMIT_RESCAN;

	/*
	 * Notify child node about limit.  Note: think not to "optimize" by
	 * skipping ExecSetTupleBound if compute_tuples_needed returns < 0.  We
	 * must update the child node anyway, in case this is a rescan and the
	 * previous time we got a different result.
	 */
	ExecSetTupleBound(compute_tuples_needed(node), outerPlanState(node));
}

/*
 * Compute the maximum number of tuples needed to satisfy this Limit node.
 * Return a negative value if there is not a determinable limit.
 */
static int64
compute_tuples_needed(LimitState *node)
{
	if ((node->noCount) || (node->limitOption == LIMIT_OPTION_WITH_TIES))
		return -1;
	/* Note: if this overflows, we'll return a negative value, which is OK */
	return node->count + node->offset;
}

/* ----------------------------------------------------------------
 *		ExecInitLimit
 *
 *		This initializes the limit node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
LimitState *
ExecInitLimit(Limit *node, EState *estate, int eflags)
{
	LimitState *limitstate;
	Plan	   *outerPlan;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/*
	 * create state structure
	 */
	limitstate = makeNode(LimitState);
	limitstate->ps.plan = (Plan *) node;
	limitstate->ps.state = estate;
	limitstate->ps.ExecProcNode = ExecLimit;

	limitstate->lstate = LIMIT_INITIAL;

	/*
	 * Miscellaneous initialization
	 *
	 * Limit nodes never call ExecQual or ExecProject, but they need an
	 * exprcontext anyway to evaluate the limit/offset parameters in.
	 */
	ExecAssignExprContext(estate, &limitstate->ps);

	/*
	 * initialize outer plan
	 */
	outerPlan = outerPlan(node);
	outerPlanState(limitstate) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * initialize child expressions
	 */
	limitstate->limitOffset = ExecInitExpr((Expr *) node->limitOffset,
										   (PlanState *) limitstate);
	limitstate->limitCount = ExecInitExpr((Expr *) node->limitCount,
										  (PlanState *) limitstate);
	limitstate->limitOption = node->limitOption;

	/*
	 * Initialize result type.
	 */
	ExecInitResultTypeTL(&limitstate->ps);

	limitstate->ps.resultopsset = true;
	limitstate->ps.resultops = ExecGetResultSlotOps(outerPlanState(limitstate),
													&limitstate->ps.resultopsfixed);

	/*
	 * limit nodes do no projections, so initialize projection info for this
	 * node appropriately
	 */
	limitstate->ps.ps_ProjInfo = NULL;

	/*
	 * Initialize the equality evaluation, to detect ties.
	 */
	if (node->limitOption == LIMIT_OPTION_WITH_TIES)
	{
		TupleDesc	desc;
		const TupleTableSlotOps *ops;

		desc = ExecGetResultType(outerPlanState(limitstate));
		ops = ExecGetResultSlotOps(outerPlanState(limitstate), NULL);

		limitstate->last_slot = ExecInitExtraTupleSlot(estate, desc, ops);
		limitstate->eqfunction = execTuplesMatchPrepare(desc,
														node->uniqNumCols,
														node->uniqColIdx,
														node->uniqOperators,
														node->uniqCollations,
														&limitstate->ps);
	}
	   /*
    * Copy backward scan flags from plan node into runtime state.
    * Actual work happens in ExecLimit() at execution time.
    */
	limitstate->scanBackward  = node->scanBackward;
	limitstate->needFlip      = node->needFlip;
	limitstate->flipOffset    = 0;
	limitstate->flipBuffer    = NULL;
	limitstate->flipBufSize   = 0;
	limitstate->flipBufCount  = 0;

	return limitstate;
}

/* ----------------------------------------------------------------
 *		ExecEndLimit
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void
ExecEndLimit(LimitState *node)
{
    /* ============================================================
     * Backward scan cleanup
     *
     * Free each copied slot individually first —
     * they were allocated with MakeSingleTupleTableSlot
     * so each one must be dropped with ExecDropSingleTupleTableSlot.
     *
     * Then free the buffer array itself.
     * Then unpin the snapshot if still pinned.
     * ============================================================
     */
    if (node->flipBuffer != NULL)
    {
        int i;

        for (i = 0; i < node->flipBufSize; i++)
        {
            if (node->flipBuffer[i] != NULL)
            {
                ExecDropSingleTupleTableSlot(node->flipBuffer[i]);
                node->flipBuffer[i] = NULL;
            }
        }

        pfree(node->flipBuffer);
        node->flipBuffer   = NULL;
        node->flipBufSize  = 0;
        node->flipBufCount = 0;
    }

    /*
     * Unpin snapshot if still active.
     * This can happen if query was cancelled mid-execution
     * before ExecBackwardScan completed — we still must
     * release the snapshot or VACUUM gets blocked forever.
     */
    if (node->scanBackward &&
        node->backwardScanSnapshot != InvalidSnapshot)
    {
        UnregisterSnapshot(node->backwardScanSnapshot);
        node->backwardScanSnapshot = InvalidSnapshot;
    }

    /*
     * existing cleanup — untouched
     */
    ExecEndNode(outerPlanState(node));
}



void
ExecReScanLimit(LimitState *node)
{
	PlanState  *outerPlan = outerPlanState(node);



	/* ============================================================
     * Backward scan rescan
     *
     * We must throw away the old buffer completely.
     * Next call to ExecBackwardScan will rebuild it fresh
     * with a new snapshot and new COUNT.
     *
     * This is correct because:
     *   - parameters may have changed (different offset/count)
     *   - underlying data may have changed
     *   - snapshot must be fresh for the new execution
     * ============================================================
     */
    if (node->flipBuffer != NULL)
    {
        int i;

        /* free each copied slot */
        for (i = 0; i < node->flipBufSize; i++)
        {
            if (node->flipBuffer[i] != NULL)
            {
                ExecDropSingleTupleTableSlot(node->flipBuffer[i]);
                node->flipBuffer[i] = NULL;
            }
        }

        pfree(node->flipBuffer);
        node->flipBuffer   = NULL;
        node->flipBufSize  = 0;
        node->flipBufCount = 0;
    }

    /*
     * Unpin old snapshot — new execution needs fresh snapshot.
     * If we kept the old snapshot, COUNT and FETCH would see
     * stale data from the previous execution.
     */
    if (node->scanBackward &&
        node->backwardScanSnapshot != InvalidSnapshot)
    {
        UnregisterSnapshot(node->backwardScanSnapshot);
        node->backwardScanSnapshot = InvalidSnapshot;
    }

    /*
     * Reset flipOffset — will be recomputed on next execution.
     */
    node->flipOffset = 0;

    /*
     * existing rescan logic — untouched
     */
    if (node->ps.ps_ExprContext != NULL)
        ReScanExprContext(node->ps.ps_ExprContext);

    node->lstate = LIMIT_INITIAL;


	/*
	 * Recompute limit/offset in case parameters changed, and reset the state
	 * machine.  We must do this before rescanning our child node, in case
	 * it's a Sort that we are passing the parameters down to.
	 */
	recompute_limits(node);

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);
}
