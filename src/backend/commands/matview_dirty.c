/*-------------------------------------------------------------------------
 *
 * matview_dirty.c
 *	  DBblue: change tracking for REFRESH MATERIALIZED VIEW skip-if-unchanged
 *
 * Goal
 * ----
 * When a matview carries auto_skip_unchanged=true, a REFRESH that can prove
 * nothing it depends on has changed returns without rebuilding.  A wrong
 * "nothing changed" answer leaves the matview silently stale, so every rule
 * below is written to fail in the conservative direction: anything we cannot
 * prove means "refresh normally".
 *
 * State
 * -----
 * Two shared-memory tables:
 *
 *	1. A per-source write counter, keyed (dbid, relid) -> gen.  'gen' is
 *	   monotone: it is only ever incremented, never cleared.  It is bumped from
 *	   XACT_EVENT_COMMIT, i.e. strictly after the writing transaction's rows
 *	   become visible (ProcArrayEndTransaction runs earlier in
 *	   CommitTransaction), and from XACT_EVENT_PREPARE so that a prepared
 *	   transaction cannot slip through.
 *
 *	2. A per-matview watermark: the (relid, gen, relfilenumber) vector that
 *	   matview M observed at its own last successful refresh, plus M's
 *	   post-swap relfilenumber and a fingerprint of its rewritten query.
 *
 * Keying the watermark by matview is essential.  A per-source "clean" flag
 * answers "was this table written since *somebody's* refresh", but the skip
 * decision needs "since *this* matview's refresh".  With a shared flag the
 * first matview to refresh consumes it and every other matview over the same
 * source is starved.
 *
 * Why the skip decision is sound
 * ------------------------------
 * Before reading a source's gen we require ConditionalLockRelationOid(relid,
 * ShareLock) to succeed, and release it again immediately.  ShareLock
 * conflicts with RowExclusiveLock, which every writer holds from its first
 * write until after its commit callbacks have run (locks are released later in
 * CommitTransaction than XACT_EVENT_COMMIT).  A prepared transaction's locks
 * are likewise held by its dummy PGPROC until COMMIT PREPARED.  So acquiring
 * ShareLock proves no writer to that source is in flight, prepared, or
 * mid-commit; every write that is visible has therefore already bumped gen.
 * Combined with gen == the watermark's gen, no visible write is unaccounted
 * for.  Taking the lock also forces AcceptInvalidationMessages(), so the
 * relfilenumber we then read is not a stale relcache value.
 *
 * Why recording a watermark is sound
 * ----------------------------------
 * The gens are captured before the data-fill snapshot is taken (the caller
 * pushes a fresh snapshot afterwards).  Any writer whose rows are absent from
 * that snapshot becomes visible after the snapshot, hence after the capture,
 * hence bumps gen after the capture, hence leaves gen != the recorded gen.  It
 * costs an extra refresh when a writer commits between capture and snapshot,
 * which is the safe direction.
 *
 * Abort safety comes for free from the matview's own relfilenumber.  The
 * watermark is written eagerly, inside the refreshing transaction, but it
 * stores the relfilenumber the matview got from the heap swap.  If the
 * transaction rolls back, pg_class reverts and the matview keeps its old
 * relfilenumber, which no longer matches the watermark, so the matview reads
 * as dirty from then on.  No commit-time callback is involved, which also
 * makes subtransaction aborts and crashes safe with no extra reasoning.
 *
 * What is deliberately rejected
 * -----------------------------
 * Enumeration runs over the REWRITTEN query, because the stored rule query is
 * parse-analyzed but not rewritten: a source view appears there as a
 * storage-less RTE_RELATION and its base tables are absent entirely.  On top
 * of that we refuse to skip for: REPEATABLE READ / SERIALIZABLE (the data-fill
 * query is pinned to a snapshot that may predate our capture); RLS on any
 * source, since policy changes alter contents with no write; sequences,
 * foreign tables, system catalogs, propgraphs and any other relkind that is
 * not a plain heap table or matview; TABLESAMPLE; any non-IMMUTABLE function;
 * virtual generated columns; an in-flight DETACH PARTITION CONCURRENTLY; and
 * any bound overflow.
 *
 * src/backend/commands/matview_dirty.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "access/xact.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "commands/matview_dirty.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "rewrite/rewriteHandler.h"
#include "storage/lmgr.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/rel.h"
#include "utils/syscache.h"

bool		dbblue_matview_skip_unchanged = true;

/* ----------
 * Shared memory
 * ----------
 */
typedef struct MatviewSrcEntry
{
	Oid			dbid;
	Oid			relid;
	uint64		gen;
} MatviewSrcEntry;

typedef struct MatviewWatermark
{
	Oid			dbid;
	Oid			mvrelid;
	RelFileNumber mv_rfn;		/* matview storage after its refresh */
	uint32		fingerprint;	/* hash of the rewritten data-fill query */
	int			nsources;
	Oid			relid[MATVIEW_SKIP_MAX_SOURCES];
	uint64		gen[MATVIEW_SKIP_MAX_SOURCES];
	RelFileNumber rfn[MATVIEW_SKIP_MAX_SOURCES];
} MatviewWatermark;

typedef struct MatviewSkipState
{
	slock_t		lock;
	int			nsrc;
	int			nwm;
	MatviewSrcEntry src[MATVIEW_SKIP_MAX_RELS];
	MatviewWatermark wm[MATVIEW_SKIP_MAX_MATVIEWS];
} MatviewSkipState;

static MatviewSkipState *matview_skip_state = NULL;

/* ----------
 * Process-local record of relations this transaction has written.
 *
 * Deliberately a static array, not a palloc'd List: TopTransactionContext is
 * reset at every commit and a subtransaction's context is destroyed on
 * rollback to savepoint, either of which would pull the list out from under
 * the commit-time flush.
 * ----------
 */
static Oid	xact_written[MATVIEW_SKIP_MAX_XACT_RELS];
static int	xact_nwritten = 0;
static bool xact_write_overflow = false;
static bool callback_registered = false;

/* ----------
 * Source enumeration
 * ----------
 */
typedef struct CollectCtx
{
	bool		unproven;
	int			nsources;
	Oid			relid[MATVIEW_SKIP_MAX_SOURCES];
} CollectCtx;

static void MatviewDirtyShmemRequest(void *arg);
static void MatviewDirtyShmemInit(void *arg);
static void matview_dirty_xact_callback(XactEvent event, void *arg);
static bool collect_walker(Node *node, CollectCtx *ctx);

const ShmemCallbacks MatviewDirtyShmemCallbacks = {
	.request_fn = MatviewDirtyShmemRequest,
	.init_fn = MatviewDirtyShmemInit,
};

static void
MatviewDirtyShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "MatviewSkipState",
					   .size = sizeof(MatviewSkipState),
					   .ptr = (void **) &matview_skip_state,
		);
}

static void
MatviewDirtyShmemInit(void *arg)
{
	SpinLockInit(&matview_skip_state->lock);
	matview_skip_state->nsrc = 0;
	matview_skip_state->nwm = 0;
}

/* ----------
 * Shared state helpers.  All callers hold the spinlock.
 * ----------
 */
static MatviewSrcEntry *
src_lookup(Oid relid)
{
	int			i;

	for (i = 0; i < matview_skip_state->nsrc; i++)
	{
		if (matview_skip_state->src[i].relid == relid &&
			matview_skip_state->src[i].dbid == MyDatabaseId)
			return &matview_skip_state->src[i];
	}
	return NULL;
}

/*
 * Find relid's counter, creating it if there is room.  A brand new entry starts
 * at gen 1 so that it can never compare equal to the zero-initialised gen of a
 * watermark slot that was never written.
 */
static MatviewSrcEntry *
src_lookup_or_create(Oid relid)
{
	MatviewSrcEntry *e = src_lookup(relid);

	if (e != NULL)
		return e;
	if (matview_skip_state->nsrc >= MATVIEW_SKIP_MAX_RELS)
		return NULL;

	e = &matview_skip_state->src[matview_skip_state->nsrc++];
	e->dbid = MyDatabaseId;
	e->relid = relid;
	e->gen = 1;
	return e;
}

static MatviewWatermark *
wm_lookup(Oid mvrelid)
{
	int			i;

	for (i = 0; i < matview_skip_state->nwm; i++)
	{
		if (matview_skip_state->wm[i].mvrelid == mvrelid &&
			matview_skip_state->wm[i].dbid == MyDatabaseId)
			return &matview_skip_state->wm[i];
	}
	return NULL;
}

/* ----------
 * MatviewDirtyNote
 *		Record that this transaction has written 'relid'.
 * ----------
 */
void
MatviewDirtyNote(Oid relid)
{
	int			i;

	if (matview_skip_state == NULL)
		return;

	/*
	 * System catalogs are never trackable sources -- a matview over one is
	 * rejected outright by the enumerator -- so there is nothing to record.
	 */
	if (relid < FirstNormalObjectId)
		return;

	/*
	 * Fast exit when no matview has ever tracked anything, which keeps this
	 * function free for every cluster that does not use the feature.
	 *
	 * Reading nsrc unlocked is safe.  It only ever grows, and the value that
	 * matters is "has any source ever been registered".  A watermark can only
	 * reference a source whose entry was created during that matview's
	 * capture, and capture holds ShareLock on the source, which conflicts with
	 * the RowExclusiveLock this writer already holds.  So a writer that could
	 * observe nsrc == 0 cannot be racing a capture of the relation it is
	 * writing, and its own lock acquisition supplied the barrier.
	 */
	if (matview_skip_state->nsrc == 0)
		return;

	/*
	 * Registered on first use rather than at backend startup so that a backend
	 * which never writes never pays for it.  The callback is what bumps the
	 * counters at commit, so it must be in place before the first recorded
	 * write can reach its commit.
	 */
	if (!callback_registered)
		MatviewDirtyRegisterCallback();

	/*
	 * Consecutive writes overwhelmingly hit the same relation, so check the
	 * most recent entry before scanning.
	 */
	if (xact_nwritten > 0 && xact_written[xact_nwritten - 1] == relid)
		return;

	for (i = 0; i < xact_nwritten; i++)
	{
		if (xact_written[i] == relid)
			return;
	}

	if (xact_nwritten < MATVIEW_SKIP_MAX_XACT_RELS)
		xact_written[xact_nwritten++] = relid;
	else
	{
		/*
		 * Out of room.  Dropping the relid would under-report the write, so
		 * remember that we did and bump every counter at commit instead.
		 */
		xact_write_overflow = true;
	}
}

/*
 * True if the current transaction has itself written 'relid' without having
 * flushed that fact to shared memory yet.  A REFRESH in the same transaction
 * as a write to one of its sources must not skip.
 */
static bool
written_locally(Oid relid)
{
	int			i;

	if (xact_write_overflow)
		return true;
	for (i = 0; i < xact_nwritten; i++)
	{
		if (xact_written[i] == relid)
			return true;
	}
	return false;
}

/* ----------
 * matview_dirty_xact_callback
 *
 * Bump the counters for everything this transaction wrote.  This runs at
 * XACT_EVENT_COMMIT, which CommitTransaction reaches only after
 * ProcArrayEndTransaction, so a bump is always strictly after the
 * corresponding rows became visible -- the ordering the mark-clean argument
 * depends on.  XACT_EVENT_PREPARE also bumps: a prepared transaction becomes
 * visible in FinishPreparedTransaction, which fires no callback at all and may
 * run in a different backend, so we must account for it up front.  Bumping
 * early is harmless because gen only ever means "something may have changed".
 *
 * This code is straight-line integer work under a spinlock.  It must stay
 * allocation-free and unable to throw: XACT_EVENT_COMMIT is post-commit, where
 * an error would report a committed transaction to the client as failed.
 * ----------
 */
static void
matview_dirty_xact_callback(XactEvent event, void *arg)
{
	int			i;

	if (matview_skip_state == NULL)
		return;

	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
			if (xact_nwritten == 0 && !xact_write_overflow)
				break;

			SpinLockAcquire(&matview_skip_state->lock);
			if (xact_write_overflow)
			{
				/* Cannot tell what was missed; assume everything changed. */
				for (i = 0; i < matview_skip_state->nsrc; i++)
					matview_skip_state->src[i].gen++;
			}
			else
			{
				for (i = 0; i < xact_nwritten; i++)
				{
					MatviewSrcEntry *e = src_lookup(xact_written[i]);

					if (e != NULL)
						e->gen++;
				}
			}
			SpinLockRelease(&matview_skip_state->lock);

			xact_nwritten = 0;
			xact_write_overflow = false;
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			/* Nothing became visible, so nothing to account for. */
			xact_nwritten = 0;
			xact_write_overflow = false;
			break;

		default:

			/*
			 * PRE_COMMIT and friends: keep the list, it is still needed at the
			 * real COMMIT event.
			 */
			break;
	}
}

void
MatviewDirtyRegisterCallback(void)
{
	if (!callback_registered)
	{
		RegisterXactCallback(matview_dirty_xact_callback, NULL);
		callback_registered = true;
	}
}

/* ----------
 * Enumeration of source relations
 * ----------
 */
static void
add_source(CollectCtx *ctx, Oid relid)
{
	int			i;

	for (i = 0; i < ctx->nsources; i++)
	{
		if (ctx->relid[i] == relid)
			return;
	}
	if (ctx->nsources >= MATVIEW_SKIP_MAX_SOURCES)
	{
		ctx->unproven = true;
		return;
	}
	ctx->relid[ctx->nsources++] = relid;
}

/*
 * True if 'relid' has a virtual generated column.  ALTER COLUMN ... SET
 * EXPRESSION recomputes every such value with no heap write and no
 * relfilenumber change, so we cannot track a relation that has one.
 */
static bool
has_virtual_generated_column(Oid relid)
{
	Relation	rel;
	TupleDesc	tupdesc;
	bool		found = false;
	int			i;

	rel = try_relation_open(relid, AccessShareLock);
	if (rel == NULL)
		return true;			/* vanished underneath us: unprovable */

	tupdesc = RelationGetDescr(rel);
	for (i = 0; i < tupdesc->natts; i++)
	{
		if (TupleDescAttr(tupdesc, i)->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL)
		{
			found = true;
			break;
		}
	}
	relation_close(rel, AccessShareLock);
	return found;
}

/*
 * Validate one relation and add it to the source set, expanding partitioned
 * and inheritance parents to their leaves.
 */
static void
collect_relation(CollectCtx *ctx, Oid relid)
{
	HeapTuple	tup;
	Form_pg_class classform;
	char		relkind;
	char		relpersistence;
	Oid			relam;
	bool		rls;

	/*
	 * A catalog can legally be a matview source, but MatviewDirtyNote ignores
	 * catalog writes, so no catalog could ever be seen to change.
	 */
	if (relid < FirstNormalObjectId)
	{
		ctx->unproven = true;
		return;
	}

	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tup))
	{
		ctx->unproven = true;
		return;
	}
	classform = (Form_pg_class) GETSTRUCT(tup);
	relkind = classform->relkind;
	relpersistence = classform->relpersistence;
	relam = classform->relam;
	rls = classform->relrowsecurity || classform->relforcerowsecurity;
	ReleaseSysCache(tup);

	/*
	 * RLS quals are injected by the rewriter from catalog state that changes
	 * with no write to any source, so a policy change would go unnoticed.
	 */
	if (rls)
	{
		ctx->unproven = true;
		return;
	}

	if (relpersistence == RELPERSISTENCE_TEMP)
	{
		ctx->unproven = true;
		return;
	}

	if (relkind == RELKIND_PARTITIONED_TABLE)
	{
		List	   *children;
		bool		detached = false;
		ListCell   *lc;

		/*
		 * omit_detached = false keeps the child list independent of the active
		 * snapshot.  detached_exist means a DETACH PARTITION CONCURRENTLY is in
		 * flight, whose visibility we cannot reason about.
		 */
		children = find_inheritance_children_extended(relid, false, NoLock,
													  &detached, NULL);
		if (detached)
		{
			ctx->unproven = true;
			list_free(children);
			return;
		}

		/*
		 * The parent itself has no storage; only the leaves are tracked.
		 * Recording the leaf set is what makes ATTACH / DETACH / DROP of a
		 * partition visible: the set no longer matches the watermark.
		 */
		foreach(lc, children)
		{
			collect_relation(ctx, lfirst_oid(lc));
			if (ctx->unproven)
				break;
		}
		list_free(children);
		return;
	}

	/*
	 * Whitelist, so that a relkind added in a future release is rejected
	 * rather than silently admitted.  Views are already expanded by the
	 * rewriter and never reach here.  Sequences and foreign tables change
	 * contents with no heap write and no relfilenumber change.
	 */
	if (relkind != RELKIND_RELATION && relkind != RELKIND_MATVIEW)
	{
		ctx->unproven = true;
		return;
	}

	/*
	 * Only the heap AM routes writes through the hooks in heapam.c.  This test
	 * comes after the relkind whitelist on purpose: a partitioned parent has
	 * relam 0, exactly like a view or a sequence, so relam alone cannot
	 * distinguish them.
	 */
	if (relam != HEAP_TABLE_AM_OID)
	{
		ctx->unproven = true;
		return;
	}

	if (has_virtual_generated_column(relid))
	{
		ctx->unproven = true;
		return;
	}

	add_source(ctx, relid);
}

static bool
collect_walker(Node *node, CollectCtx *ctx)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query	   *q = (Query *) node;

		if (q->commandType != CMD_SELECT ||
			q->hasModifyingCTE ||
			q->hasRowSecurity ||
			q->rowMarks != NIL)
		{
			ctx->unproven = true;
			return true;
		}
		return query_tree_walker(q, collect_walker, ctx,
								 QTW_EXAMINE_RTES_BEFORE);
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		/*
		 * No default: arm, so that -Wswitch flags a future RTEKind rather than
		 * letting it fall through to "harmless".
		 */
		switch (rte->rtekind)
		{
			case RTE_RELATION:
				if (rte->tablesample != NULL)
					ctx->unproven = true;	/* re-samples every refresh */
				else
					collect_relation(ctx, rte->relid);
				break;

			case RTE_SUBQUERY:
			case RTE_JOIN:
			case RTE_VALUES:
			case RTE_CTE:
			case RTE_RESULT:
			case RTE_GROUP:
				/* No storage of their own; the walker descends into them. */
				break;

			case RTE_FUNCTION:
			case RTE_TABLEFUNC:
			case RTE_NAMEDTUPLESTORE:
			case RTE_GRAPH_TABLE:
				ctx->unproven = true;
				break;
		}
		return false;
	}

	return expression_tree_walker(node, collect_walker, ctx);
}

/*
 * Rewrite the matview's stored query the same way refresh_matview_datafill
 * does, then enumerate its sources and fingerprint it.
 *
 * Rewriting is what makes view sources work at all: in the stored rule a view
 * is an RTE_RELATION with relkind 'v' and its base tables appear nowhere.  The
 * fingerprint of the rewritten tree is also what detects CREATE OR REPLACE
 * VIEW and other definition changes that touch no source.
 */
static bool
collect_sources(Query *dataQuery, CollectCtx *ctx, uint32 *fingerprint)
{
	Query	   *copied;
	List	   *rewritten;
	Query	   *rq;
	char	   *str;

	memset(ctx, 0, sizeof(*ctx));
	*fingerprint = 0;

	copied = copyObject(dataQuery);
	AcquireRewriteLocks(copied, true, false);
	rewritten = QueryRewrite(copied);

	if (list_length(rewritten) != 1)
		return false;
	rq = (Query *) linitial(rewritten);
	if (!IsA(rq, Query))
		return false;

	str = nodeToString(rq);
	*fingerprint = hash_bytes((const unsigned char *) str, strlen(str));
	pfree(str);

	(void) collect_walker((Node *) rq, ctx);

	if (ctx->unproven)
		return false;

	/*
	 * A non-IMMUTABLE function can change the contents with no write at all.
	 * This catches now(), and STABLE functions that read tables we have not
	 * enumerated.
	 */
	if (contain_mutable_functions((Node *) rq))
		return false;

	return true;
}

/* ----------
 * MatviewSkipCheck
 * ----------
 */
bool
MatviewSkipCheck(Relation matviewRel, Query *dataQuery, bool allow_skip,
				 MatviewSkipCapture *capture)
{
	CollectCtx	ctx;
	uint32		fingerprint;
	MatviewWatermark *wm;
	bool		clean;
	int			i;

	memset(capture, 0, sizeof(*capture));

	if (matview_skip_state == NULL || !dbblue_matview_skip_unchanged)
		return false;

	/*
	 * Under REPEATABLE READ or SERIALIZABLE the data-fill query is pinned to
	 * the transaction snapshot, which can predate anything we observe here, so
	 * neither skipping nor recording a watermark is sound.  Run stock.
	 */
	if (IsolationUsesXactSnapshot())
		return false;

	if (!collect_sources(dataQuery, &ctx, &fingerprint))
		return false;

	/*
	 * Capture each source's counter and storage identity.  ShareLock proves no
	 * writer to this source is in flight, prepared, or mid-commit, and forces
	 * the relcache invalidation that makes the relfilenumber read meaningful.
	 * It is released at once: we are not going to read the source's data under
	 * it, and holding it would block writers.
	 */
	capture->nsources = ctx.nsources;
	capture->fingerprint = fingerprint;

	for (i = 0; i < ctx.nsources; i++)
	{
		Oid			relid = ctx.relid[i];
		Relation	rel;
		MatviewSrcEntry *e;

		if (written_locally(relid))
			return false;

		if (!ConditionalLockRelationOid(relid, ShareLock))
			return false;

		rel = try_relation_open(relid, NoLock);
		if (rel == NULL)
		{
			UnlockRelationOid(relid, ShareLock);
			return false;
		}

		capture->relid[i] = relid;
		capture->rfn[i] = rel->rd_locator.relNumber;
		relation_close(rel, NoLock);

		SpinLockAcquire(&matview_skip_state->lock);
		e = src_lookup_or_create(relid);
		capture->gen[i] = (e != NULL) ? e->gen : 0;
		SpinLockRelease(&matview_skip_state->lock);

		UnlockRelationOid(relid, ShareLock);

		if (e == NULL)
			return false;		/* no room to track it */
	}

	capture->valid = true;

	if (!allow_skip)
		return false;

	/*
	 * Compare against this matview's own watermark.  Everything must match:
	 * the recorded source set exactly (which is how partition membership
	 * changes are caught), every counter, every source's storage identity, the
	 * query fingerprint, and the matview's own storage identity -- the last of
	 * which is also the witness that the refresh which wrote the watermark
	 * actually committed.
	 */
	clean = false;
	SpinLockAcquire(&matview_skip_state->lock);
	wm = wm_lookup(RelationGetRelid(matviewRel));
	if (wm != NULL &&
		wm->nsources == capture->nsources &&
		wm->fingerprint == capture->fingerprint &&
		wm->mv_rfn == matviewRel->rd_locator.relNumber)
	{
		clean = true;
		for (i = 0; i < capture->nsources; i++)
		{
			if (wm->relid[i] != capture->relid[i] ||
				wm->gen[i] != capture->gen[i] ||
				wm->rfn[i] != capture->rfn[i])
			{
				clean = false;
				break;
			}
		}
	}
	SpinLockRelease(&matview_skip_state->lock);

	return clean;
}

/* ----------
 * MatviewSkipMarkClean
 * ----------
 */
void
MatviewSkipMarkClean(Oid matviewOid, const MatviewSkipCapture *capture)
{
	Relation	rel;
	RelFileNumber mv_rfn;
	MatviewWatermark *wm;
	int			i;

	if (matview_skip_state == NULL || !capture->valid)
		return;

	/*
	 * Read the matview's post-swap storage identity.  Our own catalog update
	 * has to be visible to this lookup, hence the command counter bump.  That
	 * value is the witness that this transaction committed: if it aborts,
	 * pg_class reverts and the watermark stops matching.
	 */
	CommandCounterIncrement();

	rel = try_relation_open(matviewOid, NoLock);
	if (rel == NULL)
		return;
	mv_rfn = rel->rd_locator.relNumber;
	relation_close(rel, NoLock);

	SpinLockAcquire(&matview_skip_state->lock);

	wm = wm_lookup(matviewOid);
	if (wm == NULL && matview_skip_state->nwm < MATVIEW_SKIP_MAX_MATVIEWS)
	{
		wm = &matview_skip_state->wm[matview_skip_state->nwm++];
		wm->dbid = MyDatabaseId;
		wm->mvrelid = matviewOid;
	}

	if (wm != NULL)
	{
		wm->mv_rfn = mv_rfn;
		wm->fingerprint = capture->fingerprint;
		wm->nsources = capture->nsources;
		for (i = 0; i < capture->nsources; i++)
		{
			wm->relid[i] = capture->relid[i];
			wm->gen[i] = capture->gen[i];
			wm->rfn[i] = capture->rfn[i];
		}
	}

	SpinLockRelease(&matview_skip_state->lock);
}
