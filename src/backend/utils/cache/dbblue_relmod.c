/*-------------------------------------------------------------------------
 *
 * dbblue_relmod.c
 *	  Shared per-relation modification stamps for the DBblue COUNT cache.
 *
 * The COUNT cache reuses a row count captured by an earlier transaction in
 * the same backend.  That is only sound if nothing has changed the set of
 * rows visible for that relation in between -- including changes made by
 * other backends, which a session-local cache cannot otherwise observe.
 *
 * This module provides the missing signal: a small fixed-size table in
 * shared memory mapping a relation OID to a monotonically increasing
 * "write stamp".  The stamp is bumped
 *
 *	 (a) on the first write to the relation by a transaction, and
 *	 (b) again just before that transaction's commit becomes visible.
 *
 * Both bumps are needed.  (a) alone would miss a transaction that wrote
 * before a count was captured and committed after it -- the new rows would
 * become visible with no intervening stamp change.  (b) alone would miss
 * nothing for correctness but would delay invalidation until commit, which
 * is harmless; keeping both simply makes the window as tight as possible.
 * Bumping in the abort path too is conservative and therefore safe.
 *
 * The table is direct-mapped: a relation lands in one slot chosen by
 * hashing its OID, and a colliding relation simply steals the slot.  A
 * steal bumps a global eviction epoch which is folded into every stamp
 * reading, so a stolen slot can never make a stale count look fresh -- the
 * failure mode is a spurious invalidation, never a spurious validation.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/dbblue_relmod.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/hashfn.h"
#include "nodes/pg_list.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/dbblue_relmod.h"
#include "utils/memutils.h"

/*
 * Number of slots.  Power of two so the index is a mask.  8192 slots is
 * 128 kB of shared memory and is comfortably more than the number of
 * relations a typical workload writes to between COUNT captures, so slot
 * steals -- and the blanket invalidation they cause -- stay rare.
 */
#define DBBLUE_RELMOD_NSLOTS	8192
#define DBBLUE_RELMOD_MASK		(DBBLUE_RELMOD_NSLOTS - 1)

typedef struct DBBlueRelModSlot
{
	Oid			reloid;
	uint64		stamp;
} DBBlueRelModSlot;

typedef struct DBBlueRelModShared
{
	slock_t		mutex;
	uint64		next_stamp;		/* monotonic; values are never reused */
	uint64		evict_epoch;	/* bumped whenever a slot is stolen */
	DBBlueRelModSlot slots[DBBLUE_RELMOD_NSLOTS];
} DBBlueRelModShared;

static DBBlueRelModShared *relmod = NULL;

/*
 * Relations written by the current transaction.  Lives in
 * TopTransactionContext, so it is reclaimed automatically; we only have to
 * drop our pointer at end of transaction.
 */
static List *xact_written_rels = NIL;

/* One-entry memo so a bulk load pays only a comparison per row. */
static Oid	last_noted_rel = InvalidOid;

/*
 * Whether to maintain per-relation write stamps at all.  PGC_POSTMASTER,
 * because it decides whether the shared area is allocated -- and because a
 * per-session setting would be unsound: a session with it off would not bump
 * stamps while another session cached counts against them.
 *
 * With this off the COUNT cache cannot operate, since dbblue_relmod_read()
 * then returns DBBLUE_RELMOD_EPOCH_INVALID and no cached count ever validates.
 */
bool		dbblue_track_relation_writes = false;

static void DBBlueRelModShmemRequest(void *arg);
static void DBBlueRelModShmemInit(void *arg);

const ShmemCallbacks DBBlueRelModShmemCallbacks = {
	.request_fn = DBBlueRelModShmemRequest,
	.init_fn = DBBlueRelModShmemInit,
};

static void
DBBlueRelModShmemRequest(void *arg)
{
	if (!dbblue_track_relation_writes)
		return;

	ShmemRequestStruct(.name = "DBblue Relation Modification Stamps",
					   .size = sizeof(DBBlueRelModShared),
					   .ptr = (void **) &relmod,
		);
}

static void
DBBlueRelModShmemInit(void *arg)
{
	int			i;

	if (!dbblue_track_relation_writes)
		return;

	SpinLockInit(&relmod->mutex);

	/*
	 * Start stamps at 1 so that zero can mean "this relation has never been
	 * written since the slot was last (re)claimed".  A count captured for a
	 * never-written relation records stamp 0 and stays valid for as long as
	 * that remains true, which is the common read-only case.
	 */
	relmod->next_stamp = 1;
	relmod->evict_epoch = 0;

	for (i = 0; i < DBBLUE_RELMOD_NSLOTS; i++)
	{
		relmod->slots[i].reloid = InvalidOid;
		relmod->slots[i].stamp = 0;
	}
}

static inline uint32
relmod_slot_index(Oid reloid)
{
	return hash_bytes_uint32((uint32) reloid) & DBBLUE_RELMOD_MASK;
}

/*
 * Bump the shared stamp for one relation.
 */
static void
relmod_bump(Oid reloid)
{
	uint32		idx;

	if (relmod == NULL || !OidIsValid(reloid))
		return;

	idx = relmod_slot_index(reloid);

	SpinLockAcquire(&relmod->mutex);

	/*
	 * Taking the slot from another relation makes that relation's history
	 * unreadable, so advance the eviction epoch.  Every cached stamp carries
	 * the epoch, so all outstanding counts are invalidated -- conservative,
	 * but a steal is rare and the alternative would be unsound.
	 */
	if (relmod->slots[idx].reloid != reloid &&
		OidIsValid(relmod->slots[idx].reloid))
		relmod->evict_epoch++;

	relmod->slots[idx].reloid = reloid;
	relmod->slots[idx].stamp = ++relmod->next_stamp;

	SpinLockRelease(&relmod->mutex);
}

/*
 * Invalidate every cached count in every backend, by advancing the eviction
 * epoch that each cached stamp carries.
 *
 * Used where a set of rows becomes visible but the relations involved are not
 * knowable from the current backend's write set -- specifically COMMIT/ROLLBACK
 * PREPARED, where the write set belonged to the transaction that ran PREPARE
 * and is long gone.  Bumping per-relation stamps at PREPARE time is not enough:
 * the rows only become visible at COMMIT PREPARED, so a count captured in
 * between would otherwise survive with an unchanged stamp.
 *
 * A cluster-wide invalidation is heavy-handed, but two-phase commit is rare and
 * the alternative is a wrong answer.
 */
void
dbblue_relmod_invalidate_all(void)
{
	if (relmod == NULL)
		return;

	SpinLockAcquire(&relmod->mutex);
	relmod->evict_epoch++;
	SpinLockRelease(&relmod->mutex);
}

DBBlueRelModStamp
dbblue_relmod_read(Oid reloid)
{
	DBBlueRelModStamp result;
	uint32		idx;

	result.stamp = 0;
	result.evict_epoch = 0;

	if (relmod == NULL || !OidIsValid(reloid))
	{
		/*
		 * No shared state (e.g. single-user bootstrap).  Return an epoch that
		 * cannot match any real reading so nothing is ever served from cache.
		 */
		result.evict_epoch = DBBLUE_RELMOD_EPOCH_INVALID;
		return result;
	}

	idx = relmod_slot_index(reloid);

	SpinLockAcquire(&relmod->mutex);
	if (relmod->slots[idx].reloid == reloid)
		result.stamp = relmod->slots[idx].stamp;
	result.evict_epoch = relmod->evict_epoch;
	SpinLockRelease(&relmod->mutex);

	return result;
}

void
dbblue_relmod_note_write(Oid reloid)
{
	MemoryContext oldcxt;

	/*
	 * The whole point of the postmaster switch: when tracking is off this is
	 * the entire cost paid on the write path.
	 */
	if (!dbblue_track_relation_writes)
		return;

	if (!OidIsValid(reloid))
		return;

	if (reloid == last_noted_rel)
		return;
	if (list_member_oid(xact_written_rels, reloid))
	{
		last_noted_rel = reloid;
		return;
	}

	relmod_bump(reloid);

	/*
	 * Remember it for the commit-time bump.  TopTransactionContext, not the
	 * executor's context, so the list survives to end of transaction.
	 */
	if (TopTransactionContext != NULL)
	{
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		xact_written_rels = lappend_oid(xact_written_rels, reloid);
		MemoryContextSwitchTo(oldcxt);
		last_noted_rel = reloid;
	}
}

void
dbblue_relmod_bump_xact_rels(void)
{
	ListCell   *lc;

	foreach(lc, xact_written_rels)
		relmod_bump(lfirst_oid(lc));
}

void
dbblue_relmod_reset_xact(void)
{
	xact_written_rels = NIL;
	last_noted_rel = InvalidOid;
}

/*
 * Has the current transaction written to this relation?
 *
 * A count is uncacheable only if *this* relation was written, not if the
 * transaction wrote to anything at all: changes to another relation cannot
 * alter how many rows of this one are visible.  The distinction matters in
 * practice -- an application whose every request writes a session or audit
 * row (Odoo does) would otherwise never cache a single count.
 *
 * This also covers subtransaction rollback without a separate flag: a
 * relation written by a subtransaction stays in the write set even after
 * that subtransaction aborts, so counts over it remain refused for the rest
 * of the transaction.
 */
bool
dbblue_relmod_xact_wrote(Oid reloid)
{
	if (reloid == last_noted_rel)
		return true;
	return list_member_oid(xact_written_rels, reloid);
}
