/*-------------------------------------------------------------------------
 *
 * dbblue_relmod.h
 *	  Shared per-relation modification stamps for the DBblue COUNT cache.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/dbblue_relmod.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBBLUE_RELMOD_H
#define DBBLUE_RELMOD_H

#include "storage/shmem.h"

/*
 * A point-in-time reading of one relation's modification state.
 *
 * Two readings taken at different times compare equal only if no
 * transaction has written to the relation in between (and no slot
 * eviction has clouded the picture).  Equality is therefore a sound
 * licence to reuse a row count captured at the earlier reading.
 */
typedef struct DBBlueRelModStamp
{
	uint64		stamp;			/* per-relation monotonic write stamp */
	uint64		evict_epoch;	/* global slot-eviction counter */
} DBBlueRelModStamp;

extern PGDLLIMPORT const ShmemCallbacks DBBlueRelModShmemCallbacks;

/*
 * Record that the current transaction has written to reloid.  Cheap and
 * idempotent within a transaction: the shared stamp is bumped only on the
 * first write to each relation.  Also marks the transaction non-cacheable,
 * since a count taken inside a transaction that wrote to the relation
 * cannot be reused by any later transaction.
 */
extern void dbblue_relmod_note_write(Oid reloid);

/*
 * Bump the shared stamp again for every relation this transaction wrote.
 * Must be called before the transaction's commit becomes visible to other
 * backends, so that a reader which can see the new rows is guaranteed to
 * also see a changed stamp.
 */
extern void dbblue_relmod_bump_xact_rels(void);

/* Forget this transaction's write set (called at transaction start). */
extern void dbblue_relmod_reset_xact(void);

/*
 * Has the current transaction written to this relation?  Counts over such a
 * relation must not be cached.  Writes to *other* relations are irrelevant.
 */
extern bool dbblue_relmod_xact_wrote(Oid reloid);

/* Read the current modification stamp for reloid. */
extern DBBlueRelModStamp dbblue_relmod_read(Oid reloid);

/*
 * Invalidate all cached counts cluster-wide.  For paths where rows become
 * visible but the affected relations are not knowable from this backend's
 * write set -- COMMIT/ROLLBACK PREPARED.
 */
extern void dbblue_relmod_invalidate_all(void);

static inline bool
dbblue_relmod_stamp_equal(DBBlueRelModStamp a, DBBlueRelModStamp b)
{
	return a.stamp == b.stamp && a.evict_epoch == b.evict_epoch;
}

#endif							/* DBBLUE_RELMOD_H */
