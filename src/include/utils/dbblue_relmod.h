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
 * Two readings taken at different times compare equal only if no transaction
 * has written to the relation in between.  Equality is therefore a sound
 * licence to reuse a row count captured at the earlier reading.
 *
 * slot_gen guards the case where the relation's slot was taken over by another
 * relation in the meantime, which erases this relation's stamp back to the
 * zero default and would otherwise make a written-since relation look
 * untouched.  It is per-slot rather than global so that one collision
 * invalidates only the relations that shared that slot, not every cached count
 * in the cluster.  global_epoch remains for the rare cases that genuinely need
 * a cluster-wide sweep (COMMIT PREPARED).
 */
typedef struct DBBlueRelModStamp
{
	uint64		stamp;			/* per-relation monotonic write stamp */
	uint64		slot_gen;		/* generation of the slot it was read from */
	uint64		global_epoch;	/* cluster-wide invalidation counter */
} DBBlueRelModStamp;

extern PGDLLIMPORT const ShmemCallbacks DBBlueRelModShmemCallbacks;

/* Maintain write stamps at all?  PGC_POSTMASTER; see dbblue_relmod.c. */
extern PGDLLIMPORT bool dbblue_track_relation_writes;

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

/*
 * Epoch value dbblue_relmod_read() returns when there is no shared state to
 * read -- write tracking disabled, or single-user bootstrap.  Treated as never
 * equal to anything, including itself, so a reading taken without tracking can
 * never validate a cached count.
 */
#define DBBLUE_RELMOD_EPOCH_INVALID		PG_UINT64_MAX

static inline bool
dbblue_relmod_stamp_equal(DBBlueRelModStamp a, DBBlueRelModStamp b)
{
	/*
	 * Compare unequal if either reading was taken with no shared state.  Field
	 * equality alone would report two such readings as equal -- both are
	 * zeroed with an INVALID epoch -- and serve a count that nothing was
	 * tracking.
	 */
	if (a.global_epoch == DBBLUE_RELMOD_EPOCH_INVALID ||
		b.global_epoch == DBBLUE_RELMOD_EPOCH_INVALID)
		return false;

	return a.stamp == b.stamp &&
		a.slot_gen == b.slot_gen &&
		a.global_epoch == b.global_epoch;
}

#endif							/* DBBLUE_RELMOD_H */
