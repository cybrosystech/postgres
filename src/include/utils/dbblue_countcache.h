/*-------------------------------------------------------------------------
 *
 * dbblue_countcache.h
 *	  Session-local cache of exact COUNT(*) results, keyed by
 *	  (relation, predicate fingerprint, snapshot).
 *
 * The cache is populated as a side effect when Odoo's web_search_read
 * emits its leading "SELECT COUNT(*) FROM <rel> WHERE ..." query, and is
 * consulted by the planner when shaping the matching paginated SELECT.
 * Those arrive as separate transactions on the same backend, so
 * cross-transaction reuse is the whole point -- and is what makes the
 * validity gate load-bearing rather than advisory.  An entry may be served
 * only while the relation's shared modification stamp is unchanged from
 * capture; see dbblue_relmod.c.  The TTL is a memory-hygiene bound on top
 * of that, not a correctness mechanism.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/dbblue_countcache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBBLUE_COUNTCACHE_H
#define DBBLUE_COUNTCACHE_H

#include "access/transam.h"
#include "datatype/timestamp.h"
#include "utils/dbblue_relmod.h"

typedef struct CountCacheKey
{
	Oid			reloid;
	int64		qual_fingerprint;
} CountCacheKey;

typedef struct CountCacheEntry
{
	/* HTAB lookup key.  Must be the first field. */
	CountCacheKey key;

	/* Exact row count under the predicate at capture time. */
	int64		count;

	/*
	 * The relation's shared modification stamp as of capture.  This is the
	 * correctness gate: the entry may be served only while a fresh reading
	 * still compares equal, which is true exactly when no transaction --
	 * in this backend or any other -- has written to the relation since.
	 */
	DBBlueRelModStamp relmod;

	/* Wall-clock capture time; drives FIFO eviction when the cache fills. */
	TimestampTz captured_at;

	/* Counter for observability; not used in any decision. */
	int			hits;
} CountCacheEntry;

/* GUC: enable/disable COUNT caching (and the offset-flip that feeds on it). */
extern PGDLLIMPORT bool dbblue_count_cache;

/* GUC: how long (seconds) a cache entry stays valid; default 300 (5 min). */
extern PGDLLIMPORT int dbblue_countcache_ttl;

/*
 * Lookup the entry for (reloid, fingerprint).  Returns NULL on miss, on
 * snapshot-staleness, or when the cache is disabled / uninitialized.
 * A pointer into the cache is returned; callers must not free or modify
 * the entry, and must not retain it past the next cache mutation.
 */
extern const CountCacheEntry *dbblue_countcache_lookup(Oid reloid,
													   int64 fingerprint);

/*
 * Queue a count for insertion, stamped with the relation's modification
 * stamp as read right now.  The entry is not published to the cache until
 * the current transaction commits, and then only if the transaction wrote
 * nothing and the stamp is still unchanged -- so a count computed inside a
 * transaction that later rolls back, or that is overtaken by a concurrent
 * writer, is discarded rather than cached.
 *
 * No-op if fingerprint == 0 (caller signalled "uncacheable").
 */
extern void dbblue_countcache_insert(Oid reloid, int64 fingerprint,
									 int64 count);

/* Current populated size, exposed for observability / tests. */
extern int dbblue_countcache_current_size(void);

/* Discard queued (uncommitted) counts; called at transaction start. */
extern void dbblue_countcache_reset_xact(void);

/*
 * Capture-side hook called by standard_ExecutorRun.  When the planned
 * statement looks like a single-aggregate COUNT(*) shape and carries a
 * non-zero predicate fingerprint, this wraps queryDesc->dest with an
 * interceptor that records the resulting count.  Returns true when a
 * wrapper was installed; the caller must pass the same QueryDesc to
 * dbblue_count_capture_finalize() afterwards to insert and unwrap.
 *
 * Safe to call on any QueryDesc; non-matching queries return false and
 * leave queryDesc->dest untouched.
 */
struct QueryDesc;				/* forward decl to keep this header light */
extern bool dbblue_count_capture_install(struct QueryDesc *queryDesc);
extern void dbblue_count_capture_finalize(struct QueryDesc *queryDesc);

/*
 * Serve-side counterpart to capture: if the current query is a bare
 * COUNT(*) and the cache has a fresh hit, inject the cached count into
 * queryDesc->dest and return true (caller should skip ExecutePlan).
 * Must be called after rStartup, before ExecutePlan, and only when the
 * capture wrapper is already installed (i.e. dbblue_capture_installed).
 */
extern bool dbblue_count_serve_if_cached(struct QueryDesc *queryDesc);

#endif							/* DBBLUE_COUNTCACHE_H */
