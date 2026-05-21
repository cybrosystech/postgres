/*-------------------------------------------------------------------------
 *
 * dbblue_countcache.h
 *	  Session-local cache of exact COUNT(*) results, keyed by
 *	  (relation, predicate fingerprint, snapshot).
 *
 * The cache is populated as a side effect when Odoo's web_search_read
 * emits its leading "SELECT COUNT(*) FROM <rel> WHERE ..." query, and is
 * consulted by the planner when shaping the matching paginated SELECT.
 * Cross-transaction reuse is not a goal; the natural lifetime of an
 * entry is one HTTP request (~one Odoo cursor txn).
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
	 * Snapshot horizon at the time of capture.  Used as a freshness gate:
	 * if the current active snapshot's xmin differs, we treat the entry
	 * as stale and drop it on the lookup path.
	 */
	TransactionId snapshot_xmin;

	/* Wall-clock capture time; drives FIFO eviction when the cache fills. */
	TimestampTz captured_at;

	/* Counter for observability; not used in any decision. */
	int			hits;
} CountCacheEntry;

/* GUC: enable/disable COUNT caching (and the offset-flip that feeds on it). */
extern PGDLLIMPORT bool dbblue_count_cache;

/*
 * Lookup the entry for (reloid, fingerprint).  Returns NULL on miss, on
 * snapshot-staleness, or when the cache is disabled / uninitialized.
 * A pointer into the cache is returned; callers must not free or modify
 * the entry, and must not retain it past the next cache mutation.
 */
extern const CountCacheEntry *dbblue_countcache_lookup(Oid reloid,
													   int64 fingerprint);

/*
 * Insert or refresh an entry for (reloid, fingerprint) with the given
 * count, stamping it with the current active snapshot's xmin.  When the
 * cache is at capacity the oldest entry (by captured_at) is evicted first.
 *
 * No-op if fingerprint == 0 (caller signalled "uncacheable") or there is
 * no active snapshot.
 */
extern void dbblue_countcache_insert(Oid reloid, int64 fingerprint,
									 int64 count);

/* Current populated size, exposed for observability / tests. */
extern int dbblue_countcache_current_size(void);

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
