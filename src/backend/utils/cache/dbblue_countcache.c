/*-------------------------------------------------------------------------
 *
 * dbblue_countcache.c
 *	  Session-local cache of exact COUNT(*) results.
 *
 * Each entry pairs a (relation, predicate fingerprint) key with the row
 * count observed under that predicate at a particular snapshot horizon.
 * The cache is populated as a side effect of executing Odoo's leading
 * COUNT query for a list view, and the planner consults it later in the
 * same request when shaping the paginated SELECT.
 *
 * Sizing: a small fixed bound (DBBLUE_COUNTCACHE_DEFAULT_SIZE) is
 * sufficient because Odoo paginates one view at a time per HTTP request.
 * When full, the entry with the oldest captured_at is evicted; for a
 * 16-entry bound the linear scan to find it is cheap.
 *
 * Invalidation: a relcache callback drops cached entries for any
 * relation that PG flushes from the relcache.  Combined with the
 * snapshot-xmin check on lookup, this keeps the cache aligned with
 * whatever the current backend can see.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/dbblue_countcache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "miscadmin.h"
#include "utils/dbblue_countcache.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#define DBBLUE_COUNTCACHE_DEFAULT_SIZE	16

static HTAB *countcache = NULL;
static int	countcache_count = 0;
static int	countcache_max_entries = DBBLUE_COUNTCACHE_DEFAULT_SIZE;
static bool callback_registered = false;

static void countcache_relcache_callback(Datum arg, Oid relid);
static void ensure_init(void);
static void evict_oldest(void);
static TransactionId current_snapshot_xmin(void);

/*
 * Lazy initialization.  We don't pay for the HTAB until the first time
 * an Odoo COUNT actually lands.  hash_create allocates in
 * TopMemoryContext so the cache lives for the backend's lifetime.
 */
static void
ensure_init(void)
{
	HASHCTL		ctl;

	if (countcache != NULL)
		return;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(CountCacheKey);
	ctl.entrysize = sizeof(CountCacheEntry);
	ctl.hcxt = TopMemoryContext;

	countcache = hash_create("dbblue COUNT cache",
							 countcache_max_entries,
							 &ctl,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	countcache_count = 0;

	if (!callback_registered)
	{
		CacheRegisterRelcacheCallback(countcache_relcache_callback,
									  (Datum) 0);
		callback_registered = true;
	}
}

/*
 * Return the xmin of the currently active snapshot.  If no snapshot is
 * registered yet (e.g. a utility command running outside of any
 * transaction-level snapshot), we return InvalidTransactionId, which
 * makes the cache miss-by-default for that path.
 */
static TransactionId
current_snapshot_xmin(void)
{
	Snapshot	snap;

	if (!ActiveSnapshotSet())
		return InvalidTransactionId;

	snap = GetActiveSnapshot();
	if (snap == NULL)
		return InvalidTransactionId;

	return snap->xmin;
}

const CountCacheEntry *
dbblue_countcache_lookup(Oid reloid, int64 fingerprint)
{
	CountCacheKey key;
	CountCacheEntry *entry;
	bool		found;
	TransactionId xmin_now;

	if (countcache == NULL)
		return NULL;
	if (!OidIsValid(reloid) || fingerprint == INT64CONST(0))
		return NULL;

	memset(&key, 0, sizeof(key));
	key.reloid = reloid;
	key.qual_fingerprint = fingerprint;

	entry = (CountCacheEntry *) hash_search(countcache, &key, HASH_FIND,
											&found);
	if (!found)
		return NULL;

	xmin_now = current_snapshot_xmin();
	if (!TransactionIdIsValid(xmin_now) ||
		!TransactionIdEquals(entry->snapshot_xmin, xmin_now))
	{
		/*
		 * Stale by snapshot horizon.  Drop the entry so we don't have
		 * to re-check it on every subsequent lookup.
		 */
		(void) hash_search(countcache, &key, HASH_REMOVE, NULL);
		countcache_count--;
		return NULL;
	}

	entry->hits++;
	return entry;
}

void
dbblue_countcache_insert(Oid reloid, int64 fingerprint, int64 count)
{
	CountCacheKey key;
	CountCacheEntry *entry;
	bool		found;
	TransactionId xmin_now;

	if (!OidIsValid(reloid) || fingerprint == INT64CONST(0))
		return;

	xmin_now = current_snapshot_xmin();
	if (!TransactionIdIsValid(xmin_now))
		return;

	ensure_init();

	/*
	 * Evict before INSERT_ENTRY so the htab never exceeds its size budget.
	 * The check is on the *current* count (before insert); if the key
	 * happens to already exist below we'll overwrite, not grow, so an
	 * unnecessary eviction is harmless in that case.
	 */
	if (countcache_count >= countcache_max_entries)
		evict_oldest();

	memset(&key, 0, sizeof(key));
	key.reloid = reloid;
	key.qual_fingerprint = fingerprint;

	entry = (CountCacheEntry *) hash_search(countcache, &key, HASH_ENTER,
											&found);
	if (!found)
		countcache_count++;

	entry->count = count;
	entry->snapshot_xmin = xmin_now;
	entry->captured_at = GetCurrentTimestamp();
	entry->hits = 0;
}

int
dbblue_countcache_current_size(void)
{
	return countcache_count;
}

/*
 * FIFO eviction: scan the table, remember the oldest captured_at, then
 * remove it.  For a 16-entry bound this is ~32ns total -- negligible.
 *
 * Note: hash_search(HASH_REMOVE) is safe to call after a hash_seq_search
 * loop has terminated normally (returned NULL); we collect the doomed
 * key first and remove it after the scan.
 */
static void
evict_oldest(void)
{
	HASH_SEQ_STATUS scan;
	CountCacheEntry *entry;
	CountCacheKey doomed_key = {0};
	TimestampTz oldest_ts = 0;
	bool		have_doomed = false;

	if (countcache == NULL)
		return;

	hash_seq_init(&scan, countcache);
	while ((entry = (CountCacheEntry *) hash_seq_search(&scan)) != NULL)
	{
		if (!have_doomed || entry->captured_at < oldest_ts)
		{
			doomed_key = entry->key;
			oldest_ts = entry->captured_at;
			have_doomed = true;
		}
	}

	if (have_doomed)
	{
		(void) hash_search(countcache, &doomed_key, HASH_REMOVE, NULL);
		countcache_count--;
	}
}

/*
 * Drop cached entries for one relation (or all, when relid is invalid).
 * PG fires this callback whenever the relcache entry for that relation
 * is flushed -- which covers DDL on the relation and the periodic
 * relcache resets that PG performs after any heavy write activity.
 *
 * HASH_REMOVE on the entry just returned by hash_seq_search is allowed;
 * we walk the table once and drop matches inline.
 */
static void
countcache_relcache_callback(Datum arg, Oid relid)
{
	HASH_SEQ_STATUS scan;
	CountCacheEntry *entry;

	(void) arg;

	if (countcache == NULL)
		return;

	hash_seq_init(&scan, countcache);
	while ((entry = (CountCacheEntry *) hash_seq_search(&scan)) != NULL)
	{
		if (!OidIsValid(relid) || entry->key.reloid == relid)
		{
			(void) hash_search(countcache, &entry->key, HASH_REMOVE, NULL);
			countcache_count--;
		}
	}
}
