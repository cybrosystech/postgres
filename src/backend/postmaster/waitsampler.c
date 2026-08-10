/*-------------------------------------------------------------------------
 *
 * waitsampler.c
 *
 * dbblue wait sampling collector.
 *
 * A background worker, registered unconditionally from the postmaster
 * (see WaitSamplerRegister(), called from PostmasterMain()), that samples
 * every backend's current wait event and persists it as history and
 * profile rows in dbblue_catalog.wait_history / dbblue_catalog.wait_profile
 * inside the database named by dbblue_wait_sampling_database.
 *
 * The worker runs for the lifetime of the postmaster.  When
 * dbblue_wait_sampling_enabled is off it just idles; the schema/tables are
 * created lazily, the first time the GUC is observed to be on.
 *
 * Sampling (every dbblue_wait_sampling_period) only touches in-memory
 * state (no I/O): for each backend we remember its last-seen wait event
 * and when that interval started.  Persisting to disk happens on two
 * independent, much coarser cadences:
 *
 *   - dbblue_wait_sampling_flush_interval: every still-open or just-closed
 *     interval is upserted (keyed by pid+start_ts) into wait_history, so a
 *     long-running wait's end_ts keeps advancing even before it ends.
 *   - dbblue_wait_sampling_profile_period: the in-memory profile counters
 *     (bucketed by pid/database/wait event/queryid) are flushed as one
 *     snapshot into wait_profile and reset.
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/waitsampler.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "postmaster/waitsampler.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/* pgstatfuncs.c defines the same thing locally; there's no shared header for it */
#define UINT32_ACCESS_ONCE(var)		 ((uint32) (*((volatile uint32 *) &(var))))

/* GUC variables (declarations live in waitsampler.h) */
bool		dbblue_wait_sampling_enabled = false;
int			dbblue_wait_sampling_period = 10;
int			dbblue_wait_sampling_profile_period = 1000;
bool		dbblue_wait_sampling_profile_pid = true;
int			dbblue_wait_sampling_profile_queries = DBBLUE_WS_PROFILE_QUERIES_TOP;
bool		dbblue_wait_sampling_sample_cpu = true;
int			dbblue_wait_sampling_history_retention = 10080;
int			dbblue_wait_sampling_flush_interval = 2000;
char	   *dbblue_wait_sampling_database = NULL;

/* How often the retention sweep runs, regardless of history_retention's value */
#define DBBLUE_WS_RETENTION_SWEEP_MS	(60 * 60 * 1000)

/* Per-proc-slot tracked wait interval, kept in process-private memory */
typedef struct
{
	bool		valid;
	int			pid;
	Oid			datid;
	uint32		wait_event_info;
	int64		queryid;
	TimestampTz start_ts;
} DbblueWSTrackedProc;

typedef struct
{
	int			pid;			/* 0 if !dbblue_wait_sampling_profile_pid */
	Oid			datid;
	uint32		wait_event_info;
	int64		queryid;		/* 0 if profile_queries == NONE */
} DbblueWSProfileKey;

typedef struct
{
	DbblueWSProfileKey key;
	uint64		count;
} DbblueWSProfileItem;

static DbblueWSTrackedProc *tracked = NULL;
static int	tracked_count = 0;
static HTAB *profile_hash = NULL;
static MemoryContext waitsampler_cxt = NULL;

static bool schema_ready = false;
static uint32 waitsampler_wait_event = 0;

static void ensure_schema(void);
static void probe_waits(void);
static void flush_history(void);
static void flush_profile(void);
static void run_retention(void);
static HTAB *make_profile_hash(void);

/*
 * Register the dbblue wait sampler as a static background worker.  Called
 * directly from PostmasterMain(), the same way ApplyLauncherRegister()
 * registers the logical replication launcher -- this is core functionality,
 * not something an extension's _PG_init() has to opt into.
 */
void
WaitSamplerRegister(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;

	/*
	 * Use ConsistentState rather than RecoveryFinished so this also starts
	 * on hot standbys (which never reach RecoveryFinished's PM_RUN state).
	 * The RecoveryInProgress() check in WaitSamplerMain()'s loop is what
	 * then keeps a standby's copy from ever attempting to write.
	 */
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 5;
	snprintf(worker.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "WaitSamplerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "dbblue wait sampler");
	snprintf(worker.bgw_type, BGW_MAXLEN, "dbblue wait sampler");
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&worker);
}

/*
 * Main entry point for the dbblue wait sampler background worker.
 */
void
WaitSamplerMain(Datum main_arg)
{
	TimestampTz now,
				next_sample,
				next_flush,
				next_profile,
				next_retention;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(dbblue_wait_sampling_database, NULL, 0);

	waitsampler_cxt = AllocSetContextCreate(TopMemoryContext,
											 "dbblue wait sampler",
											 ALLOCSET_DEFAULT_SIZES);

	profile_hash = make_profile_hash();

	ereport(LOG, (errmsg("dbblue wait sampler started")));

	now = GetCurrentTimestamp();
	next_sample = now;
	next_flush = now;
	next_profile = now;
	next_retention = now;

	for (;;)
	{
		long		sleep_ms;

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		now = GetCurrentTimestamp();

		if (dbblue_wait_sampling_enabled && !RecoveryInProgress())
		{
			if (!schema_ready)
			{
				ensure_schema();
				schema_ready = true;
				now = GetCurrentTimestamp();
				next_sample = now;
				next_flush = now + dbblue_wait_sampling_flush_interval * INT64CONST(1000);
				next_profile = now + dbblue_wait_sampling_profile_period * INT64CONST(1000);
				next_retention = now + DBBLUE_WS_RETENTION_SWEEP_MS * INT64CONST(1000);
			}

			if (now >= next_sample)
			{
				probe_waits();
				next_sample = now + dbblue_wait_sampling_period * INT64CONST(1000);
			}

			if (now >= next_flush)
			{
				flush_history();
				next_flush = now + dbblue_wait_sampling_flush_interval * INT64CONST(1000);
			}

			if (now >= next_profile)
			{
				flush_profile();
				next_profile = now + dbblue_wait_sampling_profile_period * INT64CONST(1000);
			}

			if (now >= next_retention)
			{
				run_retention();
				next_retention = now + DBBLUE_WS_RETENTION_SWEEP_MS * INT64CONST(1000);
			}

			sleep_ms = dbblue_wait_sampling_period;
		}
		else
		{
			/*
			 * Disabled (or a standby): nothing to do, just idle.  Discard
			 * whatever we were tracking so re-enabling starts from a clean
			 * slate instead of resurrecting stale intervals/counters from
			 * before the feature was turned off.
			 */
			schema_ready = false;

			if (tracked != NULL)
			{
				int			j;

				for (j = 0; j < tracked_count; j++)
					tracked[j].valid = false;
			}
			if (profile_hash != NULL && hash_get_num_entries(profile_hash) > 0)
			{
				hash_destroy(profile_hash);
				profile_hash = make_profile_hash();
			}

			sleep_ms = 5000;
		}

		if (waitsampler_wait_event == 0)
			waitsampler_wait_event = WaitEventExtensionNew("DbblueWaitSamplerMain");

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 sleep_ms,
						 waitsampler_wait_event);
		ResetLatch(MyLatch);
	}
}

/*
 * Create the dbblue_catalog schema and its two tables if they don't
 * already exist.  Only ever called once dbblue_wait_sampling_enabled has
 * been observed to be on, so nothing is created while the feature is off.
 */
static void
ensure_schema(void)
{
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "dbblue wait sampler: ensuring schema");

	if (SPI_execute("CREATE SCHEMA IF NOT EXISTS dbblue_catalog", false, 0) != SPI_OK_UTILITY)
		elog(FATAL, "dbblue wait sampler: failed to create schema dbblue_catalog");

	if (SPI_execute("CREATE TABLE IF NOT EXISTS dbblue_catalog.wait_history ("
					"pid integer NOT NULL, "
					"datid oid NOT NULL, "
					"wait_event_type text, "
					"wait_event text, "
					"queryid bigint, "
					"start_ts timestamptz NOT NULL, "
					"end_ts timestamptz NOT NULL, "
					"CONSTRAINT wait_history_pid_start_ts_key UNIQUE (pid, start_ts))",
					false, 0) != SPI_OK_UTILITY)
		elog(FATAL, "dbblue wait sampler: failed to create table wait_history");

	if (SPI_execute("CREATE INDEX IF NOT EXISTS wait_history_start_ts_idx "
					"ON dbblue_catalog.wait_history (start_ts)",
					false, 0) != SPI_OK_UTILITY)
		elog(FATAL, "dbblue wait sampler: failed to create index on wait_history");

	if (SPI_execute("CREATE TABLE IF NOT EXISTS dbblue_catalog.wait_profile ("
					"snapshot_ts timestamptz NOT NULL, "
					"window_start timestamptz NOT NULL, "
					"pid integer NOT NULL, "
					"datid oid NOT NULL, "
					"wait_event_type text, "
					"wait_event text, "
					"queryid bigint, "
					"count bigint NOT NULL)",
					false, 0) != SPI_OK_UTILITY)
		elog(FATAL, "dbblue wait sampler: failed to create table wait_profile");

	if (SPI_execute("CREATE INDEX IF NOT EXISTS wait_profile_snapshot_ts_idx "
					"ON dbblue_catalog.wait_profile (snapshot_ts)",
					false, 0) != SPI_OK_UTILITY)
		elog(FATAL, "dbblue wait sampler: failed to create index on wait_profile");

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * Build (or rebuild, if profile_pid/profile_queries changed) the in-memory
 * profile hash table.
 */
static HTAB *
make_profile_hash(void)
{
	HASHCTL		hash_ctl;

	hash_ctl.keysize = sizeof(DbblueWSProfileKey);
	hash_ctl.entrysize = sizeof(DbblueWSProfileItem);
	hash_ctl.hcxt = waitsampler_cxt;
	return hash_create("dbblue wait sampler profile", 1024, &hash_ctl,
						HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Sample every backend's current wait event.  Cheap: shared-memory only,
 * no I/O.  Detects transitions (writing a fresh start_ts) and bumps the
 * in-memory profile counters.
 */
static void
probe_waits(void)
{
	int			i;
	TimestampTz now = GetCurrentTimestamp();

	if (tracked == NULL)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(waitsampler_cxt);

		tracked_count = ProcGlobal->allProcCount;
		tracked = palloc0_array(DbblueWSTrackedProc, tracked_count);
		MemoryContextSwitchTo(oldcxt);
	}

	LWLockAcquire(ProcArrayLock, LW_SHARED);
	for (i = 0; i < ProcGlobal->allProcCount; i++)
	{
		PGPROC	   *proc = &ProcGlobal->allProcs[i];
		int			pid = proc->pid;
		uint32		wait_event_info;
		int64		queryid = 0;
		Oid			datid;
		DbblueWSProfileKey key;
		DbblueWSProfileItem *profile_item;
		bool		found;

		if (pid == 0 || proc->procLatch.owner_pid == 0)
		{
			/*
			 * Slot isn't occupied by a live backend right now.  Close out
			 * any interval we were tracking for it, otherwise flush_history()
			 * would keep upserting a stale row's end_ts forever for a
			 * backend that's long gone.
			 */
			if (i < tracked_count)
				tracked[i].valid = false;
			continue;
		}

		if (pid == MyProcPid)
			continue;

		wait_event_info = UINT32_ACCESS_ONCE(proc->wait_event_info);

		if (wait_event_info == 0 && !dbblue_wait_sampling_sample_cpu)
			continue;

		datid = proc->databaseId;

		if (dbblue_wait_sampling_profile_queries != DBBLUE_WS_PROFILE_QUERIES_NONE)
		{
			PgBackendStatus *beentry = pgstat_get_beentry_by_proc_number(i);

			if (beentry)
				queryid = beentry->st_query_id;
		}

		/* History: detect a transition, or a slot that changed owner */
		if (i >= tracked_count)
			continue;			/* MaxBackends can't shrink at runtime, but be defensive */

		if (!tracked[i].valid || tracked[i].pid != pid ||
			tracked[i].wait_event_info != wait_event_info ||
			tracked[i].queryid != queryid)
		{
			tracked[i].valid = true;
			tracked[i].pid = pid;
			tracked[i].datid = datid;
			tracked[i].wait_event_info = wait_event_info;
			tracked[i].queryid = queryid;
			tracked[i].start_ts = now;
		}

		/* Profile: bump the bucket for this sample */
		memset(&key, 0, sizeof(key));
		key.pid = dbblue_wait_sampling_profile_pid ? pid : 0;
		key.datid = datid;
		key.wait_event_info = wait_event_info;
		key.queryid = queryid;

		profile_item = (DbblueWSProfileItem *) hash_search(profile_hash, &key, HASH_ENTER, &found);
		if (found)
			profile_item->count++;
		else
			profile_item->count = 1;
	}
	LWLockRelease(ProcArrayLock);
}

/*
 * Upsert every currently-tracked wait interval into wait_history, keyed by
 * (pid, start_ts).  Intervals that are still open get their end_ts pushed
 * forward every flush; intervals that already ended (the proc moved on to
 * a different wait, or exited) simply stop being touched again.
 */
static void
flush_history(void)
{
	static const char *sql =
		"INSERT INTO dbblue_catalog.wait_history "
		"(pid, datid, wait_event_type, wait_event, queryid, start_ts, end_ts) "
		"VALUES ($1,$2,$3,$4,$5,$6,$7) "
		"ON CONFLICT (pid, start_ts) DO UPDATE SET "
		"end_ts = EXCLUDED.end_ts, "
		"wait_event_type = EXCLUDED.wait_event_type, "
		"wait_event = EXCLUDED.wait_event, "
		"queryid = EXCLUDED.queryid";
	static const Oid argtypes[7] = {INT4OID, OIDOID, TEXTOID, TEXTOID, INT8OID, TIMESTAMPTZOID, TIMESTAMPTZOID};
	Datum		values[7];
	char		nulls[7];
	int			i;
	TimestampTz now;
	bool		any = false;

	for (i = 0; i < tracked_count; i++)
		if (tracked[i].valid)
		{
			any = true;
			break;
		}
	if (!any)
		return;

	now = GetCurrentTimestamp();

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	for (i = 0; i < tracked_count; i++)
	{
		const char *event_type,
				   *event;

		if (!tracked[i].valid)
			continue;

		event_type = pgstat_get_wait_event_type(tracked[i].wait_event_info);
		event = pgstat_get_wait_event(tracked[i].wait_event_info);

		memset(nulls, ' ', sizeof(nulls));
		values[0] = Int32GetDatum(tracked[i].pid);
		values[1] = ObjectIdGetDatum(tracked[i].datid);
		if (event_type)
			values[2] = CStringGetTextDatum(event_type);
		else
			nulls[2] = 'n';
		if (event)
			values[3] = CStringGetTextDatum(event);
		else
			nulls[3] = 'n';
		if (tracked[i].queryid != 0)
			values[4] = Int64GetDatum(tracked[i].queryid);
		else
			nulls[4] = 'n';
		values[5] = TimestampTzGetDatum(tracked[i].start_ts);
		values[6] = TimestampTzGetDatum(now);

		if (SPI_execute_with_args(sql, 7, (Oid *) argtypes, values, nulls, false, 0) != SPI_OK_INSERT)
			elog(WARNING, "dbblue wait sampler: failed to upsert wait_history row for pid %d",
				 tracked[i].pid);
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
}

/*
 * Flush the in-memory profile counters as one snapshot into wait_profile,
 * then reset them for the next window.
 */
static void
flush_profile(void)
{
	static const char *sql =
		"INSERT INTO dbblue_catalog.wait_profile "
		"(snapshot_ts, window_start, pid, datid, wait_event_type, wait_event, queryid, count) "
		"VALUES ($1,$2,$3,$4,$5,$6,$7,$8)";
	static const Oid argtypes[8] = {TIMESTAMPTZOID, TIMESTAMPTZOID, INT4OID, OIDOID, TEXTOID, TEXTOID, INT8OID, INT8OID};
	Datum		values[8];
	char		nulls[8];
	HASH_SEQ_STATUS scan;
	DbblueWSProfileItem *item;
	TimestampTz now;
	TimestampTz window_start = GetCurrentTimestamp() -
	((int64) dbblue_wait_sampling_profile_period * INT64CONST(1000));

	if (hash_get_num_entries(profile_hash) == 0)
		return;

	now = GetCurrentTimestamp();

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	hash_seq_init(&scan, profile_hash);
	while ((item = (DbblueWSProfileItem *) hash_seq_search(&scan)) != NULL)
	{
		const char *event_type = pgstat_get_wait_event_type(item->key.wait_event_info);
		const char *event = pgstat_get_wait_event(item->key.wait_event_info);

		memset(nulls, ' ', sizeof(nulls));
		values[0] = TimestampTzGetDatum(now);
		values[1] = TimestampTzGetDatum(window_start);
		values[2] = Int32GetDatum(item->key.pid);
		values[3] = ObjectIdGetDatum(item->key.datid);
		if (event_type)
			values[4] = CStringGetTextDatum(event_type);
		else
			nulls[4] = 'n';
		if (event)
			values[5] = CStringGetTextDatum(event);
		else
			nulls[5] = 'n';
		if (item->key.queryid != 0)
			values[6] = Int64GetDatum(item->key.queryid);
		else
			nulls[6] = 'n';
		values[7] = Int64GetDatum((int64) item->count);

		if (SPI_execute_with_args(sql, 8, (Oid *) argtypes, values, nulls, false, 0) != SPI_OK_INSERT)
			elog(WARNING, "dbblue wait sampler: failed to insert wait_profile row");
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	/* Reset for the next window */
	hash_destroy(profile_hash);
	profile_hash = make_profile_hash();
}

/*
 * Drop wait_history / wait_profile rows older than
 * dbblue_wait_sampling_history_retention.
 */
static void
run_retention(void)
{
	Oid			argtype = INT4OID;
	Datum		value = Int32GetDatum(dbblue_wait_sampling_history_retention);

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_execute_with_args("DELETE FROM dbblue_catalog.wait_history "
							  "WHERE end_ts < (now() - make_interval(mins => $1))",
							  1, &argtype, &value, NULL, false, 0) != SPI_OK_DELETE)
		elog(WARNING, "dbblue wait sampler: failed to delete old wait_history rows");

	if (SPI_execute_with_args("DELETE FROM dbblue_catalog.wait_profile "
							  "WHERE snapshot_ts < (now() - make_interval(mins => $1))",
							  1, &argtype, &value, NULL, false, 0) != SPI_OK_DELETE)
		elog(WARNING, "dbblue wait sampler: failed to delete old wait_profile rows");

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
}
