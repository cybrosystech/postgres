/*-------------------------------------------------------------------------
 *
 * pg_auto_index.c
 *    dbblue auto-index detector: a background worker that periodically
 *    scans the target database for unused and duplicate indexes and
 *    records its findings in a table named dbblue_unused_index_recommendations.
 *
 *    Detection only. No index is ever dropped by this feature.( in future we are planning to implement this auto index management feature )
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pg_auto_index.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"


/* ----------------------------------------------------------------
 * GUC variable definitions
 * ---------------------------------------------------------------- */
bool  dbblue_auto_index_enabled = false;
int   dbblue_auto_index_retention_days = 30;
int   dbblue_auto_index_check_interval = 1440;
bool  dbblue_auto_index_skip_on_standby = true;
char *dbblue_auto_index_database = NULL;


/*
 * Snapshot of the database name the worker connected to at startup.
 * If dbblue_auto_index_database changes at runtime the worker exits
 * with code 1 so the postmaster restarts it against the new target.
 */
static char *connected_database = NULL;


/* ----------------------------------------------------------------
 * Ensure the output table exists.
 *
 * Kept intentionally simple: BIGSERIAL id, one row per finding,
 * fresh snapshot written each scan (see dbblue_auto_index_run_scan).
 * ---------------------------------------------------------------- */
static void
dbblue_auto_index_ensure_table(void)
{
	int ret;

	ret = SPI_execute(
		"CREATE TABLE IF NOT EXISTS dbblue_unused_index_recommendations ("
		"    id                  BIGSERIAL    PRIMARY KEY,"
		"    finding_type        TEXT         NOT NULL,"
		"    schema_name         TEXT         NOT NULL,"
		"    table_name          TEXT         NOT NULL,"
		"    index_name          TEXT         NOT NULL,"
		"    duplicate_of        TEXT,"
		"    index_size_bytes    BIGINT,"
		"    index_scans         BIGINT,"
		"    stats_reset_at      TIMESTAMPTZ,"
		"    retention_days_used INT,"
		"    detected_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),"
		"    reason              TEXT         NOT NULL"
		")",
		false, 0);
	if (ret < 0)
		elog(WARNING, "dbblue_auto_index: failed to create recommendations table: %d", ret);

	CommandCounterIncrement();
}


/* ----------------------------------------------------------------
 * Clear previous findings. Each scan writes a fresh snapshot so
 * the table always reflects the current recommendation set.
 * ---------------------------------------------------------------- */
static void
dbblue_auto_index_truncate(void)
{
	int ret;

	ret = SPI_execute("TRUNCATE dbblue_unused_index_recommendations RESTART IDENTITY",
					  false, 0);
	if (ret < 0)
		elog(WARNING, "dbblue_auto_index: failed to truncate recommendations table: %d", ret);
}


/* ----------------------------------------------------------------
 * Detect unused indexes.
 *
 * An index is flagged when ALL of:
 *   - pg_stat_user_indexes.idx_scan = 0
 *   - index is not unique, primary, exclusion, or replica identity
 *   - index does not back a constraint (pg_constraint.conindid)
 *   - its schema is not pg_catalog/information_schema/pg_toast
 *   - either retention_days = 0 (test-mode bypass), or the effective
 *     "stats history start" is older than retention_days ago
 *
 * The retention gate prevents false positives right after a stats
 * reset: if the user sets retention_days = 50 and stats were reset
 * 10 days ago, we have only 10 days of data and cannot yet claim
 * an index is unused.
 *
 * Stats history start = COALESCE(pg_stat_database.stats_reset,
 *                                pg_postmaster_start_time()).
 * In PG 15+ stats_reset is NULL until someone explicitly calls
 * pg_stat_reset*(), so on most production DBs it is NULL even
 * though counters have been accumulating since server start.
 * Falling back to postmaster_start_time is a safe lower bound.
 * ---------------------------------------------------------------- */
static void
dbblue_auto_index_detect_unused(void)
{
	StringInfoData sql;
	int ret;

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO dbblue_unused_index_recommendations "
		"    (finding_type, schema_name, table_name, index_name,"
		"     index_size_bytes, index_scans, stats_reset_at,"
		"     retention_days_used, reason) "
		"SELECT 'unused',"
		"       n.nspname,"
		"       c.relname,"
		"       ic.relname,"
		"       pg_relation_size(s.indexrelid),"
		"       s.idx_scan,"
		"       COALESCE(d.stats_reset, pg_postmaster_start_time()),"
		"       %d,"
		"       format('Zero scans since %%s (>= %d days ago). "
		"Index is not unique/primary/exclusion and does not back a constraint.', "
		"              COALESCE(d.stats_reset, pg_postmaster_start_time())) "
		"  FROM pg_stat_user_indexes s"
		"  JOIN pg_index idx ON idx.indexrelid = s.indexrelid"
		"  JOIN pg_class ic  ON ic.oid = s.indexrelid"
		"  JOIN pg_class c   ON c.oid  = s.relid"
		"  JOIN pg_namespace n ON n.oid = c.relnamespace"
		"  CROSS JOIN ("
		"     SELECT stats_reset FROM pg_stat_database"
		"      WHERE datname = current_database()"
		"  ) d"
		" WHERE s.idx_scan = 0"
		"   AND NOT idx.indisunique"
		"   AND NOT idx.indisprimary"
		"   AND NOT idx.indisexclusion"
		"   AND NOT idx.indisreplident"
		"   AND NOT EXISTS ("
		"        SELECT 1 FROM pg_constraint k WHERE k.conindid = s.indexrelid"
		"   )"
		"   AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')"
		"   AND (%d = 0 OR "
		"        COALESCE(d.stats_reset, pg_postmaster_start_time())"
		"          < now() - make_interval(days => %d))",
		dbblue_auto_index_retention_days,
		dbblue_auto_index_retention_days,
		dbblue_auto_index_retention_days,
		dbblue_auto_index_retention_days);

	ret = SPI_execute(sql.data, false, 0);
	if (ret < 0)
		elog(WARNING, "dbblue_auto_index: unused-index detection failed: %d", ret);
	else
		elog(LOG, "dbblue_auto_index: flagged %lu unused index(es)",
			 (unsigned long) SPI_processed);

	pfree(sql.data);
}


/* ----------------------------------------------------------------
 * Detect exact-duplicate indexes.
 *
 * Two indexes on the same table are duplicates when their indkey,
 * indclass, indoption, partial predicate and expression text all
 * match. The "candidate" (index to consider dropping) is picked
 * deterministically:
 *   - a non-unique index loses to a unique one
 *   - otherwise the higher OID (usually the younger index) loses
 * ---------------------------------------------------------------- */
static void
dbblue_auto_index_detect_duplicates(void)
{
	const char *sql =
		"WITH ix AS ("
		"  SELECT i.indexrelid, i.indrelid, i.indkey, i.indclass, i.indoption,"
		"         COALESCE(pg_get_expr(i.indpred,  i.indrelid), '') AS pred,"
		"         COALESCE(pg_get_expr(i.indexprs, i.indrelid), '') AS exprs,"
		"         i.indisunique"
		"    FROM pg_index i"
		"    JOIN pg_class c ON c.oid = i.indrelid"
		"    JOIN pg_namespace n ON n.oid = c.relnamespace"
		"   WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')"
		"), pairs AS ("
		"  SELECT a.indrelid,"
		"         CASE"
		"           WHEN a.indisunique AND NOT b.indisunique THEN b.indexrelid"
		"           WHEN b.indisunique AND NOT a.indisunique THEN a.indexrelid"
		"           ELSE GREATEST(a.indexrelid, b.indexrelid)"
		"         END AS drop_candidate,"
		"         CASE"
		"           WHEN a.indisunique AND NOT b.indisunique THEN a.indexrelid"
		"           WHEN b.indisunique AND NOT a.indisunique THEN b.indexrelid"
		"           ELSE LEAST(a.indexrelid, b.indexrelid)"
		"         END AS keep_candidate"
		"    FROM ix a JOIN ix b"
		"      ON a.indrelid  = b.indrelid"
		"     AND a.indexrelid < b.indexrelid"
		"     AND a.indkey    = b.indkey"
		"     AND a.indclass  = b.indclass"
		"     AND a.indoption = b.indoption"
		"     AND a.pred      = b.pred"
		"     AND a.exprs     = b.exprs"
		") "
		"INSERT INTO dbblue_unused_index_recommendations "
		"    (finding_type, schema_name, table_name, index_name,"
		"     duplicate_of, index_size_bytes, reason) "
		"SELECT 'duplicate',"
		"       n.nspname,"
		"       tc.relname,"
		"       dc.relname,"
		"       kc.relname,"
		"       pg_relation_size(p.drop_candidate),"
		"       format('Exact duplicate of index %I.%I on table %I.%I.', "
		"              n.nspname, kc.relname, n.nspname, tc.relname) "
		"  FROM pairs p"
		"  JOIN pg_class tc ON tc.oid = p.indrelid"
		"  JOIN pg_namespace n ON n.oid = tc.relnamespace"
		"  JOIN pg_class dc ON dc.oid = p.drop_candidate"
		"  JOIN pg_class kc ON kc.oid = p.keep_candidate";

	int ret = SPI_execute(sql, false, 0);

	if (ret < 0)
		elog(WARNING, "dbblue_auto_index: duplicate-index detection failed: %d", ret);
	else
		elog(LOG, "dbblue_auto_index: flagged %lu duplicate index pair(s)",
			 (unsigned long) SPI_processed);
}


/* ----------------------------------------------------------------
 * Drop the recommendations table when the feature is disabled.
 *
 * Called from the worker loop whenever dbblue_auto_index_enabled is
 * false. We probe pg_class first so that turning the feature off
 * (or starting up with it off) is a silent no-op when the table is
 * already absent — avoiding a NOTICE per wake-up.
 *
 * Note: this drops historical findings. That is the documented
 * trade-off of tying table lifecycle to the GUC.
 * ---------------------------------------------------------------- */
static void
dbblue_auto_index_drop_table(void)
{
	int ret;
	bool exists;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(WARNING, "dbblue_auto_index: SPI_connect failed (drop)");
		PopActiveSnapshot();
		AbortCurrentTransaction();
		return;
	}

	ret = SPI_execute(
		"SELECT 1 FROM pg_class c"
		"  JOIN pg_namespace n ON n.oid = c.relnamespace"
		" WHERE c.relname = 'dbblue_unused_index_recommendations'"
		"   AND c.relkind = 'r'"
		"   AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')",
		true, 1);

	exists = (ret == SPI_OK_SELECT && SPI_processed > 0);

	if (exists)
	{
		ret = SPI_execute(
			"DROP TABLE dbblue_unused_index_recommendations CASCADE",
			false, 0);
		if (ret < 0)
			elog(WARNING, "dbblue_auto_index: failed to drop recommendations table: %d", ret);
		else
			elog(LOG, "dbblue_auto_index: dropped recommendations table (feature disabled)");
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
}


/* ----------------------------------------------------------------
 * One scan cycle: connect SPI, ensure table, write fresh snapshot.
 * ---------------------------------------------------------------- */
static void
dbblue_auto_index_run_scan(void)
{
	if (dbblue_auto_index_skip_on_standby && RecoveryInProgress())
	{
		elog(LOG, "dbblue_auto_index: skipping scan — server is in recovery (standby)");
		return;
	}

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(WARNING, "dbblue_auto_index: SPI_connect failed");
		PopActiveSnapshot();
		AbortCurrentTransaction();
		return;
	}

	dbblue_auto_index_ensure_table();
	dbblue_auto_index_truncate();
	dbblue_auto_index_detect_unused();
	dbblue_auto_index_detect_duplicates();

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
}


/* ----------------------------------------------------------------
 * Background worker main loop.
 * ---------------------------------------------------------------- */
void
DbblueAutoIndexMain(Datum main_arg)
{
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP,  SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	/*
	 * Record which DB we connect to so we can detect runtime changes to
	 * dbblue_auto_index_database and exit(1) to be restarted against the
	 * new target.
	 */
	connected_database = MemoryContextStrdup(TopMemoryContext,
											 dbblue_auto_index_database);

	BackgroundWorkerInitializeConnection(connected_database, NULL, 0);

	elog(LOG, "dbblue_auto_index: worker started on database \"%s\" (interval=%d min, retention=%d days)",
		 connected_database,
		 dbblue_auto_index_check_interval,
		 dbblue_auto_index_retention_days);

	/* Run once immediately so users see output without waiting a full interval. */
	if (dbblue_auto_index_enabled)
		dbblue_auto_index_run_scan();
	else
		dbblue_auto_index_drop_table();

	while (!ShutdownRequestPending)
	{
		long delay_ms;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);

			if (strcmp(connected_database, dbblue_auto_index_database) != 0)
			{
				elog(LOG, "dbblue_auto_index: target database changed from \"%s\" to \"%s\"; restarting",
					 connected_database, dbblue_auto_index_database);
				proc_exit(1);
			}
		}

		delay_ms = (long) dbblue_auto_index_check_interval * 60 * 1000;

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 delay_ms,
						 PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (ShutdownRequestPending)
			break;

		if (dbblue_auto_index_enabled)
			dbblue_auto_index_run_scan();
		else
			dbblue_auto_index_drop_table();
	}

	proc_exit(0);
}


/* ----------------------------------------------------------------
 * Static registration, called once from PostmasterMain.
 *
 * The worker is always registered; whether it performs work each
 * cycle is gated by dbblue_auto_index_enabled, which can be toggled
 * at runtime via SIGHUP without a restart.
 * ---------------------------------------------------------------- */
void
DbblueAutoIndexRegister(void)
{
	BackgroundWorker bgw;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
					BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_restart_time = 60;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "DbblueAutoIndexMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "dbblue auto index detector");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "dbblue auto index detector");

	RegisterBackgroundWorker(&bgw);
}
