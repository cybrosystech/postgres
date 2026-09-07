/*-------------------------------------------------------------------------
 *
 * dbblue_brin_worker.c
 *    Background worker: auto-create BRIN indexes on create_date columns.
 *
 *    Every BRIN_WORKER_INTERVAL ms, queries pg_stats in
 *    dbblue_brin_database for create_date columns with correlation > 0.9,
 *    and creates a BRIN index on each table that does not already have
 *    one.  Odoo gives every model a create_date column that is written in
 *    insertion order and never updated, so on an append-only table its
 *    physical correlation stays near 1.0 and a BRIN index -- a few pages
 *    against a btree's hundreds of megabytes -- serves the date-range
 *    filters Odoo's list views and reports issue constantly.
 *
 *    The worker is always registered, and is switched on and off at
 *    runtime by dbblue_create_brin (PGC_SIGHUP), so an operator can do
 *
 *        ALTER SYSTEM SET dbblue_create_brin = on;
 *        SELECT pg_reload_conf();
 *
 *    and have the next cycle start scanning without a server restart.
 *    While disabled the worker only sleeps, and it never attaches to
 *    dbblue_brin_database until the feature is first enabled -- an
 *    always-attached worker would block DROP DATABASE on it.
 *
 *    dbblue_brin_database is reloadable the same way.  A backend cannot
 *    re-point its database connection once InitPostgres has run, so on a
 *    change the worker exits non-zero and the postmaster restarts it
 *    (bgw_restart_time), whereupon it attaches to the new database.  From
 *    the operator's side an ALTER SYSTEM plus a reload is all it takes;
 *    the re-attach costs one worker restart a few seconds later.
 *
 * Copyright (c) 2026, Cybrosys Technologies
 *
 * IDENTIFICATION
 *    src/backend/commands/dbblue_brin_worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "access/xact.h"
#include "catalog/pg_database.h"
#include "commands/dbblue_brin_worker.h"
#include "executor/spi.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#define BRIN_WORKER_INTERVAL	300000	/* 5 minutes in ms */

/* How long to sleep between config checks while the feature is off. */
#define BRIN_WORKER_IDLE_INTERVAL	60000	/* 1 minute in ms */

/*
 * Worker-lifetime memory context, holding the candidate list of the cycle
 * currently running; reset at the end of every cycle and by the error
 * recovery path.
 */
static MemoryContext brin_worker_cxt = NULL;

/*
 * Set once BackgroundWorkerInitializeConnection has attached the worker to
 * dbblue_brin_database.  Deferred until the feature is first enabled, so a
 * cluster that never turns it on never attaches to that database.
 */
static bool worker_connected = false;

/*
 * The database this worker actually attached to, in TopMemoryContext.  Kept
 * so a later change to dbblue_brin_database can be detected: the connection
 * itself cannot be re-pointed, so the worker exits and is restarted against
 * the new value.
 */
static char *attached_database = NULL;

/* Throttles the "database is not configured" complaint to once per state. */
static bool warned_no_database = false;

static bool brin_connect_if_needed(void);
static void brin_check_database_change(void);
static void run_brin_scan(void);

/*
 * dbblue_check_create_brin
 *		GUC check hook for dbblue_create_brin.
 *
 * dbblue_brin_database is only consulted when the feature is switched on,
 * which is exactly when a misconfiguration would otherwise show up as
 * nothing happening at all.  Warn at that moment rather than rejecting the
 * setting -- an operator may legitimately be configuring this before the
 * target database exists -- following the convention of
 * dbblue_check_repack_enabled and dbblue_check_advisor_enabled.
 *
 * Only meaningful on an actual off->on transition in a backend that has
 * catalog access and settled GUC state; in the postmaster, or outside a
 * transaction, get_database_oid() cannot be called.
 */
bool
dbblue_check_create_brin(bool *newval, void **extra, GucSource source)
{
	if (!*newval || dbblue_create_brin)
		return true;

	if (!IsUnderPostmaster || !IsTransactionState())
		return true;

	if (dbblue_brin_database == NULL || dbblue_brin_database[0] == '\0')
		ereport(WARNING,
				(errmsg("dbblue_create_brin is on, but dbblue_brin_database is not set"),
				 errdetail("With no database configured the BRIN worker has nothing to scan."),
				 errhint("Set dbblue_brin_database to an existing database.")));
	else if (!OidIsValid(get_database_oid(dbblue_brin_database, true)))
		ereport(WARNING,
				(errmsg("dbblue BRIN worker database \"%s\" does not exist",
						dbblue_brin_database),
				 errdetail("The worker will fail to connect and will be retried every 5 seconds until the database exists."),
				 errhint("Create the database, or point dbblue_brin_database at an existing one.")));

	return true;
}

/*
 * dbblue_check_brin_database
 *		GUC check hook for dbblue_brin_database.
 *
 * Now that this is reloadable, pointing it at a database that does not
 * exist puts the worker into a connect-FATAL/restart loop rather than
 * simply doing nothing, so warn as soon as the name is set -- whether or
 * not dbblue_create_brin happens to be on at that moment.  Warning only
 * while the feature is enabled would miss the ordinary way this is
 * configured, which is to set the database first and switch the feature on
 * afterwards: neither step would say anything.
 *
 * The name is deliberately not rejected, for the same reason
 * dbblue_check_create_brin only warns.  A check hook cannot be authoritative
 * here: at server start it runs in the postmaster, which has no catalog
 * access, so a bad name in postgresql.conf is accepted regardless and
 * erroring out in a backend would only make the two paths disagree.  Nor
 * can the check stay true -- DROP DATABASE can invalidate an
 * already-accepted setting at any time -- which is why the worker treats a
 * missing database as a retryable condition at connect time rather than
 * trusting this.  Rejecting would also break the legitimate ordering of
 * configuring the setting before creating the database.
 */
bool
dbblue_check_brin_database(char **newval, void **extra, GucSource source)
{
	if (*newval == NULL || **newval == '\0')
		return true;

	/*
	 * get_database_oid() needs catalog access and a transaction: not
	 * available in the postmaster, nor while GUC state is still being set
	 * up in a starting backend.
	 */
	if (!IsUnderPostmaster || !IsTransactionState())
		return true;

	if (!OidIsValid(get_database_oid(*newval, true)))
	{
		if (dbblue_create_brin)
			ereport(WARNING,
					(errmsg("dbblue BRIN worker database \"%s\" does not exist", *newval),
					 errdetail("The worker will fail to connect and will be retried every 5 seconds until the database exists."),
					 errhint("Create the database, or point dbblue_brin_database at an existing one.")));
		else
			ereport(WARNING,
					(errmsg("dbblue BRIN worker database \"%s\" does not exist", *newval),
					 errdetail("Nothing happens while dbblue_create_brin is off, but the worker will fail to connect once it is switched on."),
					 errhint("Create the database, or point dbblue_brin_database at an existing one.")));
	}

	return true;
}

/*
 * brin_check_database_change
 *		Exit if dbblue_brin_database no longer names the database this
 *		worker is attached to.
 *
 * BackgroundWorkerInitializeConnection() can only be called once per
 * worker, so re-pointing the connection means starting a new worker.
 * Exiting non-zero is what asks the postmaster for that: the worker comes
 * back after bgw_restart_time and attaches to the new database.  Called
 * only right after a config reload, and only once attached.
 */
static void
brin_check_database_change(void)
{
	const char *newdb = dbblue_brin_database ? dbblue_brin_database : "";

	if (!worker_connected || attached_database == NULL)
		return;

	if (strcmp(attached_database, newdb) == 0)
		return;

	if (newdb[0] == '\0')
		ereport(LOG,
				(errmsg("dbblue BRIN worker stopping: dbblue_brin_database was unset"),
				 errdetail("The worker was attached to database \"%s\".",
						   attached_database)));
	else
		ereport(LOG,
				(errmsg("dbblue BRIN worker restarting to attach to database \"%s\"",
						newdb),
				 errdetail("It is attached to database \"%s\", which a running worker cannot change.",
						   attached_database)));

	/*
	 * Non-zero, so the postmaster restarts us rather than forgetting us;
	 * the new worker reads the current dbblue_brin_database at startup.
	 */
	proc_exit(1);
}

/*
 * Attach the worker to dbblue_brin_database, once, the first time the
 * feature is seen enabled.  Returns false if there is nothing to attach to,
 * in which case the caller just goes back to sleep.
 */
static bool
brin_connect_if_needed(void)
{
	if (worker_connected)
		return true;

	if (dbblue_brin_database == NULL || dbblue_brin_database[0] == '\0')
	{
		if (!warned_no_database)
		{
			ereport(WARNING,
					(errmsg("dbblue_create_brin is on, but dbblue_brin_database is not set"),
					 errhint("Set dbblue_brin_database and reload the configuration.")));
			warned_no_database = true;
		}

		return false;
	}

	BackgroundWorkerInitializeConnection(dbblue_brin_database, NULL, 0);
	worker_connected = true;

	/*
	 * Remember what we attached to; brin_check_database_change() compares
	 * against this after every reload.  TopMemoryContext, not the worker
	 * context, since that one is reset after every cycle.
	 */
	attached_database = MemoryContextStrdup(TopMemoryContext,
											dbblue_brin_database);

	ereport(LOG,
			(errmsg("dbblue BRIN worker monitoring database \"%s\"",
					dbblue_brin_database)));

	return true;
}

/*
 * Run one scan cycle: find eligible create_date columns and create BRIN
 * indexes.  Each index creation runs in its own transaction to avoid
 * holding locks across the whole cycle.
 */
static void
run_brin_scan(void)
{
	int			ret;
	uint64		i;
	/* Store results before committing the read transaction */
	char	  **schemas;
	char	  **tables;
	uint64		nrows;
	uint64		ncandidates = 0;
	MemoryContext oldctx;

	/* ---- Phase 1: read pg_stats ---- */
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		ereport(WARNING, (errmsg("dbblue BRIN: SPI_connect failed")));
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}

	ret = SPI_execute(
		"SELECT DISTINCT schemaname, tablename "
		"FROM pg_stats "
		"WHERE attname = 'create_date' "
		"  AND correlation > 0.9 "
		"  AND schemaname NOT IN ('pg_catalog', 'information_schema')",
		true, 0);

	if (ret != SPI_OK_SELECT)
	{
		ereport(WARNING, (errmsg("dbblue BRIN: pg_stats query failed (ret=%d)", ret)));
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}

	nrows = SPI_processed;

	/*
	 * Copy the results into the worker's own context so they survive
	 * SPI_finish() and the commit below.  Rows carrying a NULL name are
	 * dropped here, so phase 2 only ever walks the ncandidates entries
	 * that were actually filled in.
	 */
	oldctx = MemoryContextSwitchTo(brin_worker_cxt);
	schemas = palloc(nrows * sizeof(char *));
	tables = palloc(nrows * sizeof(char *));
	for (i = 0; i < nrows; i++)
	{
		char	   *schema_val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
		char	   *table_val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);

		if (schema_val == NULL || table_val == NULL)
		{
			ereport(WARNING, (errmsg("dbblue BRIN: NULL value in pg_stats result, skipping")));
			continue;
		}

		schemas[ncandidates] = pstrdup(schema_val);
		tables[ncandidates] = pstrdup(table_val);
		ncandidates++;
	}
	MemoryContextSwitchTo(oldctx);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	/* ---- Phase 2: create indexes, one transaction per table ---- */
	for (i = 0; i < ncandidates; i++)
	{
		char	   *schemaname = schemas[i];
		char	   *tablename = tables[i];
		char	   *indexname;
		char	   *check_sql;
		char	   *create_sql;
		int		    check_ret;

		/*
		 * Stop between tables on shutdown or a config reload, so a long
		 * cycle neither delays shutdown nor keeps creating indexes after
		 * the feature has been switched off.
		 */
		if (ShutdownRequestPending || ConfigReloadPending)
			break;

		CHECK_FOR_INTERRUPTS();

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		if (SPI_connect() != SPI_OK_CONNECT)
		{
			PopActiveSnapshot();
			CommitTransactionCommand();
			continue;
		}

		indexname = psprintf("%s_create_date_brin_idx", tablename);

		/* Check if a BRIN index on create_date already exists for this table */
		check_sql = psprintf(
			"SELECT 1 FROM pg_indexes "
			"WHERE schemaname = %s AND tablename = %s "
			"  AND indexdef ILIKE '%%brin%%create_date%%' "
			"LIMIT 1",
			quote_literal_cstr(schemaname),
			quote_literal_cstr(tablename));

		check_ret = SPI_execute(check_sql, true, 1);

		if (check_ret != SPI_OK_SELECT)
		{
			/* Query failed — explicit error logging */
			ereport(WARNING,
					(errmsg("dbblue BRIN: failed to check existing indexes for %s.%s (SPI ret=%d)",
							schemaname, tablename, check_ret)));
		}
		else if (SPI_processed == 0)
		{
			/* No BRIN index yet — create one */
			create_sql = psprintf(
				"CREATE INDEX IF NOT EXISTS %s ON %s.%s USING brin (create_date)",
				quote_identifier(indexname),
				quote_identifier(schemaname),
				quote_identifier(tablename));

			ereport(LOG,
					(errmsg("dbblue BRIN: creating index on %s.%s", schemaname, tablename)));

			ret = SPI_execute(create_sql, false, 0);

			if (ret == SPI_OK_UTILITY)
				ereport(LOG,
						(errmsg("dbblue BRIN: created index %s on %s.%s",
								indexname, schemaname, tablename)));
			else
				ereport(WARNING,
						(errmsg("dbblue BRIN: failed to create index on %s.%s (ret=%d)",
								schemaname, tablename, ret)));
		}
		else
		{
			/* Index exists — skip */
			ereport(DEBUG1,
					(errmsg("dbblue BRIN: index already exists for %s.%s, skipping",
							schemaname, tablename)));
		}

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
}

/*
 * Main entry point for the background worker.
 */
void
DBBlueBrinWorkerMain(Datum main_arg)
{
	sigjmp_buf	local_sigjmp_buf;
	TimestampTz next_run = 0;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	ereport(LOG, (errmsg("dbblue BRIN worker started")));

	brin_worker_cxt = AllocSetContextCreate(TopMemoryContext,
											"dbblue BRIN worker",
											ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(brin_worker_cxt);

	/*
	 * Recover here after any unexpected error: report it, unwind whatever
	 * transaction and SPI state was left behind, and go back to the main
	 * loop.  The wait at the end of the loop keeps a persistent failure
	 * from turning into a busy loop.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand. */
		error_context_stack = NULL;

		HOLD_INTERRUPTS();

		EmitErrorReport();
		FlushErrorState();

		AbortOutOfAnyTransaction();
		MemoryContextSwitchTo(TopMemoryContext);
		MemoryContextReset(brin_worker_cxt);
		MemoryContextSwitchTo(brin_worker_cxt);

		if (worker_connected)
			pgstat_report_activity(STATE_IDLE, NULL);

		RESUME_INTERRUPTS();
	}
	PG_exception_stack = &local_sigjmp_buf;

	for (;;)
	{
		long		sleep_ms;

		CHECK_FOR_INTERRUPTS();

		if (ShutdownRequestPending)
			break;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);

			/* May not return: exits so it can re-attach elsewhere. */
			brin_check_database_change();
		}

		/*
		 * A standby cannot create indexes, so its copy of the worker stays
		 * idle no matter how the feature is set; it will start scanning if
		 * this server is ever promoted.
		 */
		if (dbblue_create_brin && !RecoveryInProgress() &&
			brin_connect_if_needed())
		{
			if (next_run == 0 || GetCurrentTimestamp() >= next_run)
			{
				run_brin_scan();

				/*
				 * CommitTransactionCommand() leaves CurrentMemoryContext as
				 * TopMemoryContext, so re-establish the worker's own context
				 * before discarding the cycle's candidate list.
				 */
				MemoryContextSwitchTo(TopMemoryContext);
				MemoryContextReset(brin_worker_cxt);
				MemoryContextSwitchTo(brin_worker_cxt);

				next_run = GetCurrentTimestamp() +
					(int64) BRIN_WORKER_INTERVAL * 1000;
			}

			sleep_ms = (long) ((next_run - GetCurrentTimestamp()) / 1000);
			sleep_ms = Max(sleep_ms, 1000);
		}
		else
		{
			/*
			 * Disabled, a standby, or no database to attach to: scan as
			 * soon as that changes.
			 */
			next_run = 0;

			if (!dbblue_create_brin)
				warned_no_database = false;

			sleep_ms = BRIN_WORKER_IDLE_INTERVAL;
		}

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 sleep_ms,
						 WAIT_EVENT_DBBLUE_BRIN_WORKER_MAIN);
		ResetLatch(MyLatch);
	}

	ereport(LOG, (errmsg("dbblue BRIN worker shutting down")));

	/*
	 * Exit non-zero on SIGTERM, matching the other dbblue workers: exiting
	 * 0 is treated by the postmaster as "terminate and forget", which would
	 * keep a pg_terminate_backend() of this worker from coming back until
	 * the next server restart even while the feature stays enabled.
	 */
	proc_exit(1);
}

/*
 * Register the background worker at postmaster startup.
 *
 * Always registered, since dbblue_create_brin is PGC_SIGHUP context and
 * the feature must be switchable on without a restart; while disabled the
 * worker only sleeps and never attaches to a database.
 */
void
DBBlueBrinWorkerRegister(void)
{
	BackgroundWorker worker;

	/*
	 * Don't run during pg_upgrade: the postmaster is started internally, in
	 * a restricted mode, to restore schema objects in a precise sequence;
	 * this worker independently issuing its own DDL has no business
	 * happening during that window.
	 */
	if (IsBinaryUpgrade)
		return;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;

	/*
	 * ConsistentState rather than RecoveryFinished so this also starts on
	 * hot standbys (which never reach RecoveryFinished's PM_RUN state); the
	 * RecoveryInProgress() check in the main loop is what keeps a standby's
	 * copy from attempting to write.
	 */
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 5;
	snprintf(worker.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "DBBlueBrinWorkerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "dbblue BRIN worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "dbblue BRIN worker");
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&worker);
}
