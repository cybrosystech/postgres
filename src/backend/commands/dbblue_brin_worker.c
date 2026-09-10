/*-------------------------------------------------------------------------
 *
 * dbblue_brin_worker.c
 *    Auto-create BRIN indexes on create_date columns, per database.
 *
 *    Odoo gives every model a create_date column that is written in
 *    insertion order and never updated, so on an append-only table its
 *    physical correlation stays near 1.0 and a BRIN index -- a few pages
 *    against a btree's hundreds of megabytes -- serves the date-range
 *    filters Odoo's list views and reports issue constantly.  This scans
 *    pg_stats for create_date columns whose correlation is above 0.9 and
 *    creates a BRIN index on each table that does not already have one.
 *
 *    The feature is enabled per database.  dbblue_create_brin is
 *    PGC_SUSET, so each Odoo database carries its own value:
 *
 *        ALTER DATABASE odoo_1 SET dbblue_create_brin = on;
 *        ALTER DATABASE odoo_2 SET dbblue_create_brin = off;
 *
 *    Turning it off for odoo_2 leaves odoo_1 scanning, which an
 *    ALTER SYSTEM cannot do -- that value is one cluster-wide setting, so
 *    the last database provisioned would decide the feature for all of
 *    them.  The cluster-wide value remains the default for any database
 *    with no setting of its own, exactly as GUC precedence implies.
 *
 *    Two kinds of process implement that, mirroring the autovacuum
 *    launcher/worker split.  A launcher, always running, holds *no*
 *    database connection: all it does is list the cluster's databases out
 *    of the shared catalog pg_database, and start one short-lived dynamic
 *    worker in each in turn.  Each worker connects, and because
 *    InitPostgres has applied that database's own ALTER DATABASE settings
 *    by then, its dbblue_create_brin is already the effective value for
 *    that database -- so the worker itself decides whether to scan, and a
 *    disabled database costs one immediate exit.
 *
 *    Letting the worker decide is what keeps the semantics honest.  The
 *    launcher cannot read the per-database settings itself: a process with
 *    no database gets only the handful of shared catalogs nailed into the
 *    relcache by RelationCacheInitializePhase2(), and pg_db_role_setting
 *    is not one of them.  Connecting the launcher to some control database
 *    to read it would work, but that database's own ALTER DATABASE value
 *    would then override the launcher's view of the cluster-wide default,
 *    and the control database would be pinned against DROP DATABASE.
 *    Asking each database in turn instead gets full GUC precedence for
 *    free and pins nothing.
 *
 *    Workers run one at a time, so the feature costs one worker slot no
 *    matter how many databases exist, and nothing is held open between
 *    cycles.  Probing every database each cycle is the same thing the
 *    autovacuum launcher does, at a lower rate than autovacuum's default.
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

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
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

/* Time between two launcher passes over the database list. */
#define BRIN_LAUNCHER_INTERVAL		300000	/* 5 minutes in ms */

/* Shorter nap while nothing can be done at all (e.g. during recovery). */
#define BRIN_LAUNCHER_IDLE_INTERVAL	60000	/* 1 minute in ms */

/* How long to keep polling a worker that the postmaster has not started. */
#define BRIN_WORKER_STARTUP_TIMEOUT	60000	/* 1 minute in ms */

/* Poll granularity while waiting for a scan worker to finish. */
#define BRIN_WORKER_POLL_INTERVAL	1000	/* 1 second in ms */

/*
 * One database the launcher will start a worker in this cycle.  The name is
 * carried alongside the OID purely for log messages; the worker is addressed
 * by OID, which stays correct even if the database is renamed mid-cycle.
 */
typedef struct BrinDatabase
{
	Oid			dboid;
	char	   *dbname;
} BrinDatabase;

/*
 * Process-lifetime memory context: holds the launcher's database list, or the
 * worker's candidate list, for the duration of one cycle.  Reset at the end of
 * every cycle and by the error recovery path.
 */
static MemoryContext brin_worker_cxt = NULL;

static List *brin_get_database_list(void);
static void brin_scan_one_database(const BrinDatabase *db);
static void brin_wait_for_worker(BackgroundWorkerHandle *handle,
								 const BrinDatabase *db);
static void run_brin_scan(const char *dbname);

/*
 * dbblue_check_brin_database
 *		GUC check hook for the obsolete dbblue_brin_database.
 *
 * The setting no longer selects anything: which databases are scanned is now
 * decided per database by dbblue_create_brin.  It is retained only so that a
 * postgresql.conf carried over from the single-database implementation still
 * parses -- an unrecognized parameter there is a startup FATAL -- and it is
 * deliberately ignored rather than honoured as a restriction, because its old
 * boot_val was "odoo": treating a leftover value as "scan only this database"
 * would silently ignore the per-database settings on odoo_1, odoo_2, ... and
 * look exactly like the feature being broken.
 *
 * Warn instead, so a stale value is visible at the moment it is set rather
 * than never.  Not rejected: erroring out would turn an obsolete line in a
 * config file into a failure to start.
 */
bool
dbblue_check_brin_database(char **newval, void **extra, GucSource source)
{
	if (*newval == NULL || **newval == '\0')
		return true;

	ereport(WARNING,
			(errmsg("dbblue_brin_database is obsolete and is ignored"),
			 errdetail("Automatic BRIN index creation is now enabled per database."),
			 errhint("Use ALTER DATABASE %s SET dbblue_create_brin = on instead, and remove dbblue_brin_database.",
					 *newval)));

	return true;
}

/*
 * brin_get_database_list
 *		List the databases worth starting a probe worker in.
 *
 * pg_database is one of the shared catalogs nailed into the relcache before a
 * database is selected, so a connectionless launcher can read it; this
 * follows get_database_list() in autovacuum.c.  Whether the feature is
 * actually on is not decided here -- see the file header -- so this is only
 * the list of databases that can be connected to at all.
 *
 * The result is allocated in the caller's context so it survives the commit.
 */
static List *
brin_get_database_list(void)
{
	List	   *result = NIL;
	Relation	dbrel;
	TableScanDesc scan;
	HeapTuple	tup;
	MemoryContext resultcxt = CurrentMemoryContext;

	StartTransactionCommand();

	dbrel = table_open(DatabaseRelationId, AccessShareLock);
	scan = table_beginscan_catalog(dbrel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);
		BrinDatabase *db;
		MemoryContext oldcxt;

		/* A half-dropped database cannot be connected to at all. */
		if (database_is_invalid_form(pgdatabase))
			continue;

		/*
		 * Templates and databases marked as rejecting connections are skipped
		 * whatever they are set to: template contents are copied rather than
		 * queried, connecting to a template blocks CREATE DATABASE from it,
		 * and datallowconn = false is an explicit instruction that background
		 * work has no business overriding.
		 */
		if (pgdatabase->datistemplate || !pgdatabase->datallowconn)
			continue;

		/*
		 * Allocate the result outside the transaction's context, inside the
		 * loop, so the leaky scan machinery is not run in a long-lived one.
		 */
		oldcxt = MemoryContextSwitchTo(resultcxt);
		db = palloc_object(BrinDatabase);
		db->dboid = pgdatabase->oid;
		db->dbname = pstrdup(NameStr(pgdatabase->datname));
		result = lappend(result, db);
		MemoryContextSwitchTo(oldcxt);
	}

	table_endscan(scan);
	table_close(dbrel, AccessShareLock);

	CommitTransactionCommand();

	/* CommitTransactionCommand() leaves us in TopMemoryContext. */
	MemoryContextSwitchTo(resultcxt);

	return result;
}

/*
 * brin_wait_for_worker
 *		Block until one scan worker has finished.
 *
 * Waiting rather than firing off every database at once is what bounds the
 * feature to a single worker slot: with dozens of Odoo databases enabled,
 * launching them in parallel would exhaust max_worker_processes and starve
 * everything else that needs a slot.
 *
 * WaitForBackgroundWorkerShutdown() is not used because it only returns once
 * the worker is gone, which on SIGTERM would hold shutdown for the rest of a
 * scan; here a shutdown request terminates the worker instead.  The startup
 * timeout covers the case where the postmaster never starts the worker at
 * all, so the launcher cannot get stuck on one database forever.
 */
static void
brin_wait_for_worker(BackgroundWorkerHandle *handle, const BrinDatabase *db)
{
	TimestampTz startup_deadline;
	bool		started = false;
	bool		terminated = false;

	startup_deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
												   BRIN_WORKER_STARTUP_TIMEOUT);

	for (;;)
	{
		BgwHandleStatus status;
		pid_t		pid;

		CHECK_FOR_INTERRUPTS();

		status = GetBackgroundWorkerPid(handle, &pid);

		if (status == BGWH_STOPPED || status == BGWH_POSTMASTER_DIED)
			break;

		if (status == BGWH_STARTED)
			started = true;

		if (ShutdownRequestPending && !terminated)
		{
			TerminateBackgroundWorker(handle);
			terminated = true;
		}

		if (!started && !terminated &&
			GetCurrentTimestamp() >= startup_deadline)
		{
			ereport(WARNING,
					(errmsg("dbblue BRIN: worker for database \"%s\" did not start",
							db->dbname),
					 errdetail("Giving up on it for this cycle."),
					 errhint("Check max_worker_processes.")));
			TerminateBackgroundWorker(handle);
			terminated = true;
		}

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 BRIN_WORKER_POLL_INTERVAL,
						 WAIT_EVENT_DBBLUE_BRIN_LAUNCHER_MAIN);
		ResetLatch(MyLatch);
	}
}

/*
 * brin_scan_one_database
 *		Start a one-shot worker in the given database and wait for it.
 *
 * The worker decides for itself whether the feature is on there, so this is
 * called for every database, not only the enabled ones.
 *
 * The worker is addressed by OID rather than by name, so a rename between
 * building the list and starting the worker cannot send it to the wrong
 * database; if the database was dropped meanwhile the worker fails to
 * connect, which BGW_NEVER_RESTART turns into a single logged failure rather
 * than a restart loop.
 */
static void
brin_scan_one_database(const BrinDatabase *db)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;

	/* One scan and done: a failed cycle is retried by the next one. */
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "DBBlueBrinWorkerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "dbblue BRIN worker (%s)", db->dbname);
	snprintf(worker.bgw_type, BGW_MAXLEN, "dbblue BRIN worker");
	worker.bgw_main_arg = ObjectIdGetDatum(db->dboid);

	/*
	 * The name comes along for log messages only.  Every index this worker
	 * creates is reported, and without the database name those reports cannot
	 * be told apart across databases -- the whole point of the feature being
	 * per-database.  The worker cannot look the name up for itself between
	 * transactions, since resolving an OID needs a syscache lookup.
	 */
	strlcpy(worker.bgw_extra, db->dbname, BGW_EXTRALEN);

	/* So our latch is set when it starts and stops. */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		ereport(WARNING,
				(errmsg("dbblue BRIN: no free background worker slot for database \"%s\"",
						db->dbname),
				 errdetail("The database will be retried on the next cycle."),
				 errhint("Consider raising max_worker_processes.")));
		return;
	}

	brin_wait_for_worker(handle, db);

	pfree(handle);
}

/*
 * Run one scan cycle: find eligible create_date columns and create BRIN
 * indexes.  Each index creation runs in its own transaction to avoid
 * holding locks across the whole cycle.
 */
static void
run_brin_scan(const char *dbname)
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
		 * Stop between tables on shutdown, so a long cycle does not delay
		 * shutdown.  A config reload is not checked here: this worker's
		 * setting came from its database and cannot change under it, and the
		 * launcher re-reads the settings before every cycle anyway.
		 */
		if (ShutdownRequestPending)
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

		/*
		 * Does a BRIN index on create_date already exist for this table?
		 *
		 * Asked of the catalogs rather than by pattern-matching pg_indexes.
		 * indexdef -- the obvious "indexdef ILIKE '%%brin%%create_date%%'" --
		 * is the whole CREATE INDEX text, schema, table and index names
		 * included, so any table whose *name* contains "brin" and which has
		 * any create_date index matches it and is skipped forever without a
		 * BRIN index ever being created.  A table called
		 * brin_create_date_log needs no index of its own to trip that: its
		 * plain primary key definition already contains both words.
		 *
		 * The index relation's own attribute carries the indexed column's
		 * name, so joining it against pg_am asks exactly the intended
		 * question: is there a BRIN index on this table, over create_date?
		 */
		check_sql = psprintf(
			"SELECT 1 "
			"FROM pg_class t "
			"JOIN pg_namespace n ON n.oid = t.relnamespace "
			"JOIN pg_index i ON i.indrelid = t.oid "
			"JOIN pg_class ic ON ic.oid = i.indexrelid "
			"JOIN pg_am am ON am.oid = ic.relam "
			"JOIN pg_attribute ia ON ia.attrelid = i.indexrelid "
			"  AND ia.attnum > 0 "
			"WHERE n.nspname = %s AND t.relname = %s "
			"  AND am.amname = 'brin' "
			"  AND ia.attname = 'create_date' "
			"LIMIT 1",
			quote_literal_cstr(schemaname),
			quote_literal_cstr(tablename));

		check_ret = SPI_execute(check_sql, true, 1);

		if (check_ret != SPI_OK_SELECT)
		{
			/* Query failed — explicit error logging */
			ereport(WARNING,
					(errmsg("dbblue BRIN: failed to check existing indexes for %s.%s in database \"%s\" (SPI ret=%d)",
							schemaname, tablename, dbname, check_ret)));
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
					(errmsg("dbblue BRIN: creating index on %s.%s in database \"%s\"",
							schemaname, tablename, dbname)));

			ret = SPI_execute(create_sql, false, 0);

			if (ret == SPI_OK_UTILITY)
				ereport(LOG,
						(errmsg("dbblue BRIN: created index %s on %s.%s in database \"%s\"",
								indexname, schemaname, tablename, dbname)));
			else
				ereport(WARNING,
						(errmsg("dbblue BRIN: failed to create index on %s.%s in database \"%s\" (ret=%d)",
								schemaname, tablename, dbname, ret)));
		}
		else
		{
			/* Index exists — skip */
			ereport(DEBUG1,
					(errmsg("dbblue BRIN: index already exists for %s.%s in database \"%s\", skipping",
							schemaname, tablename, dbname)));
		}

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
}

/*
 * DBBlueBrinWorkerMain
 *		Per-database scan worker: connect to the database whose OID the
 *		launcher passed, run one scan, exit.
 *
 * The launcher passes every database it could connect to, not only the
 * enabled ones, because it cannot read the per-database settings itself; the
 * check below is what makes this per-database, and it costs a disabled
 * database one connect-and-exit per cycle.
 *
 * Being short-lived is the point.  Nothing is held open between cycles, so
 * DROP DATABASE on an Odoo database is never blocked by this feature, and
 * there is no long-lived connection whose GUCs could drift away from the
 * database's own settings.
 */
void
DBBlueBrinWorkerMain(Datum main_arg)
{
	Oid			dboid = DatumGetObjectId(main_arg);
	const char *dbname = MyBgworkerEntry->bgw_extra;
	sigjmp_buf	local_sigjmp_buf;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	/*
	 * By OID, not by name: the launcher resolved the name a moment ago and a
	 * rename in between must not redirect this worker somewhere else.
	 * InitPostgres applies the database's own ALTER DATABASE settings, so
	 * dbblue_create_brin below is this database's value.
	 */
	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);

	brin_worker_cxt = AllocSetContextCreate(TopMemoryContext,
											"dbblue BRIN worker",
											ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(brin_worker_cxt);

	/*
	 * One shot, so an unexpected error ends this worker rather than being
	 * recovered from: the launcher starts a fresh one next cycle.  Report it
	 * and unwind the transaction and SPI state first, so nothing is left
	 * half-open at exit.
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

		pgstat_report_activity(STATE_IDLE, NULL);

		RESUME_INTERRUPTS();

		proc_exit(1);
	}
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * This is where the feature is actually switched on or off.  InitPostgres
	 * has applied this database's ALTER DATABASE settings over the
	 * cluster-wide value, so dbblue_create_brin now holds the effective value
	 * for this database and nothing further needs resolving: a database that
	 * has been turned off costs this one immediate exit.
	 */
	if (!dbblue_create_brin)
	{
		ereport(DEBUG1,
				(errmsg("dbblue BRIN: dbblue_create_brin is off in database \"%s\", nothing to do",
						dbname)));
		proc_exit(0);
	}

	/* A standby cannot create indexes; it will scan if it is promoted. */
	if (RecoveryInProgress())
		proc_exit(0);

	if (!ShutdownRequestPending)
		run_brin_scan(dbname);

	proc_exit(0);
}

/*
 * DBBlueBrinLauncherMain
 *		Main entry point for the launcher process.
 */
void
DBBlueBrinLauncherMain(Datum main_arg)
{
	sigjmp_buf	local_sigjmp_buf;

	/* volatile: assigned in the error handler below and read after it. */
	volatile TimestampTz next_run = 0;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	/*
	 * No database.  pg_database and pg_db_role_setting are shared catalogs,
	 * so this is enough to see every database's setting, and it means the
	 * launcher never counts as a connection to any of them -- DROP DATABASE
	 * on an enabled database is not blocked by the feature being on.
	 */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	ereport(LOG, (errmsg("dbblue BRIN launcher started")));

	brin_worker_cxt = AllocSetContextCreate(TopMemoryContext,
											"dbblue BRIN launcher",
											ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(brin_worker_cxt);

	/*
	 * Recover here after any unexpected error: report it, unwind whatever
	 * transaction state was left behind, and go back to the main loop.
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

		pgstat_report_activity(STATE_IDLE, NULL);

		/*
		 * Defer the next pass.  The error aborted this one part-way through,
		 * so next_run still holds whatever it did before -- typically 0, on
		 * the very first pass -- and jumping straight back into the loop
		 * would retry immediately and spin on a persistent failure.
		 */
		next_run = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
											   BRIN_LAUNCHER_IDLE_INTERVAL);

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

			/*
			 * The cluster-wide dbblue_create_brin is the default for every
			 * database without a setting of its own, so a reload can change
			 * which databases are enabled: re-evaluate at once rather than
			 * at the end of the current interval.  This also gives an
			 * ALTER DATABASE ... SET a way to take effect immediately --
			 * that statement sends no signal of its own, so a provisioning
			 * step that wants its change picked up now can follow it with
			 * pg_reload_conf().
			 */
			next_run = 0;
		}

		/*
		 * A standby cannot create indexes, so its copy of the launcher stays
		 * idle however the feature is set; it will start scanning if this
		 * server is ever promoted.
		 */
		if (RecoveryInProgress())
		{
			next_run = 0;
			sleep_ms = BRIN_LAUNCHER_IDLE_INTERVAL;
		}
		else
		{
			/*
			 * Paced by next_run rather than by having woken up: the scan
			 * workers set our latch as they start and stop, and without this
			 * the last one of a cycle would trigger an immediate extra pass.
			 */
			if (next_run == 0 || GetCurrentTimestamp() >= next_run)
			{
				List	   *databases;
				ListCell   *lc;

				/*
				 * Re-read every cycle, so a database created since the last
				 * pass is picked up and one dropped since simply drops out
				 * of the list.  An ALTER DATABASE ... SET dbblue_create_brin
				 * needs nothing more than this either: the value is read by
				 * the worker, at connect time, on every pass.
				 */
				databases = brin_get_database_list();

				foreach(lc, databases)
				{
					if (ShutdownRequestPending)
						break;

					brin_scan_one_database((BrinDatabase *) lfirst(lc));
				}

				/* Discard this cycle's database list. */
				MemoryContextSwitchTo(TopMemoryContext);
				MemoryContextReset(brin_worker_cxt);
				MemoryContextSwitchTo(brin_worker_cxt);

				next_run = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
													   BRIN_LAUNCHER_INTERVAL);
			}

			sleep_ms = (long) ((next_run - GetCurrentTimestamp()) / 1000);
			sleep_ms = Max(sleep_ms, 1000);
		}

		if (ShutdownRequestPending)
			break;

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 sleep_ms,
						 WAIT_EVENT_DBBLUE_BRIN_LAUNCHER_MAIN);
		ResetLatch(MyLatch);
	}

	ereport(LOG, (errmsg("dbblue BRIN launcher shutting down")));

	/*
	 * Exit non-zero on SIGTERM, matching the other dbblue launchers: exiting
	 * 0 is treated by the postmaster as "terminate and forget", which would
	 * keep a pg_terminate_backend() of the launcher from coming back until
	 * the next server restart even while databases still have the feature on.
	 */
	proc_exit(1);
}

/*
 * Register the launcher at postmaster startup.
 *
 * Always registered, because dbblue_create_brin is now a per-database
 * setting: no cluster-wide value read at startup can tell whether some
 * database will have it on, and ALTER DATABASE must take effect without a
 * restart.  While no database has it on the launcher only sleeps, and it
 * holds no database connection either way.
 */
void
DBBlueBrinLauncherRegister(void)
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
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "DBBlueBrinLauncherMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "dbblue BRIN launcher");
	snprintf(worker.bgw_type, BGW_MAXLEN, "dbblue BRIN launcher");
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&worker);
}
