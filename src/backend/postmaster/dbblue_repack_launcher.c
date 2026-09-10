/*-------------------------------------------------------------------------
 *
 * dbblue_repack_launcher.c
 *	  dbblue repack launcher background worker.
 *
 * A deliberately simple, "cron job" style scheduler for REPACK, not an
 * autovacuum-parity dynamic scanner: REPACK rewrites the whole table and
 * every index (up to 2x disk space, a brief AccessExclusiveLock at the
 * heap swap, a much bigger WAL spike than VACUUM), so a worker only ever
 * considers the fixed, operator-curated table list named by
 * dbblue_repack_tables, on a naptime of dbblue_repack_naptime, gated by a
 * physical bloat ratio (dbblue_repack_threshold) and a per-table cooldown
 * (dbblue_repack_min_interval).
 *
 * The bloat ratio is relpages against an estimated ideal page count
 * derived from pg_stat_user_tables.n_live_tup and the average row width
 * from pg_stats.  This catches physical file-size bloat that VACUUM
 * already reclaimed into free space but never shrank on disk -- unlike
 * autovacuum's dead-tuple ratio, which would never flag such a table.
 * Last-repack-time is persisted in public.dbblue_repack_history, keyed by
 * (schema_name, table_name) rather than relid, so cooldown tracking
 * survives a DROP+CREATE of the same logical table, not just a rename.
 *
 * REPACK cannot be run through SPI: ExecRepack() unconditionally calls
 * PreventInTransactionBlock(), which rejects any nested (isTopLevel =
 * false) execution -- the same restriction that blocks VACUUM from SPI.
 * The worker instead calls cluster_rel() directly, the same lower-level,
 * parsenode-free API vacuum.c uses for VACUUM FULL, processing each
 * configured table in its own transaction exactly the way REPACK's own
 * multi-relation path does.
 *
 * The feature is enabled per database.  dbblue_repack_enabled,
 * dbblue_repack_tables, dbblue_repack_threshold and
 * dbblue_repack_min_interval are all PGC_SUSET, so each database carries
 * its own values:
 *
 *     ALTER DATABASE odoo_1 SET dbblue_repack_enabled = on;
 *     ALTER DATABASE odoo_1 SET dbblue_repack_tables = 'public.sale_order';
 *     ALTER DATABASE odoo_2 SET dbblue_repack_enabled = off;
 *
 * dbblue_repack_database is the exception: it stays a plain cluster-wide
 * PGC_SIGHUP string and is deliberately not retired the way the analogous
 * dbblue_brin_database/dbblue_auto_index_suggestion_database GUCs were
 * when those features went per-database.  Left empty (the default), every
 * connectable database is considered, each gated by its own
 * dbblue_repack_enabled.  Set to a name, it restricts the whole feature to
 * that one database only, whatever any other database's
 * dbblue_repack_enabled says -- useful to pin REPACK, which is far more
 * expensive than a BRIN scan or an index suggestion pass, to a single
 * known database while leaving the per-database knobs in place for later.
 *
 * Two kinds of process implement that, mirroring the autovacuum
 * launcher/worker split (and this fork's own dbblue_brin_worker.c, the
 * template this file follows).  A launcher, always running, holds *no*
 * database connection: it lists the cluster's databases out of the shared
 * catalog pg_database (applying the dbblue_repack_database filter, if
 * any), and starts one short-lived dynamic worker in each in turn.  Each
 * worker connects, and because InitPostgres has applied that database's
 * own ALTER DATABASE settings by then, its dbblue_repack_enabled is
 * already the effective value for that database -- so the worker itself
 * decides whether to run, and a disabled database costs one immediate
 * exit.  The launcher cannot read the per-database settings itself:
 * pg_db_role_setting is not one of the shared catalogs available to a
 * connectionless process.
 *
 * Workers run one at a time, so the feature costs one worker slot no
 * matter how many databases have it enabled, and nothing is held open
 * between cycles -- DROP DATABASE on an enabled database is never blocked
 * by this feature.
 *
 * Copyright (c) 2026, dbblue / Cybrosys Technologies
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/dbblue_repack_launcher.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"
#include "commands/repack.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/dbblue_repack_launcher.h"
#include "postmaster/interrupt.h"
#include "storage/bufpage.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "utils/wait_event.h"

/* GUC variables, wired up via guc_parameters.dat */
bool		dbblue_repack_enabled = false;
char	   *dbblue_repack_tables = NULL;
int			dbblue_repack_naptime = 300;
int			dbblue_repack_min_interval = 86400;
double		dbblue_repack_threshold = 1.5;
char	   *dbblue_repack_database = NULL;

/*
 * Tables at or below this many pages are too small for the bloat estimate
 * to mean anything; treat them as never bloated rather than let
 * estimation noise fire on near-empty tables.  A hardcoded safety floor,
 * not a GUC -- the same role autovacuum's vac_base_thresh plays.
 */
#define DBBLUE_REPACK_MIN_RELPAGES			20

/* Used when a table has no columns in pg_stats yet (e.g. never analyzed). */
#define DBBLUE_REPACK_FALLBACK_AVG_ROW_WIDTH 100.0

/* Heuristic per-tuple overhead: heap tuple header + line pointer, MAXALIGN'd. */
#define DBBLUE_REPACK_TUPLE_OVERHEAD		28.0

/* How long to keep polling a worker that the postmaster has not started. */
#define DBBLUE_REPACK_WORKER_STARTUP_TIMEOUT	60000	/* 1 minute in ms */

/* Poll granularity while waiting for a per-database worker to finish. */
#define DBBLUE_REPACK_WORKER_POLL_INTERVAL	1000	/* 1 second in ms */

/*
 * Sleep between passes while nothing can be done at all (recovery) or right
 * after an unexpected error, independent of dbblue_repack_naptime -- that
 * GUC can legitimately be set very large, since REPACK is far more
 * expensive per table than a BRIN scan or an index-advisor pass, but a
 * standby promotion or a transient error should still be noticed quickly
 * rather than waiting out whatever cadence the operator picked for normal
 * cycles.  Mirrors BRIN_LAUNCHER_IDLE_INTERVAL in dbblue_brin_worker.c.
 */
#define DBBLUE_REPACK_IDLE_INTERVAL			60000	/* 1 minute in ms */

/* One entry from dbblue_repack_tables, before bloat is checked. */
typedef struct RepackTableSpec
{
	char	   *schema;
	char	   *table;
} RepackTableSpec;

/* One configured table plus what the bloat scan learned about it. */
typedef struct RepackCandidate
{
	char	   *schema;
	char	   *table;
	Oid			relid;			/* InvalidOid if the table does not exist */
	double		bloat_ratio;	/* 0.0 if too small to estimate meaningfully */
	TimestampTz last_repack_at; /* 0 if never repacked by this worker */
} RepackCandidate;

/*
 * One database the launcher will start a worker in this cycle.  The name is
 * carried alongside the OID purely for log messages; the worker is addressed
 * by OID, which stays correct even if the database is renamed mid-cycle.
 */
typedef struct RepackDatabase
{
	Oid			dboid;
	char	   *dbname;
} RepackDatabase;

/*
 * Process-lifetime memory context: holds the launcher's database list, or
 * the worker's specs/candidates, for the duration of one cycle.  Reset at
 * the end of every cycle and by the error recovery path.
 */
static MemoryContext launcher_cxt = NULL;

static List *repack_get_database_list(void);
static void repack_wait_for_worker(BackgroundWorkerHandle *handle,
									const RepackDatabase *db);
static void repack_scan_one_database(const RepackDatabase *db);
static bool ensure_schema(void);
static List *parse_configured_tables(void);
static List *load_bloat_candidates(List *specs);
static void run_repack_cycle(void);
static bool repack_one_table(const char *schema, const char *table,
							 Oid relid, double bloat_ratio);
static void record_repack_history(const char *schema, const char *table,
								  Oid relid, double bloat_ratio);

/*
 * dbblue_check_repack_database
 *		GUC check hook for dbblue_repack_database.
 *
 * Best-effort only: warn, don't reject, when the named database is not
 * currently something the launcher could ever pick.  The operator may be
 * preparing configuration ahead of creating the database, so this must
 * not block the SET itself -- it only makes a dead-on-arrival filter
 * visible immediately instead of as a silent, permanent no-op.
 */
bool
dbblue_check_repack_database(char **newval, void **extra, GucSource source)
{
	Oid			dboid;
	Form_pg_database form;
	HeapTuple	tuple;

	if (*newval == NULL || **newval == '\0')
		return true;

	if (!IsUnderPostmaster || !IsTransactionState())
		return true;

	dboid = get_database_oid(*newval, true);
	if (!OidIsValid(dboid))
	{
		ereport(WARNING,
				(errmsg("dbblue repack launcher database \"%s\" does not exist",
						*newval),
				 errdetail("No database will be repacked until a database by this name exists."),
				 errhint("Create the database, or clear dbblue_repack_database to consider every database instead.")));
		return true;
	}

	tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dboid));
	if (!HeapTupleIsValid(tuple))
		return true;

	form = (Form_pg_database) GETSTRUCT(tuple);
	if (form->datistemplate || !form->datallowconn)
		ereport(WARNING,
				(errmsg("dbblue repack launcher database \"%s\" cannot be connected to",
						*newval),
				 errdetail("It is a template database, or does not allow connections."),
				 errhint("No database will be repacked until this is corrected.")));

	ReleaseSysCache(tuple);

	return true;
}

/*
 * dbblue_check_repack_tables
 *		GUC check hook for dbblue_repack_tables.
 *
 * Syntax only: each comma-separated item must be a single unquoted
 * "schema.table" pair.  Resolution to an OID is deliberately deferred to
 * each cycle (parse_configured_tables/load_bloat_candidates), since a
 * table can be dropped or renamed between GUC-set-time and repack-time.
 */
bool
dbblue_check_repack_tables(char **newval, void **extra, GucSource source)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *lc;
	bool		ok = true;

	if (*newval == NULL || **newval == '\0')
		return true;

	rawstring = pstrdup(*newval);

	if (!SplitIdentifierString(rawstring, ',', &elemlist))
	{
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(elemlist);
		return false;
	}

	foreach(lc, elemlist)
	{
		char	   *item = (char *) lfirst(lc);
		char	   *dot = strchr(item, '.');

		if (dot == NULL || dot == item || dot[1] == '\0' ||
			strchr(dot + 1, '.') != NULL)
		{
			GUC_check_errdetail("\"%s\" is not a valid schema.table name.",
								item);
			ok = false;
			break;
		}
	}

	pfree(rawstring);
	list_free(elemlist);
	return ok;
}

/*
 * RepackLauncherRegister
 *		Register the dbblue repack launcher as a static background
 *		worker.  Called directly from PostmasterMain(), the same way the
 *		other dbblue workers are, since this is core functionality, not
 *		something an extension's _PG_init() has to opt into.
 *
 * Always registered, since dbblue_repack_enabled is a per-database
 * PGC_SUSET setting: no cluster-wide value read at startup can tell
 * whether some database will have it on, and ALTER DATABASE must take
 * effect without a restart.  While no database has it on the launcher
 * only sleeps, and it holds no database connection either way.
 */
void
RepackLauncherRegister(void)
{
	BackgroundWorker worker;

	/*
	 * Don't run during pg_upgrade: the postmaster is started internally,
	 * multiple times, in a restricted mode to restore schema objects in a
	 * precise sequence; this worker independently connecting and issuing
	 * its own DDL, let alone rewriting tables, has no business happening
	 * during that window.
	 */
	if (IsBinaryUpgrade)
		return;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;

	/*
	 * Use ConsistentState rather than RecoveryFinished so this also starts
	 * on hot standbys (which never reach RecoveryFinished's PM_RUN state).
	 * The RecoveryInProgress() check in RepackLauncherMain()'s loop is what
	 * then keeps a standby's copy from ever attempting to write.
	 */
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 5;
	snprintf(worker.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "RepackLauncherMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "dbblue repack launcher");
	snprintf(worker.bgw_type, BGW_MAXLEN, "dbblue repack launcher");
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&worker);
}

/*
 * repack_get_database_list
 *		List the databases the launcher should start a worker in this
 *		cycle.
 *
 * pg_database is one of the shared catalogs nailed into the relcache
 * before a database is selected, so a connectionless launcher can read
 * it; this follows get_database_list() in autovacuum.c and
 * brin_get_database_list() in dbblue_brin_worker.c.  Whether the feature
 * is actually on for a given database is not decided here -- that is up
 * to dbblue_repack_enabled, read by the worker after it connects -- this
 * is only the list of databases that can be considered at all, filtered
 * by dbblue_repack_database when that is set.
 *
 * The result is allocated in the caller's context so it survives the
 * commit below.
 */
static List *
repack_get_database_list(void)
{
	List	   *result = NIL;
	Relation	dbrel;
	TableScanDesc scan;
	HeapTuple	tup;
	MemoryContext resultcxt = CurrentMemoryContext;
	bool		restrict_to_one = (dbblue_repack_database != NULL &&
								   dbblue_repack_database[0] != '\0');

	StartTransactionCommand();

	dbrel = table_open(DatabaseRelationId, AccessShareLock);
	scan = table_beginscan_catalog(dbrel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);
		RepackDatabase *db;
		MemoryContext oldcxt;

		/* A half-dropped database cannot be connected to at all. */
		if (database_is_invalid_form(pgdatabase))
			continue;

		/*
		 * Templates and databases marked as rejecting connections are
		 * skipped whatever they are set to, for the same reasons
		 * brin_get_database_list() skips them.
		 */
		if (pgdatabase->datistemplate || !pgdatabase->datallowconn)
			continue;

		if (restrict_to_one &&
			strcmp(NameStr(pgdatabase->datname), dbblue_repack_database) != 0)
			continue;

		oldcxt = MemoryContextSwitchTo(resultcxt);
		db = palloc_object(RepackDatabase);
		db->dboid = pgdatabase->oid;
		db->dbname = pstrdup(NameStr(pgdatabase->datname));
		result = lappend(result, db);
		MemoryContextSwitchTo(oldcxt);

		/* Once the single named database is found, nothing else qualifies. */
		if (restrict_to_one)
			break;
	}

	table_endscan(scan);
	table_close(dbrel, AccessShareLock);

	CommitTransactionCommand();

	/* CommitTransactionCommand() leaves us in TopMemoryContext. */
	MemoryContextSwitchTo(resultcxt);

	return result;
}

/*
 * repack_wait_for_worker
 *		Block until one per-database repack worker has finished.
 *
 * Waiting rather than firing off every database at once is what bounds
 * the feature to a single worker slot: with many databases enabled,
 * launching them in parallel would exhaust max_worker_processes and
 * starve everything else that needs a slot.  Mirrors
 * brin_wait_for_worker() in dbblue_brin_worker.c.
 */
static void
repack_wait_for_worker(BackgroundWorkerHandle *handle, const RepackDatabase *db)
{
	TimestampTz startup_deadline;
	bool		started = false;
	bool		terminated = false;

	startup_deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
												   DBBLUE_REPACK_WORKER_STARTUP_TIMEOUT);

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
					(errmsg("dbblue repack launcher: worker for database \"%s\" did not start",
							db->dbname),
					 errdetail("Giving up on it for this cycle."),
					 errhint("Check max_worker_processes.")));
			TerminateBackgroundWorker(handle);
			terminated = true;
		}

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 DBBLUE_REPACK_WORKER_POLL_INTERVAL,
						 WAIT_EVENT_DBBLUE_REPACK_LAUNCHER_MAIN);
		ResetLatch(MyLatch);
	}
}

/*
 * repack_scan_one_database
 *		Start a one-shot worker in the given database and wait for it.
 *
 * The worker decides for itself whether the feature is on there, so this
 * is called for every database repack_get_database_list() returned, not
 * only ones known to be enabled.
 */
static void
repack_scan_one_database(const RepackDatabase *db)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;

	/* One cycle and done: a failed attempt is retried by the next cycle. */
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "DbblueRepackWorkerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "dbblue repack worker (%s)", db->dbname);
	snprintf(worker.bgw_type, BGW_MAXLEN, "dbblue repack worker");
	worker.bgw_main_arg = ObjectIdGetDatum(db->dboid);

	/* So our latch is set when it starts and stops. */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		ereport(WARNING,
				(errmsg("dbblue repack launcher: no free background worker slot for database \"%s\"",
						db->dbname),
				 errdetail("The database will be retried on the next cycle."),
				 errhint("Consider raising max_worker_processes.")));
		return;
	}

	repack_wait_for_worker(handle, db);

	pfree(handle);
}

/*
 * RepackLauncherMain
 *		Launcher entry point.  Holds no database connection; lists the
 *		databases to consider and starts one short-lived worker in each,
 *		one at a time.
 */
void
RepackLauncherMain(Datum main_arg)
{
	sigjmp_buf	local_sigjmp_buf;

	/* volatile: assigned in the error handler below and read after it. */
	volatile TimestampTz next_run = 0;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	/*
	 * No database.  pg_database and pg_db_role_setting are shared
	 * catalogs, so this is enough to see every database's setting once a
	 * worker connects, and it means the launcher never counts as a
	 * connection to any of them -- DROP DATABASE on an enabled database is
	 * not blocked by the feature being on.
	 */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	ereport(LOG, (errmsg("dbblue repack launcher started")));

	launcher_cxt = AllocSetContextCreate(TopMemoryContext,
										 "dbblue repack launcher",
										 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(launcher_cxt);

	/*
	 * Recover here after any unexpected error: report it, clean up
	 * whatever transaction state is left, and go back to the main loop.
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
		MemoryContextReset(launcher_cxt);
		MemoryContextSwitchTo(launcher_cxt);

		pgstat_report_activity(STATE_IDLE, NULL);

		/*
		 * Defer the next pass.  The error aborted this one part-way
		 * through, so next_run still holds whatever it did before --
		 * typically 0, on the very first pass -- and jumping straight
		 * back into the loop would retry immediately and spin on a
		 * persistent failure.  A fixed interval, not dbblue_repack_naptime:
		 * see DBBLUE_REPACK_IDLE_INTERVAL.
		 */
		next_run = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
											   DBBLUE_REPACK_IDLE_INTERVAL);

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
			 * dbblue_repack_database can only be changed by reload, and a
			 * reload can also mean an ALTER DATABASE ... SET
			 * dbblue_repack_enabled has just been followed by
			 * pg_reload_conf() to pick it up sooner: re-evaluate at once
			 * rather than at the end of the current interval.
			 */
			next_run = 0;
		}

		/*
		 * A standby cannot write, so its copy of the launcher stays idle
		 * however the feature is set; it will start repacking if this
		 * server is ever promoted.  Reset next_run so promotion is picked
		 * up on the very next wakeup rather than waiting out a stale
		 * interval.
		 */
		if (RecoveryInProgress())
		{
			next_run = 0;
			sleep_ms = DBBLUE_REPACK_IDLE_INTERVAL;
		}
		else
		{
			/*
			 * Paced by next_run rather than by having woken up: the
			 * per-database workers set our latch as they start and stop,
			 * and without this the last one of a cycle would trigger an
			 * immediate extra pass.
			 */
			if (next_run == 0 || GetCurrentTimestamp() >= next_run)
			{
				List	   *databases;
				ListCell   *lc;

				/*
				 * Re-read every cycle, so a database created since the
				 * last pass is picked up and one dropped since simply
				 * drops out of the list.  An ALTER DATABASE ... SET
				 * dbblue_repack_enabled needs nothing more than this
				 * either: the value is read by the worker, at connect
				 * time, on every pass.
				 */
				databases = repack_get_database_list();

				foreach(lc, databases)
				{
					if (ShutdownRequestPending)
						break;

					repack_scan_one_database((RepackDatabase *) lfirst(lc));
				}

				/* Discard this cycle's database list. */
				MemoryContextSwitchTo(TopMemoryContext);
				MemoryContextReset(launcher_cxt);
				MemoryContextSwitchTo(launcher_cxt);

				next_run = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
													   (int64) dbblue_repack_naptime * 1000);
			}

			sleep_ms = (long) ((next_run - GetCurrentTimestamp()) / 1000);
			sleep_ms = Max(sleep_ms, 1000);
		}

		if (ShutdownRequestPending)
			break;

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 sleep_ms,
						 WAIT_EVENT_DBBLUE_REPACK_LAUNCHER_MAIN);
		ResetLatch(MyLatch);
	}

	ereport(LOG, (errmsg("dbblue repack launcher shutting down")));

	/*
	 * Exit non-zero on SIGTERM, matching the other dbblue workers: exiting
	 * 0 is treated by the postmaster as "terminate and forget", which
	 * would keep pg_terminate_backend() from this worker coming back until
	 * the next server restart even while the feature stays enabled.
	 */
	proc_exit(1);
}

/*
 * DbblueRepackWorkerMain
 *		Per-database worker: connect to the database whose OID the
 *		launcher passed, run one cycle if the feature is on there, exit.
 *
 * The launcher starts this in every database it could connect to, not
 * only ones known to be enabled, because it cannot read the per-database
 * settings itself; the check below is what makes this per-database, and
 * it costs a disabled database one connect-and-exit per cycle.
 *
 * Being short-lived is the point: nothing is held open between cycles, so
 * DROP DATABASE on an enabled database is never blocked, and there is no
 * long-lived connection whose GUCs could drift away from the database's
 * own settings.
 */
void
DbblueRepackWorkerMain(Datum main_arg)
{
	Oid			dboid = DatumGetObjectId(main_arg);
	sigjmp_buf	local_sigjmp_buf;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	/*
	 * By OID, not by name: the launcher resolved the name a moment ago and
	 * a rename in between must not redirect this worker somewhere else.
	 * InitPostgres applies the database's own ALTER DATABASE settings, so
	 * dbblue_repack_enabled/tables/threshold/min_interval below are this
	 * database's values.
	 */
	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);

	launcher_cxt = AllocSetContextCreate(TopMemoryContext,
										 "dbblue repack worker",
										 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(launcher_cxt);

	/*
	 * One shot, so an unexpected error ends this worker rather than being
	 * recovered from: the launcher starts a fresh one next cycle.  Report
	 * it and unwind the transaction and SPI state first, so nothing is
	 * left half-open at exit.
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
		MemoryContextReset(launcher_cxt);

		pgstat_report_activity(STATE_IDLE, NULL);

		RESUME_INTERRUPTS();

		proc_exit(1);
	}
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Bound how long this worker will wait for a lock on a configured
	 * table.  Without this, a table stuck behind a long-running
	 * conflicting lock would stall every later table in this cycle
	 * indefinitely; on timeout the attempt errors, is caught per-table,
	 * and is simply retried next cycle.  Unrelated to, and unaffected by,
	 * the per-database GUCs InitPostgres just merged above.
	 */
	SetConfigOption("lock_timeout", "5s", PGC_SUSET, PGC_S_SESSION);

	/*
	 * This is where the feature is actually switched on or off.
	 * InitPostgres has applied this database's ALTER DATABASE settings
	 * over the cluster-wide value, so dbblue_repack_enabled now holds the
	 * effective value for this database and nothing further needs
	 * resolving: a database that has been turned off costs this one
	 * immediate exit.
	 */
	if (!dbblue_repack_enabled)
		proc_exit(0);

	/* A standby cannot write; it will repack if it is promoted. */
	if (RecoveryInProgress())
		proc_exit(0);

	if (!ShutdownRequestPending && ensure_schema())
		run_repack_cycle();

	proc_exit(0);
}

/*
 * ensure_schema
 *		Create dbblue_repack_history if it doesn't already exist in the
 *		current database.  Run fresh on every worker invocation -- there
 *		is nothing to cache across processes, since each worker is a
 *		fresh one-shot process with no state surviving between cycles --
 *		but cheap, being just a CREATE TABLE IF NOT EXISTS and a GRANT.
 */
static bool
ensure_schema(void)
{
	bool		table_ok;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue repack launcher: ensuring schema");

	table_ok = (SPI_execute(
							"CREATE TABLE IF NOT EXISTS public.dbblue_repack_history ("
							"  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
							"  schema_name text NOT NULL,"
							"  table_name text NOT NULL,"
							"  relid oid,"
							"  last_repack_at timestamptz NOT NULL,"
							"  last_bloat_ratio double precision,"
							"  repack_count bigint NOT NULL DEFAULT 1,"
							"  CONSTRAINT dbblue_repack_history_table_key UNIQUE (schema_name, table_name)"
							")",
							false, 0) == SPI_OK_UTILITY);

	if (table_ok)
	{
		if (SPI_execute("GRANT SELECT ON public.dbblue_repack_history TO PUBLIC",
						false, 0) != SPI_OK_UTILITY)
			ereport(WARNING,
					(errmsg("dbblue repack launcher: failed to grant select on dbblue_repack_history")));
	}
	else
		ereport(WARNING,
				(errmsg("dbblue repack launcher: failed to create table dbblue_repack_history")));

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	return table_ok;
}

/*
 * parse_configured_tables
 *		Split dbblue_repack_tables into schema/table pairs.  The returned
 *		list and its entries live in launcher_cxt.
 */
static List *
parse_configured_tables(void)
{
	char	   *rawstring;
	List	   *elemlist;
	List	   *result = NIL;
	ListCell   *lc;

	if (dbblue_repack_tables == NULL || dbblue_repack_tables[0] == '\0')
		return NIL;

	rawstring = pstrdup(dbblue_repack_tables);
	if (!SplitIdentifierString(rawstring, ',', &elemlist))
	{
		pfree(rawstring);
		list_free(elemlist);
		return NIL;
	}

	foreach(lc, elemlist)
	{
		char	   *item = (char *) lfirst(lc);
		char	   *dot = strchr(item, '.');
		char	   *schema;
		char	   *table;
		RepackTableSpec *spec;
		ListCell   *lc2;
		bool		duplicate = false;

		/* Malformed entries should have been rejected by the check hook. */
		if (dot == NULL || dot == item || dot[1] == '\0')
			continue;

		schema = pnstrdup(item, dot - item);
		table = pstrdup(dot + 1);

		/*
		 * A repeated entry (e.g. a copy-paste mistake in the GUC) would
		 * otherwise turn into two candidates for the same table sharing one
		 * stale last_repack_at snapshot, letting the second repack it again
		 * immediately after the first commits, bypassing the cooldown.
		 */
		foreach(lc2, result)
		{
			RepackTableSpec *seen = (RepackTableSpec *) lfirst(lc2);

			if (strcmp(seen->schema, schema) == 0 &&
				strcmp(seen->table, table) == 0)
			{
				duplicate = true;
				break;
			}
		}
		if (duplicate)
			continue;

		spec = (RepackTableSpec *) palloc(sizeof(RepackTableSpec));
		spec->schema = schema;
		spec->table = table;
		result = lappend(result, spec);
	}

	pfree(rawstring);
	list_free(elemlist);
	return result;
}

/*
 * load_bloat_candidates
 *		One read-only SPI pass computing a physical bloat ratio and last
 *		repack time for every configured table.  The returned list and
 *		its entries live in launcher_cxt.
 */
static List *
load_bloat_candidates(List *specs)
{
	List	   *candidates = NIL;
	int			n = list_length(specs);
	Datum	   *qualnames;
	ArrayType  *arr;
	Oid			argtypes[1] = {TEXTARRAYOID};
	Datum		values[1];
	int			i;
	int			ret;
	uint64		row;
	ListCell   *lc;

	if (n == 0)
		return NIL;

	qualnames = (Datum *) palloc(n * sizeof(Datum));
	i = 0;
	foreach(lc, specs)
	{
		RepackTableSpec *spec = (RepackTableSpec *) lfirst(lc);

		qualnames[i++] = CStringGetTextDatum(psprintf("%s.%s", spec->schema,
													  spec->table));
	}
	arr = construct_array_builtin(qualnames, n, TEXTOID);
	values[0] = PointerGetDatum(arr);

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue repack launcher: checking table bloat");

	ret = SPI_execute_with_args(
							   "WITH wanted AS ("
							   "  SELECT s AS qualname, split_part(s, '.', 1) AS nspname,"
							   "         split_part(s, '.', 2) AS relname"
							   "  FROM unnest($1::text[]) AS s"
							   "), resolved AS ("
							   "  SELECT w.qualname, w.nspname, w.relname,"
							   "         to_regclass(w.qualname)::oid AS relid"
							   "  FROM wanted w"
							   ")"
							   "SELECT r.qualname, r.relid, r.nspname, r.relname,"
							   /*
							    * pg_class.relpages is a cached statistic that
							    * only VACUUM/ANALYZE/REPACK refresh; it can
							    * lag far behind reality between those runs.
							    * pg_relation_size() reads the file's actual
							    * current block count directly, so freshly
							    * created bloat is visible immediately.
							    */
							   "       (pg_relation_size(r.relid) /"
							   "        current_setting('block_size')::bigint)::int AS relpages,"
							   "       COALESCE(t.n_live_tup, 0) AS n_live_tup,"
							   "       COALESCE((SELECT SUM(s.avg_width) FROM pg_stats s"
							   "                 WHERE s.schemaname = r.nspname"
							   "                   AND s.tablename = r.relname), 0) AS sum_width,"
							   "       h.last_repack_at"
							   " FROM resolved r"
							   " LEFT JOIN pg_stat_user_tables t ON t.relid = r.relid"
							   " LEFT JOIN public.dbblue_repack_history h"
							   "   ON h.schema_name = r.nspname AND h.table_name = r.relname",
							   1, argtypes, values, NULL, false, 0);

	if (ret != SPI_OK_SELECT)
		ereport(WARNING,
				(errmsg("dbblue repack launcher: bloat check query failed (SPI result %d)",
						ret)));
	else
	{
		for (row = 0; row < SPI_processed; row++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[row];
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			bool		isnull;
			Datum		d;
			Oid			relid;
			RepackCandidate *cand;
			MemoryContext oldcxt;

			d = SPI_getbinval(tuple, tupdesc, 2, &isnull);
			relid = isnull ? InvalidOid : DatumGetObjectId(d);

			oldcxt = MemoryContextSwitchTo(launcher_cxt);

			cand = (RepackCandidate *) palloc0(sizeof(RepackCandidate));
			cand->relid = relid;

			d = SPI_getbinval(tuple, tupdesc, 3, &isnull);
			cand->schema = isnull ? NULL : TextDatumGetCString(d);
			d = SPI_getbinval(tuple, tupdesc, 4, &isnull);
			cand->table = isnull ? NULL : TextDatumGetCString(d);

			if (cand->schema == NULL || cand->table == NULL)
			{
				MemoryContextSwitchTo(oldcxt);
				continue;
			}

			if (OidIsValid(relid))
			{
				int32		relpages;
				int64		n_live_tup;
				int64		sum_width;

				d = SPI_getbinval(tuple, tupdesc, 5, &isnull);
				relpages = isnull ? 0 : DatumGetInt32(d);
				d = SPI_getbinval(tuple, tupdesc, 6, &isnull);
				n_live_tup = isnull ? 0 : DatumGetInt64(d);
				d = SPI_getbinval(tuple, tupdesc, 7, &isnull);
				sum_width = isnull ? 0 : DatumGetInt64(d);
				d = SPI_getbinval(tuple, tupdesc, 8, &isnull);
				cand->last_repack_at = isnull ? 0 : DatumGetTimestampTz(d);

				if (relpages >= DBBLUE_REPACK_MIN_RELPAGES && n_live_tup > 0)
				{
					double		avg_row_width = (sum_width > 0) ?
						(double) sum_width : DBBLUE_REPACK_FALLBACK_AVG_ROW_WIDTH;
					double		usable_bytes = BLCKSZ - SizeOfPageHeaderData;
					double		rows_per_page = usable_bytes /
						(avg_row_width + DBBLUE_REPACK_TUPLE_OVERHEAD);
					int64		ideal_pages;

					if (rows_per_page < 1.0)
						rows_per_page = 1.0;

					ideal_pages = (int64) ceil((double) n_live_tup / rows_per_page);
					if (ideal_pages > 0)
						cand->bloat_ratio = (double) relpages / (double) ideal_pages;
				}
			}

			candidates = lappend(candidates, cand);
			MemoryContextSwitchTo(oldcxt);
		}
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	return candidates;
}

/*
 * run_repack_cycle
 *		One full pass: load the current bloat picture for every
 *		configured table, then repack whichever ones are both over
 *		threshold and past their cooldown.
 */
static void
run_repack_cycle(void)
{
	List	   *specs = parse_configured_tables();
	List	   *candidates;
	ListCell   *lc;

	if (specs == NIL)
		return;

	candidates = load_bloat_candidates(specs);

	foreach(lc, candidates)
	{
		RepackCandidate *cand = (RepackCandidate *) lfirst(lc);
		TimestampTz now;

		CHECK_FOR_INTERRUPTS();

		if (ShutdownRequestPending)
			break;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);

			/*
			 * If the feature was just switched off, stop rather than
			 * start repacking a table under a configuration that no
			 * longer wants this running at all.  The remaining tables in
			 * this cycle's candidate list are simply picked up again next
			 * time the feature is enabled.
			 */
			if (!dbblue_repack_enabled)
				break;
		}

		if (!OidIsValid(cand->relid))
		{
			ereport(WARNING,
					(errmsg("dbblue repack launcher: configured table \"%s.%s\" does not exist, skipping",
							cand->schema, cand->table)));
			continue;
		}

		if (cand->bloat_ratio < dbblue_repack_threshold)
			continue;

		now = GetCurrentTimestamp();
		if (cand->last_repack_at != 0 &&
			now < cand->last_repack_at +
			(int64) dbblue_repack_min_interval * USECS_PER_SEC)
			continue;

		(void) repack_one_table(cand->schema, cand->table, cand->relid,
								cand->bloat_ratio);
	}

	/* Free this cycle's specs/candidates before the next one. */
	MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextReset(launcher_cxt);
	MemoryContextSwitchTo(launcher_cxt);
}

/*
 * repack_one_table
 *		REPACK (CONCURRENTLY) one table, in its own transaction, mirroring
 *		the per-relation loop REPACK's own multi-relation path uses
 *		(repack.c) -- one transaction per table avoids holding locks on
 *		unrelated tables simultaneously and the deadlock risk that would
 *		bring.  A failure here is caught and logged; it does not abort the
 *		rest of the cycle or the worker.
 */
static bool
repack_one_table(const char *schema, const char *table, Oid relid,
				 double bloat_ratio)
{
	bool		success = false;

	/*
	 * Set once the repack has committed, so the error path below can tell a
	 * failed repack from a failed post-repack ANALYZE -- by then the repack
	 * is already durable and must not be reported as a failure.  volatile
	 * because it is written in the PG_TRY and read in the PG_CATCH.
	 */
	volatile bool repacked = false;

	StartTransactionCommand();

	PG_TRY();
	{
		Relation	rel;

		SetCurrentStatementStartTimestamp();
		pgstat_report_activity(STATE_RUNNING,
							   psprintf("dbblue repack launcher: repacking %s.%s",
										schema, table));

		/*
		 * ShareUpdateExclusiveLock, not AccessExclusiveLock: this is the
		 * lock level REPACK (CONCURRENTLY) itself uses for the bulk of its
		 * work (see RepackLockLevel() in repack.c), since we always
		 * request CLUOPT_CONCURRENT below.
		 */
		rel = try_relation_open(relid, ShareUpdateExclusiveLock);

		if (rel == NULL)
			ereport(LOG,
					(errmsg("dbblue repack launcher: skipping \"%s.%s\": relation no longer exists",
							schema, table)));
		else if (rel->rd_rel->relkind != RELKIND_RELATION &&
				 rel->rd_rel->relkind != RELKIND_MATVIEW)
		{
			relation_close(rel, ShareUpdateExclusiveLock);
			ereport(LOG,
					(errmsg("dbblue repack launcher: skipping \"%s.%s\": not a plain table or materialized view",
							schema, table)));
		}
		else
		{
			ClusterParams params = {0};
			VacuumParams analyze_params = {0};

			params.options = CLUOPT_CONCURRENT | CLUOPT_RECHECK;

			/*
			 * No separate precondition check here: cluster_rel() itself
			 * calls check_concurrent_repack_requirements() (repack.c) as
			 * the very first thing it does whenever CLUOPT_CONCURRENT is
			 * set -- before any real work -- and throws a descriptive
			 * ERROR for exactly the cases that would otherwise need
			 * reimplementing here (wal_level, catalog/TOAST relations,
			 * unlogged tables, unsupported replica identity, missing
			 * PK).  Reusing that check instead of duplicating it means
			 * this worker can never silently drift out of sync with
			 * REPACK's own rules; the ERROR is simply caught below and
			 * logged as a WARNING instead of a custom skip message.
			 */
			PushActiveSnapshot(GetTransactionSnapshot());
			cluster_rel(REPACK_COMMAND_REPACK, rel, InvalidOid, &params, true);
			/* cluster_rel closes rel, but keeps the lock until commit. */
			PopActiveSnapshot();

			record_repack_history(schema, table, relid, bloat_ratio);

			/*
			 * Commit before analysing.
			 *
			 * cluster_rel() escalated to AccessExclusiveLock to swap the
			 * relation files, and that lock is held until this transaction
			 * commits (see the comment above cluster_rel() in repack.c) --
			 * the ShareUpdateExclusiveLock we opened the relation with is
			 * long gone.  Analysing here would therefore keep every reader
			 * and writer of the table blocked for the whole ANALYZE, which
			 * defeats the point of asking for CLUOPT_CONCURRENT: the
			 * exclusive window is meant to be just the swap.
			 *
			 * So commit first and analyse in a fresh transaction, exactly
			 * as REPACK's own ANALYZE path does (repack.c).  The repack is
			 * durable at this point, which is why it is also reported here
			 * rather than after the ANALYZE.
			 */
			CommitTransactionCommand();
			repacked = true;

			ereport(LOG,
					(errmsg("dbblue repack launcher: repacked \"%s.%s\" (bloat ratio %.2f)",
							schema, table, bloat_ratio)));

			/*
			 * The rewrite invalidates the planner's statistics -- most
			 * notably attribute correlation, which changes completely
			 * once the table is physically reordered -- even though
			 * pg_class.relpages/reltuples were already fixed up by the
			 * rewrite itself.  Run a plain ANALYZE the same way VACUUM
			 * ANALYZE does, by calling analyze_rel() directly, so
			 * pg_statistic and pg_stat_user_tables reflect the repacked
			 * table immediately instead of waiting on the next autovacuum
			 * analyze.  analyze_rel() takes its own
			 * ShareUpdateExclusiveLock, which readers and writers do not
			 * block on.
			 *
			 * in_outer_xact is false because this ANALYZE owns its
			 * transaction, which lets vac_update_relstats() advance the
			 * frozen xid as a standalone ANALYZE would.
			 */
			StartTransactionCommand();
			SetCurrentStatementStartTimestamp();
			pgstat_report_activity(STATE_RUNNING,
								   psprintf("dbblue repack launcher: analyzing %s.%s",
											schema, table));

			analyze_params.options = VACOPT_ANALYZE;
			analyze_params.log_analyze_min_duration = -1;

			PushActiveSnapshot(GetTransactionSnapshot());
			analyze_rel(relid, NULL, &analyze_params, NIL, false, NULL);
			PopActiveSnapshot();

			success = true;
		}

		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		ErrorData  *edata = CopyErrorData();

		FlushErrorState();
		AbortOutOfAnyTransaction();
		MemoryContextSwitchTo(launcher_cxt);

		if (repacked)
			ereport(WARNING,
					(errmsg("dbblue repack launcher: post-repack ANALYZE of \"%s.%s\" failed: %s",
							schema, table, edata->message),
					 errdetail("The repack itself committed; statistics will be refreshed by the next autovacuum analyze.")));
		else
			ereport(WARNING,
					(errmsg("dbblue repack launcher: repacking \"%s.%s\" failed: %s",
							schema, table, edata->message)));
		FreeErrorData(edata);
		success = repacked;
	}
	PG_END_TRY();

	pgstat_report_activity(STATE_IDLE, NULL);
	return success;
}

/*
 * record_repack_history
 *		Upsert one table's repack outcome into dbblue_repack_history.
 *		Called from within repack_one_table's own transaction, right
 *		before it commits: a crash between the REPACK and this write
 *		simply means the next cycle re-detects the bloat and repacks
 *		again, with no partially-recorded state either way.
 */
static void
record_repack_history(const char *schema, const char *table, Oid relid,
					  double bloat_ratio)
{
	static const char *sql =
		"INSERT INTO public.dbblue_repack_history "
		"(schema_name, table_name, relid, last_repack_at, last_bloat_ratio, repack_count) "
		"VALUES ($1,$2,$3,now(),$4,1) "
		"ON CONFLICT ON CONSTRAINT dbblue_repack_history_table_key DO UPDATE SET "
		"relid = EXCLUDED.relid, "
		"last_repack_at = EXCLUDED.last_repack_at, "
		"last_bloat_ratio = EXCLUDED.last_bloat_ratio, "
		"repack_count = dbblue_repack_history.repack_count + 1";
	static const Oid argtypes[4] = {TEXTOID, TEXTOID, OIDOID, FLOAT8OID};
	Datum		values[4];

	values[0] = CStringGetTextDatum(schema);
	values[1] = CStringGetTextDatum(table);
	values[2] = ObjectIdGetDatum(relid);
	values[3] = Float8GetDatum(bloat_ratio);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_execute_with_args(sql, 4, (Oid *) argtypes, values, NULL, false, 0) < 0)
		ereport(WARNING,
				(errmsg("dbblue repack launcher: failed to record history for \"%s.%s\"",
						schema, table)));

	PopActiveSnapshot();
	SPI_finish();
}
