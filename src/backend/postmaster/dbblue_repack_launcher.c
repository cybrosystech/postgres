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
#include <sys/stat.h>
#ifndef WIN32
#include <sys/statvfs.h>
#endif

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/pg_type.h"
#include "commands/repack.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/miscnodes.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/dbblue_repack_launcher.h"
#include "postmaster/interrupt.h"
#include "storage/bufpage.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/regproc.h"
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
/*
 * A table whose repack failed is not tried again for this long; see
 * record_repack_failure().  And one that a transaction open this long holds a
 * lock on is not started; see table_busy().
 */
#define DBBLUE_REPACK_RETRY_DELAY_S		3600
#define DBBLUE_REPACK_BUSY_XACT_S		30

typedef struct RepackCandidate
{
	char	   *schema;
	char	   *table;
	Oid			relid;			/* InvalidOid if the table does not exist */
	double		bloat_ratio;	/* 0.0 if too small to estimate meaningfully */
	TimestampTz last_repack_at; /* 0 if never repacked by this worker */
	TimestampTz last_failed_at; /* 0 if the last attempt did not fail */
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
static bool history_table_exists(void);
static int	history_table_check(LOCKMODE lockmode);
static List *parse_configured_tables(void);
static List *load_bloat_candidates(List *specs);
static void run_repack_cycle(void);
static bool repack_one_table(const char *schema, const char *table,
							 Oid relid, double bloat_ratio);
static void record_repack_failure(const char *schema, const char *table,
								  Oid relid, double bloat_ratio);
static bool table_busy(Oid relid, const char *schema, const char *table);
static bool repack_has_disk_room(Oid relid, double bloat_ratio,
								 const char *schema, const char *table);
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
 * split_repack_tables
 *		Parse a dbblue_repack_tables value into RepackTableSpecs.  Returns
 *		false and sets *bad to the offending entry on a syntax error.
 *
 * Each entry is a qualified name written as in SQL: unquoted parts are
 * folded to lower case, quoted ones kept as written, and each part limited
 * to NAMEDATALEN - 1 bytes on its own, so "public"."Foo" names public.Foo
 * and a long schema plus a long table both survive.  The list is split at
 * commas outside double quotes.  Splitting it with SplitIdentifierString()
 * instead treated each entry as one identifier: "public"."Foo" was a syntax
 * error, "public.Foo" became public.Foo here but public.foo when resolved,
 * and a qualified name over 63 bytes was cut short as a whole.
 */
static bool
split_repack_tables(const char *value, List **specs, char **bad)
{
	const char *p = value;

	*specs = NIL;
	*bad = NULL;
	while (*p != '\0')
	{
		const char *start = p;
		bool		inquote = false;
		char	   *item;
		List	   *names;
		RepackTableSpec *spec;
		ErrorSaveContext escontext = {T_ErrorSaveContext};

		for (; *p != '\0' && (inquote || *p != ','); p++)
			if (*p == '"')
				inquote = !inquote;		/* "" inside quotes toggles twice */
		item = pnstrdup(start, p - start);
		if (*p == ',')
			p++;

		names = stringToQualifiedNameList(item, (Node *) &escontext);
		if (escontext.error_occurred || list_length(names) != 2)
		{
			*bad = item;
			return false;
		}
		spec = palloc_object(RepackTableSpec);
		spec->schema = strVal(linitial(names));
		spec->table = strVal(lsecond(names));
		*specs = lappend(*specs, spec);
	}
	return true;
}

/*
 * dbblue_check_repack_tables
 *		GUC check hook for dbblue_repack_tables.
 *
 * Syntax only: each comma-separated item must be a schema.table name; see
 * split_repack_tables().  Resolution to an OID is deliberately deferred to
 * each cycle (parse_configured_tables/load_bloat_candidates), since a
 * table can be dropped or renamed between GUC-set-time and repack-time.
 */
bool
dbblue_check_repack_tables(char **newval, void **extra, GucSource source)
{
	List	   *specs;
	char	   *bad;

	if (*newval == NULL || **newval == '\0')
		return true;

	if (!split_repack_tables(*newval, &specs, &bad))
	{
		GUC_check_errdetail("\"%s\" is not a valid schema.table name.", bad);
		return false;
	}
	return true;
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
	/*
	 * SIGTERM ends the worker at once, as it does a backend, even in the
	 * middle of a REPACK that can run for hours: a fast shutdown, DROP
	 * DATABASE and Odoo's Drop, Duplicate and Rename must not wait for it.
	 * The REPACK rolls back and the table stays as it was; a later cycle
	 * repacks it.  The database owner may send it too; see
	 * pg_signal_backend().
	 */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/*
	 * By OID, not by name: the launcher resolved the name a moment ago and
	 * a rename in between must not redirect this worker somewhere else.
	 * InitPostgres applies the database's own ALTER DATABASE settings, so
	 * dbblue_repack_enabled/tables/threshold/min_interval below are this
	 * database's values.
	 */
	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);

	/*
	 * This worker runs as superuser in a database its owner controls, so pin
	 * search_path to the system catalogs: otherwise the owner decides how its
	 * unqualified names resolve -- with ALTER DATABASE ... SET search_path, or
	 * by planting public.pg_relation_size(oid), an exact match that wins over
	 * pg_catalog.pg_relation_size(regclass) -- and the next cycle runs that
	 * code as superuser.  REPACK and ANALYZE run index expressions as the
	 * table's owner anyway.
	 */
	SetConfigOption("search_path", "pg_catalog, pg_temp",
					PGC_SUSET, PGC_S_SESSION);

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
	 * and is retried after DBBLUE_REPACK_RETRY_DELAY_S.  Unrelated to, and
	 * unaffected by, the per-database GUCs InitPostgres just merged above.
	 *
	 * Short, because the wait that matters is REPACK (CONCURRENTLY)'s final
	 * upgrade to AccessExclusiveLock to swap the files: while it waits, every
	 * new query on the table queues behind it, so this is how long a blocked
	 * swap freezes the table for Odoo.
	 */
	SetConfigOption("lock_timeout", "2s", PGC_SUSET, PGC_S_SESSION);

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

	if (!ShutdownRequestPending)
		run_repack_cycle();

	proc_exit(0);
}

/*
 * ensure_schema
 *		Create dbblue_repack_history if it doesn't already exist in the
 *		current database.  Called only before the first repack of a cycle,
 *		not on every visit: creating it in every database, every cycle, put
 *		it into a database Odoo had just created to restore a backup into,
 *		and a backup that contained it then failed with "relation already
 *		exists".  The GRANT runs only when the table is created, since a
 *		GRANT rewrites the table's pg_class row even when nothing changes.
 */
static bool
ensure_schema(void)
{
	bool		table_ok;
	bool		created;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue repack launcher: ensuring schema");

	created = !history_table_exists();
	table_ok = (SPI_execute(
							"CREATE TABLE IF NOT EXISTS public.dbblue_repack_history ("
							"  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
							"  schema_name text NOT NULL,"
							"  table_name text NOT NULL,"
							"  relid oid,"
							"  last_repack_at timestamptz,"
							"  last_bloat_ratio double precision,"
							"  repack_count bigint NOT NULL DEFAULT 1,"
							"  last_failed_at timestamptz,"
							"  CONSTRAINT dbblue_repack_history_table_key UNIQUE (schema_name, table_name)"
							")",
							false, 0) == SPI_OK_UTILITY);

	if (!table_ok)
		ereport(WARNING,
				(errmsg("dbblue repack launcher: failed to create table dbblue_repack_history")));
	else if (history_table_check(AccessShareLock) != 1)
		table_ok = false;		/* someone else's table; warned */
	else if (created &&
			 SPI_execute("GRANT SELECT ON public.dbblue_repack_history TO PUBLIC",
						 false, 0) != SPI_OK_UTILITY)
		ereport(WARNING,
				(errmsg("dbblue repack launcher: failed to grant select on dbblue_repack_history")));

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	return table_ok;
}

/*
 * history_table_exists
 *		Does public.dbblue_repack_history exist?  Needs a transaction.
 */
static bool
history_table_exists(void)
{
	Oid			nspid = get_namespace_oid("public", true);

	return OidIsValid(nspid) &&
		OidIsValid(get_relname_relid("dbblue_repack_history", nspid));
}

/*
 * history_table_check
 *		Lock public.dbblue_repack_history in lockmode, until the transaction
 *		ends, and check that it is safe for this superuser worker to read and
 *		write.  Returns 0 if it does not exist, 1 if it is an ordinary table
 *		owned by a superuser with no triggers or rules, and -1, after a
 *		WARNING, otherwise.  Needs a transaction and an SPI connection.
 *
 * The database owner owns schema public, so they can create this table
 * before the worker does, drop the worker's and put their own in its place,
 * or own it after a pg_restore --no-owner; their triggers or rules then run
 * on the worker's writes as superuser, and a view in its place runs its
 * functions on the worker's reads.  Locking first keeps the table that was
 * checked from being dropped or replaced before it is used: any lock blocks
 * DROP and ALTER ... RENAME.  pg_trigger and pg_rewrite are read directly, as
 * pg_class.relhastriggers and relhasrules mean "has or once had".
 */
static int
history_table_check(LOCKMODE lockmode)
{
	Oid			relid;
	HeapTuple	tup;
	Form_pg_class rel;
	const char *problem = NULL;
	char	   *owner = NULL;

	relid = RangeVarGetRelidExtended(makeRangeVar("public", "dbblue_repack_history", -1),
									 lockmode, RVR_MISSING_OK, NULL, NULL);
	if (!OidIsValid(relid))
		return 0;

	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tup))
		return 0;
	rel = (Form_pg_class) GETSTRUCT(tup);
	if (rel->relkind != RELKIND_RELATION)
		problem = "is not an ordinary table";
	else if (!superuser_arg(rel->relowner))
	{
		owner = GetUserNameFromId(rel->relowner, true);
		problem = "is not owned by a superuser";
	}
	ReleaseSysCache(tup);

	if (problem == NULL)
	{
		char	   *sql = psprintf("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = %u),"
								   "       EXISTS (SELECT 1 FROM pg_catalog.pg_rewrite WHERE ev_class = %u)",
								   relid, relid);
		bool		isnull;

		if (SPI_execute(sql, false, 1) != SPI_OK_SELECT || SPI_processed != 1)
			problem = "could not be checked";
		else if (DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1, &isnull)))
			problem = "has triggers";
		else if (DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 2, &isnull)))
			problem = "has rules";
		pfree(sql);
	}

	if (problem == NULL)
		return 1;

	ereport(WARNING,
			(errmsg("dbblue repack launcher: public.dbblue_repack_history in database \"%s\" %s; not repacking there",
					get_database_name(MyDatabaseId), problem),
			 owner ? errdetail("It is owned by \"%s\".", owner) : 0,
			 errhint("The repack worker reads and writes it as superuser.  Drop it so the worker recreates it, or make it an ordinary table owned by a superuser, with no triggers or rules.")));
	return -1;
}

/*
 * parse_configured_tables
 *		Split dbblue_repack_tables into schema/table pairs.  The returned
 *		list and its entries live in launcher_cxt.
 */
static List *
parse_configured_tables(void)
{
	List	   *specs;
	List	   *result = NIL;
	char	   *bad;
	ListCell   *lc;

	if (dbblue_repack_tables == NULL || dbblue_repack_tables[0] == '\0')
		return NIL;

	/* Malformed entries should have been rejected by the check hook. */
	if (!split_repack_tables(dbblue_repack_tables, &specs, &bad))
		return NIL;

	foreach(lc, specs)
	{
		RepackTableSpec *spec = (RepackTableSpec *) lfirst(lc);
		ListCell   *lc2;
		bool		duplicate = false;

		/*
		 * A repeated entry (e.g. a copy-paste mistake in the GUC) would
		 * otherwise turn into two candidates for the same table sharing one
		 * stale last_repack_at snapshot, letting the second repack it again
		 * immediately after the first commits, bypassing the cooldown.
		 */
		foreach(lc2, result)
		{
			RepackTableSpec *seen = (RepackTableSpec *) lfirst(lc2);

			if (strcmp(seen->schema, spec->schema) == 0 &&
				strcmp(seen->table, spec->table) == 0)
			{
				duplicate = true;
				break;
			}
		}
		if (!duplicate)
			result = lappend(result, spec);
	}
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
	Datum	   *schemas;
	Datum	   *tables;
	Oid			argtypes[2] = {TEXTARRAYOID, TEXTARRAYOID};
	Datum		values[2];
	int			i;
	int			ret;
	uint64		row;
	ListCell   *lc;
	bool		have_history;

	if (n == 0)
		return NIL;

	/*
	 * Schema and table names exactly as parsed, resolved below by name rather
	 * than joined back into text for to_regclass(), which would parse them a
	 * second time under different rules.
	 *
	 * A partitioned table holds no rows itself -- its size is 0, so its bloat
	 * ratio was 0 and it was skipped without a word -- and REPACK
	 * (CONCURRENTLY) refuses it anyway.  So it stands for its leaf partitions,
	 * found afresh every cycle so that partitions pg_partman adds later are
	 * included, each measured, cooled down and repacked on its own.  UNION
	 * drops a partition that is also listed by name.
	 */
	schemas = (Datum *) palloc(n * sizeof(Datum));
	tables = (Datum *) palloc(n * sizeof(Datum));
	i = 0;
	foreach(lc, specs)
	{
		RepackTableSpec *spec = (RepackTableSpec *) lfirst(lc);

		schemas[i] = CStringGetTextDatum(spec->schema);
		tables[i++] = CStringGetTextDatum(spec->table);
	}
	values[0] = PointerGetDatum(construct_array_builtin(schemas, n, TEXTOID));
	values[1] = PointerGetDatum(construct_array_builtin(tables, n, TEXTOID));

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue repack launcher: checking table bloat");

	/*
	 * Without the history table nothing has been repacked here yet, so there
	 * is no cooldown to read; see ensure_schema().
	 */
	switch (history_table_check(AccessShareLock))
	{
		case 0:
			have_history = false;
			break;
		case 1:
			have_history = true;

			/*
			 * Bring a table created before last_failed_at existed up to
			 * date, before anything below reads or writes that column.
			 * It is locked and checked by now.
			 */
			if (get_attnum(get_relname_relid("dbblue_repack_history",
											 get_namespace_oid("public", false)),
						   "last_failed_at") == InvalidAttrNumber)
				(void) SPI_execute("ALTER TABLE public.dbblue_repack_history"
								   " ADD COLUMN last_failed_at timestamptz,"
								   " ALTER COLUMN last_repack_at DROP NOT NULL",
								   false, 0);
			break;
		default:
			/* Someone else's table, warned: no cycle in this database. */
			SPI_finish();
			PopActiveSnapshot();
			CommitTransactionCommand();
			pgstat_report_activity(STATE_IDLE, NULL);
			return NIL;
	}
	ret = SPI_execute_with_args(have_history ?
							   "WITH named AS ("
							   "  SELECT w.nspname, w.relname, c.oid AS relid, c.relkind"
							   "  FROM unnest($1::text[], $2::text[]) AS w(nspname, relname)"
							   "  LEFT JOIN pg_catalog.pg_namespace n ON n.nspname = w.nspname"
							   "  LEFT JOIN pg_catalog.pg_class c"
							   "    ON c.relnamespace = n.oid AND c.relname = w.relname"
							   "), resolved AS ("
							   "  SELECT nspname, relname, relid FROM named"
							   "  WHERE relkind IS DISTINCT FROM 'p'"
							   "  UNION"
							   "  SELECT pn.nspname, pc.relname, pc.oid"
							   "  FROM named, pg_catalog.pg_partition_tree(named.relid) t"
							   "  JOIN pg_catalog.pg_class pc ON pc.oid = t.relid"
							   "  JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace"
							   "  WHERE named.relkind = 'p' AND t.isleaf AND pc.relkind = 'r'"
							   ")"
							   "SELECT r.relid, r.nspname, r.relname,"
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
							   "       h.last_repack_at, h.last_failed_at,"
							   "       COALESCE((SELECT o.option_value::int"
							   "                 FROM pg_catalog.pg_class c2,"
							   "                      pg_catalog.pg_options_to_table(c2.reloptions) o"
							   "                 WHERE c2.oid = r.relid AND o.option_name = 'fillfactor'), 100)"
							   " FROM resolved r"
							   " LEFT JOIN pg_stat_user_tables t ON t.relid = r.relid"
							   " LEFT JOIN public.dbblue_repack_history h"
							   "   ON h.schema_name = r.nspname AND h.table_name = r.relname"
							   :
							   "WITH named AS ("
							   "  SELECT w.nspname, w.relname, c.oid AS relid, c.relkind"
							   "  FROM unnest($1::text[], $2::text[]) AS w(nspname, relname)"
							   "  LEFT JOIN pg_catalog.pg_namespace n ON n.nspname = w.nspname"
							   "  LEFT JOIN pg_catalog.pg_class c"
							   "    ON c.relnamespace = n.oid AND c.relname = w.relname"
							   "), resolved AS ("
							   "  SELECT nspname, relname, relid FROM named"
							   "  WHERE relkind IS DISTINCT FROM 'p'"
							   "  UNION"
							   "  SELECT pn.nspname, pc.relname, pc.oid"
							   "  FROM named, pg_catalog.pg_partition_tree(named.relid) t"
							   "  JOIN pg_catalog.pg_class pc ON pc.oid = t.relid"
							   "  JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace"
							   "  WHERE named.relkind = 'p' AND t.isleaf AND pc.relkind = 'r'"
							   ")"
							   "SELECT r.relid, r.nspname, r.relname,"
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
							   "       NULL::timestamptz AS last_repack_at,"
							   "       NULL::timestamptz AS last_failed_at,"
							   "       COALESCE((SELECT o.option_value::int"
							   "                 FROM pg_catalog.pg_class c2,"
							   "                      pg_catalog.pg_options_to_table(c2.reloptions) o"
							   "                 WHERE c2.oid = r.relid AND o.option_name = 'fillfactor'), 100)"
							   " FROM resolved r"
							   " LEFT JOIN pg_stat_user_tables t ON t.relid = r.relid",
							   2, argtypes, values, NULL, false, 0);

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

			d = SPI_getbinval(tuple, tupdesc, 1, &isnull);
			relid = isnull ? InvalidOid : DatumGetObjectId(d);

			oldcxt = MemoryContextSwitchTo(launcher_cxt);

			cand = (RepackCandidate *) palloc0(sizeof(RepackCandidate));
			cand->relid = relid;

			d = SPI_getbinval(tuple, tupdesc, 2, &isnull);
			cand->schema = isnull ? NULL : TextDatumGetCString(d);
			d = SPI_getbinval(tuple, tupdesc, 3, &isnull);
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
				int32		fillfactor;

				d = SPI_getbinval(tuple, tupdesc, 4, &isnull);
				relpages = isnull ? 0 : DatumGetInt32(d);
				d = SPI_getbinval(tuple, tupdesc, 5, &isnull);
				n_live_tup = isnull ? 0 : DatumGetInt64(d);
				d = SPI_getbinval(tuple, tupdesc, 6, &isnull);
				sum_width = isnull ? 0 : DatumGetInt64(d);
				d = SPI_getbinval(tuple, tupdesc, 7, &isnull);
				cand->last_repack_at = isnull ? 0 : DatumGetTimestampTz(d);
				d = SPI_getbinval(tuple, tupdesc, 8, &isnull);
				cand->last_failed_at = isnull ? 0 : DatumGetTimestampTz(d);
				d = SPI_getbinval(tuple, tupdesc, 9, &isnull);
				fillfactor = isnull ? 100 : DatumGetInt32(d);

				if (relpages >= DBBLUE_REPACK_MIN_RELPAGES && n_live_tup > 0)
				{
					double		avg_row_width = (sum_width > 0) ?
						(double) sum_width : DBBLUE_REPACK_FALLBACK_AVG_ROW_WIDTH;
					/*
					 * A fillfactor below 100 leaves part of every page
					 * empty on purpose, for HOT updates, and REPACK
					 * writes the new table that way too.  Counting that
					 * space as bloat measured a fresh fillfactor-70 table
					 * at about 1.6, over the threshold, so it was
					 * rewritten every dbblue_repack_min_interval for
					 * nothing -- and DBblue's fillfactor map gives exactly
					 * the hot Odoo tables such a fillfactor.
					 */
					double		usable_bytes = (BLCKSZ - SizeOfPageHeaderData) *
						Min(Max(fillfactor, 10), 100) / 100.0;
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
	bool		history_ready = false;

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

		/* Its last attempt failed: wait before throwing away another copy. */
		if (cand->last_failed_at != 0 &&
			now < cand->last_failed_at +
			(int64) DBBLUE_REPACK_RETRY_DELAY_S * USECS_PER_SEC)
			continue;

		/* A long transaction on it would make the final swap time out. */
		if (table_busy(cand->relid, cand->schema, cand->table))
			continue;

		/* Running out of disk, above all in pg_wal, would stop the server. */
		if (!repack_has_disk_room(cand->relid, cand->bloat_ratio,
								  cand->schema, cand->table))
			continue;

		/* The history table is needed from the first repack on. */
		if (!history_ready && !(history_ready = ensure_schema()))
			break;

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
		ErrorData  *edata;

		/*
		 * The error machinery jumps here in ErrorContext, which
		 * FlushErrorState() resets: copy the error out of it first.
		 */
		MemoryContextSwitchTo(launcher_cxt);
		edata = CopyErrorData();
		FlushErrorState();
		AbortOutOfAnyTransaction();
		MemoryContextSwitchTo(launcher_cxt);

		if (repacked)
			ereport(WARNING,
					(errmsg("dbblue repack launcher: post-repack ANALYZE of \"%s.%s\" failed: %s",
							schema, table, edata->message),
					 errdetail("The repack itself committed; statistics will be refreshed by the next autovacuum analyze.")));
		else
		{
			ereport(WARNING,
					(errmsg("dbblue repack launcher: repacking \"%s.%s\" failed: %s",
							schema, table, edata->message),
					 errdetail("It will be tried again in %d minutes at the earliest.",
							   DBBLUE_REPACK_RETRY_DELAY_S / 60)));
			record_repack_failure(schema, table, relid, bloat_ratio);
		}
		FreeErrorData(edata);
		success = repacked;
	}
	PG_END_TRY();

	pgstat_report_activity(STATE_IDLE, NULL);
	return success;
}

/*
 * table_busy
 *		Is a transaction open for DBBLUE_REPACK_BUSY_XACT_S or more holding a
 *		lock on the table?  If so, log it and return true.
 *
 * REPACK (CONCURRENTLY) copies the whole table under a light lock, then
 * upgrades to AccessExclusiveLock to swap the files, which waits for every
 * transaction that has touched the table.  A long Odoo report reading it
 * makes that upgrade time out after the copy is done: the copy is thrown
 * away, and while the upgrade waits every new query on the table queues
 * behind it.  Starting only when no long transaction is on the table avoids
 * most of those attempts; one that starts during the copy can still cause
 * one, and record_repack_failure() spaces out the retries.  Autovacuum is
 * left out: it is cancelled for REPACK.
 */
static bool
table_busy(Oid relid, const char *schema, const char *table)
{
	bool		busy = false;
	Oid			argtypes[1] = {OIDOID};
	Datum		values[1];

	values[0] = ObjectIdGetDatum(relid);
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	if (SPI_execute_with_args(psprintf("SELECT a.pid, extract(epoch FROM now() - a.xact_start)::int,"
									   "       coalesce(nullif(a.application_name, ''), a.backend_type)"
									   "  FROM pg_catalog.pg_locks l"
									   "  JOIN pg_catalog.pg_stat_activity a ON a.pid = l.pid"
									   " WHERE l.locktype = 'relation' AND l.relation = $1"
									   "   AND l.database = %u AND l.granted"
									   "   AND a.pid <> pg_catalog.pg_backend_pid()"
									   "   AND a.backend_type <> 'autovacuum worker'"
									   "   AND a.xact_start < now() - interval '%d seconds'"
									   " ORDER BY a.xact_start LIMIT 1",
									   MyDatabaseId, DBBLUE_REPACK_BUSY_XACT_S),
							  1, argtypes, values, NULL, true, 1) == SPI_OK_SELECT &&
		SPI_processed > 0)
	{
		HeapTuple	tup = SPI_tuptable->vals[0];
		TupleDesc	desc = SPI_tuptable->tupdesc;

		busy = true;
		ereport(LOG,
				(errmsg("dbblue repack launcher: postponing \"%s.%s\": a transaction holding a lock on it has been open for %s seconds",
						schema, table, SPI_getvalue(tup, desc, 2)),
				 errdetail("Process %s (%s).", SPI_getvalue(tup, desc, 1),
						   SPI_getvalue(tup, desc, 3))));
	}
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(launcher_cxt);
	return busy;
}

/*
 * repack_has_disk_room
 *		Is there disk space to repack the table?  If not, warn and return
 *		false; the table is checked again next cycle.
 *
 * REPACK writes a whole new copy of the table and its indexes beside the
 * old ones, freed only after the swap, and the WAL for all of it: the
 * logical replication slot REPACK (CONCURRENTLY) holds keeps every WAL
 * segment written meanwhile.  If pg_wal fills up the server PANICs.  So ask
 * for room for the new table (its size divided by the bloat ratio, the live
 * part) plus its indexes as they are now, once in the table's tablespace and
 * once in pg_wal -- twice where they share a file system -- with a fifth on
 * top, and leave a tenth of each file system free besides.  Deliberately
 * generous: a skipped repack costs little.  Not checked on Windows, which
 * has no statvfs().
 */
static bool
repack_has_disk_room(Oid relid, double bloat_ratio,
					 const char *schema, const char *table)
{
#ifndef WIN32
	int64		table_bytes = 0;
	int64		index_bytes = 0;
	Oid			tsoid;
	char	   *data_path;
	char	   *wal_path;
	struct statvfs data_fs;
	struct statvfs wal_fs;
	struct stat data_st;
	struct stat wal_st;
	double		need;
	double		data_free;
	double		wal_free;
	bool		ok;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	{
		Oid			argtypes[1] = {OIDOID};
		Datum		values[1] = {ObjectIdGetDatum(relid)};

		if (SPI_execute_with_args("SELECT pg_catalog.pg_table_size($1::regclass),"
								  "       pg_catalog.pg_indexes_size($1::regclass)",
								  1, argtypes, values, NULL, true, 1) == SPI_OK_SELECT &&
			SPI_processed == 1)
		{
			bool		isnull;
			Datum		d;

			d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
			table_bytes = isnull ? 0 : DatumGetInt64(d);
			d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
			index_bytes = isnull ? 0 : DatumGetInt64(d);
		}
	}
	tsoid = get_rel_tablespace(relid);
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(launcher_cxt);

	if (!OidIsValid(tsoid))
		tsoid = MyDatabaseTableSpace;
	data_path = (tsoid == DEFAULTTABLESPACE_OID) ? pstrdup(DataDir) :
		psprintf("%s/pg_tblspc/%u", DataDir, tsoid);
	wal_path = psprintf("%s/pg_wal", DataDir);

	if (statvfs(data_path, &data_fs) != 0 || statvfs(wal_path, &wal_fs) != 0 ||
		stat(data_path, &data_st) != 0 || stat(wal_path, &wal_st) != 0)
		return true;			/* cannot tell: do not block repacking */

	need = 1.2 * ((double) table_bytes / Max(bloat_ratio, 1.0) + (double) index_bytes);
	data_free = (double) data_fs.f_bavail * data_fs.f_frsize -
		0.1 * (double) data_fs.f_blocks * data_fs.f_frsize;
	wal_free = (double) wal_fs.f_bavail * wal_fs.f_frsize -
		0.1 * (double) wal_fs.f_blocks * wal_fs.f_frsize;

	if (data_st.st_dev == wal_st.st_dev)
		ok = data_free >= 2 * need;
	else
		ok = data_free >= need && wal_free >= need;

	if (!ok)
		ereport(WARNING,
				(errmsg("dbblue repack launcher: skipping \"%s.%s\": not enough free disk space to repack it",
						schema, table),
				 data_st.st_dev == wal_st.st_dev ?
				 errdetail("It needs about %.0f MB free for the new table, its indexes and their WAL, beyond a tenth of the disk kept free; %.0f MB is available.",
						   2 * need / 1048576.0, Max(data_free, 0) / 1048576.0) :
				 errdetail("It needs about %.0f MB free beyond a tenth of each disk, both for the data (%.0f MB available) and for pg_wal (%.0f MB available).",
						   need / 1048576.0, Max(data_free, 0) / 1048576.0,
						   Max(wal_free, 0) / 1048576.0)));
	return ok;
#else
	return true;
#endif
}

/*
 * record_repack_failure
 *		Note in dbblue_repack_history that repacking the table just failed,
 *		in a transaction of its own after the failed one was aborted.
 *
 * The cycle then leaves the table alone for DBBLUE_REPACK_RETRY_DELAY_S
 * instead of copying it again next cycle: a failure at the final swap throws
 * the finished copy away, and a cause such as a long report, a missing
 * primary key or wal_level below logical usually outlasts one cycle.  A
 * successful repack clears it.
 */
static void
record_repack_failure(const char *schema, const char *table, Oid relid,
					  double bloat_ratio)
{
	static const Oid argtypes[4] = {TEXTOID, TEXTOID, OIDOID, FLOAT8OID};
	Datum		values[4];

	values[0] = CStringGetTextDatum(schema);
	values[1] = CStringGetTextDatum(table);
	values[2] = ObjectIdGetDatum(relid);
	values[3] = Float8GetDatum(bloat_ratio);

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	if (history_table_check(RowExclusiveLock) == 1 &&
		SPI_execute_with_args("INSERT INTO public.dbblue_repack_history"
							  " (schema_name, table_name, relid, last_bloat_ratio,"
							  "  repack_count, last_failed_at)"
							  " VALUES ($1, $2, $3, $4, 0, now())"
							  " ON CONFLICT ON CONSTRAINT dbblue_repack_history_table_key"
							  " DO UPDATE SET last_failed_at = EXCLUDED.last_failed_at",
							  4, (Oid *) argtypes, values, NULL, false, 0) < 0)
		ereport(WARNING,
				(errmsg("dbblue repack launcher: failed to record the failed repack of \"%s.%s\"",
						schema, table)));
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(launcher_cxt);
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
		"last_failed_at = NULL, "
		"repack_count = dbblue_repack_history.repack_count + 1";
	static const Oid argtypes[4] = {TEXTOID, TEXTOID, OIDOID, FLOAT8OID};
	Datum		values[4];

	values[0] = CStringGetTextDatum(schema);
	values[1] = CStringGetTextDatum(table);
	values[2] = ObjectIdGetDatum(relid);
	values[3] = Float8GetDatum(bloat_ratio);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Checked again right before writing; see history_table_check().  If the
	 * table is missing or someone else's (already warned about), nothing is
	 * written.
	 */
	if (history_table_check(RowExclusiveLock) == 1 &&
		SPI_execute_with_args(sql, 4, (Oid *) argtypes, values, NULL, false, 0) < 0)
		ereport(WARNING,
				(errmsg("dbblue repack launcher: failed to record history for \"%s.%s\"",
						schema, table)));

	PopActiveSnapshot();
	SPI_finish();
}
