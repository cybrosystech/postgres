/*-------------------------------------------------------------------------
 *
 * dbblue_backup_launcher.c
 *	  dbblue backup launcher background worker.
 *
 * A "cron job" style scheduler that keeps a rotating pg_dump (custom
 * format) backup of each database named in dbblue_backup_databases,
 * re-backing up a database once dbblue_backup_interval has passed since
 * its last successful backup, on a wake-up cycle of dbblue_backup_naptime.
 * After each successful backup, older successful backups of the same
 * database beyond dbblue_backup_retention_count are pruned (both the dump
 * file and its history row).
 *
 * A per-database logical dump has no server-side equivalent to call
 * instead of pg_dump: PostgreSQL deliberately keeps the catalog walk,
 * dependency ordering, and DDL reconstruction that a dump requires in the
 * pg_dump client program, not the backend.  This worker therefore locates
 * the pg_dump binary installed alongside the running postgres executable
 * (find_other_exec(), the same helper pg_ctl/postmaster.c use to find
 * sibling binaries) and runs it via system(), exactly the way
 * archive_command/restore_command are executed from a backend process in
 * xlogarchive.c.  Because fe_utils (which has appendShellString) is not
 * linked into the backend, append_shell_quoted() below ports the same
 * quoting logic from fe_utils/string_utils.c's appendShellStringNoError()
 * so that the database name and file paths can never be interpreted as
 * shell syntax, regardless of what characters they contain.
 *
 * The pg_dump subprocess connects over the local Unix socket with no
 * password, so pg_hba.conf must allow local peer or trust authentication
 * for the OS user running the server -- the same operational expectation
 * archive_command already has for filesystem access.  This worker does
 * not store or supply a password.
 *
 * Copyright (c) 2026, dbblue / Cybrosys Technologies
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/dbblue_backup_launcher.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/dbblue_backup_launcher.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "utils/wait_event.h"

/* GUC variables, wired up via guc_parameters.dat */
bool		dbblue_backup_enabled = false;
char	   *dbblue_backup_database = NULL;
char	   *dbblue_backup_databases = NULL;
char	   *dbblue_backup_directory = NULL;
int			dbblue_backup_interval = 86400;
int			dbblue_backup_naptime = 300;
int			dbblue_backup_retention_count = 7;
int			dbblue_backup_timeout = 3600;

/* Permissions for per-database backup subdirectories; not a GUC. */
#define DBBLUE_BACKUP_DIR_MODE		0700

/*
 * Grace period after the initial SIGTERM (dbblue_backup_timeout) before
 * timeout(1) escalates to SIGKILL.  Without --kill-after, timeout(1) only
 * ever sends the signal once and then waits indefinitely -- a pg_dump
 * that ignores or is stuck past SIGTERM would make dbblue_backup_timeout
 * not actually be a bound at all.  Not a GUC: this is a fixed safety
 * margin on top of the configured timeout, not something operators need
 * to tune.
 */
#define DBBLUE_BACKUP_KILL_GRACE_S	30

/* pg_dump's own -V output, for find_other_exec()'s version check. */
#define DBBLUE_BACKUP_PGDUMP_VERSIONSTR "pg_dump (PostgreSQL) " PG_VERSION "\n"

/* One entry from dbblue_backup_databases, before pg_database is checked. */
typedef struct BackupDbSpec
{
	char	   *dbname;
} BackupDbSpec;

/* One configured database plus what the catalog/history lookup learned. */
typedef struct BackupCandidate
{
	char	   *dbname;
	Oid			dboid;			/* InvalidOid if the database does not exist */
	bool		allowconn;
	bool		istemplate;
	TimestampTz last_backup_at;	/* 0 if never successfully backed up */
} BackupCandidate;

/*
 * Worker-lifetime memory context; holds the specs/candidates of the cycle
 * currently running and is reset at the end of each cycle (and by the
 * error recovery path).
 */
static MemoryContext launcher_cxt = NULL;

/*
 * Set once dbblue_backup_history has been verified to exist after the
 * feature was switched on; cleared when it is switched off so a later
 * re-enable re-checks it.
 */
static bool schema_ready = false;

/*
 * Set once BackgroundWorkerInitializeConnection has attached the worker to
 * its database.  Deferred until the feature is first enabled, so a
 * cluster that never turns the launcher on never attaches to the database
 * (which would otherwise block DROP DATABASE on it).
 */
static bool launcher_connected = false;

/*
 * Set once pg_dump has been located and the local socket directory
 * resolved.  Cleared (and retried) whenever resolution fails, the same way
 * schema_ready is retried.
 */
static bool environment_ready = false;
static char pg_dump_path[MAXPGPATH];
static char *socket_dir = NULL;	/* NULL means "let libpq pick the default" */

static bool ensure_schema(void);
static bool resolve_environment(void);
static List *parse_configured_databases(void);
static List *load_backup_candidates(List *specs);
static void run_backup_cycle(void);
static bool backup_one_database(const char *dbname);
static void record_backup_history(const char *dbname, TimestampTz started_at,
								  const char *status, const char *file_path,
								  int64 file_size_bytes,
								  const char *error_message);
static void enforce_retention(const char *dbname, const char *status);
static bool append_shell_quoted(StringInfo buf, const char *str);

/*
 * dbblue_check_backup_enabled
 *		GUC check hook for dbblue_backup_enabled.
 *
 * The launcher cannot do anything
 * useful without dbblue_backup_database existing, dbblue_backup_directory
 * set, and dbblue_backup_databases naming something, but none of that is
 * fatal to reject here -- the operator may be preparing configuration
 * ahead of the pieces existing.  Warn loudly instead, at the moment the
 * feature is switched on.
 *
 * Only runs on an actual off->on transition (repeated SETs of an
 * already-on value are silent), and only once the backend has real
 * catalog access and settled GUC state (IsUnderPostmaster and
 * IsTransactionState()) -- a check hook also runs in contexts where
 * neither is true yet, e.g. while postgresql.conf is being parsed at
 * postmaster startup, before any database connection exists.
 */
bool
dbblue_check_backup_enabled(bool *newval, void **extra, GucSource source)
{
	if (!*newval || dbblue_backup_enabled)
		return true;

	if (!IsUnderPostmaster || !IsTransactionState())
		return true;

	if (dbblue_backup_database == NULL || dbblue_backup_database[0] == '\0')
		ereport(WARNING,
				(errmsg("dbblue_backup_enabled is on, but dbblue_backup_database is not set"),
				 errdetail("With no database configured the backup launcher worker will fail to connect."),
				 errhint("Set dbblue_backup_database to an existing database (changing it requires a server restart).")));
	else if (!OidIsValid(get_database_oid(dbblue_backup_database, true)))
		ereport(WARNING,
				(errmsg("dbblue backup launcher database \"%s\" does not exist",
						dbblue_backup_database),
				 errdetail("The launcher worker will fail to connect and will be retried every 5 seconds until the database exists."),
				 errhint("Create the database, or point dbblue_backup_database at an existing one (changing it requires a server restart).")));

	if (dbblue_backup_directory == NULL || dbblue_backup_directory[0] == '\0')
		ereport(WARNING,
				(errmsg("dbblue_backup_enabled is on, but dbblue_backup_directory is not set"),
				 errhint("Set dbblue_backup_directory to an existing, writable directory (changing it requires a server restart).")));

	if (dbblue_backup_databases == NULL || dbblue_backup_databases[0] == '\0')
		ereport(WARNING,
				(errmsg("dbblue_backup_enabled is on, but dbblue_backup_databases lists no databases"),
				 errhint("Set dbblue_backup_databases to a comma-separated list of database names to back up.")));

	return true;
}

/*
 * dbblue_check_backup_databases
 *		GUC check hook for dbblue_backup_databases.
 *
 * Syntax only: each comma-separated item must be a single unquoted plain
 * database name.  Resolution against pg_database is deliberately deferred
 * to each cycle (parse_configured_databases/load_backup_candidates), since
 * a database can be created, dropped, or renamed between GUC-set-time and
 * backup-time.
 */
bool
dbblue_check_backup_databases(char **newval, void **extra, GucSource source)
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

		if (item[0] == '\0' || strchr(item, '.') != NULL)
		{
			GUC_check_errdetail("\"%s\" is not a valid database name.", item);
			ok = false;
			break;
		}
	}

	pfree(rawstring);
	list_free(elemlist);
	return ok;
}

/*
 * BackupLauncherRegister
 *		Register the dbblue backup launcher as a static background
 *		worker.  Called directly from PostmasterMain(), the same way the
 *		other dbblue workers are, since this is core functionality, not
 *		something an extension's _PG_init() has to opt into.
 *
 * Always registered, since dbblue_backup_enabled is PGC_SIGHUP context
 * and the feature must be switchable on without a restart; while
 * disabled the worker only sleeps.
 */
void
BackupLauncherRegister(void)
{
	BackgroundWorker worker;

	/*
	 * Don't run during pg_upgrade: the postmaster is started internally,
	 * multiple times, in a restricted mode to restore schema objects in a
	 * precise sequence; this worker independently connecting and shelling
	 * out to pg_dump has no business happening during that window.
	 */
	if (IsBinaryUpgrade)
		return;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;

	/*
	 * Use ConsistentState rather than RecoveryFinished so this also starts
	 * on hot standbys (which never reach RecoveryFinished's PM_RUN state).
	 * The RecoveryInProgress() check in BackupLauncherMain()'s loop is what
	 * then keeps a standby's copy from ever attempting to back anything up.
	 */
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 5;
	snprintf(worker.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "BackupLauncherMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "dbblue backup launcher");
	snprintf(worker.bgw_type, BGW_MAXLEN, "dbblue backup launcher");
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&worker);
}

/*
 * BackupLauncherMain
 *		Background worker entry point.
 */
void
BackupLauncherMain(Datum main_arg)
{
	sigjmp_buf	local_sigjmp_buf;
	TimestampTz next_run = 0;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	ereport(LOG, (errmsg("dbblue backup launcher started")));

	launcher_cxt = AllocSetContextCreate(TopMemoryContext,
										 "dbblue backup launcher",
										 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(launcher_cxt);

	/*
	 * Recover here after any unexpected error: report it, clean up
	 * whatever transaction state is left, and go back to the main loop.
	 * The wait at the end of the loop keeps a persistent failure from
	 * turning into a busy loop.
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

		/* Re-verify the schema and environment before the next cycle. */
		schema_ready = false;
		environment_ready = false;

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
		}

		if (dbblue_backup_enabled && !RecoveryInProgress())
		{
			if (!launcher_connected)
			{
				BackgroundWorkerInitializeConnection(dbblue_backup_database,
													 NULL, 0);

				/*
				 * Bound how long the launcher will wait for a lock while
				 * managing its own history table.  This never blocks on
				 * the pg_dump subprocess itself (dbblue_backup_timeout
				 * does that); it only protects the SPI bookkeeping.
				 */
				SetConfigOption("lock_timeout", "5s", PGC_SUSET, PGC_S_SESSION);

				launcher_connected = true;
			}

			if (!environment_ready)
				environment_ready = resolve_environment();

			if (environment_ready && !schema_ready)
				schema_ready = ensure_schema();

			if (environment_ready && schema_ready)
			{
				TimestampTz now = GetCurrentTimestamp();

				if (next_run == 0 || now >= next_run)
				{
					run_backup_cycle();
					next_run = GetCurrentTimestamp() +
						(int64) dbblue_backup_naptime * USECS_PER_SEC;
				}

				sleep_ms = (long) ((next_run - GetCurrentTimestamp()) / 1000);
				sleep_ms = Max(sleep_ms, 1000);
			}
			else
				sleep_ms = (long) dbblue_backup_naptime * 1000;
		}
		else
		{
			/* Disabled (or a standby): re-verify everything once re-enabled. */
			schema_ready = false;
			environment_ready = false;
			next_run = 0;
			sleep_ms = 5000;
		}

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 sleep_ms,
						 WAIT_EVENT_DBBLUE_BACKUP_LAUNCHER_MAIN);
		ResetLatch(MyLatch);
	}

	ereport(LOG, (errmsg("dbblue backup launcher shutting down")));

	/*
	 * Exit non-zero on SIGTERM, matching the other dbblue workers: exiting
	 * 0 is treated by the postmaster as "terminate and forget", which
	 * would keep pg_terminate_backend() from this worker coming back until
	 * the next server restart even while the feature stays enabled.
	 */
	proc_exit(1);
}

/*
 * resolve_environment
 *		Locate the pg_dump binary installed alongside this postgres
 *		executable, and pick a local Unix socket directory for it to
 *		connect through.  Safe to retry on every wakeup until it succeeds.
 */
static bool
resolve_environment(void)
{
	int			ret;

	ret = find_other_exec(my_exec_path, "pg_dump",
						  DBBLUE_BACKUP_PGDUMP_VERSIONSTR, pg_dump_path);
	if (ret < 0)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: could not locate a matching \"pg_dump\" executable next to \"%s\"",
						my_exec_path),
				 errhint("This is a packaging problem: pg_dump must be installed in the same directory as postgres and be the same version.")));
		return false;
	}

	if (socket_dir == NULL)
	{
		const char *raw = GetConfigOption("unix_socket_directories", true, false);
		List	   *elemlist;

		if (raw != NULL && raw[0] != '\0')
		{
			char	   *rawstring = pstrdup(raw);

			if (SplitDirectoriesString(rawstring, ',', &elemlist) && elemlist != NIL)
				socket_dir = MemoryContextStrdup(TopMemoryContext,
												 (char *) linitial(elemlist));
			list_free(elemlist);
			pfree(rawstring);
		}
	}

	ereport(LOG,
			(errmsg("dbblue backup launcher: ready (using pg_dump at \"%s\")",
					pg_dump_path)));

	return true;
}

/*
 * ensure_schema
 *		Create dbblue_backup_history if it doesn't already exist.  Safe
 *		to retry on every wakeup until it succeeds.
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
						   "dbblue backup launcher: ensuring schema");

	table_ok = (SPI_execute(
							"CREATE TABLE IF NOT EXISTS public.dbblue_backup_history ("
							"  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
							"  database_name text NOT NULL,"
							"  backup_started_at timestamptz NOT NULL,"
							"  backup_finished_at timestamptz,"
							"  status text NOT NULL,"
							"  file_path text,"
							"  file_size_bytes bigint,"
							"  error_message text"
							")",
							false, 0) == SPI_OK_UTILITY);

	if (table_ok)
	{
		table_ok = (SPI_execute(
								"CREATE INDEX IF NOT EXISTS dbblue_backup_history_db_status_idx "
								"ON public.dbblue_backup_history (database_name, status, backup_finished_at)",
								false, 0) == SPI_OK_UTILITY);
		if (!table_ok)
			ereport(WARNING,
					(errmsg("dbblue backup launcher: failed to create index on dbblue_backup_history")));
	}

	if (table_ok)
	{
		if (SPI_execute("GRANT SELECT ON public.dbblue_backup_history TO PUBLIC",
						false, 0) != SPI_OK_UTILITY)
			ereport(WARNING,
					(errmsg("dbblue backup launcher: failed to grant select on dbblue_backup_history")));
	}
	else
		ereport(WARNING,
				(errmsg("dbblue backup launcher: failed to create table dbblue_backup_history")));

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	if (table_ok)
		ereport(LOG,
				(errmsg("dbblue backup launcher: ready (public.dbblue_backup_history verified in database \"%s\")",
						dbblue_backup_database)));

	return table_ok;
}

/*
 * parse_configured_databases
 *		Split dbblue_backup_databases into database names.  The returned
 *		list and its entries live in launcher_cxt.
 */
static List *
parse_configured_databases(void)
{
	char	   *rawstring;
	List	   *elemlist;
	List	   *result = NIL;
	ListCell   *lc;

	if (dbblue_backup_databases == NULL || dbblue_backup_databases[0] == '\0')
		return NIL;

	rawstring = pstrdup(dbblue_backup_databases);
	if (!SplitIdentifierString(rawstring, ',', &elemlist))
	{
		pfree(rawstring);
		list_free(elemlist);
		return NIL;
	}

	foreach(lc, elemlist)
	{
		char	   *item = (char *) lfirst(lc);
		BackupDbSpec *spec;
		ListCell   *lc2;
		bool		duplicate = false;

		if (item[0] == '\0')
			continue;

		/*
		 * A repeated entry (e.g. a copy-paste mistake in the GUC) would
		 * otherwise turn into two candidates for the same database sharing
		 * one stale last_backup_at snapshot, letting the second back it up
		 * again immediately after the first commits, bypassing the
		 * cooldown.
		 */
		foreach(lc2, result)
		{
			BackupDbSpec *seen = (BackupDbSpec *) lfirst(lc2);

			if (strcmp(seen->dbname, item) == 0)
			{
				duplicate = true;
				break;
			}
		}
		if (duplicate)
			continue;

		spec = (BackupDbSpec *) palloc(sizeof(BackupDbSpec));
		spec->dbname = pstrdup(item);
		result = lappend(result, spec);
	}

	pfree(rawstring);
	list_free(elemlist);
	return result;
}

/*
 * load_backup_candidates
 *		One read-only SPI pass resolving every configured database name
 *		against pg_database and its last successful backup time.  The
 *		returned list and its entries live in launcher_cxt.
 */
static List *
load_backup_candidates(List *specs)
{
	List	   *candidates = NIL;
	int			n = list_length(specs);
	Datum	   *dbnames;
	ArrayType  *arr;
	Oid			argtypes[1] = {TEXTARRAYOID};
	Datum		values[1];
	int			i;
	int			ret;
	uint64		row;
	ListCell   *lc;

	if (n == 0)
		return NIL;

	dbnames = (Datum *) palloc(n * sizeof(Datum));
	i = 0;
	foreach(lc, specs)
	{
		BackupDbSpec *spec = (BackupDbSpec *) lfirst(lc);

		dbnames[i++] = CStringGetTextDatum(spec->dbname);
	}
	arr = construct_array_builtin(dbnames, n, TEXTOID);
	values[0] = PointerGetDatum(arr);

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue backup launcher: checking configured databases");

	ret = SPI_execute_with_args(
							   "WITH wanted AS ("
							   "  SELECT s AS dbname FROM unnest($1::text[]) AS s"
							   ")"
							   "SELECT w.dbname, d.oid, d.datallowconn, d.datistemplate,"
							   "       (SELECT max(h.backup_finished_at)"
							   "        FROM public.dbblue_backup_history h"
							   "        WHERE h.database_name = w.dbname"
							   "          AND h.status = 'success') AS last_backup_at"
							   " FROM wanted w"
							   " LEFT JOIN pg_database d ON d.datname = w.dbname",
							   1, argtypes, values, NULL, false, 0);

	if (ret != SPI_OK_SELECT)
		ereport(WARNING,
				(errmsg("dbblue backup launcher: configured-database check query failed (SPI result %d)",
						ret)));
	else
	{
		for (row = 0; row < SPI_processed; row++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[row];
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			bool		isnull;
			Datum		d;
			BackupCandidate *cand;
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(launcher_cxt);

			cand = (BackupCandidate *) palloc0(sizeof(BackupCandidate));

			d = SPI_getbinval(tuple, tupdesc, 1, &isnull);
			cand->dbname = isnull ? NULL : TextDatumGetCString(d);

			if (cand->dbname == NULL)
			{
				MemoryContextSwitchTo(oldcxt);
				continue;
			}

			d = SPI_getbinval(tuple, tupdesc, 2, &isnull);
			cand->dboid = isnull ? InvalidOid : DatumGetObjectId(d);

			d = SPI_getbinval(tuple, tupdesc, 3, &isnull);
			cand->allowconn = isnull ? false : DatumGetBool(d);

			d = SPI_getbinval(tuple, tupdesc, 4, &isnull);
			cand->istemplate = isnull ? false : DatumGetBool(d);

			d = SPI_getbinval(tuple, tupdesc, 5, &isnull);
			cand->last_backup_at = isnull ? 0 : DatumGetTimestampTz(d);

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
 * run_backup_cycle
 *		One full pass: resolve the current state of every configured
 *		database, then back up whichever ones exist, accept connections,
 *		are not templates, and are past their cooldown.
 */
static void
run_backup_cycle(void)
{
	List	   *specs = parse_configured_databases();
	List	   *candidates;
	ListCell   *lc;

	if (specs == NIL)
		return;

	candidates = load_backup_candidates(specs);

	foreach(lc, candidates)
	{
		BackupCandidate *cand = (BackupCandidate *) lfirst(lc);
		TimestampTz now;

		CHECK_FOR_INTERRUPTS();

		if (ShutdownRequestPending)
			break;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);

			/*
			 * If the feature was just switched off, stop rather than start
			 * backing up a database under a configuration that no longer
			 * wants this running at all.  The remaining databases in this
			 * cycle's candidate list are simply picked up again next time
			 * the feature is enabled.
			 */
			if (!dbblue_backup_enabled)
				break;
		}

		if (!OidIsValid(cand->dboid))
		{
			ereport(WARNING,
					(errmsg("dbblue backup launcher: configured database \"%s\" does not exist, skipping",
							cand->dbname)));
			continue;
		}

		if (!cand->allowconn || cand->istemplate)
		{
			ereport(WARNING,
					(errmsg("dbblue backup launcher: configured database \"%s\" does not accept connections or is a template, skipping",
							cand->dbname)));
			continue;
		}

		now = GetCurrentTimestamp();
		if (cand->last_backup_at != 0 &&
			now < cand->last_backup_at +
			(int64) dbblue_backup_interval * USECS_PER_SEC)
			continue;

		(void) backup_one_database(cand->dbname);
	}

	/* Free this cycle's specs/candidates before the next one. */
	MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextReset(launcher_cxt);
	MemoryContextSwitchTo(launcher_cxt);
}

/*
 * backup_one_database
 *		pg_dump (custom format) one database to its own subdirectory
 *		under dbblue_backup_directory, record the outcome in
 *		dbblue_backup_history, and prune old backups on success.  A
 *		failure here is caught and logged; it does not abort the rest of
 *		the cycle or the worker.
 */
static bool
backup_one_database(const char *dbname)
{
	char		dbdir[MAXPGPATH];
	char		outfile[MAXPGPATH];
	char		timebuf[32];
	time_t		now_t;
	struct tm	lt;
	TimestampTz started_at = GetCurrentTimestamp();
	StringInfoData cmd;
	int			rc;
	bool		ok = true;

	if (dbblue_backup_directory == NULL || dbblue_backup_directory[0] == '\0')
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: dbblue_backup_directory is not set, skipping backup of \"%s\"",
						dbname)));
		record_backup_history(dbname, started_at, "failed", NULL, 0,
							  "dbblue_backup_directory is not set");
		enforce_retention(dbname, "failed");
		return false;
	}

	now_t = time(NULL);
	(void) localtime_r(&now_t, &lt);
	strftime(timebuf, sizeof(timebuf), "%Y%m%d_%H%M%S", &lt);

	snprintf(dbdir, sizeof(dbdir), "%s/%s", dbblue_backup_directory, dbname);
	if (pg_mkdir_p(dbdir, DBBLUE_BACKUP_DIR_MODE) != 0)
	{
		int			save_errno = errno;

		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue backup launcher: could not create directory \"%s\": %s",
						dbdir, strerror(save_errno))));
		record_backup_history(dbname, started_at, "failed", NULL, 0,
							  psprintf("could not create backup directory \"%s\": %s",
									   dbdir, strerror(save_errno)));
		enforce_retention(dbname, "failed");
		return false;
	}

	snprintf(outfile, sizeof(outfile), "%s/%s_%s.dump", dbdir, dbname, timebuf);

	initStringInfo(&cmd);

	if (dbblue_backup_timeout > 0)
	{
		appendStringInfo(&cmd, "timeout --signal=TERM --kill-after=%ds %ds ",
						 DBBLUE_BACKUP_KILL_GRACE_S, dbblue_backup_timeout);
	}

	ok &= append_shell_quoted(&cmd, pg_dump_path);
	appendStringInfoString(&cmd, " -Fc --no-password");

	if (socket_dir != NULL)
	{
		appendStringInfoString(&cmd, " -h ");
		ok &= append_shell_quoted(&cmd, socket_dir);
	}

	appendStringInfo(&cmd, " -p %d -f ", PostPortNumber);
	ok &= append_shell_quoted(&cmd, outfile);
	appendStringInfoChar(&cmd, ' ');
	ok &= append_shell_quoted(&cmd, dbname);

	if (!ok)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: database name or path for \"%s\" contains a newline or carriage return, skipping",
						dbname)));
		record_backup_history(dbname, started_at, "failed", NULL, 0,
							  "database name or path contains an invalid character");
		enforce_retention(dbname, "failed");
		pfree(cmd.data);
		return false;
	}

	pgstat_report_activity(STATE_RUNNING,
						   psprintf("dbblue backup launcher: backing up %s", dbname));

	ereport(DEBUG3,
			(errmsg_internal("dbblue backup launcher: executing \"%s\"", cmd.data)));

	fflush(NULL);
	rc = system(cmd.data);
	pfree(cmd.data);

	pgstat_report_activity(STATE_IDLE, NULL);

	if (rc == 0)
	{
		struct stat statbuf;

		if (stat(outfile, &statbuf) == 0 && statbuf.st_size > 0)
		{
			record_backup_history(dbname, started_at, "success", outfile,
								  (int64) statbuf.st_size, NULL);
			ereport(LOG,
					(errmsg("dbblue backup launcher: backed up database \"%s\" to \"%s\" (%lld bytes)",
							dbname, outfile, (long long) statbuf.st_size)));
			enforce_retention(dbname, "success");
			return true;
		}

		ereport(WARNING,
				(errmsg("dbblue backup launcher: pg_dump for \"%s\" exited successfully but \"%s\" is missing or empty",
						dbname, outfile)));
		record_backup_history(dbname, started_at, "failed", NULL, 0,
							  "pg_dump exited successfully but produced no output file");
		enforce_retention(dbname, "failed");
		unlink(outfile);
		return false;
	}

	ereport(WARNING,
			(errmsg("dbblue backup launcher: backing up database \"%s\" failed: %s",
					dbname, wait_result_to_str(rc))));
	record_backup_history(dbname, started_at, "failed", NULL, 0,
						  wait_result_to_str(rc));
	enforce_retention(dbname, "failed");
	unlink(outfile);
	return false;
}

/*
 * record_backup_history
 *		Insert one row describing the outcome of a backup attempt.
 *		Append-only -- one row per attempt, not one row per database --
 *		so retention (enforce_retention) is what keeps this table from
 *		growing without bound.
 */
static void
record_backup_history(const char *dbname, TimestampTz started_at,
					  const char *status, const char *file_path,
					  int64 file_size_bytes, const char *error_message)
{
	static const char *sql =
		"INSERT INTO public.dbblue_backup_history "
		"(database_name, backup_started_at, backup_finished_at, status, "
		" file_path, file_size_bytes, error_message) "
		"VALUES ($1, $2, now(), $3, $4, $5, $6)";
	static const Oid argtypes[6] = {TEXTOID, TIMESTAMPTZOID, TEXTOID, TEXTOID, INT8OID, TEXTOID};
	Datum		values[6];
	char		nulls[7] = "      ";	/* SPI convention: ' ' = not null, 'n' = null */

	values[0] = CStringGetTextDatum(dbname);
	values[1] = TimestampTzGetDatum(started_at);
	values[2] = CStringGetTextDatum(status);

	if (file_path != NULL)
		values[3] = CStringGetTextDatum(file_path);
	else
		nulls[3] = 'n';

	if (file_size_bytes > 0)
		values[4] = Int64GetDatum(file_size_bytes);
	else
		nulls[4] = 'n';

	if (error_message != NULL)
		values[5] = CStringGetTextDatum(error_message);
	else
		nulls[5] = 'n';

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_execute_with_args(sql, 6, (Oid *) argtypes, values, nulls, false, 0) < 0)
		ereport(WARNING,
				(errmsg("dbblue backup launcher: failed to record backup history for \"%s\"",
						dbname)));

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
}

/*
 * enforce_retention
 *		Delete backups of dbname with the given status beyond
 *		dbblue_backup_retention_count, both the dump file (if any) and the
 *		history row.  Called right after a new row of that status is
 *		recorded, so the newest is always kept even when the count is
 *		exceeded.  Applied to both "success" and "failed" rows -- without
 *		this, a persistently misconfigured target (bad directory, auth
 *		failure, etc.) would insert one more "failed" row every naptime
 *		cycle forever, since only successes used to be pruned.
 */
static void
enforce_retention(const char *dbname, const char *status)
{
	static const char *select_sql =
		"SELECT id, file_path FROM public.dbblue_backup_history "
		"WHERE database_name = $1 AND status = $2 "
		"ORDER BY backup_finished_at DESC "
		"OFFSET $3";
	static const Oid select_argtypes[3] = {TEXTOID, TEXTOID, INT8OID};
	Datum		select_values[3];
	int			ret;
	Datum	   *ids_to_delete;
	int			n_to_delete = 0;
	uint64		row;

	select_values[0] = CStringGetTextDatum(dbname);
	select_values[1] = CStringGetTextDatum(status);
	select_values[2] = Int64GetDatum((int64) dbblue_backup_retention_count);

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute_with_args(select_sql, 3, (Oid *) select_argtypes,
								select_values, NULL, false, 0);

	if (ret != SPI_OK_SELECT)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: retention lookup for \"%s\" failed (SPI result %d)",
						dbname, ret)));
		ids_to_delete = NULL;
	}
	else
	{
		ids_to_delete = (Datum *) palloc(SPI_processed * sizeof(Datum));

		for (row = 0; row < SPI_processed; row++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[row];
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			bool		isnull;
			Datum		id;
			Datum		file_path_datum;
			char	   *file_path;

			id = SPI_getbinval(tuple, tupdesc, 1, &isnull);
			if (isnull)
				continue;

			file_path_datum = SPI_getbinval(tuple, tupdesc, 2, &isnull);
			file_path = isnull ? NULL : TextDatumGetCString(file_path_datum);

			if (file_path != NULL && unlink(file_path) != 0 && errno != ENOENT)
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("dbblue backup launcher: could not remove old backup \"%s\": %m",
								file_path)));

			ids_to_delete[n_to_delete++] = id;
		}
	}

	if (n_to_delete > 0)
	{
		static const char *delete_sql =
			"DELETE FROM public.dbblue_backup_history WHERE id = ANY($1::bigint[])";
		static const Oid delete_argtypes[1] = {INT8ARRAYOID};
		Datum		delete_values[1];
		ArrayType  *idarr;

		idarr = construct_array_builtin(ids_to_delete, n_to_delete, INT8OID);
		delete_values[0] = PointerGetDatum(idarr);

		if (SPI_execute_with_args(delete_sql, 1, (Oid *) delete_argtypes,
								  delete_values, NULL, false, 0) < 0)
			ereport(WARNING,
					(errmsg("dbblue backup launcher: failed to prune backup history for \"%s\"",
							dbname)));
	}

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
}

/*
 * append_shell_quoted
 *		Append str to buf, single-quoted for /bin/sh unless it is already
 *		composed only of characters that are never special to a shell.
 *		Ported from fe_utils/string_utils.c's appendShellStringNoError(),
 *		since fe_utils is not linked into the backend.  Returns false (and
 *		leaves *some* partial output in buf) if str contains a newline or
 *		carriage return, which cannot be represented as a single shell
 *		argument; callers must treat that as a hard error rather than run
 *		the resulting command.
 */
static bool
append_shell_quoted(StringInfo buf, const char *str)
{
	bool		ok = true;
	const char *p;

	if (*str != '\0' &&
		strspn(str, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./:") == strlen(str))
	{
		appendStringInfoString(buf, str);
		return ok;
	}

	appendStringInfoChar(buf, '\'');
	for (p = str; *p; p++)
	{
		if (*p == '\n' || *p == '\r')
		{
			ok = false;
			continue;
		}

		if (*p == '\'')
			appendStringInfoString(buf, "'\"'\"'");
		else
			appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '\'');

	return ok;
}
