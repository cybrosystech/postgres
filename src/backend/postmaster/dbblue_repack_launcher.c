/*-------------------------------------------------------------------------
 *
 * dbblue_repack_launcher.c
 *	  dbblue repack launcher background worker.
 *
 * A deliberately simple, "cron job" style scheduler for REPACK, not an
 * autovacuum-parity dynamic scanner: REPACK rewrites the whole table and
 * every index (up to 2x disk space, a brief AccessExclusiveLock at the
 * heap swap, a much bigger WAL spike than VACUUM), so this worker only
 * ever considers the fixed, operator-curated table list named by
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
 * This worker instead calls cluster_rel() directly, the same lower-level,
 * parsenode-free API vacuum.c uses for VACUUM FULL, processing each
 * configured table in its own transaction exactly the way REPACK's own
 * multi-relation path does.
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

#include "access/relation.h"
#include "access/table.h"
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
 * Worker-lifetime memory context; holds the specs/candidates of the cycle
 * currently running and is reset at the end of each cycle (and by the
 * error recovery path).
 */
static MemoryContext launcher_cxt = NULL;

/*
 * Set once dbblue_repack_history has been verified to exist after the
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

static bool ensure_schema(void);
static List *parse_configured_tables(void);
static List *load_bloat_candidates(List *specs);
static void run_repack_cycle(void);
static bool repack_one_table(const char *schema, const char *table,
							 Oid relid, double bloat_ratio);
static void record_repack_history(const char *schema, const char *table,
								  Oid relid, double bloat_ratio);

/*
 * dbblue_check_repack_enabled
 *		GUC check hook for dbblue_repack_enabled.
 *
 * The launcher cannot start at all when dbblue_repack_database does not
 * name an existing database: BackgroundWorkerInitializeConnection() would
 * FATAL every time, and with bgw_restart_time = 5 that becomes a silent
 * crash loop with no operator-facing warning.  Rather than rejecting the
 * setting (the operator may be preparing configuration for the next
 * restart, before the database exists), warn loudly at the moment the
 * feature is switched on -- the same convention dbblue_check_advisor_enabled
 * uses for the analogous index-advisor GUC.
 *
 * Only runs on an actual off->on transition in a backend with catalog
 * access and settled GUC state, for the same reasons documented on
 * dbblue_check_advisor_enabled.
 */
bool
dbblue_check_repack_enabled(bool *newval, void **extra, GucSource source)
{
	if (!*newval || dbblue_repack_enabled)
		return true;

	if (!IsUnderPostmaster || !IsTransactionState())
		return true;

	if (dbblue_repack_database == NULL || dbblue_repack_database[0] == '\0')
		ereport(WARNING,
				(errmsg("dbblue_repack_enabled is on, but dbblue_repack_database is not set"),
				 errdetail("With no database configured the repack launcher worker will fail to connect."),
				 errhint("Set dbblue_repack_database to an existing database (changing it requires a server restart).")));
	else if (!OidIsValid(get_database_oid(dbblue_repack_database, true)))
		ereport(WARNING,
				(errmsg("dbblue repack launcher database \"%s\" does not exist",
						dbblue_repack_database),
				 errdetail("The launcher worker will fail to connect and will be retried every 5 seconds until the database exists."),
				 errhint("Create the database, or point dbblue_repack_database at an existing one (changing it requires a server restart).")));

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
 * Always registered, since dbblue_repack_enabled is PGC_SIGHUP context
 * and the feature must be switchable on without a restart; while
 * disabled the worker only sleeps.
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
 * RepackLauncherMain
 *		Background worker entry point.
 */
void
RepackLauncherMain(Datum main_arg)
{
	sigjmp_buf	local_sigjmp_buf;
	TimestampTz next_run = 0;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	ereport(LOG, (errmsg("dbblue repack launcher started")));

	launcher_cxt = AllocSetContextCreate(TopMemoryContext,
										 "dbblue repack launcher",
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

		/* Re-verify the schema before the next cycle. */
		schema_ready = false;

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

		if (dbblue_repack_enabled && !RecoveryInProgress())
		{
			if (!launcher_connected)
			{
				BackgroundWorkerInitializeConnection(dbblue_repack_database,
													 NULL, 0);

				/*
				 * Bound how long the launcher will wait for a lock on a
				 * configured table.  Without this, a table stuck behind a
				 * long-running conflicting lock would stall every later
				 * table in the same cycle indefinitely; on timeout the
				 * attempt errors, is caught per-table, and is simply
				 * retried next cycle.
				 */
				SetConfigOption("lock_timeout", "5s", PGC_SUSET, PGC_S_SESSION);

				launcher_connected = true;
			}

			if (!schema_ready)
				schema_ready = ensure_schema();

			if (schema_ready)
			{
				TimestampTz now = GetCurrentTimestamp();

				if (next_run == 0 || now >= next_run)
				{
					run_repack_cycle();
					next_run = GetCurrentTimestamp() +
						(int64) dbblue_repack_naptime * USECS_PER_SEC;
				}

				sleep_ms = (long) ((next_run - GetCurrentTimestamp()) / 1000);
				sleep_ms = Max(sleep_ms, 1000);
			}
			else
				sleep_ms = (long) dbblue_repack_naptime * 1000;
		}
		else
		{
			/* Disabled (or a standby): re-verify everything once re-enabled. */
			schema_ready = false;
			next_run = 0;
			sleep_ms = 5000;
		}

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
 * ensure_schema
 *		Create dbblue_repack_history if it doesn't already exist.  Safe
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

	if (table_ok)
		ereport(LOG,
				(errmsg("dbblue repack launcher: ready (public.dbblue_repack_history verified in database \"%s\")",
						dbblue_repack_database)));

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
