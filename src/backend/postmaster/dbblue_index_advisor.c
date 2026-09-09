/*-------------------------------------------------------------------------
 *
 * dbblue_index_advisor.c
 *	  dbblue auto index suggestion background worker.
 *
 * The advisor wakes up every dbblue_auto_index_suggestion_interval seconds
 * and performs one analysis pass over the workload of the database it is
 * connected to (dbblue_auto_index_suggestion_database):
 *
 *	 1. Fetch the top-N queries by total execution time from
 *		pg_stat_statements, restricted to the current database and to
 *		queries executed at least min_calls times.
 *	 2. For each query, run the raw parser and parse analysis
 *		(parse_analyze_varparams, so the $n placeholders left behind by
 *		pg_stat_statements' normalisation are accepted) and collect the
 *		columns of user tables appearing in WHERE / JOIN / HAVING
 *		predicates, GROUP BY and ORDER BY.
 *	 3. Turn that column usage into candidate CREATE INDEX statements:
 *		one single-column candidate per referenced column plus one
 *		composite candidate per table (equality columns first, then
 *		grouping/ordering columns, range columns last), capped at
 *		max_index_columns keys.
 *	 4. Discard candidates whose key list is already the leading prefix
 *		of an existing index.
 *	 5. Obtain the planner's baseline cost for the query with
 *		EXPLAIN (GENERIC_PLAN), materialise each candidate as a HypoPG
 *		hypothetical index, EXPLAIN again, and compute the relative cost
 *		reduction.
 *	 6. Candidates whose reduction reaches min_cost_improvement are
 *		upserted into public.dbblue_index_suggestions for review.
 *
 * The worker never creates real indexes; it only records suggestions.
 *
 * The advisor is enabled per database.  dbblue_auto_index_suggestion_enabled
 * is PGC_SUSET, so each database carries its own value:
 *
 *	   ALTER DATABASE odoo_1 SET dbblue_auto_index_suggestion_enabled = on;
 *	   ALTER DATABASE odoo_2 SET dbblue_auto_index_suggestion_enabled = off;
 *
 * Turning it off for odoo_2 leaves odoo_1 being analysed, which one
 * cluster-wide setting plus one target database could not do: pointing the
 * old single-database advisor at another database stopped the first one.
 * The cluster-wide value remains the default for any database with no
 * setting of its own, exactly as GUC precedence implies.
 *
 * Two kinds of process implement that, the same split the dbblue BRIN
 * feature and autovacuum use.  A launcher, always running, holds *no*
 * database connection: it lists the cluster's databases out of the shared
 * catalog pg_database and starts one short-lived dynamic worker in each in
 * turn, waiting for each to finish.  Each worker connects, and because
 * InitPostgres has applied that database's own ALTER DATABASE settings by
 * then, its dbblue_auto_index_suggestion_enabled is already the effective
 * value there -- so the worker itself decides whether to analyse, and a
 * disabled database costs one immediate exit.
 *
 * Letting the worker decide is what keeps the semantics honest: a process
 * with no database can only read the handful of shared catalogs nailed
 * into the relcache before a database is selected, and pg_db_role_setting
 * is not one of them.  It also means one worker slot covers any number of
 * databases, and nothing is held open between passes, so DROP DATABASE on
 * an analysed database is never blocked.
 *
 * dbblue_auto_index_suggestion_interval is the time between two sweeps of
 * the whole cluster, not per database: one sweep visits every enabled
 * database in turn, then the launcher sleeps for the remainder.
 *
 * Copyright (c) 2026, dbblue / Cybrosys Technologies
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/dbblue_index_advisor.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "executor/spi.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/dbblue_index_advisor.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/* GUC variables, wired up via guc_parameters.dat */
bool		dbblue_auto_index_suggestion_enabled = false;
int			dbblue_auto_index_suggestion_interval = 3600;
int			dbblue_auto_index_suggestion_min_calls = 100;
int			dbblue_auto_index_suggestion_max_index_columns = 3;
int			dbblue_auto_index_suggestion_top_n_queries = 50;
double		dbblue_auto_index_suggestion_min_cost_improvement = 0.30;
double		dbblue_auto_index_suggestion_min_baseline_cost = 0.0;
char	   *dbblue_auto_index_suggestion_database = NULL;

/* Queries whose text exceeds this many bytes are not analysed. */
#define ADVISOR_MAX_QUERY_LEN		32768

/* Upper bound on candidates generated for one query. */
#define ADVISOR_MAX_CANDIDATES		16

/* Name of the table suggestions are written to (in schema public). */
#define ADVISOR_RESULT_TABLE		"dbblue_index_suggestions"

/*
 * One query pulled from pg_stat_statements.  Allocated in advisor_cxt so
 * it survives across the per-query transactions of a pass.
 */
typedef struct WorkloadEntry
{
	int64		queryid;
	int64		calls;
	double		total_exec_ms;
	char	   *query_text;
	Oid			userid;			/* role that ran the query (for planning) */
} WorkloadEntry;

/*
 * How a column is used by a query.  The role decides its position in a
 * composite index candidate.
 */
typedef enum ColumnRole
{
	COLROLE_EQUALITY,			/* col = ..., col IN (...), IS [NOT] NULL */
	COLROLE_RANGE,				/* col < / <= / > / >= ... */
	COLROLE_GROUP,				/* GROUP BY col */
	COLROLE_SORT,				/* ORDER BY col */
} ColumnRole;

/* Per-table column usage collected from one query. */
typedef struct TableUsage
{
	Oid			relid;
	List	   *eq_cols;		/* attnos as ints, in first-seen order */
	List	   *range_cols;
	List	   *group_cols;
	List	   *sort_cols;
} TableUsage;

/* One candidate index to be priced with HypoPG. */
typedef struct IndexCandidate
{
	Oid			relid;
	char	   *schema_name;
	char	   *table_name;
	int			nkeys;
	AttrNumber *keys;
	List	   *colnames;		/* of char *, same order as keys */
	char	   *ddl;			/* CREATE INDEX ON tab (col, ...) */
} IndexCandidate;

/*
 * Worker-lifetime memory context; holds the workload list of the pass
 * currently running and is reset when the pass ends (and by the error
 * recovery path).
 */
static MemoryContext advisor_cxt = NULL;

/*
 * Set once the extensions and the results table have been verified after
 * the feature was switched on; cleared when it is switched off so that a
 * later re-enable re-checks them.
 */
static bool advisor_env_ready = false;

/*
 * One database the launcher will start a worker in this sweep.  The name is
 * carried alongside the OID only for log messages; the worker is addressed by
 * OID, which stays correct even if the database is renamed mid-sweep.
 */
typedef struct AdvisorDatabase
{
	Oid			dboid;
	char	   *dbname;
} AdvisorDatabase;

/* How long to keep polling a worker the postmaster has not started. */
#define ADVISOR_STARTUP_TIMEOUT		60000	/* 1 minute in ms */

/* Poll granularity while waiting for a per-database worker to finish. */
#define ADVISOR_POLL_INTERVAL		1000	/* 1 second in ms */

/*
 * The database this worker is analysing, for log messages.  Points into
 * MyBgworkerEntry->bgw_extra, which the launcher filled in.
 */
static const char *advisor_dbname = "";

/*
 * Start time of the pass currently running.  Read by advisor_store_suggestion
 * to decide whether a conflicting row is from this pass (accumulate the
 * estimated benefit) or an earlier one (reset it): a fresh snapshot.
 */
static TimestampTz advisor_pass_start = 0;

static List *advisor_get_database_list(void);
static void advisor_run_one_database(const AdvisorDatabase *db);
static void advisor_wait_for_worker(BackgroundWorkerHandle *handle,
									const AdvisorDatabase *db);
static void advisor_ensure_environment(void);
static bool advisor_exec_in_subxact(const char *sql, const char *what);
static void advisor_run_pass(void);
static List *advisor_load_workload(void);
static void advisor_process_entry(WorkloadEntry *entry);
static void collect_query_columns(Query *query, List **tables);
static void collect_from_node(Query *query, Node *node, List **tables);
static void classify_qual(Query *query, Node *clause, List **tables);
static void record_column(Query *query, Node *node, ColumnRole role,
						  List **tables);
static List *build_candidates(List *tables);
static void add_candidate(List **candidates, Oid relid,
						  int nkeys, const AttrNumber *keys);
static bool existing_index_covers(Oid relid, int nkeys,
								  const AttrNumber *keys);
static void advisor_prune_stale(TimestampTz pass_start);
static double advisor_plan_cost(const char *query_text, Oid userid);
static void advisor_evaluate_candidate(WorkloadEntry *entry,
									   IndexCandidate *cand,
									   double baseline);
static void advisor_store_suggestion(WorkloadEntry *entry,
									 IndexCandidate *cand,
									 double baseline, double hypo_cost,
									 double reduction);

/*
 * dbblue_check_advisor_enabled
 *		GUC check hook for dbblue_auto_index_suggestion_enabled.
 *
 * The advisor is useless without pg_stat_statements loaded via
 * shared_preload_libraries, and it cannot start at all when the
 * configured database does not exist.  Rather than rejecting the
 * setting (the operator may be preparing the configuration for the
 * next restart), warn loudly at the moment the feature is switched on.
 *
 * Both checks run only in a normal backend with an open transaction —
 * that is, when an operator enables the feature interactively (SET /
 * ALTER SYSTEM followed by reload).  While the postmaster is still
 * reading its configuration file the other settings this hook inspects
 * may simply not have been applied yet (the file is processed top to
 * bottom), and catalog access is impossible there anyway, so warnings
 * from that phase would be unreliable noise.
 */
bool
dbblue_check_advisor_enabled(bool *newval, void **extra, GucSource source)
{
	const char *libs;

	/* Nothing to validate when turning the feature off. */
	if (!*newval)
		return true;

	/* Warn only on an actual off->on transition, not on every reload. */
	if (dbblue_auto_index_suggestion_enabled)
		return true;

	/* Only in a backend with catalog access and settled GUC state. */
	if (!IsUnderPostmaster || !IsTransactionState())
		return true;

	libs = GetConfigOption("shared_preload_libraries", true, false);
	if (libs == NULL || strstr(libs, "pg_stat_statements") == NULL)
		ereport(WARNING,
				(errmsg("dbblue_auto_index_suggestion_enabled is on, but pg_stat_statements is not in shared_preload_libraries"),
				 errdetail("The dbblue index advisor reads its workload from pg_stat_statements, which collects statistics only when preloaded."),
				 errhint("Add pg_stat_statements to shared_preload_libraries and restart the server.")));

	/*
	 * No database to validate any more: the value is per database, and the
	 * database it applies to is the one it is being set on.
	 */
	return true;
}

/*
 * dbblue_check_advisor_database
 *		GUC check hook for dbblue_auto_index_suggestion_database.
 *
 * Now that this is reloadable, pointing it at a database that does not
 * exist puts the worker into a connect-FATAL/restart loop rather than
 * simply doing nothing, so warn as soon as the name is set -- whether or
 * not the advisor happens to be enabled at that moment.  Warning only
 * while the feature is on would miss the ordinary way this is configured,
 * which is to set the database first and switch the feature on afterwards:
 * neither step would say anything.
 *
 * The name is deliberately not rejected, for the same reason
 * dbblue_check_advisor_enabled only warns.  A check hook cannot be
 * authoritative here: at server start it runs in the postmaster, which has
 * no catalog access, so a bad name in postgresql.conf is accepted
 * regardless and erroring out in a backend would only make the two paths
 * disagree.  Nor can the check stay true -- DROP DATABASE can invalidate an
 * already-accepted setting at any time -- which is why the worker treats a
 * missing database as a retryable condition at connect time rather than
 * trusting this.  Rejecting would also break the legitimate ordering of
 * configuring the setting before creating the database.
 */
bool
dbblue_check_advisor_database(char **newval, void **extra, GucSource source)
{
	if (*newval == NULL || **newval == '\0')
		return true;

	ereport(WARNING,
			(errmsg("dbblue_auto_index_suggestion_database is obsolete and is ignored"),
			 errdetail("The index advisor is now enabled per database."),
			 errhint("Use ALTER DATABASE %s SET dbblue_auto_index_suggestion_enabled = on instead, and remove dbblue_auto_index_suggestion_database.",
					 *newval)));

	return true;
}

/*
 * advisor_get_database_list
 *		List the databases the launcher will start a worker in this sweep.
 *
 * pg_database is one of the shared catalogs nailed into the relcache before
 * a database is selected, so the connectionless launcher can read it; this
 * follows get_database_list() in autovacuum.c.  Whether the advisor is
 * actually enabled for a database is not decided here -- see the file
 * header -- so this is only the list of databases that can be connected to.
 *
 * The result is allocated in the caller's context so it survives the commit.
 */
static List *
advisor_get_database_list(void)
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
		AdvisorDatabase *db;
		MemoryContext oldcxt;

		/* A half-dropped database cannot be connected to at all. */
		if (database_is_invalid_form(pgdatabase))
			continue;

		/*
		 * Templates and databases marked as rejecting connections are
		 * skipped: connecting to a template blocks CREATE DATABASE from it,
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
		db = palloc_object(AdvisorDatabase);
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
 * advisor_wait_for_worker
 *		Block until one per-database worker has finished.
 *
 * One database at a time, so the advisor costs a single worker slot however
 * many databases are enabled.  It also bounds the cost of the pass itself:
 * a pass plans the top-N queries with hypothetical indexes, and running
 * several databases' worth of that concurrently would be a large, bursty
 * load on the planner.
 *
 * WaitForBackgroundWorkerShutdown() is not used because it only returns once
 * the worker is gone, which on SIGTERM would hold shutdown for the rest of a
 * pass; here a shutdown request terminates the worker instead.  The startup
 * timeout covers the case where the postmaster never starts the worker at
 * all, so the launcher cannot get stuck on one database forever.
 */
static void
advisor_wait_for_worker(BackgroundWorkerHandle *handle,
						const AdvisorDatabase *db)
{
	TimestampTz startup_deadline;
	bool		started = false;
	bool		terminated = false;

	startup_deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
												   ADVISOR_STARTUP_TIMEOUT);

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
					(errmsg("dbblue index advisor: worker for database \"%s\" did not start",
							db->dbname),
					 errdetail("Giving up on it for this sweep."),
					 errhint("Check max_worker_processes.")));
			TerminateBackgroundWorker(handle);
			terminated = true;
		}

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 ADVISOR_POLL_INTERVAL,
						 WAIT_EVENT_DBBLUE_INDEX_ADVISOR_LAUNCHER_MAIN);
		ResetLatch(MyLatch);
	}
}

/*
 * advisor_run_one_database
 *		Start a one-shot worker in the given database and wait for it.
 *
 * Addressed by OID rather than by name, so a rename between building the
 * list and starting the worker cannot send it to the wrong database; if the
 * database was dropped meanwhile the worker fails to connect, which
 * BGW_NEVER_RESTART turns into a single logged failure rather than a restart
 * loop.
 */
static void
advisor_run_one_database(const AdvisorDatabase *db)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *handle;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;

	/* One pass and done: a failed sweep is retried by the next one. */
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "DbblueIndexAdvisorMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "dbblue index advisor (%s)",
			 db->dbname);
	snprintf(bgw.bgw_type, BGW_MAXLEN, "dbblue index advisor");
	bgw.bgw_main_arg = ObjectIdGetDatum(db->dboid);

	/*
	 * The name comes along for log messages only.  The worker cannot resolve
	 * its own OID to a name between transactions, and every suggestion it
	 * reports needs to say which database it came from.
	 */
	strlcpy(bgw.bgw_extra, db->dbname, BGW_EXTRALEN);

	/* So our latch is set when it starts and stops. */
	bgw.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&bgw, &handle))
	{
		ereport(WARNING,
				(errmsg("dbblue index advisor: no free background worker slot for database \"%s\"",
						db->dbname),
				 errdetail("The database will be retried on the next sweep."),
				 errhint("Consider raising max_worker_processes.")));
		return;
	}

	advisor_wait_for_worker(handle, db);

	pfree(handle);
}

/*
 * advisor_exec_in_subxact
 *		Execute one SQL statement through SPI inside an internal
 *		subtransaction, so that a failure is reported as a WARNING
 *		instead of aborting the surrounding transaction.
 *
 * The caller must hold an open SPI connection.  Returns true on success.
 */
static bool
advisor_exec_in_subxact(const char *sql, const char *what)
{
	MemoryContext oldcxt = CurrentMemoryContext;
	ResourceOwner oldowner = CurrentResourceOwner;
	bool		ok = false;

	BeginInternalSubTransaction(NULL);

	PG_TRY();
	{
		(void) SPI_execute(sql, false, 0);

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcxt);
		CurrentResourceOwner = oldowner;
		ok = true;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcxt);
		edata = CopyErrorData();
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcxt);
		CurrentResourceOwner = oldowner;

		ereport(WARNING,
				(errmsg("dbblue index advisor: %s failed in database \"%s\": %s",
						what, advisor_dbname, edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();

	return ok;
}

/*
 * advisor_ensure_environment
 *		Make sure pg_stat_statements, hypopg and the results table exist
 *		in the connected database.  Sets advisor_env_ready when all three
 *		are usable.
 *
 * Everything here is idempotent, so it is safe to retry on every wakeup
 * until it succeeds.
 */
static void
advisor_ensure_environment(void)
{
	bool		pgss_ok;
	bool		hypopg_ok;
	bool		table_ok;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue index advisor: preparing environment");

	pgss_ok = advisor_exec_in_subxact("CREATE EXTENSION IF NOT EXISTS pg_stat_statements",
									  "installing pg_stat_statements");

	/*
	 * CREATE EXTENSION IF NOT EXISTS is a no-op when the extension already
	 * exists at an older version, so bring it up to the newest installed
	 * version explicitly.  The workload query reads columns (toplevel,
	 * total_exec_time) that only exist in pg_stat_statements 1.9+; without
	 * this an old pre-existing install would make every pass fail.  UPDATE
	 * is itself a no-op when already current.
	 */
	if (pgss_ok)
		(void) advisor_exec_in_subxact("ALTER EXTENSION pg_stat_statements UPDATE",
									   "upgrading pg_stat_statements");
	hypopg_ok = advisor_exec_in_subxact("CREATE EXTENSION IF NOT EXISTS hypopg",
										"installing hypopg");
	table_ok = advisor_exec_in_subxact(
									   "CREATE TABLE IF NOT EXISTS public." ADVISOR_RESULT_TABLE " ("
									   "  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
									   "  schema_name text NOT NULL,"
									   "  table_name text NOT NULL,"
									   "  index_columns text[] NOT NULL,"
									   "  suggested_index text NOT NULL,"
									   "  queryid bigint,"
									   "  sample_query text,"
									   "  query_calls bigint,"
									   "  query_total_exec_ms double precision,"
									   "  cost_before double precision NOT NULL,"
									   "  cost_after double precision NOT NULL,"
									   "  cost_reduction double precision NOT NULL,"
									   "  estimated_benefit double precision,"
									   "  queries_helped bigint NOT NULL DEFAULT 1,"
									   "  first_suggested timestamptz NOT NULL DEFAULT now(),"
									   "  last_suggested timestamptz NOT NULL DEFAULT now(),"
									   "  times_suggested bigint NOT NULL DEFAULT 1,"
									   "  CONSTRAINT dbblue_index_suggestions_ddl_unique UNIQUE (suggested_index)"
									   ")",
									   "creating results table");

	/* Schema migration for tables created by older advisor versions. */
	if (table_ok)
	{
		(void) advisor_exec_in_subxact("ALTER TABLE public." ADVISOR_RESULT_TABLE
									   " ADD COLUMN IF NOT EXISTS estimated_benefit double precision",
									   "migrating results table");
		(void) advisor_exec_in_subxact("ALTER TABLE public." ADVISOR_RESULT_TABLE
									   " ADD COLUMN IF NOT EXISTS queries_helped bigint NOT NULL DEFAULT 1",
									   "migrating results table (queries_helped)");
	}

	/*
	 * CREATE TABLE IF NOT EXISTS is a no-op against a pre-existing table of
	 * the same name but a different shape.  The upsert relies on the unique
	 * constraint, so verify it is present; if it is missing, try to add it,
	 * and if that fails the table is unusable - refuse to mark the
	 * environment ready rather than warn on every candidate of every pass.
	 */
	if (table_ok)
	{
		MemoryContext envcxt = CurrentMemoryContext;
		ResourceOwner envowner = CurrentResourceOwner;
		bool		has_constraint = false;

		/*
		 * Guard the catalog probe with a subtransaction.  Everything else in
		 * this function is throw-safe (advisor_exec_in_subxact); a bare
		 * SPI_execute that threw here would abort the whole environment setup
		 * and, because advisor_env_ready stays false, make the main loop retry
		 * it with no wait in between.
		 */
		BeginInternalSubTransaction(NULL);
		PG_TRY();
		{
			int			spi_ret;

			/*
			 * read_only = false, deliberately.  A read-only SPI query reuses
			 * the ActiveSnapshot pushed at the top of this function, which
			 * predates the CREATE TABLE above -- so on the very first pass in
			 * a database, where the table and its constraint are created
			 * here, the new pg_constraint row is invisible.  The probe then
			 * reported "no constraint", the ALTER TABLE below failed with
			 * "already exists", and the whole environment was marked not
			 * ready: the first pass after enabling the advisor in a database
			 * silently produced nothing.  Passing false makes SPI increment
			 * the command counter and take a fresh snapshot.
			 */
			spi_ret = SPI_execute(
								  "SELECT 1 FROM pg_catalog.pg_constraint "
								  "WHERE conname = 'dbblue_index_suggestions_ddl_unique' "
								  "AND conrelid = 'public." ADVISOR_RESULT_TABLE "'::regclass",
								  false, 1);
			has_constraint = (spi_ret == SPI_OK_SELECT && SPI_processed > 0);

			ReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(envcxt);
			CurrentResourceOwner = envowner;
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(envcxt);
			FlushErrorState();
			RollbackAndReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(envcxt);
			CurrentResourceOwner = envowner;
			has_constraint = false;
		}
		PG_END_TRY();

		if (!has_constraint)
		{
			bool		added;

			added = advisor_exec_in_subxact(
										   "ALTER TABLE public." ADVISOR_RESULT_TABLE
										   " ADD CONSTRAINT dbblue_index_suggestions_ddl_unique"
										   " UNIQUE (suggested_index)",
										   "adding missing unique constraint");

			if (!added)
			{
				ereport(WARNING,
						(errmsg("dbblue index advisor: results table public.%s in database \"%s\" exists but lacks the required unique constraint",
								ADVISOR_RESULT_TABLE, advisor_dbname),
						 errhint("Drop or rename the conflicting table so the advisor can recreate it.")));
				table_ok = false;
			}
		}
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	advisor_env_ready = pgss_ok && hypopg_ok && table_ok;

	if (advisor_env_ready)
		ereport(DEBUG1,
				(errmsg("dbblue index advisor: ready (pg_stat_statements, hypopg and public.%s verified in database \"%s\")",
						ADVISOR_RESULT_TABLE, advisor_dbname)));
}

/*
 * usage_for_relation
 *		Find or create the TableUsage record for relid.
 */
static TableUsage *
usage_for_relation(List **tables, Oid relid)
{
	ListCell   *lc;
	TableUsage *usage;

	foreach(lc, *tables)
	{
		usage = (TableUsage *) lfirst(lc);
		if (usage->relid == relid)
			return usage;
	}

	usage = (TableUsage *) palloc0(sizeof(TableUsage));
	usage->relid = relid;
	*tables = lappend(*tables, usage);
	return usage;
}

/*
 * record_column
 *		If the node (after stripping implicit coercions) is a Var on an
 *		indexable user-table column of the current query level, record it
 *		under the given role.
 */
static void
record_column(Query *query, Node *node, ColumnRole role, List **tables)
{
	Var		   *var;
	RangeTblEntry *rte;
	TableUsage *usage;
	List	  **roster;

	if (node == NULL)
		return;

	node = strip_implicit_coercions(node);
	if (!IsA(node, Var))
		return;
	var = (Var *) node;

	/* Only columns of this query level, no system/whole-row attributes. */
	if (var->varlevelsup != 0 || var->varattno <= 0)
		return;
	if (var->varno <= 0 || var->varno > list_length(query->rtable))
		return;

	rte = rt_fetch(var->varno, query->rtable);
	if (rte->rtekind != RTE_RELATION)
		return;
	if (rte->relkind != RELKIND_RELATION &&
		rte->relkind != RELKIND_PARTITIONED_TABLE &&
		rte->relkind != RELKIND_MATVIEW)
		return;

	/* Catalogs and information_schema all have preassigned OIDs. */
	if (rte->relid < FirstNormalObjectId)
		return;

	/* Never advise on the advisor's own output table. */
	{
		char	   *relname = get_rel_name(rte->relid);

		if (relname == NULL)
			return;
		if (strcmp(relname, ADVISOR_RESULT_TABLE) == 0 &&
			get_rel_namespace(rte->relid) == PG_PUBLIC_NAMESPACE)
		{
			pfree(relname);
			return;
		}
		pfree(relname);
	}

	usage = usage_for_relation(tables, rte->relid);

	switch (role)
	{
		case COLROLE_EQUALITY:
			roster = &usage->eq_cols;
			break;
		case COLROLE_RANGE:
			roster = &usage->range_cols;
			break;
		case COLROLE_GROUP:
			roster = &usage->group_cols;
			break;
		case COLROLE_SORT:
			roster = &usage->sort_cols;
			break;
		default:
			return;
	}

	if (!list_member_int(*roster, (int) var->varattno))
		*roster = lappend_int(*roster, (int) var->varattno);
}

/*
 * classify_qual
 *		Recursively classify one predicate tree, recording column usage.
 *
 * Recognised shapes: AND/OR/NOT chains, binary operator expressions
 * using the standard comparison operator names, col IN (...) via
 * ScalarArrayOpExpr, NULL / boolean tests, bare boolean columns, and
 * sub-SELECTs (whose inner queries are analysed too).
 */
static void
classify_qual(Query *query, Node *clause, List **tables)
{
	check_stack_depth();

	if (clause == NULL)
		return;

	if (IsA(clause, BoolExpr))
	{
		BoolExpr   *bexpr = (BoolExpr *) clause;
		ListCell   *lc;

		foreach(lc, bexpr->args)
			classify_qual(query, (Node *) lfirst(lc), tables);
	}
	else if (IsA(clause, OpExpr))
	{
		OpExpr	   *opexpr = (OpExpr *) clause;
		char	   *opname;
		ListCell   *lc;

		if (list_length(opexpr->args) != 2)
			return;

		opname = get_opname(opexpr->opno);
		if (opname != NULL)
		{
			ColumnRole	role;
			bool		recognised = true;

			if (strcmp(opname, "=") == 0)
				role = COLROLE_EQUALITY;
			else if (strcmp(opname, "<") == 0 || strcmp(opname, "<=") == 0 ||
					 strcmp(opname, ">") == 0 || strcmp(opname, ">=") == 0)
				role = COLROLE_RANGE;
			else
				recognised = false;

			if (recognised)
			{
				record_column(query, linitial(opexpr->args), role, tables);
				record_column(query, lsecond(opexpr->args), role, tables);
			}
			pfree(opname);
		}

		/* An operand may be a sub-SELECT: analyse its innards as well. */
		foreach(lc, opexpr->args)
		{
			Node	   *arg = (Node *) lfirst(lc);

			if (IsA(arg, SubLink))
				classify_qual(query, arg, tables);
		}
	}
	else if (IsA(clause, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
		char	   *opname = get_opname(saop->opno);

		/* col = ANY(...) is how "col IN (list)" comes out of analysis. */
		if (opname != NULL && strcmp(opname, "=") == 0 && saop->useOr)
			record_column(query, linitial(saop->args), COLROLE_EQUALITY,
						  tables);
		if (opname != NULL)
			pfree(opname);
	}
	else if (IsA(clause, NullTest))
	{
		NullTest   *ntest = (NullTest *) clause;

		record_column(query, (Node *) ntest->arg, COLROLE_EQUALITY, tables);
	}
	else if (IsA(clause, BooleanTest))
	{
		BooleanTest *btest = (BooleanTest *) clause;

		record_column(query, (Node *) btest->arg, COLROLE_EQUALITY, tables);
	}
	else if (IsA(clause, SubLink))
	{
		SubLink    *sublink = (SubLink *) clause;

		/* Outer-query columns compared against the sub-SELECT's output. */
		if (sublink->testexpr != NULL)
			classify_qual(query, sublink->testexpr, tables);

		/* And the sub-SELECT itself. */
		if (sublink->subselect != NULL && IsA(sublink->subselect, Query))
			collect_query_columns((Query *) sublink->subselect, tables);
	}
	else if (IsA(clause, Var))
	{
		/* Bare boolean column used directly as a predicate. */
		record_column(query, clause, COLROLE_EQUALITY, tables);
	}
}

/*
 * collect_from_node
 *		Recurse through the join tree, classifying every WHERE and JOIN
 *		ON predicate found on the way.
 */
static void
collect_from_node(Query *query, Node *node, List **tables)
{
	check_stack_depth();

	if (node == NULL)
		return;

	if (IsA(node, FromExpr))
	{
		FromExpr   *fromexpr = (FromExpr *) node;
		ListCell   *lc;

		foreach(lc, fromexpr->fromlist)
			collect_from_node(query, (Node *) lfirst(lc), tables);
		classify_qual(query, fromexpr->quals, tables);
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExpr   *joinexpr = (JoinExpr *) node;

		collect_from_node(query, joinexpr->larg, tables);
		collect_from_node(query, joinexpr->rarg, tables);
		classify_qual(query, joinexpr->quals, tables);
	}
	/* RangeTblRef and anything else: nothing to do here. */
}

/*
 * collect_query_columns
 *		Gather per-table column usage for one analysed query, descending
 *		into subqueries, CTEs and set operations.
 */
static void
collect_query_columns(Query *query, List **tables)
{
	ListCell   *lc;

	check_stack_depth();

	if (query == NULL)
		return;

	if (query->commandType != CMD_SELECT &&
		query->commandType != CMD_UPDATE &&
		query->commandType != CMD_DELETE &&
		query->commandType != CMD_INSERT &&
		query->commandType != CMD_MERGE)
		return;

	/*
	 * WHERE clause and JOIN ON conditions.  For INSERT ... SELECT the source
	 * SELECT is a subquery RTE, reached by the rtable loop below; for MERGE
	 * the join lives in mergeJoinCondition, handled further down.
	 */
	collect_from_node(query, (Node *) query->jointree, tables);

	/* HAVING. */
	classify_qual(query, query->havingQual, tables);

	/* GROUP BY and ORDER BY refer to target list entries. */
	foreach(lc, query->groupClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);

		if (tle != NULL)
			record_column(query, (Node *) tle->expr, COLROLE_GROUP, tables);
	}
	foreach(lc, query->sortClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);

		if (tle != NULL)
			record_column(query, (Node *) tle->expr, COLROLE_SORT, tables);
	}

	/* DISTINCT / DISTINCT ON keys behave like ordering keys. */
	foreach(lc, query->distinctClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);

		if (tle != NULL)
			record_column(query, (Node *) tle->expr, COLROLE_SORT, tables);
	}

	/* Window PARTITION BY (grouping) and ORDER BY (sort) keys. */
	foreach(lc, query->windowClause)
	{
		WindowClause *wc = (WindowClause *) lfirst(lc);
		ListCell   *wlc;

		foreach(wlc, wc->partitionClause)
		{
			SortGroupClause *sgc = (SortGroupClause *) lfirst(wlc);
			TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);

			if (tle != NULL)
				record_column(query, (Node *) tle->expr, COLROLE_GROUP, tables);
		}
		foreach(wlc, wc->orderClause)
		{
			SortGroupClause *sgc = (SortGroupClause *) lfirst(wlc);
			TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);

			if (tle != NULL)
				record_column(query, (Node *) tle->expr, COLROLE_SORT, tables);
		}
	}

	/*
	 * MERGE: the ON condition (kept apart from the jointree at parse-analysis
	 * time) and each WHEN action's qual.
	 */
	if (query->commandType == CMD_MERGE)
	{
		classify_qual(query, query->mergeJoinCondition, tables);

		foreach(lc, query->mergeActionList)
		{
			MergeAction *action = (MergeAction *) lfirst(lc);

			classify_qual(query, action->qual, tables);
		}
	}

	/* Subqueries in FROM (this also covers UNION/INTERSECT branches). */
	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_SUBQUERY && rte->subquery != NULL)
			collect_query_columns(rte->subquery, tables);
	}

	/* WITH queries. */
	foreach(lc, query->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

		if (cte->ctequery != NULL && IsA(cte->ctequery, Query))
			collect_query_columns((Query *) cte->ctequery, tables);
	}
}

/*
 * candidate_index_name
 *		Build a deterministic name for a suggested index:
 *		dbblue_<table>_<col1>_..._idx, hashed down when it would exceed
 *		NAMEDATALEN.
 *
 * A stable name plus IF NOT EXISTS in the generated DDL makes running
 * a suggestion twice a no-op, instead of silently creating a duplicate
 * index the way an unnamed CREATE INDEX does.
 */
static char *
candidate_index_name(const char *table_name, List *colnames)
{
	StringInfoData buf;
	ListCell   *lc;

	initStringInfo(&buf);
	appendStringInfo(&buf, "dbblue_%s", table_name);
	foreach(lc, colnames)
		appendStringInfo(&buf, "_%s", (char *) lfirst(lc));
	appendStringInfoString(&buf, "_idx");

	if (buf.len >= NAMEDATALEN)
	{
		/*
		 * Too long to survive as an identifier.  Keep a recognisable
		 * prefix and disambiguate with a hash of the full name, so two
		 * long candidates that share a prefix cannot	 collide after
		 * truncation (IF NOT EXISTS would then skip the second one).
		 */
		uint32		hash = hash_bytes((const unsigned char *) buf.data,
									  buf.len);

		/*
		 * Clip on a character boundary, not a raw byte offset: a byte-offset
		 * cut can split a multibyte (e.g. UTF-8) identifier mid-character and
		 * leave invalid bytes in the stored DDL.
		 */
		buf.len = pg_mbcliplen(buf.data, buf.len, NAMEDATALEN - 10);
		buf.data[buf.len] = '\0';
		appendStringInfo(&buf, "_%08x", hash);
	}

	return buf.data;
}

/*
 * add_candidate
 *		Resolve names for a key list and append an IndexCandidate,
 *		unless the same candidate is already on the list or the list is
 *		full.  Silently skips candidates whose table or columns have
 *		disappeared since parse analysis.
 */
static void
add_candidate(List **candidates, Oid relid, int nkeys, const AttrNumber *keys)
{
	IndexCandidate *cand;
	StringInfoData ddl;
	char	   *schema_name;
	char	   *table_name;
	char	   *index_name;
	ListCell   *lc;
	int			i;

	if (list_length(*candidates) >= ADVISOR_MAX_CANDIDATES)
	{
		ereport(DEBUG1,
				(errmsg("dbblue index advisor: candidate cap (%d) reached for relation %u, dropping further candidates",
						ADVISOR_MAX_CANDIDATES, relid)));
		return;
	}

	/* Dedup against candidates gathered so far. */
	foreach(lc, *candidates)
	{
		IndexCandidate *other = (IndexCandidate *) lfirst(lc);

		if (other->relid == relid && other->nkeys == nkeys &&
			memcmp(other->keys, keys, nkeys * sizeof(AttrNumber)) == 0)
			return;
	}

	schema_name = get_namespace_name(get_rel_namespace(relid));
	table_name = get_rel_name(relid);
	if (schema_name == NULL || table_name == NULL)
		return;

	cand = (IndexCandidate *) palloc0(sizeof(IndexCandidate));
	cand->relid = relid;
	cand->schema_name = schema_name;
	cand->table_name = table_name;
	cand->nkeys = nkeys;
	cand->keys = (AttrNumber *) palloc(nkeys * sizeof(AttrNumber));
	memcpy(cand->keys, keys, nkeys * sizeof(AttrNumber));
	cand->colnames = NIL;

	for (i = 0; i < nkeys; i++)
	{
		char	   *colname = get_attname(relid, keys[i], true);

		if (colname == NULL)
			return;
		cand->colnames = lappend(cand->colnames, colname);
	}

	index_name = candidate_index_name(table_name, cand->colnames);

	initStringInfo(&ddl);
	appendStringInfo(&ddl, "CREATE INDEX IF NOT EXISTS %s ON %s (",
					 quote_identifier(index_name),
					 quote_qualified_identifier(schema_name, table_name));
	foreach(lc, cand->colnames)
	{
		if (lc != list_head(cand->colnames))
			appendStringInfoString(&ddl, ", ");
		appendStringInfoString(&ddl, quote_identifier((char *) lfirst(lc)));
	}
	appendStringInfoChar(&ddl, ')');
	cand->ddl = ddl.data;

	*candidates = lappend(*candidates, cand);
}

/*
 * build_candidates
 *		Turn the per-table column usage of one query into a list of
 *		IndexCandidates.
 *
 * For every referenced column a single-column candidate is emitted, and
 * for every table with at least two usable columns one composite
 * candidate.  The composite follows the equality → group/sort → range
 * ordering rule: equality columns narrow the scan the most, ordering
 * columns can then satisfy GROUP BY / ORDER BY, and a range column stops
 * the b-tree from being useful for anything that follows it, so it goes
 * last.
 */
static List *
build_candidates(List *tables)
{
	List	   *candidates = NIL;
	ListCell   *lc;

	foreach(lc, tables)
	{
		TableUsage *usage = (TableUsage *) lfirst(lc);
		List	   *composite = NIL;
		List	   *role_lists[4];
		ListCell   *lc2;
		int			i;

		role_lists[0] = usage->eq_cols;
		role_lists[1] = usage->group_cols;
		role_lists[2] = usage->sort_cols;
		role_lists[3] = usage->range_cols;

		/*
		 * Build the composite key list first, in equality -> group -> sort ->
		 * range order, capped at max_index_columns.
		 */
		for (i = 0; i < 4; i++)
		{
			foreach(lc2, role_lists[i])
			{
				AttrNumber	attno = (AttrNumber) lfirst_int(lc2);

				if (!list_member_int(composite, (int) attno) &&
					list_length(composite) <
					dbblue_auto_index_suggestion_max_index_columns)
					composite = lappend_int(composite, (int) attno);
			}
		}

		/*
		 * Emit the composite candidate before the single-column ones.  It is
		 * usually the most valuable suggestion, and add_candidate enforces a
		 * hard cap (ADVISOR_MAX_CANDIDATES); adding singles first could push
		 * the composite past the cap on a wide, multi-column query.
		 */
		if (list_length(composite) >= 2)
		{
			int			nkeys = list_length(composite);
			AttrNumber *keys = (AttrNumber *) palloc(nkeys * sizeof(AttrNumber));

			i = 0;
			foreach(lc2, composite)
				keys[i++] = (AttrNumber) lfirst_int(lc2);

			add_candidate(&candidates, usage->relid, nkeys, keys);
			pfree(keys);
		}

		/* Then the single-column candidates. */
		for (i = 0; i < 4; i++)
		{
			foreach(lc2, role_lists[i])
			{
				AttrNumber	attno = (AttrNumber) lfirst_int(lc2);

				add_candidate(&candidates, usage->relid, 1, &attno);
			}
		}

		list_free(composite);
	}

	return candidates;
}

/*
 * existing_index_covers
 *		True when a candidate is redundant next to an existing valid
 *		index.  Two rules:
 *
 *		1. Some existing index already has the candidate's whole key
 *		   list as its leading key columns (an index on (a, b) makes
 *		   candidates (a) and (a, b) pointless).
 *		2. Some existing UNIQUE index's key list is a leading prefix of
 *		   the candidate (a unique index on (id) makes (id, x, y)
 *		   pointless: at most one row matches the prefix, so the extra
 *		   columns can never narrow the scan).
 *
 * pg_index.indkey is an int2vector whose text form is space-separated
 * attnos, so string_to_array gives us a comparable int2[]; only the
 * first indnkeyatts entries are key columns (the rest are INCLUDE).
 */
static bool
existing_index_covers(Oid relid, int nkeys, const AttrNumber *keys)
{
	StringInfoData arr;
	StringInfoData sql;
	bool		covered = false;
	int			i;

	/* The candidate's key list as an int2[] literal. */
	initStringInfo(&arr);
	appendStringInfoString(&arr, "ARRAY[");
	for (i = 0; i < nkeys; i++)
		appendStringInfo(&arr, "%s%d", i > 0 ? "," : "", (int) keys[i]);
	appendStringInfoString(&arr, "]::int2[]");

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT 1 FROM pg_catalog.pg_index i "
					 "JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid "
					 "WHERE i.indrelid = %u AND i.indisvalid "
					 "AND i.indpred IS NULL AND c.relam = %u "
					 "AND ((i.indnkeyatts >= %d AND "
					 "      (string_to_array(i.indkey::text, ' ')::int2[])[1:%d] = %s) "
					 " OR  (i.indisunique AND i.indnkeyatts <= %d AND "
					 "      (string_to_array(i.indkey::text, ' ')::int2[])[1:i.indnkeyatts] "
					 "      = (%s)[1:i.indnkeyatts])) "
					 "LIMIT 1",
					 relid, BTREE_AM_OID, nkeys, nkeys, arr.data, nkeys, arr.data);

	if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
		covered = true;

	pfree(sql.data);
	pfree(arr.data);
	return covered;
}

/*
 * advisor_plan_cost
 *		Return the planner's estimated total cost for a query, or -1 if
 *		it cannot be planned.
 *
 * GENERIC_PLAN makes EXPLAIN accept the $n parameter placeholders that
 * pg_stat_statements leaves in normalised query text.  The first
 * "Total Cost" key in the JSON output belongs to the topmost plan node.
 */
static double
advisor_plan_cost(const char *query_text, Oid userid)
{
	char	   *sql;
	double		cost = -1.0;
	int			ret;
	Oid			save_userid;
	int			save_sec_context;

	sql = psprintf("EXPLAIN (GENERIC_PLAN, COSTS ON, FORMAT JSON) %s",
				   query_text);

	/*
	 * Plan the untrusted query text as the user who ran it, inside a
	 * restricted security context.  Planning constant-folds immutable
	 * functions embedded in the query text, which executes them; doing so
	 * under the advisor's (superuser) identity would let any user who can
	 * get a query into pg_stat_statements run code as superuser.  Planning
	 * as the original user also makes the estimated cost honour that user's
	 * row-level security policies.
	 *
	 * On error SPI_execute longjmps out before the identity is restored;
	 * the enclosing subtransaction's abort path restores it.  On success it
	 * is restored explicitly right after the call.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(userid,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);

	ret = SPI_execute(sql, false, 0);

	SetUserIdAndSecContext(save_userid, save_sec_context);

	if ((ret == SPI_OK_UTILITY || ret == SPI_OK_SELECT) &&
		SPI_processed > 0 && SPI_tuptable != NULL)
	{
		char	   *plan = SPI_getvalue(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc, 1);

		if (plan != NULL)
		{
			const char *key = strstr(plan, "\"Total Cost\":");

			if (key != NULL)
				cost = strtod(key + strlen("\"Total Cost\":"), NULL);
			pfree(plan);
		}
	}

	pfree(sql);
	return cost;
}

/*
 * advisor_store_suggestion
 *		Upsert one winning candidate into the results table.  Values are
 *		passed as parameters, so hostile query text cannot inject SQL.
 */
static void
advisor_store_suggestion(WorkloadEntry *entry, IndexCandidate *cand,
						 double baseline, double hypo_cost, double reduction)
{
	Oid			argtypes[13] = {
		TEXTOID, TEXTOID, TEXTARRAYOID, TEXTOID, INT8OID, TEXTOID,
		INT8OID, FLOAT8OID, FLOAT8OID, FLOAT8OID, FLOAT8OID, FLOAT8OID,
		TIMESTAMPTZOID
	};
	Datum		values[13];
	Datum	   *colnames;
	ListCell   *lc;
	int			i = 0;
	int			ret;

	colnames = (Datum *) palloc(list_length(cand->colnames) * sizeof(Datum));
	foreach(lc, cand->colnames)
		colnames[i++] = CStringGetTextDatum((char *) lfirst(lc));

	values[0] = CStringGetTextDatum(cand->schema_name);
	values[1] = CStringGetTextDatum(cand->table_name);
	values[2] = PointerGetDatum(construct_array_builtin(colnames,
														list_length(cand->colnames),
														TEXTOID));
	values[3] = CStringGetTextDatum(cand->ddl);
	values[4] = Int64GetDatum(entry->queryid);
	values[5] = CStringGetTextDatum(entry->query_text);
	values[6] = Int64GetDatum(entry->calls);
	values[7] = Float8GetDatum(entry->total_exec_ms);
	values[8] = Float8GetDatum(baseline);
	values[9] = Float8GetDatum(hypo_cost);
	values[10] = Float8GetDatum(reduction);

	/*
	 * Per-query planner cost the index would save across all recorded calls
	 * of this query.  The upsert below sums this over every query in the
	 * current pass that the same index helps (tracked in queries_helped), so
	 * the stored estimated_benefit reflects the index's total value, not just
	 * the last query processed.  Sorting by it separates a big win on a hot
	 * query from an equally large *percentage* win on a trivial one.
	 */
	values[11] = Float8GetDatum((baseline - hypo_cost) * (double) entry->calls);

	/* Pass start, so the upsert can tell "this pass" from "an earlier one". */
	values[12] = TimestampTzGetDatum(advisor_pass_start);

	ret = SPI_execute_with_args(
								"INSERT INTO public." ADVISOR_RESULT_TABLE
								" (schema_name, table_name, index_columns, suggested_index,"
								"  queryid, sample_query, query_calls, query_total_exec_ms,"
								"  cost_before, cost_after, cost_reduction, estimated_benefit)"
								" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"
								" ON CONFLICT ON CONSTRAINT dbblue_index_suggestions_ddl_unique"
								" DO UPDATE SET"
								"  last_suggested = now(),"
								"  times_suggested = " ADVISOR_RESULT_TABLE ".times_suggested + 1,"
								"  queryid = EXCLUDED.queryid,"
								"  sample_query = EXCLUDED.sample_query,"
								"  query_calls = EXCLUDED.query_calls,"
								"  query_total_exec_ms = EXCLUDED.query_total_exec_ms,"
								"  cost_before = EXCLUDED.cost_before,"
								"  cost_after = EXCLUDED.cost_after,"
								"  cost_reduction = EXCLUDED.cost_reduction,"
								/*
								 * A row last touched before this pass started is
								 * a stale snapshot: reset the benefit and the
								 * helped-query count.  A row already touched in
								 * this pass came from another query hitting the
								 * same index: accumulate.
								 */
								"  estimated_benefit = CASE"
								"    WHEN " ADVISOR_RESULT_TABLE ".last_suggested < $13"
								"      THEN EXCLUDED.estimated_benefit"
								"    ELSE COALESCE(" ADVISOR_RESULT_TABLE ".estimated_benefit, 0)"
								"         + EXCLUDED.estimated_benefit END,"
								"  queries_helped = CASE"
								"    WHEN " ADVISOR_RESULT_TABLE ".last_suggested < $13"
								"      THEN 1"
								"    ELSE " ADVISOR_RESULT_TABLE ".queries_helped + 1 END",
								13, argtypes, values, NULL, false, 0);

	if (ret != SPI_OK_INSERT && ret != SPI_OK_UPDATE)
		ereport(WARNING,
				(errmsg("dbblue index advisor: storing suggestion \"%s\" failed (SPI result %d)",
						cand->ddl, ret)));
}

/*
 * advisor_evaluate_candidate
 *		Price one candidate with HypoPG and record it when the planner
 *		cost drops enough.  Runs in its own subtransaction so a failing
 *		candidate (unsupported column type, dropped table, ...) does not
 *		spoil its siblings.
 */
static void
advisor_evaluate_candidate(WorkloadEntry *entry, IndexCandidate *cand,
						   double baseline)
{
	MemoryContext oldcxt = CurrentMemoryContext;
	ResourceOwner oldowner = CurrentResourceOwner;

	BeginInternalSubTransaction(NULL);

	PG_TRY();
	{
		char	   *hypo_sql;
		double		hypo_cost;

		hypo_sql = psprintf("SELECT hypopg_create_index(%s)",
							quote_literal_cstr(cand->ddl));
		(void) SPI_execute(hypo_sql, false, 0);
		pfree(hypo_sql);

		hypo_cost = advisor_plan_cost(entry->query_text, entry->userid);

		if (hypo_cost >= 0.0 && hypo_cost < baseline)
		{
			double		reduction = (baseline - hypo_cost) / baseline;

			if (reduction >= dbblue_auto_index_suggestion_min_cost_improvement)
			{
				advisor_store_suggestion(entry, cand, baseline, hypo_cost,
										 reduction);
				ereport(LOG,
						(errmsg("dbblue index advisor: suggestion recorded for database \"%s\": %s (cost %.2f -> %.2f, %.1f%% reduction, queryid %lld)",
								advisor_dbname, cand->ddl, baseline, hypo_cost,
								reduction * 100.0,
								(long long) entry->queryid)));
			}
			else
				ereport(DEBUG1,
						(errmsg("dbblue index advisor: %s below threshold (cost %.2f -> %.2f)",
								cand->ddl, baseline, hypo_cost)));
		}

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcxt);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcxt);
		edata = CopyErrorData();
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcxt);
		CurrentResourceOwner = oldowner;

		ereport(LOG,
				(errmsg("dbblue index advisor: evaluating candidate \"%s\" in database \"%s\" failed: %s",
						cand->ddl, advisor_dbname, edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();

	/*
	 * HypoPG's hypothetical indexes live in backend-local memory and are
	 * not rolled back with the subtransaction, so always drop them before
	 * the next candidate is priced.
	 */
	(void) advisor_exec_in_subxact("SELECT hypopg_reset()",
								   "resetting hypothetical indexes");
}

/*
 * advisor_process_entry
 *		Analyse one pg_stat_statements entry end-to-end inside its own
 *		transaction.
 */
static void
advisor_process_entry(WorkloadEntry *entry)
{
	List	   *volatile candidates = NIL;
	volatile double baseline;
	MemoryContext oldcxt;
	ResourceOwner oldowner;
	ListCell   *lc;

	if (strlen(entry->query_text) > ADVISOR_MAX_QUERY_LEN)
	{
		ereport(DEBUG1,
				(errmsg("dbblue index advisor: queryid %lld skipped, text longer than %d bytes",
						(long long) entry->queryid, ADVISOR_MAX_QUERY_LEN)));
		return;
	}

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue index advisor: analysing workload");

	/*
	 * Parse analysis can fail for all sorts of legitimate reasons (the
	 * query references dropped objects, parameter types cannot be
	 * derived, ...), so it runs subtransaction-protected; on failure the
	 * entry is skipped.
	 */
	oldcxt = CurrentMemoryContext;
	oldowner = CurrentResourceOwner;

	BeginInternalSubTransaction(NULL);
	PG_TRY();
	{
		List	   *parsetree_list;
		List	   *tables = NIL;

		parsetree_list = pg_parse_query(entry->query_text);

		if (list_length(parsetree_list) == 1)
		{
			RawStmt    *raw = linitial_node(RawStmt, parsetree_list);

			if (IsA(raw->stmt, SelectStmt) ||
				IsA(raw->stmt, UpdateStmt) ||
				IsA(raw->stmt, DeleteStmt) ||
				IsA(raw->stmt, InsertStmt) ||
				IsA(raw->stmt, MergeStmt))
			{
				Oid		   *paramtypes = NULL;
				int			numparams = 0;
				Query	   *query;

				query = parse_analyze_varparams(raw, entry->query_text,
												&paramtypes, &numparams,
												NULL);

				collect_query_columns(query, &tables);
				candidates = build_candidates(tables);
			}
		}

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcxt);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcxt);
		edata = CopyErrorData();
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcxt);
		CurrentResourceOwner = oldowner;

		ereport(DEBUG1,
				(errmsg("dbblue index advisor: queryid %lld not analysable: %s",
						(long long) entry->queryid, edata->message)));
		FreeErrorData(edata);
		candidates = NIL;
	}
	PG_END_TRY();

	/*
	 * Drop candidates an existing index already covers.
	 *
	 * This queries the catalogs through SPI, so it runs
	 * subtransaction-protected like every other fallible step of the pass.
	 * Without that, an error here would longjmp all the way out to the
	 * worker's main loop, skipping not just this entry but every remaining
	 * statement in the pass and the stale-suggestion prune at the end of
	 * it.  On failure we keep the unfiltered list: a redundant suggestion
	 * costs the user a glance, whereas losing the pass costs the interval.
	 */
	if (candidates != NIL)
	{
		BeginInternalSubTransaction(NULL);
		PG_TRY();
		{
			List	   *kept = NIL;

			foreach(lc, candidates)
			{
				IndexCandidate *cand = (IndexCandidate *) lfirst(lc);

				if (!existing_index_covers(cand->relid, cand->nkeys,
										   cand->keys))
					kept = lappend(kept, cand);
			}
			candidates = kept;

			ReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcxt);
			CurrentResourceOwner = oldowner;
		}
		PG_CATCH();
		{
			ErrorData  *edata;

			MemoryContextSwitchTo(oldcxt);
			edata = CopyErrorData();
			FlushErrorState();
			RollbackAndReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcxt);
			CurrentResourceOwner = oldowner;

			ereport(DEBUG1,
					(errmsg("dbblue index advisor: queryid %lld redundancy check failed, keeping all candidates: %s",
							(long long) entry->queryid, edata->message)));
			FreeErrorData(edata);
		}
		PG_END_TRY();
	}

	if (candidates != NIL)
	{
		/* Make sure no hypothetical leftovers skew the baseline. */
		(void) advisor_exec_in_subxact("SELECT hypopg_reset()",
									   "resetting hypothetical indexes");

		baseline = -1.0;

		BeginInternalSubTransaction(NULL);
		PG_TRY();
		{
			baseline = advisor_plan_cost(entry->query_text, entry->userid);
			ReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcxt);
			CurrentResourceOwner = oldowner;
		}
		PG_CATCH();
		{
			ErrorData  *edata;

			MemoryContextSwitchTo(oldcxt);
			edata = CopyErrorData();
			FlushErrorState();
			RollbackAndReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcxt);
			CurrentResourceOwner = oldowner;

			ereport(DEBUG1,
					(errmsg("dbblue index advisor: queryid %lld baseline EXPLAIN failed: %s",
							(long long) entry->queryid, edata->message)));
			FreeErrorData(edata);
			baseline = -1.0;
		}
		PG_END_TRY();

		/*
		 * Skip cheap queries: an index that halves a cost of 8 saves
		 * nothing worth acting on, even though it clears any relative
		 * improvement threshold.
		 */
		if (baseline > 0.0 &&
			baseline < dbblue_auto_index_suggestion_min_baseline_cost)
		{
			ereport(DEBUG1,
					(errmsg("dbblue index advisor: queryid %lld skipped, baseline cost %.2f below dbblue_auto_index_suggestion_min_baseline_cost",
							(long long) entry->queryid, baseline)));
			baseline = -1.0;
		}

		if (baseline > 0.0)
		{
			ereport(DEBUG1,
					(errmsg("dbblue index advisor: queryid %lld: %d candidate(s), baseline cost %.2f",
							(long long) entry->queryid,
							list_length(candidates), baseline)));

			foreach(lc, candidates)
			{
				if (ShutdownRequestPending)
					break;
				advisor_evaluate_candidate(entry,
										   (IndexCandidate *) lfirst(lc),
										   baseline);
			}
		}
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * advisor_load_workload
 *		Read the top-N most expensive statements of the connected
 *		database from pg_stat_statements.  The returned list and its
 *		entries live in advisor_cxt.
 */
static List *
advisor_load_workload(void)
{
	List	   *entries = NIL;
	char	   *sql;
	int			ret;
	uint64		i;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue index advisor: reading pg_stat_statements");

	sql = psprintf("SELECT queryid, calls, total_exec_time, query, userid "
				   "FROM pg_stat_statements "
				   "WHERE dbid = %u AND toplevel "
				   "AND queryid IS NOT NULL AND calls >= %d "
				   "ORDER BY total_exec_time DESC "
				   "LIMIT %d",
				   MyDatabaseId,
				   dbblue_auto_index_suggestion_min_calls,
				   dbblue_auto_index_suggestion_top_n_queries);

	ret = SPI_execute(sql, true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errmsg("dbblue index advisor: querying pg_stat_statements failed (SPI result %d)",
						ret)));

	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tuple = SPI_tuptable->vals[i];
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		bool		null_qid,
					null_calls,
					null_time,
					null_text,
					null_user;
		Datum		d_qid,
					d_calls,
					d_time,
					d_text,
					d_user;
		WorkloadEntry *entry;
		MemoryContext oldcxt;

		d_qid = SPI_getbinval(tuple, tupdesc, 1, &null_qid);
		d_calls = SPI_getbinval(tuple, tupdesc, 2, &null_calls);
		d_time = SPI_getbinval(tuple, tupdesc, 3, &null_time);
		d_text = SPI_getbinval(tuple, tupdesc, 4, &null_text);
		d_user = SPI_getbinval(tuple, tupdesc, 5, &null_user);

		if (null_qid || null_calls || null_time || null_text || null_user)
			continue;

		oldcxt = MemoryContextSwitchTo(advisor_cxt);
		entry = (WorkloadEntry *) palloc(sizeof(WorkloadEntry));
		entry->queryid = DatumGetInt64(d_qid);
		entry->calls = DatumGetInt64(d_calls);
		entry->total_exec_ms = DatumGetFloat8(d_time);
		entry->query_text = TextDatumGetCString(d_text);
		entry->userid = DatumGetObjectId(d_user);
		entries = lappend(entries, entry);
		MemoryContextSwitchTo(oldcxt);
	}

	pfree(sql);
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	return entries;
}

/*
 * advisor_prune_stale
 *		Delete suggestions that the pass which started at pass_start did
 *		not re-confirm.  Every still-valid candidate gets its
 *		last_suggested refreshed during the pass, so anything older is
 *		obsolete: the operator created the index in the meantime, or the
 *		workload changed and the candidate no longer wins.  This keeps
 *		the results table an up-to-date snapshot of the current advice.
 */
static void
advisor_prune_stale(TimestampTz pass_start)
{
	Oid			argtypes[1] = {TIMESTAMPTZOID};
	Datum		values[1];
	int			ret;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue index advisor: pruning stale suggestions");

	values[0] = TimestampTzGetDatum(pass_start);
	ret = SPI_execute_with_args("DELETE FROM public." ADVISOR_RESULT_TABLE
								" WHERE last_suggested < $1",
								1, argtypes, values, NULL, false, 0);

	if (ret == SPI_OK_DELETE && SPI_processed > 0)
		ereport(LOG,
				(errmsg("dbblue index advisor: removed " UINT64_FORMAT " stale suggestion(s) from database \"%s\"",
						(uint64) SPI_processed, advisor_dbname)));

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * advisor_run_pass
 *		One full analysis pass.
 */
static void
advisor_run_pass(void)
{
	List	   *entries;
	ListCell   *lc;
	TimestampTz pass_start = GetCurrentTimestamp();

	advisor_pass_start = pass_start;

	entries = advisor_load_workload();

	ereport(LOG,
			(errmsg("dbblue index advisor: analysing %d statement(s) from pg_stat_statements in database \"%s\"",
					list_length(entries), advisor_dbname)));

	foreach(lc, entries)
	{
		if (ShutdownRequestPending)
			break;
		CHECK_FOR_INTERRUPTS();

		advisor_process_entry((WorkloadEntry *) lfirst(lc));
	}

	/*
	 * Prune only after a complete pass; an interrupted pass must not wipe
	 * suggestions it did not get around to re-confirming.
	 *
	 * Also skip pruning when the workload came back empty: a
	 * pg_stat_statements_reset() (or eviction from its fixed-size hash)
	 * leaves nothing to re-confirm, and pruning then would delete every
	 * accumulated suggestion.  Keeping stale advice for one more interval is
	 * far better than discarding all of it because statistics were reset.
	 */
	if (!ShutdownRequestPending && entries != NIL)
		advisor_prune_stale(pass_start);
	else if (entries == NIL)
		ereport(DEBUG1,
				(errmsg("dbblue index advisor: empty workload, keeping existing suggestions")));

	/* Free the workload copies. */
	MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextReset(advisor_cxt);
	MemoryContextSwitchTo(advisor_cxt);
}

/*
 * DbblueIndexAdvisorMain
 *		Per-database worker: connect to the database whose OID the launcher
 *		passed, run one analysis pass, exit.
 *
 * The launcher passes every database it could connect to, not only the
 * enabled ones, because it cannot read the per-database settings itself.
 * InitPostgres has applied this database's ALTER DATABASE settings by the
 * time this runs, so the check below is reading this database's effective
 * dbblue_auto_index_suggestion_enabled: that is what makes the feature
 * per-database, and a disabled database costs one connect-and-exit.
 *
 * Being short-lived is the point: nothing is held open between sweeps, so
 * DROP DATABASE on an analysed database is never blocked.
 */
void
DbblueIndexAdvisorMain(Datum main_arg)
{
	Oid			dboid = DatumGetObjectId(main_arg);
	sigjmp_buf	local_sigjmp_buf;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	advisor_dbname = MyBgworkerEntry->bgw_extra;

	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);

	advisor_cxt = AllocSetContextCreate(TopMemoryContext,
										"dbblue index advisor",
										ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(advisor_cxt);

	/*
	 * One shot, so an unexpected error ends this worker rather than being
	 * recovered from: the launcher starts a fresh one next sweep.  Report it
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
		MemoryContextReset(advisor_cxt);

		pgstat_report_activity(STATE_IDLE, NULL);

		RESUME_INTERRUPTS();

		proc_exit(1);
	}
	PG_exception_stack = &local_sigjmp_buf;

	/* This is where the feature is actually switched on or off. */
	if (!dbblue_auto_index_suggestion_enabled)
	{
		ereport(DEBUG1,
				(errmsg("dbblue index advisor: disabled in database \"%s\", nothing to do",
						advisor_dbname)));
		proc_exit(0);
	}

	/* The advisor writes its suggestions, so it cannot run on a standby. */
	if (RecoveryInProgress())
		proc_exit(0);

	if (ShutdownRequestPending)
		proc_exit(0);

	/*
	 * Keep the advisor's own statements (EXPLAINs, hypopg calls, the
	 * result-table upserts) out of pg_stat_statements.  Without this, every
	 * pass roughly doubles the extension's entry count, evicts genuine
	 * workload queries from its fixed-size hash table, and the advisor
	 * eventually starts analysing its own queries.  The setting is
	 * session-local to this worker; client backends are unaffected.
	 */
	SetConfigOption("pg_stat_statements.track", "none",
					PGC_SUSET, PGC_S_SESSION);

	/*
	 * Bound how long the advisor will wait for a lock.  Planning an
	 * EXPLAINed UPDATE/DELETE takes RowExclusiveLock on the target table at
	 * parse-analysis time; if that queues behind an ACCESS EXCLUSIVE waiter
	 * (ALTER TABLE, VACUUM FULL, ...), the worker would wait indefinitely and
	 * stall everything queued behind it.  On timeout the statement errors and
	 * the candidate is skipped by its subtransaction.  lock_timeout is
	 * honoured in ProcSleep regardless of the command path, unlike
	 * statement_timeout, which is armed only by the normal client command
	 * loop this worker does not use.
	 */
	SetConfigOption("lock_timeout", "5s", PGC_SUSET, PGC_S_SESSION);

	advisor_ensure_environment();

	if (!advisor_env_ready)
	{
		/*
		 * pg_stat_statements, hypopg or the results table could not be set up
		 * here.  advisor_ensure_environment() has already warned with the
		 * reason; exit quietly rather than analysing half-configured.
		 */
		proc_exit(0);
	}

	advisor_run_pass();

	proc_exit(0);
}

/*
 * DbblueIndexAdvisorLauncherMain
 *		Main entry point for the launcher process.
 */
void
DbblueIndexAdvisorLauncherMain(Datum main_arg)
{
	sigjmp_buf	local_sigjmp_buf;

	/* volatile: assigned in the error handler below and read after it. */
	volatile TimestampTz next_run = 0;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	/*
	 * No database.  pg_database is a shared catalog, so this is enough to
	 * list the cluster's databases, and it means the launcher never counts as
	 * a connection to any of them -- DROP DATABASE on an analysed database is
	 * not blocked by the advisor being enabled.
	 */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	ereport(LOG, (errmsg("dbblue index advisor launcher started")));

	advisor_cxt = AllocSetContextCreate(TopMemoryContext,
										"dbblue index advisor launcher",
										ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(advisor_cxt);

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
		MemoryContextReset(advisor_cxt);
		MemoryContextSwitchTo(advisor_cxt);

		pgstat_report_activity(STATE_IDLE, NULL);

		/*
		 * Defer the next sweep.  The error aborted this one part-way through,
		 * so next_run still holds whatever it did before -- typically 0, on
		 * the very first sweep -- and jumping straight back into the loop
		 * would retry immediately and spin on a persistent failure.
		 */
		next_run = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
											   (int64) dbblue_auto_index_suggestion_interval * 1000);

		RESUME_INTERRUPTS();
	}
	PG_exception_stack = &local_sigjmp_buf;

	for (;;)
	{
		long		sleep_ms;
		int64		interval_ms;

		CHECK_FOR_INTERRUPTS();

		if (ShutdownRequestPending)
			break;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);

			/*
			 * The cluster-wide dbblue_auto_index_suggestion_enabled is the
			 * default for every database without a setting of its own, and
			 * the interval itself may have changed, so re-evaluate at once
			 * rather than at the end of the current interval.
			 */
			next_run = 0;
		}

		interval_ms = (int64) dbblue_auto_index_suggestion_interval * 1000;

		/*
		 * The advisor writes suggestions, so nothing can run on a standby;
		 * not starting the workers at all keeps a standby from spawning one
		 * per database for no reason.
		 */
		if (RecoveryInProgress())
		{
			next_run = 0;
			sleep_ms = (long) interval_ms;
		}
		else
		{
			/*
			 * Paced by next_run rather than by having woken up: the
			 * per-database workers set our latch as they start and stop, and
			 * without this the last one of a sweep would trigger an
			 * immediate extra sweep.
			 */
			if (next_run == 0 || GetCurrentTimestamp() >= next_run)
			{
				List	   *databases;
				ListCell   *lc;

				/*
				 * Re-read every sweep, so a database created since the last
				 * one is picked up and one dropped since simply drops out of
				 * the list.  An ALTER DATABASE ... SET
				 * dbblue_auto_index_suggestion_enabled needs nothing more
				 * than this either: the value is read by the worker, at
				 * connect time, on every sweep.
				 */
				databases = advisor_get_database_list();

				foreach(lc, databases)
				{
					if (ShutdownRequestPending)
						break;

					advisor_run_one_database((AdvisorDatabase *) lfirst(lc));
				}

				/* Discard this sweep's database list. */
				MemoryContextSwitchTo(TopMemoryContext);
				MemoryContextReset(advisor_cxt);
				MemoryContextSwitchTo(advisor_cxt);

				next_run = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
													   interval_ms);
			}

			sleep_ms = (long) ((next_run - GetCurrentTimestamp()) / 1000);
			sleep_ms = Max(sleep_ms, 1000);
		}

		if (ShutdownRequestPending)
			break;

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 sleep_ms,
						 WAIT_EVENT_DBBLUE_INDEX_ADVISOR_LAUNCHER_MAIN);
		ResetLatch(MyLatch);
	}

	ereport(LOG, (errmsg("dbblue index advisor launcher shutting down")));

	/*
	 * Exit non-zero on SIGTERM.  A background worker that exits 0 is treated
	 * by the postmaster as "terminate and forget" (rw_terminate), so a
	 * pg_terminate_backend() on the launcher would remove it until the next
	 * server restart even while the feature is still enabled.  Exiting 1 is
	 * not treated as a crash (no cluster-wide restart), but it does let
	 * bgw_restart_time bring the launcher back.
	 */
	proc_exit(1);
}

/*
 * DbblueIndexAdvisorRegister
 *		Register the advisor launcher at postmaster startup.
 *
 * Always registered, and just the one worker: which databases are analysed
 * is decided per sweep by the launcher, from each database's own
 * dbblue_auto_index_suggestion_enabled.  Nothing here depends on a setting
 * read at startup, which is what removes the restart the old single-database
 * advisor needed to be re-pointed.
 */
void
DbblueIndexAdvisorRegister(void)
{
	BackgroundWorker bgw;

	if (IsBinaryUpgrade)
		return;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN,
			 "DbblueIndexAdvisorLauncherMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "dbblue index advisor launcher");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "dbblue index advisor launcher");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}
