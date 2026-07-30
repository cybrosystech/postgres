/*-------------------------------------------------------------------------
 *
 * dbblue_partition_bgw.c
 *		DBblue-branded partition maintenance background worker.
 *
 * DBblue vendors pg_partman unmodified, but everything a DBblue operator
 * touches must be namespaced dbblue_* -- nobody configuring an Odoo
 * database should need to know which third-party engine maintains the
 * partitions underneath.  This worker therefore replaces pg_partman_bgw
 * as the supported way to run scheduled maintenance: it exposes only
 * dbblue_partition.* GUCs and simply calls pg_partman's run_maintenance()
 * in the configured database(s), resolving pg_partman's schema at run
 * time.  The vendored pg_partman_bgw keeps building but is not meant to
 * be preloaded on a DBblue cluster.
 *
 * Configuration (postgresql.conf):
 *
 *		shared_preload_libraries = 'dbblue_partition_bgw'
 *		dbblue_partition.maintenance_dbname   = 'odoo'   # comma-separated list
 *		dbblue_partition.maintenance_interval = 3600     # seconds, SIGHUP
 *		dbblue_partition.maintenance_role     = ''       # '' = cluster owner
 *
 * One static worker is registered per listed database.  With no database
 * configured the library loads but starts nothing, per the DBblue
 * off-by-default convention.
 *
 * Preloading this library also turns dbblue_partition.enabled into a
 * declared GUC (still off by default), so SHOW works before any SET.
 *
 * contrib/dbblue_partition/src/dbblue_partition_bgw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC;

#define DBBLUE_PARTITION_BGW_MAX_DBS	8

static bool dbblue_partition_enabled = false;
static char *dbblue_partition_maintenance_dbname = NULL;
static int	dbblue_partition_maintenance_interval = 3600;
static char *dbblue_partition_maintenance_role = NULL;

pg_noreturn PGDLLEXPORT void dbblue_partition_bgw_main(Datum main_arg);

/*
 * Run one maintenance pass in the connected database: locate pg_partman
 * and call its run_maintenance().  A database without pg_partman (or with
 * nothing to maintain) is not an error; the extension may simply not be
 * installed there yet.
 *
 * Errors are the caller's problem: see dbblue_partition_bgw_run_maintenance().
 */
static void
dbblue_partition_bgw_maintenance_pass(void)
{
	StringInfoData buf;
	char	   *partman_schema;
	bool		isnull;
	int			ret;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "dbblue_partition: partition maintenance");

	initStringInfo(&buf);
	appendStringInfoString(&buf,
						   "SELECT pg_catalog.quote_ident(n.nspname)"
						   " FROM pg_catalog.pg_extension e"
						   " JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace"
						   " WHERE e.extname = 'pg_partman'");

	ret = SPI_execute(buf.data, true, 1);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "could not determine pg_partman schema: SPI error code %d", ret);

	if (SPI_processed == 0)
	{
		ereport(DEBUG1,
				(errmsg("dbblue_partition maintenance: pg_partman is not installed in database \"%s\", nothing to do",
						MyBgworkerEntry->bgw_extra)));
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
		return;
	}

	{
		Datum		schema_datum;

		schema_datum = SPI_getbinval(SPI_tuptable->vals[0],
									 SPI_tuptable->tupdesc,
									 1, &isnull);
		if (isnull)
			elog(ERROR, "pg_partman schema lookup returned NULL");
		partman_schema = TextDatumGetCString(schema_datum);
	}

	resetStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT %s.run_maintenance(p_analyze := false, p_jobmon := false)",
					 partman_schema);
	pgstat_report_activity(STATE_RUNNING, buf.data);

	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "could not run partition maintenance: SPI error code %d", ret);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	/*
	 * No catalog access here: the transaction is closed, so resolving the
	 * database name again would read the syscache without a snapshot.
	 */
	ereport(DEBUG1,
			(errmsg("dbblue_partition maintenance completed in database \"%s\"",
					MyBgworkerEntry->bgw_extra)));
}

/*
 * Run one maintenance pass, surviving any error it raises.
 *
 * Letting an error escape would terminate the worker, and the postmaster
 * would restart it after bgw_restart_time seconds -- which, because the
 * restarted worker runs a pass immediately, turns any persistent failure
 * (a partition set whose table was dropped, a lock timeout, insufficient
 * privilege on one table) into a retry loop far tighter than the
 * configured interval.  Log the failure instead and wait for the next
 * scheduled pass.
 */
static void
dbblue_partition_bgw_run_maintenance(void)
{
	MemoryContext caller_context = CurrentMemoryContext;

	PG_TRY();
	{
		dbblue_partition_bgw_maintenance_pass();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(caller_context);
		edata = CopyErrorData();
		FlushErrorState();

		/* Release whatever the failed pass left open. */
		AbortOutOfAnyTransaction();
		pgstat_report_activity(STATE_IDLE, NULL);

		ereport(LOG,
				(errmsg("dbblue_partition maintenance failed in database \"%s\": %s",
						MyBgworkerEntry->bgw_extra, edata->message),
				 errdetail("Retrying at the next scheduled run, in %d second(s).",
						   dbblue_partition_maintenance_interval)));
		FreeErrorData(edata);
	}
	PG_END_TRY();
}

/*
 * Main loop: connect to the database named in bgw_extra and run
 * maintenance every dbblue_partition.maintenance_interval seconds.
 */
void
dbblue_partition_bgw_main(Datum main_arg)
{
	char	   *dbname = MyBgworkerEntry->bgw_extra;
	char	   *role = dbblue_partition_maintenance_role;
	TimestampTz next_run;

	/*
	 * SIGTERM maps to die(), not SignalHandlerForShutdownRequest: an
	 * in-flight run_maintenance() can take minutes and must be cancellable
	 * (fast shutdown, DROP DATABASE ... FORCE), and exiting 0 on a targeted
	 * pg_terminate_backend() would deregister the worker for the life of
	 * the postmaster, silently stopping maintenance.  die() exits 1, so the
	 * postmaster restarts the worker after bgw_restart_time instead.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	if (role != NULL && role[0] == '\0')
		role = NULL;			/* default: the bootstrap superuser */

	BackgroundWorkerInitializeConnection(dbname, role, 0);
	pgstat_report_appname("dbblue_partition maintenance worker");

	ereport(LOG,
			(errmsg("dbblue_partition maintenance worker started for database \"%s\" (interval: %ds)",
					dbname, dbblue_partition_maintenance_interval)));

	dbblue_partition_bgw_run_maintenance();
	next_run = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
										   dbblue_partition_maintenance_interval * 1000L);

	for (;;)
	{
		long		timeout;

		/*
		 * Wait until the next scheduled run.  Anything that sets the latch
		 * (a config reload, a shutdown request) wakes us early, so the
		 * decision to run is made from the clock rather than from the fact
		 * that we woke up -- otherwise a SIGHUP arriving during a pass would
		 * immediately trigger another one.
		 */
		timeout = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), next_run);

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 timeout, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (GetCurrentTimestamp() >= next_run)
		{
			dbblue_partition_bgw_run_maintenance();
			next_run = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
												   dbblue_partition_maintenance_interval * 1000L);
		}
	}
}

void
_PG_init(void)
{
	List	   *dblist;
	ListCell   *lc;
	char	   *rawstring;
	int			nworkers = 0;

	DefineCustomBoolVariable("dbblue_partition.enabled",
							 "Allow dbblue_partition's table conversion functions to run.",
							 NULL,
							 &dbblue_partition_enabled,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("dbblue_partition.maintenance_dbname",
							   "Database(s) in which the DBblue partition maintenance worker runs.",
							   "Comma-separated list; empty disables the worker (DBblue default).",
							   &dbblue_partition_maintenance_dbname,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomIntVariable("dbblue_partition.maintenance_interval",
							"Seconds between partition maintenance runs.",
							NULL,
							&dbblue_partition_maintenance_interval,
							3600, 10, INT_MAX / 1000,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomStringVariable("dbblue_partition.maintenance_role",
							   "Role the maintenance worker connects as.",
							   "Empty means the bootstrap superuser.",
							   &dbblue_partition_maintenance_role,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	MarkGUCPrefixReserved("dbblue_partition");

	if (!process_shared_preload_libraries_in_progress)
		return;

	if (dbblue_partition_maintenance_dbname == NULL ||
		dbblue_partition_maintenance_dbname[0] == '\0')
	{
		ereport(LOG,
				(errmsg("dbblue_partition maintenance worker not started: dbblue_partition.maintenance_dbname is not set")));
		return;
	}

	rawstring = pstrdup(dbblue_partition_maintenance_dbname);

	/*
	 * A malformed list must not stop the cluster: _PG_init runs in the
	 * postmaster during shared_preload_libraries processing, where there is
	 * no exception stack, so an ERROR here would prevent startup entirely --
	 * an easy thing to trigger with a trailing comma while editing the list.
	 * Log and start no worker instead.
	 *
	 * Note that unquoted names are folded to lower case, as everywhere else
	 * in PostgreSQL: a mixed-case database needs '"MixedCase"'.
	 */
	if (!SplitIdentifierString(rawstring, ',', &dblist))
	{
		ereport(LOG,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid list syntax in dbblue_partition.maintenance_dbname: \"%s\"",
						dbblue_partition_maintenance_dbname),
				 errdetail("No partition maintenance worker was started."),
				 errhint("Use a comma-separated list of database names, and double-quote any name that is not all lower case.")));
		pfree(rawstring);
		return;
	}

	foreach(lc, dblist)
	{
		char	   *dbname = (char *) lfirst(lc);
		BackgroundWorker worker;

		if (nworkers >= DBBLUE_PARTITION_BGW_MAX_DBS)
		{
			ereport(WARNING,
					(errmsg("dbblue_partition.maintenance_dbname lists more than %d databases; ignoring the rest",
							DBBLUE_PARTITION_BGW_MAX_DBS)));
			break;
		}
		if (strlen(dbname) >= BGW_EXTRALEN)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("database name \"%s\" in dbblue_partition.maintenance_dbname is too long",
							dbname)));

		memset(&worker, 0, sizeof(worker));
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = 60;
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "dbblue_partition_bgw");
		snprintf(worker.bgw_function_name, BGW_MAXLEN, "dbblue_partition_bgw_main");
		snprintf(worker.bgw_name, BGW_MAXLEN,
				 "dbblue_partition maintenance worker for database %s", dbname);
		snprintf(worker.bgw_type, BGW_MAXLEN, "dbblue_partition maintenance");
		snprintf(worker.bgw_extra, BGW_EXTRALEN, "%s", dbname);
		worker.bgw_main_arg = (Datum) 0;
		worker.bgw_notify_pid = 0;

		RegisterBackgroundWorker(&worker);
		nworkers++;
	}

	pfree(rawstring);
	list_free(dblist);
}
