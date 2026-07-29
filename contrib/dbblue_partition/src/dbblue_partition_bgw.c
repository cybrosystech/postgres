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
#include "commands/dbcommands.h"
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
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
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
 */
static void
dbblue_partition_bgw_run_maintenance(void)
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
						get_database_name(MyDatabaseId))));
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
		return;
	}

	partman_schema = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
													   SPI_tuptable->tupdesc,
													   1, &isnull));
	if (isnull)
		elog(ERROR, "pg_partman schema lookup returned NULL");

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

	ereport(DEBUG1,
			(errmsg("dbblue_partition maintenance completed in database \"%s\"",
					get_database_name(MyDatabaseId))));
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

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	if (role != NULL && role[0] == '\0')
		role = NULL;			/* default: the bootstrap superuser */

	BackgroundWorkerInitializeConnection(dbname, role, 0);
	pgstat_report_appname("dbblue_partition maintenance worker");

	ereport(LOG,
			(errmsg("dbblue_partition maintenance worker started for database \"%s\" (interval: %ds)",
					dbname, dbblue_partition_maintenance_interval)));

	dbblue_partition_bgw_run_maintenance();

	for (;;)
	{
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 dbblue_partition_maintenance_interval * 1000L,
						 PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		if (ShutdownRequestPending)
			break;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		dbblue_partition_bgw_run_maintenance();
	}

	ereport(LOG,
			(errmsg("dbblue_partition maintenance worker for database \"%s\" shutting down",
					dbname)));

	proc_exit(0);
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
	if (!SplitIdentifierString(rawstring, ',', &dblist))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid list syntax in dbblue_partition.maintenance_dbname")));

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
