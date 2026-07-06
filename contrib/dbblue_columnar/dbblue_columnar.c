/*-------------------------------------------------------------------------
 *
 * dbblue_columnar.c
 *		DBblue Columnar Engine - Milestone 1 skeleton.
 *
 * An AlloyDB-inspired, in-memory columnar accelerator built as a preloaded
 * module (shared_preload_libraries = 'dbblue_columnar'). This file is a
 * *do-nothing but fully wired* skeleton: it registers the dbblue_columnar.*
 * GUCs, a CustomScan provider (which currently offers no path, so the planner
 * falls through to ordinary Seq/Index/Bitmap scans), a background refresh
 * worker (which currently idles), and the dbblue_columnar_add() registration
 * function.
 *
 * Standalone by design: it depends only on core PostgreSQL and never on the
 * IVM engine. Later-milestone correctness will come from the visibility map
 * plus page LSN, not from IVM change-capture.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "nodes/extensible.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/wait_event.h"

#include "dbblue_columnar.h"

PG_MODULE_MAGIC;

/* ---- GUC variables (declared in dbblue_columnar.h) ---- */
bool		dbblue_columnar_enabled = false;
bool		dbblue_columnar_enable_columnar_scan = true;
int			dbblue_columnar_memory_mb = 128;
static bool dbblue_columnar_auto_columnarize = false;

/* ---- forward declarations (exported entry points) ---- */
PGDLLEXPORT void _PG_init(void);
PGDLLEXPORT void dbblue_columnar_worker_main(Datum main_arg);

/*
 * Background refresh worker.
 *
 * Milestone 1: idles. Later this loop will scan the registered relations for
 * invalid column blocks (visibility-map bit cleared or page LSN advanced past
 * the block's build LSN) and rebuild them from the heap.
 */
void
dbblue_columnar_worker_main(Datum main_arg)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	ereport(LOG,
			(errmsg("dbblue_columnar refresh worker started")));

	for (;;)
	{
		int			rc;

		if (ShutdownRequestPending)
			proc_exit(0);

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Milestone 1: nothing to refresh yet. */

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   60000L,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (rc & WL_LATCH_SET)
			CHECK_FOR_INTERRUPTS();
	}
}

/*
 * dbblue_columnar_add(rel regclass, columns text[]) -> int
 *
 * Register columns of a relation for columnarization. Milestone 1: records the
 * registration in dbblue_columnar_relations only; no column store is built.
 * Returns the number of newly-recorded (relid, attnum) pairs.
 */
PG_FUNCTION_INFO_V1(dbblue_columnar_add);

Datum
dbblue_columnar_add(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(1);
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int			i;
	int64		added = 0;
	char	   *insert_sql;

	if (ARR_NDIM(arr) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("column list must be a one-dimensional array")));

	deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT,
					  &elems, &nulls, &nelems);

	/*
	 * Schema-qualify the registry, exactly like the populate-side reader:
	 * resolving through search_path could target (or be captured by) an
	 * unrelated same-named table.
	 */
	insert_sql = psprintf("INSERT INTO %s (relid, attnum) "
						  "VALUES ($1, $2) ON CONFLICT (relid, attnum) DO NOTHING",
						  dbbc_registry_table_name());

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "dbblue_columnar: SPI_connect failed");

	for (i = 0; i < nelems; i++)
	{
		char	   *colname;
		AttrNumber	attnum;
		Oid			argtypes[2] = {REGCLASSOID, INT2OID};
		Datum		values[2];
		int			ret;

		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("column name cannot be NULL")));

		colname = TextDatumGetCString(elems[i]);
		attnum = get_attnum(relid, colname);
		if (attnum == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" does not exist in relation \"%s\"",
							colname, get_rel_name(relid))));
		if (attnum < 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot columnarize system column \"%s\"",
							colname)));

		values[0] = ObjectIdGetDatum(relid);
		values[1] = Int16GetDatum(attnum);

		ret = SPI_execute_with_args(insert_sql,
									2, argtypes, values, NULL, false, 0);
		if (ret != SPI_OK_INSERT)
			elog(ERROR, "dbblue_columnar: registration insert failed (%d)", ret);

		added += SPI_processed;
	}

	SPI_finish();

	PG_RETURN_INT32((int32) added);
}

/*
 * Module entry point. Only meaningful when preloaded via
 * shared_preload_libraries; if the library is instead loaded on demand (e.g.
 * to run dbblue_columnar_add()), this returns without registering anything.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomBoolVariable("dbblue_columnar.enabled",
							 "Enables the DBblue Columnar Engine.",
							 NULL,
							 &dbblue_columnar_enabled,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("dbblue_columnar.enable_columnar_scan",
							 "Allows the planner to read from the column store.",
							 "When off, the engine stays enabled and keeps its "
							 "column store, but queries do not use columnar data.",
							 &dbblue_columnar_enable_columnar_scan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("dbblue_columnar.auto_columnarize",
							 "Automatically choose which columns to columnarize.",
							 NULL,
							 &dbblue_columnar_auto_columnarize,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("dbblue_columnar.memory_mb",
							"Memory budget for the column store, in megabytes.",
							NULL,
							&dbblue_columnar_memory_mb,
							128, 128, INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_MB,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("dbblue_columnar");

	/* planner hook + CustomScan provider (columnar_scan.c) */
	dbbc_scan_init();

	/* register the background refresh worker */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 10;
	snprintf(worker.bgw_name, BGW_MAXLEN, "dbblue_columnar refresh worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "dbblue_columnar");
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "dbblue_columnar");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "dbblue_columnar_worker_main");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;
	RegisterBackgroundWorker(&worker);
}
