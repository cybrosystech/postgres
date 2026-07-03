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

PG_MODULE_MAGIC;

/* ---- GUC variables ---- */
static bool dbblue_columnar_enabled = false;
static bool dbblue_columnar_enable_columnar_scan = true;
static bool dbblue_columnar_auto_columnarize = false;
static int	dbblue_columnar_memory_mb = 128;

/* ---- saved hook ---- */
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;

/* ---- forward declarations (exported entry points) ---- */
PGDLLEXPORT void _PG_init(void);
PGDLLEXPORT void dbblue_columnar_worker_main(Datum main_arg);

/* ---- CustomScan provider callbacks ---- */
static bool dbblue_columnar_rel_is_ready(RelOptInfo *rel, RangeTblEntry *rte);
static void dbblue_columnar_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
											 Index rti, RangeTblEntry *rte);
static Plan *dbblue_columnar_plan_custom_path(PlannerInfo *root, RelOptInfo *rel,
											  CustomPath *best_path, List *tlist,
											  List *clauses, List *custom_plans);
static Node *dbblue_columnar_create_scan_state(CustomScan *cscan);
static void dbblue_columnar_begin_scan(CustomScanState *node, EState *estate,
									   int eflags);
static TupleTableSlot *dbblue_columnar_exec_scan(CustomScanState *node);
static void dbblue_columnar_end_scan(CustomScanState *node);
static void dbblue_columnar_rescan_scan(CustomScanState *node);

/* ---- CustomScan provider method tables ---- */
static const CustomPathMethods dbblue_columnar_path_methods = {
	.CustomName = "DBBlueColumnarScan",
	.PlanCustomPath = dbblue_columnar_plan_custom_path,
};

static const CustomScanMethods dbblue_columnar_scan_methods = {
	.CustomName = "DBBlueColumnarScan",
	.CreateCustomScanState = dbblue_columnar_create_scan_state,
};

static const CustomExecMethods dbblue_columnar_exec_methods = {
	.CustomName = "DBBlueColumnarScan",
	.BeginCustomScan = dbblue_columnar_begin_scan,
	.ExecCustomScan = dbblue_columnar_exec_scan,
	.EndCustomScan = dbblue_columnar_end_scan,
	.ReScanCustomScan = dbblue_columnar_rescan_scan,
};

/*
 * Is this relation ready to be served from the column store?
 *
 * Milestone 1: nothing is columnarized yet, so this is always false and the
 * planner never sees a columnar path. This is the single growth point where
 * later milestones will check that (a) every column the scan node needs is
 * registered and populated, and (b) enough of the relation's blocks are valid
 * (visibility-map all-visible AND page LSN unchanged since build) to be worth
 * a columnar path.
 */
static bool
dbblue_columnar_rel_is_ready(RelOptInfo *rel, RangeTblEntry *rte)
{
	return false;
}

/*
 * Planner hook: offer a columnar CustomPath for eligible base relations.
 *
 * Milestone 1: because dbblue_columnar_rel_is_ready() is always false, we
 * always fall through, leaving the planner's normal paths in place. The
 * guarded block below shows exactly how the columnar path will be injected.
 */
static void
dbblue_columnar_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
								 Index rti, RangeTblEntry *rte)
{
	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);

	if (!dbblue_columnar_enabled || !dbblue_columnar_enable_columnar_scan)
		return;

	if (dbblue_columnar_rel_is_ready(rel, rte))
	{
		CustomPath *cpath = makeNode(CustomPath);

		cpath->path.pathtype = T_CustomScan;
		cpath->path.parent = rel;
		cpath->path.pathtarget = rel->reltarget;
		cpath->path.rows = rel->rows;
		cpath->flags = 0;
		cpath->methods = &dbblue_columnar_path_methods;

		add_path(rel, (Path *) cpath);
	}
}

static Plan *
dbblue_columnar_plan_custom_path(PlannerInfo *root, RelOptInfo *rel,
								 CustomPath *best_path, List *tlist,
								 List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = NIL;
	cscan->scan.scanrelid = rel->relid;
	cscan->flags = best_path->flags;
	cscan->methods = &dbblue_columnar_scan_methods;

	return &cscan->scan.plan;
}

static Node *
dbblue_columnar_create_scan_state(CustomScan *cscan)
{
	CustomScanState *cstate = (CustomScanState *) palloc0(sizeof(CustomScanState));

	NodeSetTag(cstate, T_CustomScanState);
	cstate->methods = &dbblue_columnar_exec_methods;

	return (Node *) cstate;
}

static void
dbblue_columnar_begin_scan(CustomScanState *node, EState *estate, int eflags)
{
	/* Milestone 1: no columnar scan state to initialize. */
}

static TupleTableSlot *
dbblue_columnar_exec_scan(CustomScanState *node)
{
	/* Milestone 1: never reached (no columnar path is ever chosen). */
	return NULL;
}

static void
dbblue_columnar_end_scan(CustomScanState *node)
{
}

static void
dbblue_columnar_rescan_scan(CustomScanState *node)
{
}

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

	if (ARR_NDIM(arr) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("column list must be a one-dimensional array")));

	deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT,
					  &elems, &nulls, &nelems);

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

		values[0] = ObjectIdGetDatum(relid);
		values[1] = Int16GetDatum(attnum);

		ret = SPI_execute_with_args(
									"INSERT INTO dbblue_columnar_relations (relid, attnum) "
									"VALUES ($1, $2) ON CONFLICT (relid, attnum) DO NOTHING",
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

	/* register the CustomScan provider (no paths are offered yet) */
	RegisterCustomScanMethods(&dbblue_columnar_scan_methods);

	/* install the path-injection hook */
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = dbblue_columnar_set_rel_pathlist;

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
