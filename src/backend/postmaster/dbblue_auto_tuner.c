/*-------------------------------------------------------------------------
 *
 * dbblue_auto_tuner.c
 *	  dbblue Auto Tuner background worker.
 *
 * The auto tuner inspects the host machine (total RAM, CPU count) and
 * the current cluster configuration (max_connections), then records a
 * recommended value for each of the core memory/parallelism GUCs into
 * the public.dbblue_auto_tune_suggestions table for the DBA to review.
 *
 * Phase 1 (this file):
 *	 - When dbblue_auto_tune is on, the worker computes recommendations
 *	   once and upserts them into dbblue_auto_tune_suggestions, then
 *	   parks until the GUC is toggled off and on again (specs and
 *	   max_connections only change at server start, so re-running on a
 *	   timer would add nothing).
 *	 - Tuned parameters (pgtune-style, OLTP defaults, SSD assumed):
 *	     shared_buffers, effective_cache_size, work_mem,
 *	     maintenance_work_mem, max_worker_processes,
 *	     max_parallel_workers, max_parallel_workers_per_gather,
 *	     wal_buffers, checkpoint_completion_target, random_page_cost,
 *	     effective_io_concurrency, min_wal_size, max_wal_size.
 *
 * Portions Copyright (c) 2026, dbblue / Cybrosys Technologies.
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/dbblue_auto_tuner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/dbblue_auto_tuner.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"

/* GUC-backed variables (real values come from guc_parameters.dat). */
bool		dbblue_auto_tune = false;
char	   *dbblue_auto_tune_database = NULL;

/* Idle wake-up interval — long, since specs do not change at runtime. */
#define DBBLUE_AUTO_TUNE_IDLE_MS	(60L * 1000L)

/*
 * Snapshot of the host's resource profile, captured once per "tick"
 * (i.e. once per off→on transition).
 */
typedef struct AutoTunerSpecs
{
	int64		total_memory_mb;
	int			cpu_count;
	int			max_connections;
} AutoTunerSpecs;

/*
 * One row to upsert into dbblue_auto_tune_suggestions.
 *
 * `unit` is a human-readable hint shown to the DBA (kB, MB, GB, "", ...).
 * `reason` is a one-line explanation of the formula used.
 */
typedef struct TuningSuggestion
{
	const char *parameter_name;
	char	   *suggested_value;
	const char *unit;
	char	   *reason;
} TuningSuggestion;

static const char *const dbblue_create_table_sql =
	"CREATE TABLE IF NOT EXISTS public.dbblue_auto_tune_suggestions ("
	"    id                          bigserial PRIMARY KEY,"
	"    parameter_name              text NOT NULL UNIQUE,"
	"    current_value               text,"
	"    suggested_value             text NOT NULL,"
	"    unit                        text,"
	"    reason                      text,"
	"    detected_total_memory_mb    bigint,"
	"    detected_cpu_count          integer,"
	"    created_at                  timestamptz NOT NULL DEFAULT now(),"
	"    updated_at                  timestamptz NOT NULL DEFAULT now()"
	")";

static const char *const dbblue_upsert_sql =
	"INSERT INTO public.dbblue_auto_tune_suggestions ("
	"    parameter_name, current_value, suggested_value, unit, reason,"
	"    detected_total_memory_mb, detected_cpu_count, updated_at"
	") VALUES ($1, $2, $3, $4, $5, $6, $7, now()) "
	"ON CONFLICT (parameter_name) DO UPDATE SET "
	"    current_value = EXCLUDED.current_value,"
	"    suggested_value = EXCLUDED.suggested_value,"
	"    unit = EXCLUDED.unit,"
	"    reason = EXCLUDED.reason,"
	"    detected_total_memory_mb = EXCLUDED.detected_total_memory_mb,"
	"    detected_cpu_count = EXCLUDED.detected_cpu_count,"
	"    updated_at = now()";


/*
 * dbblue_auto_tuner_detect_specs
 *		Capture total RAM, CPU count, and current max_connections.
 *
 * Uses sysconf(_SC_PHYS_PAGES) and sysconf(_SC_NPROCESSORS_ONLN),
 * which are GNU extensions also implemented by glibc on Linux and by
 * libSystem on macOS — covering every platform dbblue currently runs
 * on.  Falls back to conservative defaults if either call returns 0
 * (sandboxed environments, exotic builds).
 */
static void
dbblue_auto_tuner_detect_specs(AutoTunerSpecs *specs)
{
	long		pages;
	long		page_size;

	specs->total_memory_mb = 0;
	specs->cpu_count = 0;
	specs->max_connections = MaxConnections;

	pages = sysconf(_SC_PHYS_PAGES);
	page_size = sysconf(_SC_PAGESIZE);
	ereport(LOG,
			(errmsg("dbblue auto tuner: sysconf reports %ld pages, page size %ld bytes",
					pages, page_size)));
	if (pages > 0 && page_size > 0)
		specs->total_memory_mb =
			((int64) pages * (int64) page_size) / (1024 * 1024);

	specs->cpu_count = (int) sysconf(_SC_NPROCESSORS_ONLN);
	if (specs->cpu_count <= 0)
		specs->cpu_count = 1;

	/* Last-ditch defaults so the rest of the code never divides by zero. */
	if (specs->total_memory_mb <= 0)
		specs->total_memory_mb = 1024;	/* assume 1 GB */
	if (specs->max_connections <= 0)
		specs->max_connections = 100;
}

/*
 * make_suggestion
 *		Allocate and partially populate a TuningSuggestion.
 *
 * `reason` is left NULL — callers fill it in via psprintf so they can
 * stay variadic without re-implementing printf retry logic here.
 */
static TuningSuggestion *
make_suggestion(const char *name, const char *unit, char *reason)
{
	TuningSuggestion *s = palloc0(sizeof(*s));

	s->parameter_name = name;
	s->unit = unit;
	s->reason = reason;
	return s;
}

static void
set_suggested_int(TuningSuggestion *s, int64 value)
{
	s->suggested_value = psprintf(INT64_FORMAT, value);
}

static void
set_suggested_real(TuningSuggestion *s, double value)
{
	s->suggested_value = psprintf("%.2f", value);
}

/*
 * dbblue_auto_tuner_build_suggestions
 *		Compute the recommended value for every tuned parameter and
 *		return a List of TuningSuggestion *.
 *
 * Formulas follow the well-known pgtune defaults for an OLTP workload
 * on SSD-backed storage.  They are intentionally conservative — the
 * DBA reviews and applies them manually in phase 1.
 */
static List *
dbblue_auto_tuner_build_suggestions(const AutoTunerSpecs *specs)
{
	List	   *result = NIL;
	int64		ram_mb = specs->total_memory_mb;
	int			cpus = specs->cpu_count;
	int			conns = specs->max_connections;
	int64		shared_buffers_mb;
	int64		effective_cache_mb;
	int64		work_mem_kb;
	int64		maintenance_mb;
	int64		wal_buffers_mb;
	int			parallel_per_gather;
	TuningSuggestion *s;

	/* shared_buffers = 25% of RAM */
	shared_buffers_mb = ram_mb / 4;
	if (shared_buffers_mb < 128)
		shared_buffers_mb = 128;
	s = make_suggestion("shared_buffers", "MB",
						psprintf("25%% of detected RAM (%lld MB)",
								 (long long) ram_mb));
	set_suggested_int(s, shared_buffers_mb);
	result = lappend(result, s);

	/* effective_cache_size = 75% of RAM */
	effective_cache_mb = (ram_mb * 3) / 4;
	s = make_suggestion("effective_cache_size", "MB",
						pstrdup("75% of detected RAM — planner hint, no allocation"));
	set_suggested_int(s, effective_cache_mb);
	result = lappend(result, s);

	/*
	 * work_mem = (RAM - shared_buffers) / max_connections / 3
	 * Per-sort/hash node, multiplied by parallel workers — keep it
	 * conservative so a fan-out doesn't OOM the host.
	 */
	work_mem_kb = (((ram_mb - shared_buffers_mb) * 1024) / conns) / 3;
	if (work_mem_kb < 4096)			/* floor at 4 MB */
		work_mem_kb = 4096;
	s = make_suggestion("work_mem", "kB",
						psprintf("(RAM - shared_buffers) / max_connections (%d) / 3",
								 conns));
	set_suggested_int(s, work_mem_kb);
	result = lappend(result, s);

	/* maintenance_work_mem = RAM / 16, capped at 2 GB */
	maintenance_mb = ram_mb / 16;
	if (maintenance_mb < 64)
		maintenance_mb = 64;
	if (maintenance_mb > 2048)
		maintenance_mb = 2048;
	s = make_suggestion("maintenance_work_mem", "MB",
						pstrdup("RAM / 16, capped at 2 GB — used by VACUUM, CREATE INDEX"));
	set_suggested_int(s, maintenance_mb);
	result = lappend(result, s);

	/* max_worker_processes = CPU count */
	s = make_suggestion("max_worker_processes", "",
						psprintf("one slot per detected logical CPU (%d)", cpus));
	set_suggested_int(s, cpus);
	result = lappend(result, s);

	/* max_parallel_workers = CPU count */
	s = make_suggestion("max_parallel_workers", "",
						psprintf("one slot per detected logical CPU (%d)", cpus));
	set_suggested_int(s, cpus);
	result = lappend(result, s);

	/* max_parallel_workers_per_gather = CPUs / 2 (min 1) */
	parallel_per_gather = cpus / 2;
	if (parallel_per_gather < 1)
		parallel_per_gather = 1;
	s = make_suggestion("max_parallel_workers_per_gather", "",
						pstrdup("half of detected CPUs (rounded down, min 1)"));
	set_suggested_int(s, parallel_per_gather);
	result = lappend(result, s);

	/* wal_buffers = shared_buffers / 32, capped at 16 MB, min 1 MB */
	wal_buffers_mb = shared_buffers_mb / 32;
	if (wal_buffers_mb < 1)
		wal_buffers_mb = 1;
	if (wal_buffers_mb > 16)
		wal_buffers_mb = 16;
	s = make_suggestion("wal_buffers", "MB",
						pstrdup("shared_buffers / 32, clamped to [1 MB, 16 MB]"));
	set_suggested_int(s, wal_buffers_mb);
	result = lappend(result, s);

	/* checkpoint_completion_target = 0.9 (modern default) */
	s = make_suggestion("checkpoint_completion_target", "",
						pstrdup("smooth checkpoint I/O over 90% of the interval"));
	set_suggested_real(s, 0.90);
	result = lappend(result, s);

	/* random_page_cost = 1.1 — SSD assumption */
	s = make_suggestion("random_page_cost", "",
						pstrdup("SSD-class storage assumed (1.1); raise to 4 for spinning disk"));
	set_suggested_real(s, 1.10);
	result = lappend(result, s);

	/* effective_io_concurrency = 200 — SSD assumption */
	s = make_suggestion("effective_io_concurrency", "",
						pstrdup("SSD-class storage assumed; lower to ~2 for spinning disk"));
	set_suggested_int(s, 200);
	result = lappend(result, s);

	/* min_wal_size = 1 GB */
	s = make_suggestion("min_wal_size", "MB",
						pstrdup("floor for WAL recycling — keeps checkpoint segments warm"));
	set_suggested_int(s, 1024);
	result = lappend(result, s);

	/* max_wal_size = 4 GB */
	s = make_suggestion("max_wal_size", "MB",
						pstrdup("ceiling between checkpoints — larger reduces checkpoint pressure"));
	set_suggested_int(s, 4096);
	result = lappend(result, s);

	return result;
}

/*
 * dbblue_auto_tuner_ensure_table
 *		Create public.dbblue_auto_tune_suggestions if it does not exist.
 *		Runs in its own transaction.
 */
static void
dbblue_auto_tuner_ensure_table(void)
{
	int			ret;

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue auto tuner: ensuring suggestions table");

	ret = SPI_execute(dbblue_create_table_sql, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(WARNING,
				(errmsg("dbblue auto tuner: could not create dbblue_auto_tune_suggestions (SPI rc=%d)",
						ret)));

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * dbblue_auto_tuner_write_suggestions
 *		Upsert every suggestion into dbblue_auto_tune_suggestions in a
 *		single transaction.  current_value is read from the live GUC
 *		via GetConfigOption so the DBA can see the delta at a glance.
 */
static void
dbblue_auto_tuner_write_suggestions(const AutoTunerSpecs *specs,
									List *suggestions)
{
	ListCell   *lc;

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue auto tuner: writing suggestions");

	foreach(lc, suggestions)
	{
		TuningSuggestion *s = (TuningSuggestion *) lfirst(lc);
		char	   *current;
		Datum		values[7];
		char		nulls[7] = {' ', ' ', ' ', ' ', ' ', ' ', ' '};
		Oid			argtypes[7] = {TEXTOID, TEXTOID, TEXTOID, TEXTOID,
								   TEXTOID, INT8OID, INT4OID};
		int			ret;

		/*
		 * Use the SHOW-style display value (e.g. "128MB", "4GB") so the
		 * DBA can compare current_value against suggested_value
		 * directly.  GetConfigOption returns the raw base-unit integer
		 * which is hard to read for memory parameters.
		 */
		current = GetConfigOptionByName(s->parameter_name, NULL, true);

		values[0] = CStringGetTextDatum(s->parameter_name);
		if (current == NULL)
			nulls[1] = 'n';
		else
			values[1] = CStringGetTextDatum(current);
		values[2] = CStringGetTextDatum(s->suggested_value);
		if (s->unit == NULL || s->unit[0] == '\0')
			nulls[3] = 'n';
		else
			values[3] = CStringGetTextDatum(s->unit);
		if (s->reason == NULL)
			nulls[4] = 'n';
		else
			values[4] = CStringGetTextDatum(s->reason);
		values[5] = Int64GetDatum(specs->total_memory_mb);
		values[6] = Int32GetDatum(specs->cpu_count);

		ret = SPI_execute_with_args(dbblue_upsert_sql,
									7, argtypes, values, nulls,
									false, 0);
		if (ret != SPI_OK_INSERT && ret != SPI_OK_UPDATE)
			ereport(WARNING,
					(errmsg("dbblue auto tuner: upsert for \"%s\" failed (SPI rc=%d)",
							s->parameter_name, ret)));
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * dbblue_auto_tuner_run_once
 *		One full pass: detect specs, compute suggestions, persist them.
 */
static void
dbblue_auto_tuner_run_once(void)
{
	AutoTunerSpecs specs;
	List	   *suggestions;

	dbblue_auto_tuner_detect_specs(&specs);

	ereport(LOG,
			(errmsg("dbblue auto tuner: detected %lld MB RAM, %d CPUs, max_connections=%d",
					(long long) specs.total_memory_mb,
					specs.cpu_count,
					specs.max_connections)));

	suggestions = dbblue_auto_tuner_build_suggestions(&specs);
	dbblue_auto_tuner_write_suggestions(&specs, suggestions);

	ereport(LOG,
			(errmsg("dbblue auto tuner: wrote %d suggestion(s) to public.dbblue_auto_tune_suggestions",
					list_length(suggestions))));
}

/*
 * DbblueAutoTunerMain
 *		Background worker entry point.
 */
void
DbblueAutoTunerMain(Datum main_arg)
{
	bool		ran_for_current_enable = false;

	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(dbblue_auto_tune_database,
										 NULL, 0);

	ereport(LOG,
			(errmsg("dbblue auto tuner started (database=\"%s\")",
					dbblue_auto_tune_database)));

	/*
	 * Always materialise the suggestions table at startup so the DBA
	 * can inspect prior runs even while the feature is paused.
	 */
	dbblue_auto_tuner_ensure_table();

	while (!ShutdownRequestPending)
	{
		int			rc;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (dbblue_auto_tune)
		{
			if (!ran_for_current_enable)
			{
				dbblue_auto_tuner_run_once();
				ran_for_current_enable = true;
			}
		}
		else
		{
			/* Re-arm so the next off→on transition triggers a fresh run. */
			ran_for_current_enable = false;
		}

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   DBBLUE_AUTO_TUNE_IDLE_MS,
					   WAIT_EVENT_DBBLUE_AUTO_TUNER_MAIN);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	ereport(LOG, (errmsg("dbblue auto tuner shutting down")));
	proc_exit(0);
}

/*
 * DbblueAutoTunerRegister
 *		Register the auto tuner as a static background worker.
 */
void
DbblueAutoTunerRegister(void)
{
	BackgroundWorker bgw;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
					BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_restart_time = 60;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "DbblueAutoTunerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "dbblue auto tuner");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "dbblue auto tuner");
	bgw.bgw_main_arg = (Datum) 0;
	bgw.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&bgw);
}
