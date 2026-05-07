/*-------------------------------------------------------------------------
 *
 * autopartition.c
 *	  DBblue auto-partition launcher background worker.
 *
 *	The launcher wakes every `auto_partition_naptime` seconds, scans
 *	pg_class for relations whose `auto_partition_strategy` reloption is
 *	set to anything other than 'off', and emits a LOG line summarizing
 *	each one.  No catalog mutation happens here — partition creation and
 *	detach automation is the next step on this branch.
 *
 *	Registration happens at postmaster startup (see postmaster.c calling
 *	AutoPartitionLauncherRegister()), so the worker is built into the
 *	server binary and runs without any shared_preload_libraries config.
 *
 *	The worker connects to the database named by `auto_partition_database`
 *	(default: "postgres") to read pg_class.  Multi-database scanning is a
 *	follow-up — Phase 2 only needs to prove that we can observe the
 *	configured relations from a worker.
 *
 * Portions Copyright (c) 2026 Cybrosys / DBblue R&D
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/autopartition.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autopartition.h"
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
#include "utils/wait_event.h"

/*
 * GUCs
 *
 * These are registered in guc_parameters.dat once we've validated the
 * approach end-to-end; for the moment we DefineCustom them at module init
 * time so a running server picks them up without needing a regenerated
 * guc_tables.
 */
bool		auto_partition_enabled = true;
int			auto_partition_naptime = 60;	/* seconds */

/*
 * Phase 2 connects to a single hardcoded database.  Multi-DB scanning
 * (one worker per database, via dynamic registration) is a follow-up.
 */
#define AUTO_PARTITION_DEFAULT_DB "postgres"

static void scan_configured_relations(void);

/* ---------------------------------------------------------------- */

/*
 * Postmaster-time registration.  Called once from PostmasterMain() before
 * any backends are forked, alongside ApplyLauncherRegister().  This is
 * also where we define the launcher's GUCs — they must be created during
 * postmaster startup so that PGC_POSTMASTER variables can be set, and so
 * that regular backends see them at SHOW time.
 *
 * Always registers a worker; the worker itself checks
 * `auto_partition_enabled` each loop iteration and idles when off.
 */
void
AutoPartitionLauncherRegister(void)
{
	BackgroundWorker bgw;

	DefineCustomBoolVariable("auto_partition_enabled",
							 "Whether the auto-partition launcher should perform work.",
							 NULL,
							 &auto_partition_enabled,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("auto_partition_naptime",
							"Seconds between auto-partition launcher iterations.",
							"Set to 0 to wait until signaled.",
							&auto_partition_naptime,
							60,
							0, INT_MAX / 1000,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "AutoPartitionLauncherMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "auto-partition launcher");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "auto-partition launcher");
	bgw.bgw_restart_time = 30;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}

/* ---------------------------------------------------------------- */

/*
 * Worker main loop.  Cleans up its own transactions every iteration to
 * avoid keeping snapshots open across long sleeps.
 */
void
AutoPartitionLauncherMain(Datum main_arg)
{
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(AUTO_PARTITION_DEFAULT_DB, NULL, 0);

	ereport(LOG,
			(errmsg("auto-partition launcher started: database=\"%s\", naptime=%ds",
					AUTO_PARTITION_DEFAULT_DB, auto_partition_naptime)));

	while (!ShutdownRequestPending)
	{
		long		delay_ms;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (auto_partition_enabled)
			scan_configured_relations();

		delay_ms = (auto_partition_naptime > 0)
			? (long) auto_partition_naptime * 1000L : -1L;

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 delay_ms,
						 PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}

	ereport(LOG, (errmsg("auto-partition launcher shutting down")));
}

/* ---------------------------------------------------------------- */

/*
 * One scan iteration: open a transaction, find every relation with a
 * non-OFF auto_partition_strategy reloption, log a summary line for
 * each, then commit.  Reads pg_class via SPI; no catalog mutation.
 */
static void
scan_configured_relations(void)
{
	int			ret;
	uint64		i;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "auto-partition scan");

	ret = SPI_execute(
					  "SELECT c.oid, "
					  "       n.nspname, "
					  "       c.relname, "
					  "       c.relkind, "
					  "       opts.config->>'auto_partition_strategy' AS strategy, "
					  "       opts.config->>'auto_partition_column' AS partition_column, "
					  "       opts.config->>'auto_partition_interval' AS partition_interval, "
					  "       NULLIF(opts.config->>'auto_partition_retention','')::int AS retention "
					  "FROM pg_class c "
					  "JOIN pg_namespace n ON n.oid = c.relnamespace "
					  "LEFT JOIN LATERAL ( "
					  "  SELECT jsonb_object_agg(split_part(o,'=',1), "
					  "                          substring(o from position('=' in o)+1)) AS config "
					  "  FROM unnest(c.reloptions) o "
					  "  WHERE o LIKE 'auto_partition%'"
					  ") opts ON TRUE "
					  "WHERE c.relkind IN ('r','p') "
					  "  AND opts.config IS NOT NULL "
					  "  AND opts.config ? 'auto_partition_strategy' "
					  "  AND opts.config->>'auto_partition_strategy' <> 'off'",
					  true, 0);

	if (ret != SPI_OK_SELECT)
	{
		ereport(WARNING,
				(errmsg("auto-partition launcher: SPI_execute failed (rc=%d)", ret)));
		goto done;
	}

	if (SPI_processed == 0)
	{
		ereport(DEBUG1,
				(errmsg("auto-partition launcher: no configured relations")));
		goto done;
	}

	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	row = SPI_tuptable->vals[i];
		TupleDesc	td = SPI_tuptable->tupdesc;
		bool		isnull;
		Oid			reloid;
		char	   *nsname;
		char	   *relname;
		char		relkind;
		char	   *strategy;
		char	   *partcol;
		char	   *partinterval;
		int32		retention = 0;

		reloid = DatumGetObjectId(SPI_getbinval(row, td, 1, &isnull));
		nsname = SPI_getvalue(row, td, 2);
		relname = SPI_getvalue(row, td, 3);
		relkind = DatumGetChar(SPI_getbinval(row, td, 4, &isnull));
		strategy = SPI_getvalue(row, td, 5);
		partcol = SPI_getvalue(row, td, 6);
		partinterval = SPI_getvalue(row, td, 7);
		(void) SPI_getbinval(row, td, 8, &isnull);
		if (!isnull)
			retention = DatumGetInt32(SPI_getbinval(row, td, 8, &isnull));

		ereport(LOG,
				(errmsg("auto-partition: %s.%s (oid=%u, relkind=%c) "
						"strategy=%s column=%s interval=%s retention=%d %s",
						nsname ? nsname : "?",
						relname ? relname : "?",
						reloid,
						relkind,
						strategy ? strategy : "?",
						partcol ? partcol : "(none)",
						partinterval ? partinterval : "(none)",
						retention,
						(relkind == 'p') ? "[partitioned, ready for rotation]"
						: "[heap, awaiting online migration]")));
	}

done:
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_stat(false);
	pgstat_report_activity(STATE_IDLE, NULL);
}
