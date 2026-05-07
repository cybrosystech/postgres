/*-------------------------------------------------------------------------
 *
 * autopartition.c
 *	  DBblue auto-partition launcher background worker.
 *
 *	The launcher wakes every `auto_partition_naptime` seconds, scans
 *	pg_class for relations whose `auto_partition_strategy` reloption is
 *	set to anything other than 'off', and processes each one:
 *
 *	  range_int  : if the relation is partitioned, find the highest
 *	               existing upper bound and create the next partition
 *	               with bounds [upper, upper + interval).  If retention
 *	               is set and exceeded, DETACH + DROP the oldest
 *	               partitions until the count is at retention.
 *
 *	  range_date : not yet implemented (logged-only).
 *	  list_int   : not yet implemented (logged-only).
 *
 *	Heap relations (relkind='r') are logged but not modified — converting
 *	an unpartitioned table to partitioned is the online-migration job
 *	(Phase 3).
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
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
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
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ruleutils.h"
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
 * Process a single range_int-partitioned relation.
 *
 * Steps:
 *   1. List existing partitions ordered by upper bound DESC.
 *   2. If none exist, log and return — initial-partition creation is
 *      Phase 3 (online migration).
 *   3. Compute next bounds = [highest_upper, highest_upper + interval)
 *      and CREATE the partition if it doesn't already exist.
 *   4. If retention is set and partition_count >= retention, DETACH +
 *      DROP the oldest partitions until count == retention.
 *
 * The DDL is wrapped in a subtransaction so a failure on one relation
 * doesn't abort the whole launcher iteration.  Errors are logged at
 * WARNING; recovery is "try again next iteration."
 */
static void
process_range_int_partition(Oid reloid, const char *nsname, const char *relname,
							const char *partinterval, int32 retention)
{
	int64		interval_size;
	int			ret;
	int			partition_count = 0;
	int64		max_upper;
	bool		have_max_upper = false;
	List	   *oldest_oids = NIL;	/* oids to detach + drop, in order */
	StringInfoData buf;
	char	   *endptr;

	/* Parse the interval as a 64-bit row count. */
	if (partinterval == NULL || *partinterval == '\0')
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: range_int strategy needs auto_partition_interval",
						nsname, relname)));
		return;
	}
	errno = 0;
	interval_size = strtoll(partinterval, &endptr, 10);
	if (errno != 0 || endptr == partinterval || *endptr != '\0' || interval_size <= 0)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: invalid range_int interval \"%s\" (need positive integer)",
						nsname, relname, partinterval)));
		return;
	}

	/*
	 * Discover existing partitions.  We deparse the partition bound via
	 * pg_get_expr and pull out the bounds using regex.  This is the
	 * pragmatic approach for Phase 2; reading the boundinfo tree
	 * directly is a follow-up cleanup.
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT child.oid::oid AS child_oid, "
					 "       child.relname AS child_name, "
					 "       (regexp_match(pg_get_expr(child.relpartbound, child.oid), "
					 "                     'FROM \\(''?(-?[0-9]+)''?\\)'))[1]::bigint AS lower_bound, "
					 "       (regexp_match(pg_get_expr(child.relpartbound, child.oid), "
					 "                     'TO \\(''?(-?[0-9]+)''?\\)'))[1]::bigint AS upper_bound "
					 "FROM pg_inherits i "
					 "JOIN pg_class child ON child.oid = i.inhrelid "
					 "WHERE i.inhparent = %u "
					 "ORDER BY upper_bound DESC NULLS LAST",
					 reloid);

	ret = SPI_execute(buf.data, true, 0);
	pfree(buf.data);

	if (ret != SPI_OK_SELECT)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: failed to list partitions (rc=%d)",
						nsname, relname, ret)));
		return;
	}

	partition_count = (int) SPI_processed;

	if (partition_count == 0)
	{
		ereport(LOG,
				(errmsg("auto-partition: %s.%s: no existing partitions, skipping (Phase 3 covers initial creation)",
						nsname, relname)));
		return;
	}

	/*
	 * Newest partition first.  Pull max_upper out of row 0 and remember
	 * the oldest partitions (rows after retention threshold) as
	 * detach/drop candidates.
	 */
	{
		bool		isnull;

		max_upper = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												SPI_tuptable->tupdesc,
												4, &isnull));
		if (!isnull)
			have_max_upper = true;
	}

	if (retention > 0 && partition_count > retention)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		for (int i = retention; i < partition_count; i++)
		{
			bool		isnull;
			Oid			child_oid;

			child_oid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i],
													   SPI_tuptable->tupdesc,
													   1, &isnull));
			if (!isnull)
				oldest_oids = lappend_oid(oldest_oids, child_oid);
		}
		MemoryContextSwitchTo(oldcxt);
	}

	/*
	 * Try to create the next partition.  Wrap in a subtransaction so a
	 * failure (typically: partition already exists, or DDL conflict)
	 * doesn't break this iteration of the scan loop.
	 */
	if (have_max_upper)
	{
		int64		lower_bound = max_upper;
		int64		upper_bound = max_upper + interval_size;
		char		child_name[NAMEDATALEN];

		snprintf(child_name, sizeof(child_name),
				 "%s_p_%lld",
				 relname, (long long) lower_bound);

		BeginInternalSubTransaction("autopart_create");
		PG_TRY();
		{
			initStringInfo(&buf);
			appendStringInfo(&buf,
							 "CREATE TABLE IF NOT EXISTS %s.%s "
							 "PARTITION OF %s.%s "
							 "FOR VALUES FROM (%lld) TO (%lld)",
							 quote_identifier(nsname),
							 quote_identifier(child_name),
							 quote_identifier(nsname),
							 quote_identifier(relname),
							 (long long) lower_bound,
							 (long long) upper_bound);

			ret = SPI_exec(buf.data, 0);
			pfree(buf.data);

			if (ret == SPI_OK_UTILITY)
				ereport(LOG,
						(errmsg("auto-partition: created %s.%s for [%lld, %lld) on %s.%s",
								nsname, child_name,
								(long long) lower_bound, (long long) upper_bound,
								nsname, relname)));

			ReleaseCurrentSubTransaction();
		}
		PG_CATCH();
		{
			ErrorData  *edata;

			MemoryContextSwitchTo(TopTransactionContext);
			edata = CopyErrorData();
			ereport(WARNING,
					(errmsg("auto-partition: %s.%s: create-partition failed: %s",
							nsname, relname, edata->message)));
			FreeErrorData(edata);
			FlushErrorState();
			RollbackAndReleaseCurrentSubTransaction();
		}
		PG_END_TRY();
	}

	/* Retention enforcement: detach + drop oldest beyond `retention`. */
	if (oldest_oids != NIL)
	{
		ListCell   *lc;

		foreach(lc, oldest_oids)
		{
			Oid			old_oid = lfirst_oid(lc);

			BeginInternalSubTransaction("autopart_detach");
			PG_TRY();
			{
				char	   *qual;

				qual = quote_qualified_identifier(get_namespace_name(get_rel_namespace(old_oid)),
												  get_rel_name(old_oid));

				initStringInfo(&buf);
				appendStringInfo(&buf,
								 "ALTER TABLE %s.%s DETACH PARTITION %s",
								 quote_identifier(nsname),
								 quote_identifier(relname),
								 qual);
				SPI_exec(buf.data, 0);
				pfree(buf.data);

				initStringInfo(&buf);
				appendStringInfo(&buf, "DROP TABLE %s", qual);
				SPI_exec(buf.data, 0);
				pfree(buf.data);

				ereport(LOG,
						(errmsg("auto-partition: detached + dropped %s (retention=%d)",
								qual, retention)));

				ReleaseCurrentSubTransaction();
			}
			PG_CATCH();
			{
				ErrorData  *edata;

				MemoryContextSwitchTo(TopTransactionContext);
				edata = CopyErrorData();
				ereport(WARNING,
						(errmsg("auto-partition: %s.%s: retention drop failed for oid %u: %s",
								nsname, relname, old_oid, edata->message)));
				FreeErrorData(edata);
				FlushErrorState();
				RollbackAndReleaseCurrentSubTransaction();
			}
			PG_END_TRY();
		}
	}
}

/* ---------------------------------------------------------------- */

/*
 * One scan iteration: open a transaction, find every relation with a
 * non-OFF auto_partition_strategy reloption, dispatch by strategy, then
 * commit.
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

		ereport(DEBUG1,
				(errmsg("auto-partition: %s.%s (oid=%u, relkind=%c) "
						"strategy=%s column=%s interval=%s retention=%d",
						nsname ? nsname : "?",
						relname ? relname : "?",
						reloid,
						relkind,
						strategy ? strategy : "?",
						partcol ? partcol : "(none)",
						partinterval ? partinterval : "(none)",
						retention)));

		/*
		 * Dispatch by strategy + relkind.  Unpartitioned relations
		 * (relkind='r') are skipped — Phase 3 (online migration) will
		 * handle initial conversion from heap to partitioned.
		 *
		 * range_date and list_int are not yet implemented; logged at
		 * DEBUG and otherwise no-op so configuring those strategies
		 * doesn't cause spurious WARNINGs every iteration.
		 */
		if (relkind != 'p')
			continue;

		if (strategy && strcmp(strategy, "range_int") == 0)
			process_range_int_partition(reloid, nsname, relname,
										partinterval, retention);
		else if (strategy &&
				 (strcmp(strategy, "range_date") == 0 ||
				  strcmp(strategy, "list_int") == 0))
			ereport(DEBUG1,
					(errmsg("auto-partition: %s.%s: strategy '%s' not yet implemented",
							nsname, relname, strategy)));
	}

done:
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_stat(false);
	pgstat_report_activity(STATE_IDLE, NULL);
}
