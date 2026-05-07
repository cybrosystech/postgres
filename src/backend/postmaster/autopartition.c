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
 * Phase 3a: convert an unpartitioned heap into a partitioned table with
 * one initial partition covering the existing data range.
 *
 * This is offline-with-brief-lock: the data copy runs while concurrent
 * writes are still happening, then we take ACCESS EXCLUSIVE on the
 * source for the rename swap.  Concurrent writers see a brief (typically
 * sub-second on small tables, longer on big ones) blockage during the
 * swap.  Phase 3b will add live change-capture so even the swap is
 * effectively zero-pause.
 *
 * Constraints (relaxed in later phases):
 *   * range_int strategy only.
 *   * PK must be a single column equal to partition_column.
 *   * No FKs in or out, no user triggers.
 *   * Brief ACCESS EXCLUSIVE during swap.
 *
 * On success the source name now points at a partitioned relation; the
 * old heap is dropped.  On failure the subtransaction is rolled back
 * and a WARNING is logged; the launcher will retry on the next
 * iteration.
 *
 * Returns true if the source was successfully converted (so the caller
 * skips further processing this iteration), false if conversion was
 * skipped or failed.
 */
static bool
convert_heap_to_partitioned(Oid reloid, const char *nsname, const char *relname,
							const char *partcol, const char *partinterval,
							int32 retention)
{
	StringInfoData buf;
	int			ret;
	int64		interval_int;
	char	   *endptr;
	bool		converted = false;
	int64		min_val = 0,
				max_val = 0;
	int64		row_count = 0;
	int64		lower_bound,
				upper_bound;
	bool		has_fk_in,
				has_fk_out,
				has_trigger;
	bool		pk_ok;

	/* Parse interval */
	if (partinterval == NULL || *partinterval == '\0')
		return false;
	errno = 0;
	interval_int = strtoll(partinterval, &endptr, 10);
	if (errno != 0 || endptr == partinterval || *endptr != '\0' || interval_int <= 0)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: invalid range_int interval \"%s\" for conversion",
						nsname, relname, partinterval)));
		return false;
	}

	/*
	 * Validate prereqs: PK is a single column matching partcol; no FKs
	 * in/out; no user triggers.  All checks via separate SPI queries
	 * because mixing them in one query gets ugly.
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT EXISTS ( "
					 "  SELECT 1 FROM pg_constraint con "
					 "  JOIN pg_attribute a "
					 "    ON a.attrelid = con.conrelid "
					 "   AND a.attnum = ANY(con.conkey) "
					 "  WHERE con.conrelid = %u "
					 "    AND con.contype = 'p' "
					 "    AND array_length(con.conkey,1) = 1 "
					 "    AND a.attname = %s)",
					 reloid, quote_literal_cstr(partcol));
	ret = SPI_execute(buf.data, true, 0);
	pfree(buf.data);
	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: conversion check failed",
						nsname, relname)));
		return false;
	}
	{
		bool		isnull;

		pk_ok = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
										   SPI_tuptable->tupdesc, 1, &isnull));
	}
	if (!pk_ok)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: cannot convert (PK must be single column equal to partition_column %s)",
						nsname, relname, partcol)));
		return false;
	}

	/* FK / trigger checks */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT "
					 "  EXISTS(SELECT 1 FROM pg_constraint WHERE confrelid=%u AND contype='f') AS fk_in, "
					 "  EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=%u  AND contype='f') AS fk_out, "
					 "  EXISTS(SELECT 1 FROM pg_trigger    WHERE tgrelid=%u   AND NOT tgisinternal) AS trig",
					 reloid, reloid, reloid);
	ret = SPI_execute(buf.data, true, 0);
	pfree(buf.data);
	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: FK/trigger check failed",
						nsname, relname)));
		return false;
	}
	{
		bool		isnull;

		has_fk_in = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
											   SPI_tuptable->tupdesc, 1, &isnull));
		has_fk_out = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
											   SPI_tuptable->tupdesc, 2, &isnull));
		has_trigger = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc, 3, &isnull));
	}
	if (has_fk_in || has_fk_out || has_trigger)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: skipping conversion — relation has %s%s%s",
						nsname, relname,
						has_fk_in ? "inbound FKs " : "",
						has_fk_out ? "outbound FKs " : "",
						has_trigger ? "user triggers" : ""),
				 errhint("Drop FKs/triggers, let the launcher convert, then recreate.")));
		return false;
	}

	/* Compute initial partition bounds */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT min(%s), max(%s), count(*)::bigint FROM %s.%s",
					 quote_identifier(partcol),
					 quote_identifier(partcol),
					 quote_identifier(nsname),
					 quote_identifier(relname));
	ret = SPI_execute(buf.data, true, 0);
	pfree(buf.data);
	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: failed to read min/max/count",
						nsname, relname)));
		return false;
	}
	{
		bool		isnull;

		min_val = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc, 1, &isnull));
		if (isnull)
			row_count = 0;
		else
		{
			max_val = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												  SPI_tuptable->tupdesc, 2, &isnull));
			row_count = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
													SPI_tuptable->tupdesc, 3, &isnull));
		}
	}

	if (row_count == 0)
	{
		lower_bound = 0;
		upper_bound = interval_int;
	}
	else
	{
		lower_bound = (min_val / interval_int) * interval_int;
		if (min_val < 0 && (min_val % interval_int) != 0)
			lower_bound -= interval_int;
		upper_bound = ((max_val / interval_int) + 1) * interval_int;
	}

	ereport(LOG,
			(errmsg("auto-partition: converting %s.%s (rows=%lld, %s=[%lld..%lld]) -> partitioned [%lld, %lld)",
					nsname, relname,
					(long long) row_count, partcol,
					(long long) min_val, (long long) max_val,
					(long long) lower_bound, (long long) upper_bound)));

	/*
	 * The actual conversion runs inside a subtransaction: any failure
	 * leaves the source heap untouched and the launcher will retry next
	 * iteration.
	 */
	BeginInternalSubTransaction("autopart_convert");
	PG_TRY();
	{
		char		tmp_name[NAMEDATALEN];
		char		init_name[NAMEDATALEN];
		char		final_init_name[NAMEDATALEN];

		snprintf(tmp_name, sizeof(tmp_name), "%s__autopart_new", relname);
		snprintf(init_name, sizeof(init_name), "%s_initial", tmp_name);
		snprintf(final_init_name, sizeof(final_init_name),
				 "%s_p_%lld", relname, (long long) lower_bound);

		/* 1. Create empty partitioned table mirroring the source. */
		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "CREATE TABLE %s.%s (LIKE %s.%s INCLUDING ALL) "
						 "PARTITION BY RANGE (%s)",
						 quote_identifier(nsname),
						 quote_identifier(tmp_name),
						 quote_identifier(nsname),
						 quote_identifier(relname),
						 quote_identifier(partcol));
		SPI_exec(buf.data, 0);
		pfree(buf.data);

		/* 2. Initial partition covering the existing data range. */
		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "CREATE TABLE %s.%s PARTITION OF %s.%s "
						 "FOR VALUES FROM (%lld) TO (%lld)",
						 quote_identifier(nsname),
						 quote_identifier(init_name),
						 quote_identifier(nsname),
						 quote_identifier(tmp_name),
						 (long long) lower_bound,
						 (long long) upper_bound);
		SPI_exec(buf.data, 0);
		pfree(buf.data);

		/* 3. Copy existing rows.  Single statement, single transaction. */
		if (row_count > 0)
		{
			initStringInfo(&buf);
			appendStringInfo(&buf,
							 "INSERT INTO %s.%s SELECT * FROM %s.%s",
							 quote_identifier(nsname),
							 quote_identifier(tmp_name),
							 quote_identifier(nsname),
							 quote_identifier(relname));
			SPI_exec(buf.data, 0);
			pfree(buf.data);
		}

		/* 4. Atomic swap (brief ACCESS EXCLUSIVE on source).
		 *
		 *    Tricky bit: CREATE TABLE LIKE INCLUDING ALL copies the
		 *    DEFAULT clause for each column, including DEFAULT
		 *    nextval('<seq>') from bigserial/serial columns.  The new
		 *    table's column then depends on a sequence owned by the
		 *    old table, so DROP TABLE on the source fails with
		 *    "other objects depend on it".
		 *
		 *    We unhook the sequences (OWNED BY NONE) before the DROP,
		 *    then re-own them to the swapped-in relation's column
		 *    after the rename so the new table cleans up properly on
		 *    eventual DROP.
		 */
		{
			SPITupleTable *seqtab;
			int			seq_ret;
			int			seq_count;
			char	  **seq_names = NULL;
			char	  **seq_columns = NULL;

			initStringInfo(&buf);
			appendStringInfo(&buf,
							 "SELECT s.relname, a.attname "
							 "FROM pg_depend d "
							 "JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S' "
							 "JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid "
							 "WHERE d.refobjid = %u "
							 "  AND d.classid = 'pg_class'::regclass "
							 "  AND d.refclassid = 'pg_class'::regclass "
							 "  AND d.deptype = 'a'",
							 reloid);
			seq_ret = SPI_execute(buf.data, true, 0);
			pfree(buf.data);

			if (seq_ret == SPI_OK_SELECT && SPI_processed > 0)
			{
				MemoryContext oldcxt;

				seq_count = (int) SPI_processed;
				seqtab = SPI_tuptable;
				oldcxt = MemoryContextSwitchTo(TopTransactionContext);
				seq_names = palloc(sizeof(char *) * seq_count);
				seq_columns = palloc(sizeof(char *) * seq_count);
				for (int s = 0; s < seq_count; s++)
				{
					seq_names[s] = pstrdup(SPI_getvalue(seqtab->vals[s], seqtab->tupdesc, 1));
					seq_columns[s] = pstrdup(SPI_getvalue(seqtab->vals[s], seqtab->tupdesc, 2));
				}
				MemoryContextSwitchTo(oldcxt);
			}
			else
				seq_count = 0;

			/* Detach all owned sequences from the source. */
			for (int s = 0; s < seq_count; s++)
			{
				initStringInfo(&buf);
				appendStringInfo(&buf, "ALTER SEQUENCE %s.%s OWNED BY NONE",
								 quote_identifier(nsname),
								 quote_identifier(seq_names[s]));
				SPI_exec(buf.data, 0);
				pfree(buf.data);
			}

			/* Now the actual swap. */
			initStringInfo(&buf);
			appendStringInfo(&buf, "LOCK TABLE %s.%s IN ACCESS EXCLUSIVE MODE",
							 quote_identifier(nsname),
							 quote_identifier(relname));
			SPI_exec(buf.data, 0);
			pfree(buf.data);

			initStringInfo(&buf);
			appendStringInfo(&buf, "DROP TABLE %s.%s",
							 quote_identifier(nsname),
							 quote_identifier(relname));
			SPI_exec(buf.data, 0);
			pfree(buf.data);

			initStringInfo(&buf);
			appendStringInfo(&buf, "ALTER TABLE %s.%s RENAME TO %s",
							 quote_identifier(nsname),
							 quote_identifier(tmp_name),
							 quote_identifier(relname));
			SPI_exec(buf.data, 0);
			pfree(buf.data);

			initStringInfo(&buf);
			appendStringInfo(&buf, "ALTER TABLE %s.%s RENAME TO %s",
							 quote_identifier(nsname),
							 quote_identifier(init_name),
							 quote_identifier(final_init_name));
			SPI_exec(buf.data, 0);
			pfree(buf.data);

			/* Re-attach sequences to the new (now correctly-named) table. */
			for (int s = 0; s < seq_count; s++)
			{
				initStringInfo(&buf);
				appendStringInfo(&buf,
								 "ALTER SEQUENCE %s.%s OWNED BY %s.%s.%s",
								 quote_identifier(nsname),
								 quote_identifier(seq_names[s]),
								 quote_identifier(nsname),
								 quote_identifier(relname),
								 quote_identifier(seq_columns[s]));
				SPI_exec(buf.data, 0);
				pfree(buf.data);
			}
		}

		/*
		 * 5. Re-apply the auto_partition reloptions on the swapped-in
		 *    relation.  CREATE TABLE LIKE INCLUDING ALL did NOT copy
		 *    reloptions (and shouldn't have — they referenced the old
		 *    heap); we set them now so the launcher sees the new
		 *    partitioned table on the next iteration.
		 */
		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "ALTER TABLE %s.%s SET ("
						 "auto_partition_strategy = %s, "
						 "auto_partition_column = %s, "
						 "auto_partition_interval = %s, "
						 "auto_partition_retention = %d)",
						 quote_identifier(nsname),
						 quote_identifier(relname),
						 quote_literal_cstr("range_int"),
						 quote_literal_cstr(partcol),
						 quote_literal_cstr(partinterval),
						 retention);
		SPI_exec(buf.data, 0);
		pfree(buf.data);

		ereport(LOG,
				(errmsg("auto-partition: converted %s.%s, initial partition %s.%s",
						nsname, relname, nsname, final_init_name)));
		converted = true;

		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(TopTransactionContext);
		edata = CopyErrorData();
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: conversion failed: %s",
						nsname, relname, edata->message)));
		FreeErrorData(edata);
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		converted = false;
	}
	PG_END_TRY();

	return converted;
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
		 * Dispatch by strategy + relkind.
		 *
		 * relkind='r' (regular heap) + range_int: try to convert the
		 *   heap to partitioned (Phase 3a — brief lock during swap).
		 *   On success the relation will be relkind='p' next iteration
		 *   and ordinary rotation takes over.
		 *
		 * relkind='p' + range_int: ordinary rotation (create next
		 *   partition, enforce retention).
		 *
		 * range_date / list_int strategies are not yet implemented for
		 * either path; logged at DEBUG.
		 */
		if (strategy && strcmp(strategy, "range_int") == 0)
		{
			if (relkind == 'r')
				(void) convert_heap_to_partitioned(reloid, nsname, relname,
												   partcol, partinterval,
												   retention);
			else if (relkind == 'p')
				process_range_int_partition(reloid, nsname, relname,
											partinterval, retention);
		}
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
