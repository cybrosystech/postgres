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
 * any backends are forked, alongside ApplyLauncherRegister().
 *
 * GUC definitions for the launcher live in guc_parameters.dat (so they
 * are known to the GUC system before postgresql.conf /
 * postgresql.auto.conf are parsed at startup) — see the auto_partition_*
 * entries near the top of that file.
 *
 * Always registers a worker; the worker itself checks
 * `auto_partition_enabled` each loop iteration and idles when off.
 */
void
AutoPartitionLauncherRegister(void)
{
	BackgroundWorker bgw;

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
/*
 * Phase 3a-date: convert an unpartitioned heap to a date-partitioned
 * table.  Sibling of convert_heap_to_partitioned() but with timestamp /
 * interval arithmetic.
 *
 * Preconditions (the launcher logs a WARNING and skips otherwise):
 *   * partition_column has type timestamp / timestamptz / date.
 *   * The relation has a primary key that includes partition_column.
 *     Composite PKs like (id, create_date) are fine and are the typical
 *     shape.  PK validity is confirmed by the eventual CREATE TABLE
 *     LIKE — if it fails the subtransaction rolls back cleanly.
 *   * No FKs in/out and no user triggers.
 *
 * Initial partition spans [min(col), max(col) + interval).  No interval
 * snapping — date_trunc only works for standard units, and supporting
 * arbitrary intervals like '7 days' or '36 hours' generically is a
 * follow-up.  The initial partition is one large bucket; subsequent
 * partitions follow the user's interval cleanly.
 */
static bool
convert_heap_to_partitioned_date(Oid reloid, const char *nsname,
								 const char *relname, const char *partcol,
								 const char *partinterval, int32 retention)
{
	StringInfoData buf;
	int			ret;
	bool		converted = false;
	bool		has_fk_in,
				has_fk_out,
				has_trigger;
	bool		col_ok;
	bool		pk_ok;
	int64		row_count = 0;
	char	   *min_text = NULL;
	char	   *max_text = NULL;
	char	   *upper_text = NULL;
	char	   *suffix = NULL;
	MemoryContext savecxt;

	if (partinterval == NULL || *partinterval == '\0')
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: range_date strategy needs auto_partition_interval",
						nsname, relname)));
		return false;
	}

	/* 1. Type check: partcol is timestamp/timestamptz/date. */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT EXISTS ("
					 "  SELECT 1 FROM pg_attribute a "
					 "  JOIN pg_type t ON t.oid = a.atttypid "
					 "  WHERE a.attrelid = %u "
					 "    AND a.attname = %s "
					 "    AND t.typname IN ('timestamp','timestamptz','date'))",
					 reloid, quote_literal_cstr(partcol));
	ret = SPI_execute(buf.data, true, 0);
	pfree(buf.data);
	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: column-type check failed",
						nsname, relname)));
		return false;
	}
	{
		bool		isnull;

		col_ok = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1, &isnull));
	}
	if (!col_ok)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: column %s must be timestamp/timestamptz/date for range_date",
						nsname, relname, partcol)));
		return false;
	}

	/* 2. PK must include partition_column. */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT EXISTS ("
					 "  SELECT 1 FROM pg_constraint con "
					 "  JOIN pg_attribute a "
					 "    ON a.attrelid = con.conrelid "
					 "   AND a.attnum = ANY(con.conkey) "
					 "  WHERE con.conrelid = %u "
					 "    AND con.contype = 'p' "
					 "    AND a.attname = %s)",
					 reloid, quote_literal_cstr(partcol));
	ret = SPI_execute(buf.data, true, 0);
	pfree(buf.data);
	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: PK check failed",
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
				(errmsg("auto-partition: %s.%s: range_date conversion needs %s in the primary key",
						nsname, relname, partcol)));
		return false;
	}

	/* 3. FK / trigger checks (same as int variant). */
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
						has_trigger ? "user triggers" : "")));
		return false;
	}

	/* 4. Compute initial partition bounds. */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT min(%s)::text, "
					 "       (max(%s) + %s::interval)::text, "
					 "       to_char(min(%s), 'YYYYMMDDHH24MISS'), "
					 "       count(*)::bigint "
					 "FROM %s.%s",
					 quote_identifier(partcol),
					 quote_identifier(partcol),
					 quote_literal_cstr(partinterval),
					 quote_identifier(partcol),
					 quote_identifier(nsname),
					 quote_identifier(relname));
	ret = SPI_execute(buf.data, true, 0);
	pfree(buf.data);
	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: failed to compute initial bounds",
						nsname, relname)));
		return false;
	}
	{
		bool		isnull;
		char	   *s;

		s = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		savecxt = MemoryContextSwitchTo(TopTransactionContext);
		min_text = s ? pstrdup(s) : NULL;
		s = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
		upper_text = s ? pstrdup(s) : NULL;
		s = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
		suffix = s ? pstrdup(s) : NULL;
		MemoryContextSwitchTo(savecxt);
		row_count = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												SPI_tuptable->tupdesc, 4, &isnull));
		(void) max_text;		/* not currently used; kept for clarity */
	}

	if (row_count == 0)
	{
		/* Empty table: anchor on now() so the launcher has a starting bound. */
		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "SELECT now()::text, "
						 "       (now() + %s::interval)::text, "
						 "       to_char(now(), 'YYYYMMDDHH24MISS')",
						 quote_literal_cstr(partinterval));
		ret = SPI_execute(buf.data, true, 0);
		pfree(buf.data);
		if (ret == SPI_OK_SELECT && SPI_processed > 0)
		{
			char	   *s;

			savecxt = MemoryContextSwitchTo(TopTransactionContext);
			s = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
			min_text = s ? pstrdup(s) : NULL;
			s = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
			upper_text = s ? pstrdup(s) : NULL;
			s = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
			suffix = s ? pstrdup(s) : NULL;
			MemoryContextSwitchTo(savecxt);
		}
	}

	if (min_text == NULL || upper_text == NULL || suffix == NULL)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: bound computation produced NULL",
						nsname, relname)));
		return false;
	}

	ereport(LOG,
			(errmsg("auto-partition: converting %s.%s (rows=%lld, %s) -> partitioned [%s, %s)",
					nsname, relname, (long long) row_count, partcol,
					min_text, upper_text)));

	/* 5. Conversion proper, in a subtransaction. */
	BeginInternalSubTransaction("autopart_convert_date");
	PG_TRY();
	{
		char		tmp_name[NAMEDATALEN];
		char		init_name[NAMEDATALEN];
		char		final_init_name[NAMEDATALEN];

		snprintf(tmp_name, sizeof(tmp_name), "%s__autopart_new", relname);
		snprintf(init_name, sizeof(init_name), "%s_initial", tmp_name);
		snprintf(final_init_name, sizeof(final_init_name),
				 "%s_p_%s", relname, suffix);

		/* a. New partitioned parent. */
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

		/* b. Initial partition spanning the existing data. */
		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "CREATE TABLE %s.%s PARTITION OF %s.%s "
						 "FOR VALUES FROM (%s) TO (%s)",
						 quote_identifier(nsname),
						 quote_identifier(init_name),
						 quote_identifier(nsname),
						 quote_identifier(tmp_name),
						 quote_literal_cstr(min_text),
						 quote_literal_cstr(upper_text));
		SPI_exec(buf.data, 0);
		pfree(buf.data);

		/* c. Copy rows. */
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

		/*
		 * d. Detach owned sequences before DROP, then re-attach to the
		 *    swapped-in relation.  Same dance as the int variant —
		 *    LIKE INCLUDING ALL copies DEFAULT clauses that reference
		 *    sequences owned by the source.
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

			for (int s = 0; s < seq_count; s++)
			{
				initStringInfo(&buf);
				appendStringInfo(&buf, "ALTER SEQUENCE %s.%s OWNED BY NONE",
								 quote_identifier(nsname),
								 quote_identifier(seq_names[s]));
				SPI_exec(buf.data, 0);
				pfree(buf.data);
			}

			/* The actual swap. */
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

		/* e. Re-apply auto_partition reloptions. */
		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "ALTER TABLE %s.%s SET ("
						 "auto_partition_strategy = %s, "
						 "auto_partition_column = %s, "
						 "auto_partition_interval = %s, "
						 "auto_partition_retention = %d)",
						 quote_identifier(nsname),
						 quote_identifier(relname),
						 quote_literal_cstr("range_date"),
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
				(errmsg("auto-partition: %s.%s: date conversion failed: %s",
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
	bool		has_fk_in PG_USED_FOR_ASSERTS_ONLY = false,
				has_fk_out PG_USED_FOR_ASSERTS_ONLY = false,
				has_trigger;
	bool		pk_ok;

	/*
	 * Captured FK state, lives in TopTransactionContext so it survives
	 * subtxn rollback if we have to retry.  All four arrays for inbound
	 * are parallel; out_fk_* are parallel.
	 */
	int			out_fk_count = 0;
	char	  **out_fk_names = NULL;
	char	  **out_fk_defs = NULL;
	int			in_fk_count = 0;
	char	  **in_fk_names = NULL;
	char	  **in_fk_defs = NULL;
	char	  **in_fk_owners = NULL;
	char	  **in_fk_owner_schemas = NULL;

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

	/* Trigger check: still refuse user triggers (Phase 3c+ TODO). */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT EXISTS(SELECT 1 FROM pg_trigger "
					 "              WHERE tgrelid=%u AND NOT tgisinternal)",
					 reloid);
	ret = SPI_execute(buf.data, true, 0);
	pfree(buf.data);
	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: trigger check failed",
						nsname, relname)));
		return false;
	}
	{
		bool		isnull;

		has_trigger = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc, 1, &isnull));
	}
	if (has_trigger)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: skipping conversion — relation has user triggers",
						nsname, relname),
				 errhint("Drop triggers, let the launcher convert, then recreate.")));
		return false;
	}
	(void) has_fk_in;			/* now captured + restored, not refused */
	(void) has_fk_out;

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
	 * Capture FK definitions before the subtransaction, in
	 * TopTransactionContext so the strings survive any subtxn rollback.
	 *
	 * Outbound FKs live on the source itself (con.conrelid = reloid).
	 * Inbound FKs live on other tables and reference the source
	 * (con.confrelid = reloid).  For each we save (a) the qualified
	 * table on which the constraint sits, and (b) pg_get_constraintdef
	 * which renders an ALTER-able definition like
	 *    "FOREIGN KEY (author_id) REFERENCES public.res_partner(id) ..."
	 *
	 * After the swap the source name resolves to the new partitioned
	 * relation, so re-adding the constraints with the same definitions
	 * just works.
	 */
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		/* Outbound */
		MemoryContextSwitchTo(oldcxt);

		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "SELECT con.conname, pg_get_constraintdef(con.oid) "
						 "FROM pg_constraint con "
						 "WHERE con.conrelid = %u AND con.contype = 'f' "
						 "ORDER BY con.conname",
						 reloid);
		ret = SPI_execute(buf.data, true, 0);
		pfree(buf.data);
		if (ret == SPI_OK_SELECT && SPI_processed > 0)
		{
			MemoryContext c = MemoryContextSwitchTo(TopTransactionContext);

			out_fk_count = (int) SPI_processed;
			out_fk_names = palloc(sizeof(char *) * out_fk_count);
			out_fk_defs = palloc(sizeof(char *) * out_fk_count);
			for (int k = 0; k < out_fk_count; k++)
			{
				out_fk_names[k] = pstrdup(SPI_getvalue(SPI_tuptable->vals[k],
													   SPI_tuptable->tupdesc, 1));
				out_fk_defs[k] = pstrdup(SPI_getvalue(SPI_tuptable->vals[k],
													  SPI_tuptable->tupdesc, 2));
			}
			MemoryContextSwitchTo(c);
		}

		/* Inbound */
		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "SELECT con.conname, "
						 "       pg_get_constraintdef(con.oid), "
						 "       n.nspname, c.relname "
						 "FROM pg_constraint con "
						 "JOIN pg_class c     ON c.oid = con.conrelid "
						 "JOIN pg_namespace n ON n.oid = c.relnamespace "
						 "WHERE con.confrelid = %u AND con.contype = 'f' "
						 "ORDER BY con.conname",
						 reloid);
		ret = SPI_execute(buf.data, true, 0);
		pfree(buf.data);
		if (ret == SPI_OK_SELECT && SPI_processed > 0)
		{
			MemoryContext c = MemoryContextSwitchTo(TopTransactionContext);

			in_fk_count = (int) SPI_processed;
			in_fk_names = palloc(sizeof(char *) * in_fk_count);
			in_fk_defs = palloc(sizeof(char *) * in_fk_count);
			in_fk_owners = palloc(sizeof(char *) * in_fk_count);
			in_fk_owner_schemas = palloc(sizeof(char *) * in_fk_count);
			for (int k = 0; k < in_fk_count; k++)
			{
				in_fk_names[k] = pstrdup(SPI_getvalue(SPI_tuptable->vals[k],
													  SPI_tuptable->tupdesc, 1));
				in_fk_defs[k] = pstrdup(SPI_getvalue(SPI_tuptable->vals[k],
													 SPI_tuptable->tupdesc, 2));
				in_fk_owner_schemas[k] = pstrdup(SPI_getvalue(SPI_tuptable->vals[k],
															  SPI_tuptable->tupdesc, 3));
				in_fk_owners[k] = pstrdup(SPI_getvalue(SPI_tuptable->vals[k],
													   SPI_tuptable->tupdesc, 4));
			}
			MemoryContextSwitchTo(c);
		}
	}

	if (out_fk_count + in_fk_count > 0)
		ereport(LOG,
				(errmsg("auto-partition: %s.%s: capturing %d outbound + %d inbound FKs across conversion",
						nsname, relname, out_fk_count, in_fk_count)));

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

		/*
		 * 0. Drop FKs first.  Outbound ones go away with their owning
		 * table (the soon-to-be-DROP'd source) anyway, but explicitly
		 * dropping outbound FKs before the source DROP also frees us
		 * from any LIKE-INCLUDING-ALL pickups.  Inbound FKs MUST be
		 * dropped first because they'd otherwise block DROP TABLE on
		 * the source.
		 */
		for (int k = 0; k < in_fk_count; k++)
		{
			initStringInfo(&buf);
			appendStringInfo(&buf,
							 "ALTER TABLE %s.%s DROP CONSTRAINT %s",
							 quote_identifier(in_fk_owner_schemas[k]),
							 quote_identifier(in_fk_owners[k]),
							 quote_identifier(in_fk_names[k]));
			SPI_exec(buf.data, 0);
			pfree(buf.data);
		}
		for (int k = 0; k < out_fk_count; k++)
		{
			initStringInfo(&buf);
			appendStringInfo(&buf,
							 "ALTER TABLE %s.%s DROP CONSTRAINT %s",
							 quote_identifier(nsname),
							 quote_identifier(relname),
							 quote_identifier(out_fk_names[k]));
			SPI_exec(buf.data, 0);
			pfree(buf.data);
		}

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

		/*
		 * 6. Re-create FKs.  Outbound first (constraint owned by the
		 *    swapped-in relation), then inbound (constraints on other
		 *    tables that reference us).  pg_get_constraintdef rendered
		 *    each definition with full schema-qualified targets, so the
		 *    same string works against the new partitioned relation.
		 *
		 *    PG validates each constraint against existing data when it
		 *    is added; for a freshly-copied table that's a full scan
		 *    of every relevant tuple.  Phase 3d will switch to NOT
		 *    VALID + later VALIDATE so the swap window stays small even
		 *    on huge tables.
		 */
		for (int k = 0; k < out_fk_count; k++)
		{
			initStringInfo(&buf);
			appendStringInfo(&buf,
							 "ALTER TABLE %s.%s ADD CONSTRAINT %s %s",
							 quote_identifier(nsname),
							 quote_identifier(relname),
							 quote_identifier(out_fk_names[k]),
							 out_fk_defs[k]);
			SPI_exec(buf.data, 0);
			pfree(buf.data);
		}
		for (int k = 0; k < in_fk_count; k++)
		{
			initStringInfo(&buf);
			appendStringInfo(&buf,
							 "ALTER TABLE %s.%s ADD CONSTRAINT %s %s",
							 quote_identifier(in_fk_owner_schemas[k]),
							 quote_identifier(in_fk_owners[k]),
							 quote_identifier(in_fk_names[k]),
							 in_fk_defs[k]);
			SPI_exec(buf.data, 0);
			pfree(buf.data);
		}

		ereport(LOG,
				(errmsg("auto-partition: converted %s.%s, initial partition %s.%s%s",
						nsname, relname, nsname, final_init_name,
						(out_fk_count + in_fk_count > 0)
							? " (FKs preserved)" : "")));
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
 * Process a single range_date-partitioned relation.
 *
 * Mirrors process_range_int_partition() but with timestamp / interval
 * arithmetic instead of int64 row counts.  Bounds in pg_class.relpartbound
 * print as quoted ISO-8601 timestamps (e.g. 'FOR VALUES FROM (''2026-01-01
 * 00:00:00+00'') TO (''2026-02-01 00:00:00+00'')'); we round-trip them
 * through SPI so PG handles all timestamp parsing and interval math
 * natively.
 *
 * Naming: child partitions are named <parent>_p_YYYYMMDDHH24MISS using
 * the lower bound, which gives sortable, unique, fixed-width suffixes
 * for any interval granularity (daily/monthly/yearly all fit).
 */
static void
process_range_date_partition(Oid reloid, const char *nsname, const char *relname,
							 const char *partcol, const char *partinterval,
							 int32 retention)
{
	StringInfoData buf;
	int			ret;
	int			partition_count;
	char	   *head_relname = NULL;
	char	   *head_lower_text = NULL;
	char	   *head_upper_text = NULL;
	List	   *oldest_oids = NIL;
	MemoryContext savecxt;

	if (partinterval == NULL || *partinterval == '\0')
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: range_date strategy needs auto_partition_interval (e.g. '1 month')",
						nsname, relname)));
		return;
	}

	/*
	 * List partitions, deparsed bounds parsed as text (we don't ::cast in
	 * SQL so a malformed bound — e.g. MINVALUE/MAXVALUE — comes back as
	 * NULL rather than failing the whole query).
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT child.oid::oid AS child_oid, "
					 "       child.relname AS child_name, "
					 "       (regexp_match(pg_get_expr(child.relpartbound, child.oid), "
					 "                     'FROM \\(''([^'']+)''\\)'))[1] AS lower_text, "
					 "       (regexp_match(pg_get_expr(child.relpartbound, child.oid), "
					 "                     'TO \\(''([^'']+)''\\)'))[1] AS upper_text "
					 "FROM pg_inherits i "
					 "JOIN pg_class child ON child.oid = i.inhrelid "
					 "WHERE i.inhparent = %u "
					 "ORDER BY (regexp_match(pg_get_expr(child.relpartbound, child.oid), "
					 "                       'TO \\(''([^'']+)''\\)'))[1]::timestamptz DESC NULLS LAST",
					 reloid);
	ret = SPI_execute(buf.data, true, 0);
	pfree(buf.data);

	if (ret != SPI_OK_SELECT)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: failed to list date partitions (rc=%d)",
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

	/* Capture row 0 (newest) before SPI_tuptable gets clobbered. */
	{
		char	   *namestr;
		char	   *lstr;
		char	   *ustr;

		namestr = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
		lstr = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
		ustr = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4);

		savecxt = MemoryContextSwitchTo(TopTransactionContext);
		if (namestr)
			head_relname = pstrdup(namestr);
		if (lstr)
			head_lower_text = pstrdup(lstr);
		if (ustr)
			head_upper_text = pstrdup(ustr);
		MemoryContextSwitchTo(savecxt);
	}

	if (head_upper_text == NULL || head_relname == NULL)
	{
		ereport(WARNING,
				(errmsg("auto-partition: %s.%s: head partition bounds unparseable, skipping",
						nsname, relname)));
		return;
	}

	/* Save oldest partition oids for retention enforcement. */
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
	 * Fill-check: probe head partition for max(<partcol>).  Empty head, or
	 * head used < 80% of interval, means no new partition needed yet.
	 *
	 * For range_date the "used" / "interval" comparison happens entirely
	 * in SQL since we can't do timestamp - timestamp math in C trivially.
	 * The query returns a boolean: TRUE if 5 * (max - lower) >= 4 *
	 * (upper - lower), i.e. the head is at least 80% full.
	 */
	{
		bool		head_full = false;
		bool		head_empty;

		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "SELECT max(%s) IS NULL AS empty, "
						 "       CASE WHEN max(%s) IS NULL THEN false "
						 "            ELSE 5 * EXTRACT(EPOCH FROM max(%s) - %s::timestamptz) "
						 "                 >= 4 * EXTRACT(EPOCH FROM %s::timestamptz - %s::timestamptz) "
						 "       END AS at_threshold "
						 "FROM %s.%s",
						 quote_identifier(partcol),
						 quote_identifier(partcol),
						 quote_identifier(partcol),
						 quote_literal_cstr(head_lower_text),
						 quote_literal_cstr(head_upper_text),
						 quote_literal_cstr(head_lower_text),
						 quote_identifier(nsname),
						 quote_identifier(head_relname));
		ret = SPI_execute(buf.data, true, 0);
		pfree(buf.data);

		if (ret != SPI_OK_SELECT || SPI_processed == 0)
		{
			ereport(WARNING,
					(errmsg("auto-partition: %s.%s: fill-check failed",
							nsname, relname)));
			return;
		}
		{
			bool		isnull;

			head_empty = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
													SPI_tuptable->tupdesc,
													1, &isnull));
			head_full = !head_empty &&
				DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
										   SPI_tuptable->tupdesc,
										   2, &isnull));
		}

		if (head_empty)
		{
			ereport(DEBUG1,
					(errmsg("auto-partition: %s.%s: head partition %s is empty, skipping create",
							nsname, relname, head_relname)));
		}
		else if (!head_full)
		{
			ereport(DEBUG1,
					(errmsg("auto-partition: %s.%s: head %s not yet at 80%%, waiting",
							nsname, relname, head_relname)));
		}

		/*
		 * Create next partition only if head is past 80%.
		 *
		 * Compute next bounds via SQL ($latest_upper, $latest_upper +
		 * INTERVAL '...') and a partition-name suffix from the lower bound.
		 * We render the new lower/upper as text so the resulting CREATE
		 * TABLE can use them as quoted literals in FOR VALUES.
		 */
		if (head_full)
		{
			char	   *new_lower_text = NULL;
			char	   *new_upper_text = NULL;
			char	   *suffix = NULL;

			initStringInfo(&buf);
			appendStringInfo(&buf,
							 "SELECT (%s::timestamptz)::text AS lo, "
							 "       (%s::timestamptz + %s::interval)::text AS hi, "
							 "       to_char(%s::timestamptz, 'YYYYMMDDHH24MISS') AS sfx",
							 quote_literal_cstr(head_upper_text),
							 quote_literal_cstr(head_upper_text),
							 quote_literal_cstr(partinterval),
							 quote_literal_cstr(head_upper_text));
			ret = SPI_execute(buf.data, true, 0);
			pfree(buf.data);

			if (ret == SPI_OK_SELECT && SPI_processed > 0)
			{
				char	   *lo,
						   *hi,
						   *sfx;

				lo = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
				hi = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
				sfx = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);

				savecxt = MemoryContextSwitchTo(TopTransactionContext);
				new_lower_text = lo ? pstrdup(lo) : NULL;
				new_upper_text = hi ? pstrdup(hi) : NULL;
				suffix = sfx ? pstrdup(sfx) : NULL;
				MemoryContextSwitchTo(savecxt);
			}

			if (new_lower_text && new_upper_text && suffix)
			{
				char		child_name[NAMEDATALEN];

				snprintf(child_name, sizeof(child_name),
						 "%s_p_%s", relname, suffix);

				BeginInternalSubTransaction("autopart_create_date");
				PG_TRY();
				{
					initStringInfo(&buf);
					appendStringInfo(&buf,
									 "CREATE TABLE IF NOT EXISTS %s.%s "
									 "PARTITION OF %s.%s "
									 "FOR VALUES FROM (%s) TO (%s)",
									 quote_identifier(nsname),
									 quote_identifier(child_name),
									 quote_identifier(nsname),
									 quote_identifier(relname),
									 quote_literal_cstr(new_lower_text),
									 quote_literal_cstr(new_upper_text));
					ret = SPI_exec(buf.data, 0);
					pfree(buf.data);

					if (ret == SPI_OK_UTILITY)
						ereport(LOG,
								(errmsg("auto-partition: created %s.%s for [%s, %s) on %s.%s",
										nsname, child_name,
										new_lower_text, new_upper_text,
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
		}
	}

	/* Retention: detach + drop oldest beyond `retention`. */
	if (oldest_oids != NIL)
	{
		ListCell   *lc;

		foreach(lc, oldest_oids)
		{
			Oid			old_oid = lfirst_oid(lc);

			BeginInternalSubTransaction("autopart_detach_date");
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
							const char *partcol, const char *partinterval,
							int32 retention)
{
	int64		interval_size;
	int			ret;
	int			partition_count = 0;
	int64		max_upper;
	int64		head_lower = 0;
	char	   *head_relname = NULL;
	bool		have_max_upper = false;
	bool		create_next = true;
	List	   *oldest_oids = NIL;	/* oids to detach + drop, in order */
	StringInfoData buf;
	char	   *endptr;
	MemoryContext savecxt;

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
	 * detach/drop candidates.  Also save the head partition's name and
	 * lower bound so we can probe its fill level before deciding to
	 * create the next partition (the SPI tuptable is clobbered when we
	 * issue another SPI query).
	 */
	{
		bool		isnull;
		char	   *namestr;

		max_upper = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												SPI_tuptable->tupdesc,
												4, &isnull));
		if (!isnull)
			have_max_upper = true;

		head_lower = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc,
												 3, &isnull));
		if (isnull)
			have_max_upper = false;	/* malformed bound; bail on creation */

		namestr = SPI_getvalue(SPI_tuptable->vals[0],
							   SPI_tuptable->tupdesc, 2);
		if (namestr != NULL)
		{
			savecxt = MemoryContextSwitchTo(TopTransactionContext);
			head_relname = pstrdup(namestr);
			MemoryContextSwitchTo(savecxt);
		}
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
	 * Fill-check: probe the head partition for its largest partition-column
	 * value and decide whether the next partition is actually needed.
	 * Without this guard the launcher would create one partition every
	 * iteration whether or not the current head is anywhere close to full,
	 * burning through partitions on quiet tables and racing retention into
	 * dropping useful data.
	 *
	 * The probe is a max(<partcol>) which the planner satisfies via an
	 * index-only scan on the partition's PK (which we know exists because
	 * partition_column IS the PK from Phase 3a's prereq check).  Cost is
	 * effectively constant time.
	 *
	 * Threshold is hard-coded at 80%.  Move to a GUC if customers want
	 * to tune.
	 */
	if (have_max_upper && head_relname != NULL)
	{
		int64		head_max = head_lower;
		bool		head_empty = true;

		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "SELECT max(%s)::bigint FROM %s.%s",
						 quote_identifier(partcol),
						 quote_identifier(nsname),
						 quote_identifier(head_relname));
		ret = SPI_execute(buf.data, true, 0);
		pfree(buf.data);

		if (ret == SPI_OK_SELECT && SPI_processed > 0)
		{
			bool		isnull;
			int64		v;

			v = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc,
											1, &isnull));
			if (!isnull)
			{
				head_max = v;
				head_empty = false;
			}
		}

		if (head_empty)
		{
			ereport(DEBUG1,
					(errmsg("auto-partition: %s.%s: head partition %s is empty, skipping create",
							nsname, relname, head_relname)));
			create_next = false;
		}
		else
		{
			int64		used = head_max - head_lower;
			/*
			 * Compare 5 * used >= 4 * interval to avoid float arithmetic,
			 * i.e. the head is at least 80% full.
			 */
			if (used < 0 || (5 * used < 4 * interval_size))
			{
				ereport(DEBUG1,
						(errmsg("auto-partition: %s.%s: head %s at %lld/%lld (%.0f%%), waiting",
								nsname, relname, head_relname,
								(long long) used, (long long) interval_size,
								interval_size > 0 ? (100.0 * used / interval_size) : 0.0)));
				create_next = false;
			}
		}
	}

	/*
	 * Try to create the next partition.  Wrap in a subtransaction so a
	 * failure (typically: partition already exists, or DDL conflict)
	 * doesn't break this iteration of the scan loop.
	 */
	if (have_max_upper && create_next)
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

	/*
	 * The per-relation processors below run further SPI queries which
	 * clobber SPI_tuptable, so we can't keep referencing the outer scan's
	 * tuples across iterations.  Snapshot every row's values into local
	 * pstrdup'd memory in TopTransactionContext first, then dispatch.
	 */
	{
		typedef struct ScanRow
		{
			Oid			reloid;
			char	   *nsname;
			char	   *relname;
			char		relkind;
			char	   *strategy;
			char	   *partcol;
			char	   *partinterval;
			int32		retention;
		} ScanRow;

		ScanRow    *rows;
		int			nrows = (int) SPI_processed;
		MemoryContext oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		rows = palloc0(sizeof(ScanRow) * nrows);
		for (i = 0; i < (uint64) nrows; i++)
		{
			HeapTuple	row = SPI_tuptable->vals[i];
			TupleDesc	td = SPI_tuptable->tupdesc;
			bool		isnull;
			char	   *s;

			rows[i].reloid = DatumGetObjectId(SPI_getbinval(row, td, 1, &isnull));
			s = SPI_getvalue(row, td, 2); rows[i].nsname = s ? pstrdup(s) : NULL;
			s = SPI_getvalue(row, td, 3); rows[i].relname = s ? pstrdup(s) : NULL;
			rows[i].relkind = DatumGetChar(SPI_getbinval(row, td, 4, &isnull));
			s = SPI_getvalue(row, td, 5); rows[i].strategy = s ? pstrdup(s) : NULL;
			s = SPI_getvalue(row, td, 6); rows[i].partcol = s ? pstrdup(s) : NULL;
			s = SPI_getvalue(row, td, 7); rows[i].partinterval = s ? pstrdup(s) : NULL;
			(void) SPI_getbinval(row, td, 8, &isnull);
			if (!isnull)
				rows[i].retention = DatumGetInt32(SPI_getbinval(row, td, 8, &isnull));
		}
		MemoryContextSwitchTo(oldcxt);

		for (i = 0; i < (uint64) nrows; i++)
		{
			ScanRow    *r = &rows[i];

			ereport(DEBUG1,
					(errmsg("auto-partition: %s.%s (oid=%u, relkind=%c) "
							"strategy=%s column=%s interval=%s retention=%d",
							r->nsname ? r->nsname : "?",
							r->relname ? r->relname : "?",
							r->reloid, r->relkind,
							r->strategy ? r->strategy : "?",
							r->partcol ? r->partcol : "(none)",
							r->partinterval ? r->partinterval : "(none)",
							r->retention)));

			/*
			 * Dispatch by strategy + relkind.
			 *
			 * relkind='r' (regular heap) + range_int: try to convert the
			 *   heap to partitioned (Phase 3a — brief lock during swap).
			 *
			 * relkind='p' + range_int / range_date: rotation.
			 *
			 * list_int and heap-conversion-for-range_date are not yet
			 * implemented; logged at DEBUG.
			 */
			if (r->strategy && strcmp(r->strategy, "range_int") == 0)
			{
				if (r->relkind == 'r')
					(void) convert_heap_to_partitioned(r->reloid, r->nsname, r->relname,
													   r->partcol, r->partinterval,
													   r->retention);
				else if (r->relkind == 'p')
					process_range_int_partition(r->reloid, r->nsname, r->relname,
												r->partcol, r->partinterval, r->retention);
			}
			else if (r->strategy && strcmp(r->strategy, "range_date") == 0)
			{
				if (r->relkind == 'r')
					(void) convert_heap_to_partitioned_date(r->reloid, r->nsname, r->relname,
															r->partcol, r->partinterval,
															r->retention);
				else if (r->relkind == 'p')
					process_range_date_partition(r->reloid, r->nsname, r->relname,
												 r->partcol, r->partinterval, r->retention);
			}
			else if (r->strategy && strcmp(r->strategy, "list_int") == 0)
				ereport(DEBUG1,
						(errmsg("auto-partition: %s.%s: list_int not yet implemented",
								r->nsname, r->relname)));
		}
	}

done:
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_stat(false);
	pgstat_report_activity(STATE_IDLE, NULL);
}
