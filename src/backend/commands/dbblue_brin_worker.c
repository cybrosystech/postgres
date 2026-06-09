/*-------------------------------------------------------------------------
 *
 * dbblue_brin_worker.c
 *    Background worker: auto-create BRIN indexes on create_date columns.
 *
 *    Every BRIN_WORKER_INTERVAL ms, connects to dbblue_brin_database,
 *    queries pg_stats for create_date columns with correlation > 0.9,
 *    and creates a BRIN index on each table that does not already have one.
 *
 * Copyright (c) 2026, Cybrosys Technologies
 *
 * IDENTIFICATION
 *    src/backend/commands/dbblue_brin_worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "executor/spi.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

/* GUCs */
extern bool		dbblue_create_brin;
extern char	   *dbblue_brin_database;

#define BRIN_WORKER_INTERVAL	300000	/* 5 minutes in ms */

void		DBBlueBrinWorkerMain(Datum main_arg);
void		DBBlueBrinWorkerRegister(void);

static volatile sig_atomic_t got_sigterm = false;

static void
brin_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

/*
 * Run one scan cycle: find eligible create_date columns and create BRIN indexes.
 * Each index creation runs in its own transaction to avoid blocking.
 */
static void
run_brin_scan(void)
{
	int			ret;
	uint64		i;
	/* Store results before committing the read transaction */
	char	  **schemas;
	char	  **tables;
	uint64		nrows;
	MemoryContext oldctx;

	/* ---- Phase 1: read pg_stats ---- */
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		ereport(WARNING, (errmsg("dbblue BRIN: SPI_connect failed")));
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}

	ret = SPI_execute(
		"SELECT DISTINCT schemaname, tablename "
		"FROM pg_stats "
		"WHERE attname = 'create_date' "
		"  AND correlation > 0.9 "
		"  AND schemaname NOT IN ('pg_catalog', 'information_schema')",
		true, 0);

	if (ret != SPI_OK_SELECT)
	{
		ereport(WARNING, (errmsg("dbblue BRIN: pg_stats query failed (ret=%d)", ret)));
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}

	nrows = SPI_processed;

	/* Copy results into TopMemoryContext so they survive SPI_finish */
	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	schemas = palloc(nrows * sizeof(char *));
	tables = palloc(nrows * sizeof(char *));
	for (i = 0; i < nrows; i++)
	{
		schemas[i] = pstrdup(SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1));
		tables[i] = pstrdup(SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2));
	}
	MemoryContextSwitchTo(oldctx);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	/* ---- Phase 2: create indexes, one transaction per table ---- */
	for (i = 0; i < nrows && !got_sigterm; i++)
	{
		char	   *schemaname = schemas[i];
		char	   *tablename = tables[i];
		char	   *indexname = psprintf("%s_create_date_brin_idx", tablename);
		char	   *check_sql;
		char	   *create_sql;
		int		    check_ret;

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		if (SPI_connect() != SPI_OK_CONNECT)
		{
			PopActiveSnapshot();
			CommitTransactionCommand();
			continue;
		}

		/* Check if a BRIN index on create_date already exists for this table */
		check_sql = psprintf(
			"SELECT 1 FROM pg_indexes "
			"WHERE schemaname = %s AND tablename = %s "
			"  AND indexdef ILIKE '%%brin%%create_date%%' "
			"LIMIT 1",
			quote_literal_cstr(schemaname),
			quote_literal_cstr(tablename));

		check_ret = SPI_execute(check_sql, true, 1);

		if (check_ret == SPI_OK_SELECT && SPI_processed == 0)
		{
			/* No BRIN index yet — create one */
			create_sql = psprintf(
				"CREATE INDEX IF NOT EXISTS %s ON %s.%s USING brin (create_date)",
				quote_identifier(indexname),
				quote_identifier(schemaname),
				quote_identifier(tablename));

			ereport(LOG,
					(errmsg("dbblue BRIN: creating index on %s.%s", schemaname, tablename)));

			ret = SPI_execute(create_sql, false, 0);

			if (ret == SPI_OK_UTILITY)
				ereport(LOG,
						(errmsg("dbblue BRIN: created index %s on %s.%s",
								indexname, schemaname, tablename)));
			else
				ereport(WARNING,
						(errmsg("dbblue BRIN: failed to create index on %s.%s (ret=%d)",
								schemaname, tablename, ret)));

			pfree(create_sql);
		}
		else
		{
			ereport(DEBUG1,
					(errmsg("dbblue BRIN: index already exists for %s.%s, skipping",
							schemaname, tablename)));
		}

		pfree(check_sql);
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();

		pfree(indexname);
		pfree(schemaname);
		pfree(tablename);
	}

	pfree(schemas);
	pfree(tables);
}

/*
 * Main entry point for the background worker.
 */
void
DBBlueBrinWorkerMain(Datum main_arg)
{
	pqsignal(SIGTERM, brin_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Connect to the configured target database */
	BackgroundWorkerInitializeConnection(dbblue_brin_database, NULL, 0);

	ereport(LOG,
			(errmsg("dbblue BRIN worker started, monitoring database \"%s\"",
					dbblue_brin_database)));

	while (!got_sigterm)
	{
		int			rc;

		PG_TRY();
		{
			run_brin_scan();
		}
		PG_CATCH();
		{
			EmitErrorReport();
			FlushErrorState();
		}
		PG_END_TRY();

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   BRIN_WORKER_INTERVAL,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (rc & WL_EXIT_ON_PM_DEATH)
			break;

		CHECK_FOR_INTERRUPTS();
	}

	ereport(LOG, (errmsg("dbblue BRIN worker shutting down")));
	proc_exit(0);
}

/*
 * Register the background worker at postmaster startup.
 */
void
DBBlueBrinWorkerRegister(void)
{
	BackgroundWorker worker;

	if (!dbblue_create_brin)
		return;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "DBBlueBrinWorkerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "dbblue BRIN worker");
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}
