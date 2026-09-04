/*-------------------------------------------------------------------------
 *
 * dbblue_audit_pruner.c
 *	  dbblue audit log retention worker.
 *
 * dbblue_audit_retention states how long the dedicated audit log keeps
 * history.  Enforcing that from inside the audit write path alone is not
 * enough: that code only runs when somebody modifies an audited table, so
 * a database that goes quiet keeps expired rows indefinitely while the
 * setting claims they are gone.  A retention policy has to be a property
 * of time, not of traffic.
 *
 * This worker closes that gap.  It wakes on dbblue_audit_prune_naptime,
 * deletes whatever has aged out, and sleeps again, whether or not anything
 * is writing.  The sweep inside the write path is kept as well -- it costs
 * nothing and keeps a busy database tidy between wakeups.
 *
 * One worker is registered per database named in dbblue_audit_database,
 * because a background worker binds to a single database for its lifetime
 * and each database has its own dbblue.dbblue_audit_log.  With that
 * setting empty there is nothing to bind to, so no worker starts and
 * pruning falls back to the write path; naming the databases is what
 * turns time-based retention on.  Changing the list requires a restart,
 * the same as dbblue_repack_database.
 *
 * Copyright (c) 2026, dbblue / Cybrosys Technologies
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/dbblue_audit_pruner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/dbblue_audit_pruner.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pg_audit.h"
#include "utils/snapmgr.h"
#include "utils/varlena.h"
#include "utils/wait_event.h"

/* How often each worker wakes to sweep, in seconds. */
int			dbblue_audit_prune_naptime = 60;

/*
 * Registering more than this many workers would spend a connection slot per
 * database for a job that is nearly always idle.  Databases past the cap
 * still prune from the write path.
 */
#define DBBLUE_AUDIT_MAX_PRUNE_WORKERS 8

static MemoryContext pruner_cxt = NULL;
static bool pruner_connected = false;

/*
 * Return the nth database named in dbblue_audit_database, palloc'd, or NULL
 * when there is no such entry.
 */
static char *
audit_prune_database(int n)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *lc;
	int			i = 0;
	char	   *result = NULL;

	if (dbblue_audit_database == NULL || dbblue_audit_database[0] == '\0')
		return NULL;

	rawstring = pstrdup(dbblue_audit_database);
	if (!SplitIdentifierString(rawstring, ',', &elemlist))
	{
		pfree(rawstring);
		list_free(elemlist);
		return NULL;
	}

	foreach(lc, elemlist)
	{
		if (i++ == n)
		{
			result = pstrdup((const char *) lfirst(lc));
			break;
		}
	}

	pfree(rawstring);
	list_free(elemlist);
	return result;
}

/*
 * DbblueAuditPrunerRegister
 *		Register one retention worker per database in
 *		dbblue_audit_database.  Called from PostmasterMain(), the same way
 *		the other dbblue workers are.
 */
void
DbblueAuditPrunerRegister(void)
{
	int			n;

	/*
	 * pg_upgrade runs the server in a restricted mode to move schema
	 * objects around; a worker deleting rows underneath that has no
	 * business running.
	 */
	if (IsBinaryUpgrade)
		return;

	for (n = 0; n < DBBLUE_AUDIT_MAX_PRUNE_WORKERS; n++)
	{
		BackgroundWorker worker;
		char	   *dbname = audit_prune_database(n);

		if (dbname == NULL)
			break;

		memset(&worker, 0, sizeof(worker));
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_ConsistentState;
		worker.bgw_restart_time = 5;
		snprintf(worker.bgw_library_name, MAXPGPATH, "postgres");
		snprintf(worker.bgw_function_name, BGW_MAXLEN, "AuditPrunerMain");
		snprintf(worker.bgw_name, BGW_MAXLEN,
				 "dbblue audit pruner (%s)", dbname);
		snprintf(worker.bgw_type, BGW_MAXLEN, "dbblue audit pruner");
		worker.bgw_notify_pid = 0;
		worker.bgw_main_arg = Int32GetDatum(n);

		RegisterBackgroundWorker(&worker);
		pfree(dbname);
	}
}

/*
 * Delete one bounded batch of expired rows, repeating while a full batch
 * comes back so a large backlog is cleared over one wakeup rather than one
 * batch per minute.  Returns the number deleted.
 */
static int64
audit_prune_once(void)
{
	int64		total = 0;

	for (;;)
	{
		int64		removed;

		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		SPI_connect();

		removed = dbblue_audit_prune_batch();

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();

		total += removed;

		if (removed < DBBLUE_AUDIT_PRUNE_BATCH)
			break;
		if (ShutdownRequestPending)
			break;
		CHECK_FOR_INTERRUPTS();
	}

	return total;
}

/*
 * AuditPrunerMain
 *		Background worker entry point.
 */
void
AuditPrunerMain(Datum main_arg)
{
	sigjmp_buf	local_sigjmp_buf;
	int			slot = DatumGetInt32(main_arg);
	char	   *dbname;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	pruner_cxt = AllocSetContextCreate(TopMemoryContext,
									   "dbblue audit pruner",
									   ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(pruner_cxt);

	dbname = audit_prune_database(slot);
	if (dbname == NULL)
	{
		ereport(LOG,
				(errmsg("dbblue audit pruner: no database in slot %d, exiting",
						slot)));
		return;
	}

	ereport(LOG,
			(errmsg("dbblue audit pruner started (database \"%s\")", dbname)));

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand. */
		error_context_stack = NULL;

		HOLD_INTERRUPTS();

		EmitErrorReport();
		FlushErrorState();

		AbortOutOfAnyTransaction();
		MemoryContextSwitchTo(TopMemoryContext);
		MemoryContextReset(pruner_cxt);
		MemoryContextSwitchTo(pruner_cxt);

		pgstat_report_activity(STATE_IDLE, NULL);

		RESUME_INTERRUPTS();
	}
	PG_exception_stack = &local_sigjmp_buf;

	for (;;)
	{
		long		sleep_ms;

		CHECK_FOR_INTERRUPTS();

		if (ShutdownRequestPending)
			break;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * A standby has a read-only audit log; the primary's pruner is what
		 * removes rows, and they arrive here through replay.
		 */
		if (dbblue_audit_enabled && !RecoveryInProgress())
		{
			if (!pruner_connected)
			{
				BackgroundWorkerInitializeConnection(dbname, NULL, 0);

				/*
				 * Never let the sweep sit behind someone else's lock; on
				 * timeout the attempt errors, is reported, and is simply
				 * retried at the next wakeup.
				 */
				SetConfigOption("lock_timeout", "5s", PGC_SUSET,
								PGC_S_SESSION);
				pruner_connected = true;
			}

			if (dbblue_audit_retention_is_active())
			{
				int64		removed;

				pgstat_report_activity(STATE_RUNNING,
									   "dbblue audit pruner: sweeping");
				removed = audit_prune_once();
				if (removed > 0)
					ereport(LOG,
							(errmsg("dbblue audit pruner: removed %lld expired audit row(s)",
									(long long) removed)));
				pgstat_report_activity(STATE_IDLE, NULL);
			}
		}

		sleep_ms = (long) dbblue_audit_prune_naptime * 1000L;
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 sleep_ms,
						 WAIT_EVENT_PG_SLEEP);
		ResetLatch(MyLatch);
	}

	ereport(LOG, (errmsg("dbblue audit pruner shutting down")));
	proc_exit(0);
}
