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
 * This closes that gap.  Every dbblue_audit_prune_naptime, whatever has
 * aged out is deleted, whether or not anything is writing.  The sweep
 * inside the write path is kept as well -- it costs nothing and keeps a
 * busy database tidy between wakeups.
 *
 * A background worker binds to one database for its lifetime, and each
 * database has its own dbblue.dbblue_audit_log, so covering several
 * databases means several connections.  Rather than one permanent worker
 * per database, this is a launcher plus short-lived workers, the same
 * split the dbblue BRIN feature uses and the one autovacuum uses:
 *
 *	- The launcher runs always and holds no database connection.  Each
 *	  cycle it lists the databases to sweep and starts one dynamic worker
 *	  in each, in turn, waiting for each to finish.
 *
 *	- Each worker connects, sweeps its database once, and exits.
 *
 * That costs one worker slot no matter how many databases are audited,
 * and holds no connection between cycles, so DROP DATABASE on an audited
 * database is not blocked.  It also means the set of databases is re-read
 * every cycle: adding one to dbblue_audit_database takes effect on the
 * next sweep, where one permanent worker per database could only be
 * registered at postmaster start and so needed a restart.
 *
 * Which databases are swept follows dbblue_audit_database, the same
 * setting that decides where the audit log records changes: the databases
 * it names, or -- when it is empty, which means audit everything -- every
 * database that can be connected to.  Whether a given database is
 * actually swept is then up to the worker, which sees that database's own
 * dbblue_audit_enabled and dbblue_audit_retention once connected.
 *
 * Copyright (c) 2026, dbblue / Cybrosys Technologies
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/dbblue_audit_pruner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
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
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "utils/wait_event.h"

/* How often the launcher starts a sweep of every audited database, in seconds. */
int			dbblue_audit_prune_naptime = 60;

/* How long to keep polling a worker the postmaster has not started. */
#define AUDIT_PRUNE_STARTUP_TIMEOUT		60000	/* 1 minute in ms */

/* Poll granularity while waiting for a sweep worker to finish. */
#define AUDIT_PRUNE_POLL_INTERVAL		1000	/* 1 second in ms */

/*
 * One database to sweep this cycle.  The name is carried alongside the OID
 * only for log messages; the worker is addressed by OID, which stays correct
 * even if the database is renamed mid-cycle -- unlike the position in
 * dbblue_audit_database, which moves whenever that setting is edited.
 */
typedef struct AuditPruneDatabase
{
	Oid			dboid;
	char	   *dbname;
} AuditPruneDatabase;

static MemoryContext pruner_cxt = NULL;

static bool audit_prune_wanted(const char *dbname);
static List *audit_prune_get_databases(void);
static void audit_prune_one_database(const AuditPruneDatabase *db);
static void audit_prune_wait_for_worker(BackgroundWorkerHandle *handle,
										const AuditPruneDatabase *db);
static bool audit_log_table_exists(void);
static int64 audit_prune_once(void);

/*
 * audit_prune_wanted
 *		Is this database named in dbblue_audit_database?
 *
 * An empty setting means the audit log records changes in every database, so
 * it must mean sweep every database too; reading it as "no databases" is what
 * used to leave the default configuration auditing everywhere while nothing
 * pruned on a timer.
 */
static bool
audit_prune_wanted(const char *dbname)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *lc;
	bool		found = false;

	if (dbblue_audit_database == NULL || dbblue_audit_database[0] == '\0')
		return true;

	rawstring = pstrdup(dbblue_audit_database);

	if (!SplitIdentifierString(rawstring, ',', &elemlist))
	{
		/*
		 * Malformed list: sweep nothing rather than everything.  The audit
		 * write path rejects the same value, so this only mirrors what is
		 * already being recorded.
		 */
		pfree(rawstring);
		list_free(elemlist);
		return false;
	}

	foreach(lc, elemlist)
	{
		if (strcmp((const char *) lfirst(lc), dbname) == 0)
		{
			found = true;
			break;
		}
	}

	pfree(rawstring);
	list_free(elemlist);

	return found;
}

/*
 * audit_prune_get_databases
 *		Build the list of databases to sweep this cycle.
 *
 * pg_database is one of the shared catalogs nailed into the relcache before a
 * database is selected, so the connectionless launcher can read it; this
 * follows get_database_list() in autovacuum.c.  The result is allocated in
 * the caller's context so it survives the commit.
 */
static List *
audit_prune_get_databases(void)
{
	List	   *result = NIL;
	Relation	dbrel;
	TableScanDesc scan;
	HeapTuple	tup;
	MemoryContext resultcxt = CurrentMemoryContext;

	StartTransactionCommand();

	dbrel = table_open(DatabaseRelationId, AccessShareLock);
	scan = table_beginscan_catalog(dbrel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);
		AuditPruneDatabase *db;
		MemoryContext oldcxt;

		/* A half-dropped database cannot be connected to at all. */
		if (database_is_invalid_form(pgdatabase))
			continue;

		/*
		 * Templates and databases marked as rejecting connections are
		 * skipped: connecting to a template blocks CREATE DATABASE from it,
		 * and datallowconn = false is an explicit instruction that background
		 * work has no business overriding.
		 */
		if (pgdatabase->datistemplate || !pgdatabase->datallowconn)
			continue;

		if (!audit_prune_wanted(NameStr(pgdatabase->datname)))
			continue;

		/*
		 * Allocate the result outside the transaction's context, inside the
		 * loop, so the leaky scan machinery is not run in a long-lived one.
		 */
		oldcxt = MemoryContextSwitchTo(resultcxt);
		db = palloc_object(AuditPruneDatabase);
		db->dboid = pgdatabase->oid;
		db->dbname = pstrdup(NameStr(pgdatabase->datname));
		result = lappend(result, db);
		MemoryContextSwitchTo(oldcxt);
	}

	table_endscan(scan);
	table_close(dbrel, AccessShareLock);

	CommitTransactionCommand();

	/* CommitTransactionCommand() leaves us in TopMemoryContext. */
	MemoryContextSwitchTo(resultcxt);

	return result;
}

/*
 * audit_prune_wait_for_worker
 *		Block until one sweep worker has finished.
 *
 * Sweeping one database at a time is what bounds the feature to a single
 * worker slot; starting one per audited database at once would spend a
 * connection slot each on a job that is nearly always idle, which is what the
 * old eight-database cap existed to limit.  Nothing needs capping now.
 *
 * WaitForBackgroundWorkerShutdown() is not used because it only returns once
 * the worker is gone, which on SIGTERM would hold shutdown for the rest of a
 * sweep; here a shutdown request terminates the worker instead.  The startup
 * timeout covers the case where the postmaster never starts the worker at
 * all, so the launcher cannot get stuck on one database forever.
 */
static void
audit_prune_wait_for_worker(BackgroundWorkerHandle *handle,
							const AuditPruneDatabase *db)
{
	TimestampTz startup_deadline;
	bool		started = false;
	bool		terminated = false;

	startup_deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
												   AUDIT_PRUNE_STARTUP_TIMEOUT);

	for (;;)
	{
		BgwHandleStatus status;
		pid_t		pid;

		CHECK_FOR_INTERRUPTS();

		status = GetBackgroundWorkerPid(handle, &pid);

		if (status == BGWH_STOPPED || status == BGWH_POSTMASTER_DIED)
			break;

		if (status == BGWH_STARTED)
			started = true;

		if (ShutdownRequestPending && !terminated)
		{
			TerminateBackgroundWorker(handle);
			terminated = true;
		}

		if (!started && !terminated &&
			GetCurrentTimestamp() >= startup_deadline)
		{
			ereport(WARNING,
					(errmsg("dbblue audit pruner: worker for database \"%s\" did not start",
							db->dbname),
					 errdetail("Giving up on it for this cycle."),
					 errhint("Check max_worker_processes.")));
			TerminateBackgroundWorker(handle);
			terminated = true;
		}

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 AUDIT_PRUNE_POLL_INTERVAL,
						 WAIT_EVENT_DBBLUE_AUDIT_PRUNE_LAUNCHER_MAIN);
		ResetLatch(MyLatch);
	}
}

/*
 * audit_prune_one_database
 *		Start a one-shot sweep worker in the given database and wait for it.
 *
 * Addressed by OID rather than by name, so a rename between building the list
 * and starting the worker cannot send it to the wrong database; if the
 * database was dropped meanwhile the worker fails to connect, which
 * BGW_NEVER_RESTART turns into a single logged failure rather than a restart
 * loop.
 */
static void
audit_prune_one_database(const AuditPruneDatabase *db)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;

	/* One sweep and done: a failed cycle is retried by the next one. */
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "AuditPrunerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN,
			 "dbblue audit pruner (%s)", db->dbname);
	snprintf(worker.bgw_type, BGW_MAXLEN, "dbblue audit pruner");
	worker.bgw_main_arg = ObjectIdGetDatum(db->dboid);

	/*
	 * The name comes along for log messages only.  The worker cannot look it
	 * up for itself after the sweep: resolving an OID to a name needs a
	 * syscache lookup, and by then the sweep has committed its last
	 * transaction and there is none open.
	 */
	strlcpy(worker.bgw_extra, db->dbname, BGW_EXTRALEN);

	/* So our latch is set when it starts and stops. */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		ereport(WARNING,
				(errmsg("dbblue audit pruner: no free background worker slot for database \"%s\"",
						db->dbname),
				 errdetail("The database will be retried on the next cycle."),
				 errhint("Consider raising max_worker_processes.")));
		return;
	}

	audit_prune_wait_for_worker(handle, db);

	pfree(handle);
}

/*
 * audit_log_table_exists
 *		Does this database have an audit log to sweep at all?
 *
 * dbblue.dbblue_audit_log is created lazily, by the first write to an audited
 * table, so a database that has never recorded anything does not have it.
 * Sweeping such a database would fail with "relation ... does not exist" once
 * per cycle, which matters now that an empty dbblue_audit_database means
 * every database rather than none: most of them will never have been audited.
 *
 * Uses the same to_regclass() test as the write path's fast path in
 * pg_audit.c, so the two agree on what "the log exists" means.
 */
static bool
audit_log_table_exists(void)
{
	bool		exists = false;
	int			ret;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();

	ret = SPI_execute("SELECT to_regclass('dbblue.dbblue_audit_log') IS NOT NULL",
					  true, 1);

	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool		isnull;
		Datum		datum;

		datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
							  1, &isnull);
		exists = !isnull && DatumGetBool(datum);
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	return exists;
}

/*
 * Delete one bounded batch of expired rows, repeating while a full batch
 * comes back so a large backlog is cleared over one wakeup rather than one
 * batch per cycle.  Returns the number deleted.
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
 *		Per-database sweep worker: connect to the database whose OID the
 *		launcher passed, sweep it once, exit.
 *
 * The launcher decides which databases are audited, but not whether this one
 * is switched on: dbblue_audit_enabled and dbblue_audit_retention are
 * per-database settings, and only a process connected to the database can
 * resolve them.  InitPostgres has applied them by the time this runs, so the
 * checks below are simply reading this database's effective values.
 */
void
AuditPrunerMain(Datum main_arg)
{
	Oid			dboid = DatumGetObjectId(main_arg);
	const char *dbname = MyBgworkerEntry->bgw_extra;
	sigjmp_buf	local_sigjmp_buf;
	int64		removed;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);

	pruner_cxt = AllocSetContextCreate(TopMemoryContext,
									   "dbblue audit pruner",
									   ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(pruner_cxt);

	/*
	 * One shot, so an unexpected error ends this worker rather than being
	 * recovered from: the launcher starts a fresh one next cycle.  Report it
	 * and unwind the transaction and SPI state first, so nothing is left
	 * half-open at exit.
	 */
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

		pgstat_report_activity(STATE_IDLE, NULL);

		RESUME_INTERRUPTS();

		proc_exit(1);
	}
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * A standby has a read-only audit log; the primary's pruner is what
	 * removes rows, and they arrive here through replay.
	 */
	if (!dbblue_audit_enabled || RecoveryInProgress())
		proc_exit(0);

	if (!dbblue_audit_retention_is_active())
		proc_exit(0);

	if (ShutdownRequestPending)
		proc_exit(0);

	if (!audit_log_table_exists())
	{
		ereport(DEBUG1,
				(errmsg("dbblue audit pruner: database \"%s\" has no audit log, nothing to sweep",
						dbname)));
		proc_exit(0);
	}

	/*
	 * Never let the sweep sit behind someone else's lock; on timeout the
	 * attempt errors, is reported, and is simply retried on the next cycle.
	 */
	SetConfigOption("lock_timeout", "5s", PGC_SUSET, PGC_S_SESSION);

	pgstat_report_activity(STATE_RUNNING, "dbblue audit pruner: sweeping");
	removed = audit_prune_once();
	pgstat_report_activity(STATE_IDLE, NULL);

	if (removed > 0)
		ereport(LOG,
				(errmsg("dbblue audit pruner: removed %lld expired audit row(s) from database \"%s\"",
						(long long) removed, dbname)));

	proc_exit(0);
}

/*
 * DbblueAuditPruneLauncherMain
 *		Main entry point for the launcher process.
 */
void
DbblueAuditPruneLauncherMain(Datum main_arg)
{
	sigjmp_buf	local_sigjmp_buf;

	/* volatile: assigned in the error handler below and read after it. */
	volatile TimestampTz next_run = 0;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	/*
	 * No database.  pg_database is a shared catalog, so this is enough to
	 * list the cluster's databases, and it means the launcher never counts as
	 * a connection to any of them -- DROP DATABASE on an audited database is
	 * not blocked by the pruner being active.
	 */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	ereport(LOG, (errmsg("dbblue audit prune launcher started")));

	pruner_cxt = AllocSetContextCreate(TopMemoryContext,
									   "dbblue audit prune launcher",
									   ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(pruner_cxt);

	/*
	 * Recover here after any unexpected error: report it, unwind whatever
	 * transaction state was left behind, and go back to the main loop.
	 */
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

		/*
		 * Defer the next cycle.  The error aborted this one part-way through,
		 * so next_run still holds whatever it did before -- typically 0, on
		 * the very first cycle -- and jumping straight back into the loop
		 * would retry immediately and spin on a persistent failure.
		 */
		next_run = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
											   (int64) dbblue_audit_prune_naptime * 1000);

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

			/*
			 * dbblue_audit_database and dbblue_audit_prune_naptime are both
			 * read below, so re-evaluate at once rather than at the end of
			 * the current interval.  This is also what lets an operator add a
			 * database and have it swept immediately.
			 */
			next_run = 0;
		}

		/*
		 * Nothing here can write on a standby, and each worker checks
		 * RecoveryInProgress() for itself; not starting them at all keeps a
		 * standby from spawning a worker per database for no reason.
		 */
		if (RecoveryInProgress())
		{
			next_run = 0;
			sleep_ms = (long) dbblue_audit_prune_naptime * 1000L;
		}
		else
		{
			/*
			 * Paced by next_run rather than by having woken up: the sweep
			 * workers set our latch as they start and stop, and without this
			 * the last one of a cycle would trigger an immediate extra pass.
			 */
			if (next_run == 0 || GetCurrentTimestamp() >= next_run)
			{
				List	   *databases;
				ListCell   *lc;

				/*
				 * Re-read every cycle, so a database added to
				 * dbblue_audit_database, or created since the last cycle, is
				 * swept without a restart, and one dropped since simply
				 * drops out of the list.
				 */
				databases = audit_prune_get_databases();

				foreach(lc, databases)
				{
					if (ShutdownRequestPending)
						break;

					audit_prune_one_database((AuditPruneDatabase *) lfirst(lc));
				}

				/* Discard this cycle's database list. */
				MemoryContextSwitchTo(TopMemoryContext);
				MemoryContextReset(pruner_cxt);
				MemoryContextSwitchTo(pruner_cxt);

				next_run = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
													   (int64) dbblue_audit_prune_naptime * 1000);
			}

			sleep_ms = (long) ((next_run - GetCurrentTimestamp()) / 1000);
			sleep_ms = Max(sleep_ms, 1000);
		}

		if (ShutdownRequestPending)
			break;

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 sleep_ms,
						 WAIT_EVENT_DBBLUE_AUDIT_PRUNE_LAUNCHER_MAIN);
		ResetLatch(MyLatch);
	}

	ereport(LOG, (errmsg("dbblue audit prune launcher shutting down")));

	/*
	 * Exit non-zero on SIGTERM, matching the other dbblue launchers: exiting
	 * 0 is treated by the postmaster as "terminate and forget", which would
	 * keep a pg_terminate_backend() of the launcher from coming back until
	 * the next server restart.
	 */
	proc_exit(1);
}

/*
 * DbblueAuditPrunerRegister
 *		Register the prune launcher at postmaster startup.
 *
 * Always registered, and just the one worker: which databases are swept is
 * decided per cycle by the launcher, so nothing here depends on a setting
 * read at startup.  This is what removes the restart that registering one
 * worker per named database used to require.
 */
void
DbblueAuditPrunerRegister(void)
{
	BackgroundWorker worker;

	/*
	 * pg_upgrade runs the server in a restricted mode to move schema
	 * objects around; a worker deleting rows underneath that has no
	 * business running.
	 */
	if (IsBinaryUpgrade)
		return;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;

	/*
	 * ConsistentState rather than RecoveryFinished so this also starts on
	 * hot standbys (which never reach RecoveryFinished's PM_RUN state); the
	 * RecoveryInProgress() checks are what keep a standby from writing.
	 */
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 5;
	snprintf(worker.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "DbblueAuditPruneLauncherMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "dbblue audit prune launcher");
	snprintf(worker.bgw_type, BGW_MAXLEN, "dbblue audit prune launcher");
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&worker);
}
