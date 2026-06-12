/*-------------------------------------------------------------------------
 *
 * dbblue_create_standby.c
 *	  dbblue Create Standby background worker.
 *
 * When dbblue_create_standby is on, this worker provisions and starts a
 * streaming-replication standby of the running cluster on the same host:
 *
 *   1. If the standby data directory (dbblue_standby_directory, default
 *      "<DataDir>_standby") does not exist yet, it is created with
 *      pg_basebackup -R against the local primary.
 *   2. A free TCP port is picked by probing PostPortNumber+1, +2, ...
 *      and appended to the standby's postgresql.auto.conf.
 *   3. The standby is started with pg_ctl, and the chosen port is
 *      reported in the server log and written to
 *      <DataDir>/dbblue_standby.port so tooling (e.g. an odoo.conf
 *      generator setting db_replica_port) can pick it up.
 *
 * The worker then babysits the standby: every tick it checks the standby
 * postmaster is still alive and restarts it if not.  The standby is left
 * running when the primary shuts down — it remains usable for read-only
 * traffic and simply resumes streaming when the primary returns.
 *
 * The worker never runs on a cluster that is itself in recovery, which
 * stops the freshly created standby (whose copied configuration also has
 * dbblue_create_standby=on) from recursively spawning standbys of its own.
 *
 * Portions Copyright (c) 2026, dbblue / Cybrosys Technologies.
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/dbblue_create_standby.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/dbblue_create_standby.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/guc.h"
#include "utils/wait_event.h"

/* GUC-backed variables (real values come from guc_parameters.dat). */
bool		dbblue_create_standby = false;
char	   *dbblue_standby_directory = NULL;
char	   *dbblue_standby_user = NULL;

/* How often the worker wakes up to check on the standby. */
#define DBBLUE_STANDBY_NAPTIME_MS	10000L

/* How many ports above PostPortNumber to probe before giving up. */
#define DBBLUE_STANDBY_PORT_ATTEMPTS	10

/* Stop retrying after this many consecutive failures (reset on SIGHUP). */
#define DBBLUE_STANDBY_MAX_FAILURES		5

static int	consecutive_failures = 0;

/*
 * resolve_standby_directory
 *		Return a palloc'd absolute path for the standby data directory.
 *
 * An empty dbblue_standby_directory means "<DataDir>_standby".  Relative
 * paths are interpreted against the parent directory of DataDir, so the
 * standby never ends up inside the primary's data directory (which would
 * bloat every future base backup).
 */
static char *
resolve_standby_directory(void)
{
	const char *dir = dbblue_standby_directory;
	char		parent[MAXPGPATH];

	if (dir == NULL || dir[0] == '\0')
		return psprintf("%s_standby", DataDir);

	if (is_absolute_path(dir))
		return pstrdup(dir);

	strlcpy(parent, DataDir, sizeof(parent));
	get_parent_directory(parent);
	return psprintf("%s/%s", parent, dir);
}

/*
 * resolve_bin_path
 *		Compose an absolute path to a binary that ships in the same bindir
 *		as the running postgres binary.
 */
static void
resolve_bin_path(const char *prog, char *out, size_t outlen)
{
	char		bindir[MAXPGPATH];

	strlcpy(bindir, my_exec_path, sizeof(bindir));
	get_parent_directory(bindir);
	snprintf(out, outlen, "%s/%s", bindir, prog);
}

/*
 * shell_quote
 *		Wrap s in single quotes, escaping any embedded single quotes, so the
 *		result is safe to splice into a /bin/sh command line.  Result is
 *		palloc'd.
 */
static char *
shell_quote(const char *s)
{
	StringInfoData buf;
	const char *p;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '\'');
	for (p = s; *p; p++)
	{
		if (*p == '\'')
			appendStringInfoString(&buf, "'\\''");
		else
			appendStringInfoChar(&buf, *p);
	}
	appendStringInfoChar(&buf, '\'');
	return buf.data;
}

/*
 * standby_has_data
 *		True if dir looks like an initialized cluster (has PG_VERSION).
 */
static bool
standby_has_data(const char *dir)
{
	char		path[MAXPGPATH];
	struct stat st;

	snprintf(path, sizeof(path), "%s/PG_VERSION", dir);
	return (stat(path, &st) == 0 && S_ISREG(st.st_mode));
}

/*
 * standby_is_running
 *		True if dir/postmaster.pid names a live process.  Same heuristic
 *		pg_ctl status uses; a stale pidfile whose PID was recycled can give
 *		a false positive, which only delays a restart by one tick.
 */
static bool
standby_is_running(const char *dir)
{
	char		path[MAXPGPATH];
	FILE	   *fp;
	long		pid = 0;

	snprintf(path, sizeof(path), "%s/postmaster.pid", dir);
	fp = fopen(path, "r");
	if (fp == NULL)
		return false;
	if (fscanf(fp, "%ld", &pid) != 1)
		pid = 0;
	fclose(fp);

	return (pid > 1 && kill((pid_t) pid, 0) == 0);
}

/*
 * find_free_port
 *		Probe TCP ports on the loopback interface starting just above the
 *		primary's own port and return the first one we can bind, or -1.
 */
static int
find_free_port(void)
{
	int			port;

	for (port = PostPortNumber + 1;
		 port <= PostPortNumber + DBBLUE_STANDBY_PORT_ATTEMPTS;
		 port++)
	{
		int			sock;
		struct sockaddr_in addr;
		bool		free_port;

		sock = socket(AF_INET, SOCK_STREAM, 0);
		// here we create a socket in kernal level to check that the postgres port is available with this socket number
		if (sock < 0)
			return -1;

		memset(&addr, 0, sizeof(addr));
		addr.sin_family = AF_INET;
		addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		addr.sin_port = htons((uint16) port);

		free_port = (bind(sock, (struct sockaddr *) &addr, sizeof(addr)) == 0);
		close(sock);

		if (free_port)
			return port;
	}

	return -1;
}

/*
 * append_standby_port
 *		Append a port setting to the standby's postgresql.auto.conf, the
 *		same file pg_basebackup -R appends primary_conninfo to.  Duplicate
 *		entries are harmless: the last one in the file wins.
 */
static bool
append_standby_port(const char *dir, int port)
{
	char		path[MAXPGPATH];
	FILE	   *fp;

	snprintf(path, sizeof(path), "%s/postgresql.auto.conf", dir);
	fp = fopen(path, "a");
	if (fp == NULL)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not open \"%s\": %m",
						path)));
		return false;
	}
	fprintf(fp, "port = %d\n", port);
	fclose(fp);
	return true;
}

/*
 * report_standby_port
 *		Announce the standby's port in the log and persist it to
 *		<DataDir>/dbblue_standby.port for external tooling.
 */
static void
report_standby_port(const char *dir, int port)
{
	char		path[MAXPGPATH];
	FILE	   *fp;

	ereport(LOG,
			(errmsg("dbblue create standby: standby is running on port %d (data directory \"%s\")",
					port, dir),
			 errhint("For Odoo, set db_replica_host = localhost and db_replica_port = %d.",
					 port)));

	snprintf(path, sizeof(path), "%s/dbblue_standby.port", DataDir);
	fp = fopen(path, "w");
	if (fp == NULL)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not write \"%s\": %m",
						path)));
		return;
	}
	fprintf(fp, "%d\n", port);
	fclose(fp);
}

/*
 * run_shell
 *		Run a command via system(), appending its output to logfile.
 *		Returns true on exit status 0.
 */
static bool
run_shell(const char *cmd, const char *logfile, const char *what)
{
	char	   *full;
	char	   *qlog;
	int			rc;

	qlog = shell_quote(logfile);
	full = psprintf("%s >> %s 2>&1", cmd, qlog);

	fflush(NULL);
	pgstat_report_wait_start(WAIT_EVENT_DBBLUE_CREATE_STANDBY_MAIN);
	rc = system(full);
	pgstat_report_wait_end();

	if (rc != 0)
	{
		ereport(WARNING,
				(errmsg("dbblue create standby: %s failed with status %d",
						what, rc),
				 errdetail("Command: %s", full),
				 errhint("See \"%s\" for details.", logfile)));
		pfree(qlog);
		pfree(full);
		return false;
	}

	pfree(qlog);
	pfree(full);
	return true;
}

/*
 * run_standby_tick
 *		Create the standby if needed and make sure it is running.
 *		Returns true if the standby is (now) up, false on failure.
 */
static bool
run_standby_tick(void)
{
	char	   *dir;
	char	   *logfile;
	char		binpath[MAXPGPATH];
	char	   *qbin;
	char	   *qdir;
	char	   *cmd;
	int			port;
	bool		ok;

	dir = resolve_standby_directory();

	/* Nothing to do while the standby postmaster is alive. */
	if (standby_is_running(dir))
	{
		pfree(dir);
		return true;
	}

	logfile = psprintf("%s.log", dir);

	if (!standby_has_data(dir))
	{
		struct stat st;

		/*
		 * Refuse to touch an existing directory that is not a cluster:
		 * pg_basebackup would fail on it anyway if non-empty, and we must
		 * never guess about deleting an operator's data.
		 */
		if (stat(dir, &st) == 0 && !standby_has_data(dir))
		{
			/* an empty directory is fine, pg_basebackup accepts it */
			DIR		   *d = opendir(dir);
			bool		empty = true;

			if (d != NULL)
			{
				struct dirent *de;

				while ((de = readdir(d)) != NULL)
				{
					if (strcmp(de->d_name, ".") != 0 &&
						strcmp(de->d_name, "..") != 0)
					{
						empty = false;
						break;
					}
				}
				closedir(d);
			}

			if (!empty)
			{
				ereport(WARNING,
						(errmsg("dbblue create standby: directory \"%s\" exists but is not a PostgreSQL cluster; remove it or point dbblue_standby_directory elsewhere",
								dir)));
				pfree(logfile);
				pfree(dir);
				return false;
			}
		}

		resolve_bin_path("pg_basebackup", binpath, sizeof(binpath));
		qbin = shell_quote(binpath);
		qdir = shell_quote(dir);

		/*
		 * -R writes standby.signal and primary_conninfo; -X stream keeps up
		 * with WAL during the copy; -c fast avoids waiting for a checkpoint;
		 * -w forbids password prompts (there is no terminal to prompt on).
		 * An empty dbblue_standby_user means "connect as the OS user", which
		 * works out of the box with initdb's default replication pg_hba
		 * entries.
		 */
		if (dbblue_standby_user != NULL && dbblue_standby_user[0] != '\0')
		{
			char	   *quser = shell_quote(dbblue_standby_user);

			cmd = psprintf("%s -h localhost -p %d -U %s -D %s -X stream -c fast -R -w",
						   qbin, PostPortNumber, quser, qdir);
			pfree(quser);
		}
		else
			cmd = psprintf("%s -h localhost -p %d -D %s -X stream -c fast -R -w",
						   qbin, PostPortNumber, qdir);

		ereport(LOG,
				(errmsg("dbblue create standby: creating standby in \"%s\" from primary on port %d",
						dir, PostPortNumber)));

		ok = run_shell(cmd, logfile, "pg_basebackup");
		pfree(cmd);
		pfree(qbin);
		pfree(qdir);

		if (!ok)
		{
			pfree(logfile);
			pfree(dir);
			return false;
		}
	}

	/*
	 * Pick a free port now, just before starting: 5433 may have been free
	 * when the standby was first created but taken since.
	 */
	port = find_free_port();
	if (port < 0)
	{
		ereport(WARNING,
				(errmsg("dbblue create standby: no free port found in range %d..%d",
						PostPortNumber + 1,
						PostPortNumber + DBBLUE_STANDBY_PORT_ATTEMPTS)));
		pfree(logfile);
		pfree(dir);
		return false;
	}

	if (!append_standby_port(dir, port))
	{
		pfree(logfile);
		pfree(dir);
		return false;
	}

	resolve_bin_path("pg_ctl", binpath, sizeof(binpath));
	qbin = shell_quote(binpath);
	qdir = shell_quote(dir);
	{
		char	   *qlog = shell_quote(logfile);

		cmd = psprintf("%s -D %s -l %s -w -t 60 start", qbin, qdir, qlog);
		pfree(qlog);
	}

	ok = run_shell(cmd, logfile, "pg_ctl start");
	pfree(cmd);
	pfree(qbin);
	pfree(qdir);

	if (ok)
		report_standby_port(dir, port);

	pfree(logfile);
	pfree(dir);
	return ok;
}

/*
 * DbblueCreateStandbyMain
 *		Entry point for the dbblue create standby background worker.
 */
void
DbblueCreateStandbyMain(Datum main_arg)
{
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	ereport(LOG,
			(errmsg("dbblue create standby worker started (dbblue_create_standby=%s)",
					dbblue_create_standby ? "on" : "off")));

	while (!ShutdownRequestPending)
	{
		int			rc;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			/* give a hand-fixed setup a fresh set of attempts */
			consecutive_failures = 0;
		}

		/*
		 * Never act while in recovery: a standby must not spawn standbys of
		 * its own just because it inherited dbblue_create_standby=on from
		 * the primary's configuration.
		 */
		if (dbblue_create_standby &&
			!RecoveryInProgress() &&
			consecutive_failures < DBBLUE_STANDBY_MAX_FAILURES)
		{
			if (run_standby_tick())
				consecutive_failures = 0;
			else
			{
				consecutive_failures++;
				if (consecutive_failures == DBBLUE_STANDBY_MAX_FAILURES)
					ereport(WARNING,
							(errmsg("dbblue create standby: giving up after %d consecutive failures",
									consecutive_failures),
							 errhint("Fix the underlying problem, then reload the configuration (SELECT pg_reload_conf()) to retry.")));
			}
		}

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   DBBLUE_STANDBY_NAPTIME_MS,
					   WAIT_EVENT_DBBLUE_CREATE_STANDBY_MAIN);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	ereport(LOG, (errmsg("dbblue create standby worker shutting down")));
	proc_exit(0);
}

/*
 * DbblueCreateStandbyRegister
 *		Register the dbblue create standby worker as a static background
 *		worker.  Called from PostmasterMain.
 *
 * The worker does not request a database connection slot: it only shells
 * out to pg_basebackup and pg_ctl, which are their own libpq clients, so
 * it stays available even when max_connections is exhausted.
 */
void
DbblueCreateStandbyRegister(void)
{
	BackgroundWorker bgw;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_restart_time = 60;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "DbblueCreateStandbyMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "dbblue create standby");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "dbblue create standby");
	bgw.bgw_main_arg = (Datum) 0;
	bgw.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&bgw);
}
