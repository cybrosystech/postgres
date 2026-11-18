/*-------------------------------------------------------------------------
 *
 * dbblue_backup_launcher.c
 *	  dbblue backup launcher background worker.
 *
 * A "cron job" style scheduler that keeps a rotating pg_dump (custom
 * format) backup of each database named in dbblue_backup_databases,
 * re-backing up a database once dbblue_backup_interval has passed since
 * the start of its last successful backup, on a wake-up cycle of
 * dbblue_backup_naptime.
 * After each successful backup, older successful backups of the same
 * database beyond dbblue_backup_retention_count are pruned (both the dump
 * file and its history row).
 *
 * A per-database logical dump has no server-side equivalent to call
 * instead of pg_dump: PostgreSQL deliberately keeps the catalog walk,
 * dependency ordering, and DDL reconstruction that a dump requires in the
 * pg_dump client program, not the backend.  This worker therefore locates
 * the pg_dump binary installed alongside the running postgres executable
 * (find_other_exec(), the same helper pg_ctl/postmaster.c use to find
 * sibling binaries) and runs it via system(), exactly the way
 * archive_command/restore_command are executed from a backend process in
 * xlogarchive.c.  Because fe_utils (which has appendShellString) is not
 * linked into the backend, append_shell_quoted() below ports the same
 * quoting logic from fe_utils/string_utils.c's appendShellStringNoError()
 * so that the database name and file paths can never be interpreted as
 * shell syntax, regardless of what characters they contain.
 *
 * The pg_dump subprocess connects over the local Unix socket with no
 * password, so pg_hba.conf must allow local peer or trust authentication
 * for the OS user running the server -- the same operational expectation
 * archive_command already has for filesystem access.  This worker does
 * not store or supply a password.
 *
 * Copyright (c) 2026, dbblue / Cybrosys Technologies
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/dbblue_backup_launcher.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "access/parallel.h"
#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"
#include "common/string.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/bgworker.h"
#include "postmaster/dbblue_backup_launcher.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "utils/wait_event.h"

/* GUC variables, wired up via guc_parameters.dat */
bool		dbblue_backup_enabled = false;
char	   *dbblue_backup_database = NULL;
char	   *dbblue_backup_databases = NULL;
char	   *dbblue_backup_directory = NULL;
int			dbblue_backup_interval = 86400;
int			dbblue_backup_naptime = 300;
int			dbblue_backup_retention_count = 7;
int			dbblue_backup_timeout = 3600;
int			dbblue_backup_min_free_space = 2048;	/* MB; 0 disables */

/* Permissions for per-database backup subdirectories; not a GUC. */
#define DBBLUE_BACKUP_DIR_MODE		0700

/*
 * Grace period after the initial SIGTERM (dbblue_backup_timeout) before
 * timeout(1) escalates to SIGKILL.  Without --kill-after, timeout(1) only
 * ever sends the signal once and then waits indefinitely -- a pg_dump
 * that ignores or is stuck past SIGTERM would make dbblue_backup_timeout
 * not actually be a bound at all.  Not a GUC: this is a fixed safety
 * margin on top of the configured timeout, not something operators need
 * to tune.
 */
#define DBBLUE_BACKUP_KILL_GRACE_S	30

/*
 * How long pg_dump may wait for each table lock it takes at the start
 * (--lock-wait-timeout).  pg_dump sets its own lock_timeout to 0, so without
 * this a table locked by someone else -- e.g. an Odoo module upgrade running
 * ALTER TABLE -- holds the dump until dbblue_backup_timeout, and since
 * databases are dumped one at a time, every database after it waits too.
 * With it, that dump fails after this long, is retried after the usual retry
 * delay, and the cycle moves on.  It also keeps pg_dump from queueing behind
 * an ALTER TABLE that is itself waiting, which would make that jam longer.
 */
#define DBBLUE_BACKUP_LOCK_WAIT_TIMEOUT	"30s"

/*
 * After this many timeouts in a row for one database, warn that its dump has
 * outgrown dbblue_backup_timeout: each attempt is stopped, so it is not being
 * backed up at all, while each timeout's own WARNING reads like a one-off.
 */
#define DBBLUE_BACKUP_TIMEOUT_WARN_AFTER	5

/* stop_reason of a dump stopped by dbblue_backup_timeout */
static const char *const timed_out_reason = "stopped: exceeded dbblue_backup_timeout";

/* Set by backup_one_database(): its last attempt was stopped by the timeout. */
static bool last_attempt_timed_out = false;

/*
 * Process group of the running pg_dump, 0 if none.  backup_launcher_quickdie()
 * kills it when the postmaster tells this worker to exit at once.
 */
static volatile pid_t dump_pgid = 0;

/*
 * backup_launcher_quickdie
 *		SIGQUIT handler: on an immediate shutdown or a crash-restart, kill the
 *		running pg_dump's process group too, then exit at once, as
 *		SignalHandlerForCrashExit() does.  pg_dump runs in a process group of
 *		its own, which the postmaster's signal to this worker does not reach;
 *		without this it would run on until its own server connection died.
 *		Only async-signal-safe calls here.
 */
static void
backup_launcher_quickdie(SIGNAL_ARGS)
{
	if (dump_pgid > 0)
		kill(-dump_pgid, SIGKILL);
	_exit(2);
}

/* Seconds on a monotonic clock, for timeouts that a time change must not affect. */
static double
monotonic_seconds(void)
{
	instr_time	now;

	INSTR_TIME_SET_CURRENT(now);
	return INSTR_TIME_GET_DOUBLE(now);
}

/*
 * pg_dump writes to <dbname>_<time>.dump.partial, which is renamed to
 * <dbname>_<time>.dump only once the dump is complete and on disk.  So a file
 * ending in .dump is always a whole backup, and one ending in .dump.partial
 * is a dump that never finished (the worker was killed mid-dump, e.g. by a
 * crash-restart, before it could clean up); remove_partial_dumps() deletes
 * those.  Retention only ever removes files ending in .dump.
 */
#define DBBLUE_BACKUP_PARTIAL_SUFFIX ".partial"

/*
 * Where pg_dump's error output goes, relative to the data directory (the
 * worker's working directory).  One fixed file: dumps run one at a time.  Not
 * in the backup directory, which may be the very thing pg_dump cannot write
 * to; pg_stat_tmp's contents are left out of base backups.
 */
#define DBBLUE_BACKUP_STDERR_FILE	PG_STAT_TMP_DIR "/dbblue_backup_pg_dump.stderr"

/* pg_dump's own -V output, for find_other_exec()'s version check. */
#define DBBLUE_BACKUP_PGDUMP_VERSIONSTR "pg_dump (PostgreSQL) " PG_VERSION "\n"

/* One entry from dbblue_backup_databases, before pg_database is checked. */
typedef struct BackupDbSpec
{
	char	   *dbname;
} BackupDbSpec;

/* One configured database plus what the catalog/history lookup learned. */
typedef struct BackupCandidate
{
	char	   *dbname;
	Oid			dboid;			/* InvalidOid if the database does not exist */
	bool		allowconn;
	bool		istemplate;
	/*
	 * Start of the last successful backup, 0 if none.  The cooldown runs from
	 * the start, not the finish, so that a daily backup stays at the same
	 * time of day instead of moving later by its own duration every day.
	 */
	TimestampTz last_backup_at;
	int64		last_backup_size;	/* bytes; 0 if none */
	char	   *last_backup_path;	/* its dump file; NULL if none */
} BackupCandidate;

/*
 * Worker-lifetime memory context; holds the specs/candidates of the cycle
 * currently running and is reset at the end of each cycle (and by the
 * error recovery path).
 */
static MemoryContext launcher_cxt = NULL;

/*
 * Set once dbblue_backup_history has been verified to exist after the
 * feature was switched on; cleared when it is switched off so a later
 * re-enable re-checks it.
 */
static bool schema_ready = false;

/*
 * Set once BackgroundWorkerInitializeConnection has attached the worker to
 * its database.  Deferred until the feature is first enabled, so a
 * cluster that never turns the launcher on never attaches to the database
 * (which would otherwise block DROP DATABASE on it).
 */
static bool launcher_connected = false;

/*
 * Set once pg_dump has been located and the local socket directory
 * resolved.  Cleared (and retried) whenever resolution fails, the same way
 * schema_ready is retried.
 */
static bool environment_ready = false;
static char pg_dump_path[MAXPGPATH];
static char *socket_dir = NULL;	/* NULL means "let libpq pick the default" */

/*
 * realpath() of the data directory and of dbblue_backup_directory, set by
 * check_backup_directory() (with symlinks resolved, so a link pointing into
 * the data directory is seen for what it is).
 */
static char data_dir_real[MAXPGPATH];
static char backup_dir_real[MAXPGPATH];

/*
 * Role name pg_dump connects as (-U), cached in TopMemoryContext.  This is the
 * bootstrap superuser -- the identity the launcher itself attaches as -- named
 * explicitly so pg_dump does not fall back to the operating-system user name,
 * which need not be a valid or privileged database role.  Resolved in
 * ensure_schema(), which runs with catalog access.
 */
static char *backup_role = NULL;

/*
 * Retry state of a database whose backups are failing.
 *
 * The cooldown (dbblue_backup_interval) runs from the last *successful*
 * backup, so on its own a database whose backups keep failing would be
 * retried every cycle -- for a dump that always runs into
 * dbblue_backup_timeout, an hour-long pg_dump every naptime, around the clock,
 * holding its snapshot (blocking VACUUM) and its table locks (blocking DDL),
 * and delaying every database after it in the list; for a full disk, filling
 * it again every naptime.  So after the N-th failure in a row, the next
 * attempt waits dbblue_backup_naptime * 2^(N-1), capped at
 * dbblue_backup_interval: a passing problem is still retried soon, a lasting
 * one costs about one attempt per interval.
 *
 * Kept in worker memory, not derived from dbblue_backup_history: retention
 * keeps as few as one "failed" row per database, which cannot count
 * failures.  A success or a restart clears it.  A configuration reload ends
 * the current wait -- reloading being what an administrator does after
 * fixing the cause (e.g. pg_hba.conf), so the fix is tried at the next cycle,
 * not up to an interval later -- but keeps the count, so that should that
 * attempt fail too, the waits go on growing rather than start over.
 */
typedef struct BackupRetryState
{
	char		dbname[NAMEDATALEN];	/* hash key */
	int			failures;		/* consecutive failed attempts */
	int			timeouts;		/* of which the latest ones in a row timed out */
	TimestampTz last_failure_at;	/* when the latest one ended */
} BackupRetryState;

static HTAB *retry_state = NULL;	/* in TopMemoryContext */

/*
 * What report_unmanaged_backups() last logged (-1 = nothing yet), and when:
 * it logs only when that changes, or once a day as a reminder.
 */
static int	last_report_count = -1;
static int64 last_report_bytes = -1;
static TimestampTz last_report_at = 0;

static bool ensure_schema(void);
static bool resolve_environment(void);
static List *parse_configured_databases(void);
static List *load_backup_candidates(List *specs);
static TimestampTz run_backup_cycle(void);
static void backup_launcher_quickdie(SIGNAL_ARGS);
static void reload_configuration(void);
static void warn_if_backup_database_listed(void);
static int64 retry_delay_secs(int failures);
static void note_backup_result(const char *dbname, bool ok,
							   TimestampTz last_backup_at);
static bool backup_one_database(const char *dbname, int64 last_backup_size);
static void apply_retention(const char *dbname);
static void prune_resolved_failures(const char *dbname);
static void fix_future_dated_rows(const char *dbname);
static bool record_backup_history(const char *dbname, TimestampTz started_at,
								  const char *status, const char *file_path,
								  int64 file_size_bytes,
								  const char *error_message);
static void enforce_retention(const char *dbname, const char *status);
static bool append_shell_quoted(StringInfo buf, const char *str);
static void append_conninfo_dbname(StringInfo buf, const char *dbname);
static void report_unmanaged_backups(void);
static bool path_has_parent_component(const char *path);
static const char *database_list_problem(const char *value, const char *item);
static const char *parent_folder_unsafe_reason(const char *path);
static bool backup_directory_still_usable(void);

/*
 * dbblue_check_backup_enabled
 *		GUC check hook for dbblue_backup_enabled.
 *
 * The launcher cannot do anything
 * useful without dbblue_backup_database existing, dbblue_backup_directory
 * set, and dbblue_backup_databases naming something, but none of that is
 * fatal to reject here -- the operator may be preparing configuration
 * ahead of the pieces existing.  Warn loudly instead, at the moment the
 * feature is switched on.
 *
 * Only runs on an actual off->on transition (repeated SETs of an
 * already-on value are silent), and only once the backend has real
 * catalog access and settled GUC state (IsUnderPostmaster and
 * IsTransactionState()) -- a check hook also runs in contexts where
 * neither is true yet, e.g. while postgresql.conf is being parsed at
 * postmaster startup, before any database connection exists.
 */
bool
dbblue_check_backup_enabled(bool *newval, void **extra, GucSource source)
{
	if (!*newval || dbblue_backup_enabled)
		return true;

	/*
	 * A parallel worker copies the leader's settings one by one, so this
	 * can run before dbblue_backup_databases has arrived and warn falsely
	 * -- once per worker of every parallel query.  The leader has already
	 * warned about anything real.
	 */
	if (!IsUnderPostmaster || !IsTransactionState() || IsParallelWorker())
		return true;

	if (dbblue_backup_database == NULL || dbblue_backup_database[0] == '\0')
		ereport(WARNING,
				(errmsg("dbblue_backup_enabled is on, but dbblue_backup_database is not set"),
				 errdetail("With no database configured the backup launcher worker will fail to connect."),
				 errhint("Set dbblue_backup_database to an existing database (changing it requires a server restart).")));
	else if (!OidIsValid(get_database_oid(dbblue_backup_database, true)))
		ereport(WARNING,
				(errmsg("dbblue backup launcher database \"%s\" does not exist",
						dbblue_backup_database),
				 errdetail("The launcher worker will fail to connect and will be retried every minute until the database exists."),
				 errhint("Create the database, or point dbblue_backup_database at an existing one (changing it requires a server restart).")));

	if (dbblue_backup_directory == NULL || dbblue_backup_directory[0] == '\0')
		ereport(WARNING,
				(errmsg("dbblue_backup_enabled is on, but dbblue_backup_directory is not set"),
				 errhint("Set dbblue_backup_directory to an existing, writable directory (changing it requires a server restart).")));

	if (dbblue_backup_databases == NULL || dbblue_backup_databases[0] == '\0')
		ereport(WARNING,
				(errmsg("dbblue_backup_enabled is on, but dbblue_backup_databases lists no databases"),
				 errhint("Set dbblue_backup_databases to a comma-separated list of database names to back up.")));

	return true;
}

/*
 * dbblue_check_backup_databases
 *		GUC check hook for dbblue_backup_databases.
 *
 * Syntax only: a comma-separated list of database names, matched
 * case-sensitively and exactly as written; double quotes are needed only
 * around a name containing a comma or whitespace.  Resolution against
 * pg_database is deliberately deferred to each cycle
 * (parse_configured_databases/load_backup_candidates), since a database can
 * be created, dropped, or renamed between GUC-set-time and backup-time.
 *
 * Names are split with SplitGUCList(), not SplitIdentifierString(): the
 * latter downcases unquoted names, so a database created as "MyCompany" (as
 * Odoo does) would be looked up as "mycompany" and never be found.
 *
 * Each name becomes a path component of the backup directory and file
 * (<dbblue_backup_directory>/<name>/<name>_<time>.dump), so the names that
 * cannot be one -- empty, ".", "..", or containing '/' -- are refused.  Any
 * other character, '.' included (Odoo allows it), is safe there.
 *
 * Only ALTER SYSTEM rejects a bad value (a session in a transaction).  At
 * server start a rejected value in postgresql.conf would be FATAL -- far
 * worse than not backing up some database -- so there, and on a reload, it
 * is accepted, and the launcher skips the bad names and says why (see
 * parse_configured_databases()).  A parallel worker only copies the value
 * its leader already accepted.
 */
bool
dbblue_check_backup_databases(char **newval, void **extra, GucSource source)
{
	const char *problem;

	if (*newval == NULL || **newval == '\0')
		return true;

	problem = database_list_problem(*newval, NULL);
	if (problem == NULL)
		return true;

	if (IsUnderPostmaster && IsTransactionState() && !IsParallelWorker())
	{
		GUC_check_errdetail("%s", problem);
		GUC_check_errhint("List database names separated by commas. A database named \".\" or \"..\", or whose name contains \"/\", cannot be listed.");
		return false;
	}

	return true;
}

/*
 * database_list_problem
 *		NULL if value is a valid dbblue_backup_databases list, else why not.
 *		If item is not NULL, check only that one name of the list.
 */
static const char *
database_list_problem(const char *value, const char *item)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *lc;
	const char *problem = NULL;

	if (item != NULL)
	{
		if (item[0] == '\0' ||
			strcmp(item, ".") == 0 ||
			strcmp(item, "..") == 0 ||
			strchr(item, '/') != NULL)
			return psprintf("\"%s\" cannot be backed up, as its name cannot be used as a backup directory name.",
							item);
		return NULL;
	}

	rawstring = pstrdup(value);
	if (!SplitGUCList(rawstring, ',', &elemlist))
		problem = "List syntax is invalid.";
	else
	{
		foreach(lc, elemlist)
		{
			problem = database_list_problem(NULL, (char *) lfirst(lc));
			if (problem != NULL)
				break;
		}
	}

	list_free(elemlist);
	pfree(rawstring);
	return problem;
}

/*
 * path_is_within
 *		True if path is dir or lies inside it.  Both must be canonical
 *		absolute paths.
 */
static bool
path_is_within(const char *path, const char *dir)
{
	size_t		len = strlen(dir);

	if (strcmp(dir, "/") == 0)
		return true;

	return strncmp(path, dir, len) == 0 &&
		(path[len] == '\0' || path[len] == '/');
}

/*
 * get_real_path
 *		realpath() into a MAXPGPATH buffer.  False, with errno set, if path
 *		does not exist or cannot be resolved.
 */
static bool
get_real_path(const char *path, char *result)
{
	char	   *resolved = realpath(path, NULL);

	if (resolved == NULL)
		return false;

	if (strlcpy(result, resolved, MAXPGPATH) >= MAXPGPATH)
	{
		free(resolved);
		errno = ENAMETOOLONG;
		return false;
	}

	free(resolved);
	return true;
}

/*
 * dbblue_check_backup_directory
 *		GUC check hook for dbblue_backup_directory.
 *
 * Warns, at ALTER SYSTEM time, about a value the launcher will refuse to back
 * up into (see check_backup_directory()): a relative path, a path with a ".."
 * component, or the data directory or a directory inside it.  Never rejects
 * the value: this setting
 * is read from postgresql.conf at postmaster start, where a rejected value
 * stops the server from starting at all -- far worse than not backing up --
 * and where the data directory is not even known yet on the first pass.  So
 * it is only checked where a session can see the warning (IsUnderPostmaster
 * and IsTransactionState()), the same way dbblue_check_backup_enabled() does.
 */
bool
dbblue_check_backup_directory(char **newval, void **extra, GucSource source)
{
	char		path[MAXPGPATH];
	char		datadir[MAXPGPATH];

	if (*newval == NULL || **newval == '\0')
		return true;

	/* A parallel worker would repeat the leader's warning; see above. */
	if (!IsUnderPostmaster || !IsTransactionState() || IsParallelWorker() ||
		DataDir == NULL)
		return true;

	if (!is_absolute_path(*newval))
	{
		ereport(WARNING,
				(errmsg("dbblue_backup_directory \"%s\" is not an absolute path, so the backup launcher will not back up into it",
						*newval),
				 errdetail("A relative path is interpreted against the data directory, so backups would be written inside it."),
				 errhint("Use an absolute path to a directory outside the data directory.")));
		return true;
	}

	if (path_has_parent_component(*newval))
	{
		ereport(WARNING,
				(errmsg("dbblue_backup_directory \"%s\" contains a \"..\" component, so the backup launcher will not back up into it",
						*newval),
				 errdetail("Retention never removes a dump whose path contains \"..\", so old backups would pile up."),
				 errhint("Write the path without \"..\".")));
		return true;
	}

	/* Resolve symlinks where the paths exist; compare lexically otherwise. */
	if (!get_real_path(*newval, path))
	{
		strlcpy(path, *newval, sizeof(path));
		canonicalize_path(path);
	}
	if (!get_real_path(DataDir, datadir))
	{
		strlcpy(datadir, DataDir, sizeof(datadir));
		canonicalize_path(datadir);
	}

	if (path_is_within(path, datadir))
		ereport(WARNING,
				(errmsg("dbblue_backup_directory \"%s\" is the data directory or inside it, so the backup launcher will not back up into it",
						*newval),
				 errhint("Use a directory outside \"%s\".", DataDir)));

	return true;
}

/*
 * min_free_space_bytes
 *		dbblue_backup_min_free_space in bytes; 0 means no free-space checks.
 */
static int64
min_free_space_bytes(void)
{
	return (int64) dbblue_backup_min_free_space * 1024 * 1024;
}

/*
 * get_free_space
 *		Bytes available to the server's OS user on the file system holding
 *		path, or -1 (with errno set) if that cannot be determined.
 */
static int64
get_free_space(const char *path)
{
	struct statvfs fs;

	if (statvfs(path, &fs) != 0)
		return -1;

	return (int64) fs.f_bavail * (int64) fs.f_frsize;
}

/*
 * folder_unsafe_reason
 *		NULL if path is a directory owned by the operating-system user the
 *		server runs as, writable by no one else (no group or other write
 *		permission) and, when follow_link is false, not a symbolic link.
 *		Otherwise why not, for a message (palloc'd).
 *
 * Anyone else who can create files in a folder the launcher dumps into can
 * plant a symlink under the predictable name of the next dump, which pg_dump
 * then opens and overwrites as the server's user -- pg_control, relation
 * files, TDE keys -- or replace a finished dump with one whose SQL runs as
 * superuser when it is restored.
 */
static const char *
folder_unsafe_reason(const char *path, bool follow_link)
{
	struct stat st;

	if ((follow_link ? stat(path, &st) : lstat(path, &st)) != 0)
		return psprintf("cannot be checked: %s", strerror(errno));
	if (S_ISLNK(st.st_mode))
		return "is a symbolic link";
	if (!S_ISDIR(st.st_mode))
		return "is not a directory";
	if (st.st_uid != geteuid())
		return "is not owned by the operating-system user the server runs as";
	if (st.st_mode & (S_IWGRP | S_IWOTH))
		return psprintf("is writable by other users (mode %04o)",
						(unsigned int) (st.st_mode & 07777));
	return NULL;
}

/*
 * parent_folder_unsafe_reason
 *		Why a folder above path (a canonical absolute path) would let another
 *		user put something else in path's place, or NULL if none would.
 *
 * Whoever can write to a folder can rename what is in it, so a backup
 * directory no one else can write to is still not safe inside one they can:
 * they could move it aside and put a symlink to, say, the data directory in
 * its place.  So each folder above it must be owned by root or the server's
 * user, and not writable by group or others -- unless it has the sticky bit,
 * as /tmp does, which keeps others from renaming what they do not own.
 */
static const char *
parent_folder_unsafe_reason(const char *path)
{
	char		parent[MAXPGPATH];
	struct stat st;

	strlcpy(parent, path, sizeof(parent));
	while (strcmp(parent, "/") != 0)
	{
		char		previous[MAXPGPATH];

		strlcpy(previous, parent, sizeof(previous));
		get_parent_directory(parent);
		if (parent[0] == '\0')
			strlcpy(parent, "/", sizeof(parent));
		if (strcmp(parent, previous) == 0)
			break;

		if (stat(parent, &st) != 0)
			return psprintf("is inside \"%s\", which cannot be checked: %s",
							parent, strerror(errno));
		if (st.st_uid != 0 && st.st_uid != geteuid())
			return psprintf("is inside \"%s\", which is owned by another operating-system user",
							parent);
		if ((st.st_mode & (S_IWGRP | S_IWOTH)) && !(st.st_mode & S_ISVTX))
			return psprintf("is inside \"%s\", which is writable by other users (mode %04o)",
							parent, (unsigned int) (st.st_mode & 07777));
	}
	return NULL;
}

/*
 * check_backup_directory
 *		Verify that dbblue_backup_directory is somewhere the launcher may
 *		write dumps, and resolve it and the data directory into
 *		backup_dir_real and data_dir_real.  False, after a WARNING, if not.
 *
 * Refused:
 *	- a relative path: it is interpreted against the data directory (the
 *	  server's working directory), so the dumps would land inside it;
 *	- a path with a ".." component: retention refuses to remove any dump
 *	  whose path contains one (see get_prunable_backup_path()), so dumps
 *	  written there would never be pruned;
 *	- a directory that does not exist (or is not a directory): it must exist
 *	  for symlinks in it to be resolved, and creating it is the operator's job,
 *	  with the ownership and permissions they choose;
 *	- the data directory itself, or any directory inside it, symlinks
 *	  resolved: every base backup of the data directory would copy the dumps,
 *	  including the pg_basebackup dbblue_create_standby builds its standby
 *	  from, and a database named after one of the server's own subdirectories
 *	  (base, global, pg_wal, ...) would have its dumps written into it.
 *
 * An unset value is let through; backup_one_database() records each skipped
 * backup.
 */
static bool
check_backup_directory(void)
{
	struct stat st;
	bool		usable = true;

	backup_dir_real[0] = '\0';

	if (dbblue_backup_directory == NULL || dbblue_backup_directory[0] == '\0')
		return true;

	if (!is_absolute_path(dbblue_backup_directory))
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: dbblue_backup_directory \"%s\" is not an absolute path, not backing up",
						dbblue_backup_directory),
				 errdetail("A relative path is interpreted against the data directory, so backups would be written inside it."),
				 errhint("Set dbblue_backup_directory to an absolute path outside the data directory (changing it requires a server restart).")));
		return false;
	}

	if (path_has_parent_component(dbblue_backup_directory))
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: dbblue_backup_directory \"%s\" contains a \"..\" component, not backing up",
						dbblue_backup_directory),
				 errdetail("Retention never removes a dump whose path contains \"..\", so old backups would pile up."),
				 errhint("Write the path without \"..\" (changing it requires a server restart).")));
		return false;
	}

	if (!get_real_path(DataDir, data_dir_real))
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue backup launcher: could not resolve data directory \"%s\": %m, not backing up",
						DataDir)));
		return false;
	}

	/* Each failure leaves its own errno for %m. */
	if (!get_real_path(dbblue_backup_directory, backup_dir_real))
		usable = false;
	else if (stat(backup_dir_real, &st) != 0)
		usable = false;
	else if (!S_ISDIR(st.st_mode))
	{
		errno = ENOTDIR;
		usable = false;
	}

	if (!usable)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue backup launcher: cannot use dbblue_backup_directory \"%s\": %m, not backing up",
						dbblue_backup_directory),
				 errhint("Create the directory, owned by the operating-system user the server runs as, outside the data directory.")));
		backup_dir_real[0] = '\0';
		return false;
	}

	/*
	 * Only a directory no one else can write to (see folder_unsafe_reason()).
	 * The setting itself may name a symlink: it is the operator's own choice,
	 * and backup_dir_real is where it leads.
	 */
	{
		const char *reason = folder_unsafe_reason(backup_dir_real, true);

		if (reason != NULL)
		{
			ereport(WARNING,
					(errmsg("dbblue backup launcher: dbblue_backup_directory \"%s\" %s, not backing up",
							dbblue_backup_directory, reason),
					 errdetail("Another user who can write there could make a backup overwrite the server's own files, or replace a backup with a malicious one."),
					 errhint("Make it owned by the operating-system user the server runs as, and not writable by group or others (chmod go-w).")));
			backup_dir_real[0] = '\0';
			return false;
		}

		reason = parent_folder_unsafe_reason(backup_dir_real);
		if (reason != NULL)
		{
			ereport(WARNING,
					(errmsg("dbblue backup launcher: dbblue_backup_directory \"%s\" %s, not backing up",
							dbblue_backup_directory, reason),
					 errdetail("Another user who can write there could move the backup directory aside and put a link to the server's own files in its place."),
					 errhint("Use a directory whose parent folders are owned by root or by the operating-system user the server runs as, and are not writable by group or others.")));
			backup_dir_real[0] = '\0';
			return false;
		}
	}

	if (path_is_within(backup_dir_real, data_dir_real))
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: dbblue_backup_directory \"%s\" is the data directory or inside it, not backing up",
						dbblue_backup_directory),
				 errdetail("Dumps there would be copied by every base backup of the data directory, including the one the standby is built from, and a database named after one of the server's own subdirectories, such as pg_wal, would have its dumps written into it."),
				 errhint("Set dbblue_backup_directory to a directory outside \"%s\" (changing it requires a server restart).",
						 data_dir_real)));
		backup_dir_real[0] = '\0';
		return false;
	}

	/*
	 * Allowed, but say so if it shares a file system with the data
	 * directory: a backup that filled it would stop the database's own
	 * writes too, possibly shutting the server down.  Only
	 * dbblue_backup_min_free_space guards against that, so warn when it is
	 * off.
	 */
	{
		struct stat data_st;

		if (stat(data_dir_real, &data_st) == 0 && data_st.st_dev == st.st_dev)
		{
			if (dbblue_backup_min_free_space == 0)
				ereport(WARNING,
						(errmsg("dbblue backup launcher: dbblue_backup_directory \"%s\" is on the same file system as the data directory, and dbblue_backup_min_free_space is 0",
								dbblue_backup_directory),
						 errdetail("A backup that fills the file system would also stop the database from writing, and could shut the server down."),
						 errhint("Set dbblue_backup_min_free_space, or use a directory on another disk.")));
			else
				ereport(LOG,
						(errmsg("dbblue backup launcher: dbblue_backup_directory \"%s\" is on the same file system as the data directory; backups keep %d MB of it free (dbblue_backup_min_free_space)",
								dbblue_backup_directory,
								dbblue_backup_min_free_space)));
		}
	}

	return true;
}

/*
 * backup_directory_still_usable
 *		Re-check, at the start of each cycle, what check_backup_directory()
 *		verified when the launcher got ready: since then the directory may
 *		have been removed, made writable by others, put inside a folder
 *		others can write to, or -- through a symlink in its path -- moved.
 *		False, after a WARNING, if this cycle must neither back up nor prune.
 *
 * A missing directory is no longer re-created: backup_one_database() only
 * creates the per-database folder inside it, as the setting's own
 * description says it must exist.
 */
static bool
backup_directory_still_usable(void)
{
	char		real[MAXPGPATH];
	const char *reason;

	if (dbblue_backup_directory == NULL || dbblue_backup_directory[0] == '\0')
		return true;			/* backup_one_database() records each skip */

	if (!get_real_path(dbblue_backup_directory, real))
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue backup launcher: cannot use dbblue_backup_directory \"%s\": %m, not backing up",
						dbblue_backup_directory),
				 errhint("Is the backup disk mounted?  Backups resume once the directory is back.")));
		return false;
	}

	if (strcmp(real, backup_dir_real) != 0)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: dbblue_backup_directory \"%s\" now leads to \"%s\" instead of \"%s\", not backing up",
						dbblue_backup_directory, real, backup_dir_real),
				 errhint("Restart the server to check and use the new location.")));
		return false;
	}

	reason = folder_unsafe_reason(backup_dir_real, true);
	if (reason == NULL)
		reason = parent_folder_unsafe_reason(backup_dir_real);
	if (reason != NULL)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: dbblue_backup_directory \"%s\" %s, not backing up",
						dbblue_backup_directory, reason),
				 errdetail("Another user who can write there could make a backup overwrite the server's own files, or replace a backup with a malicious one."),
				 errhint("Make it, and the folders above it, owned by the operating-system user the server runs as (or root), and not writable by group or others.")));
		return false;
	}

	return true;
}

/*
 * BackupLauncherRegister
 *		Register the dbblue backup launcher as a static background
 *		worker.  Called directly from PostmasterMain(), the same way the
 *		other dbblue workers are, since this is core functionality, not
 *		something an extension's _PG_init() has to opt into.
 *
 * Always registered, since dbblue_backup_enabled is PGC_SIGHUP context
 * and the feature must be switchable on without a restart; while
 * disabled the worker only sleeps.
 */
void
BackupLauncherRegister(void)
{
	BackgroundWorker worker;

	/*
	 * Don't run during pg_upgrade: the postmaster is started internally,
	 * multiple times, in a restricted mode to restore schema objects in a
	 * precise sequence; this worker independently connecting and shelling
	 * out to pg_dump has no business happening during that window.
	 */
	if (IsBinaryUpgrade)
		return;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;

	/*
	 * Use ConsistentState rather than RecoveryFinished so this also starts
	 * on hot standbys (which never reach RecoveryFinished's PM_RUN state).
	 * The RecoveryInProgress() check in BackupLauncherMain()'s loop is what
	 * then keeps a standby's copy from ever attempting to back anything up.
	 */
	worker.bgw_start_time = BgWorkerStart_ConsistentState;

	/*
	 * Restarted a minute after it exits.  If dbblue_backup_database does not
	 * exist or does not allow connections, connecting to it fails with FATAL
	 * on every start; a 5-second restart filled the log with that FATAL
	 * around the clock.  Backups run at most every naptime, so a minute's
	 * wait after a crash or pg_terminate_backend() costs nothing.
	 */
	worker.bgw_restart_time = 60;
	snprintf(worker.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "BackupLauncherMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "dbblue backup launcher");
	snprintf(worker.bgw_type, BGW_MAXLEN, "dbblue backup launcher");
	worker.bgw_notify_pid = 0;
	worker.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&worker);
}

/*
 * BackupLauncherMain
 *		Background worker entry point.
 */
void
BackupLauncherMain(Datum main_arg)
{
	sigjmp_buf	local_sigjmp_buf;
	TimestampTz next_run = 0;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGQUIT, backup_launcher_quickdie);
	BackgroundWorkerUnblockSignals();

	ereport(LOG, (errmsg("dbblue backup launcher started")));

	launcher_cxt = AllocSetContextCreate(TopMemoryContext,
										 "dbblue backup launcher",
										 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(launcher_cxt);

	/*
	 * Recover here after any unexpected error: report it, clean up
	 * whatever transaction state is left, wait, and go back to the main loop.
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
		MemoryContextReset(launcher_cxt);
		MemoryContextSwitchTo(launcher_cxt);

		/* Re-verify the schema and environment before the next cycle. */
		schema_ready = false;
		environment_ready = false;

		pgstat_report_activity(STATE_IDLE, NULL);

		RESUME_INTERRUPTS();

		/*
		 * Wait one naptime before trying again.  An error jumps over the wait
		 * at the end of the main loop, so without this an error that keeps
		 * happening (a read-only setting, a full disk, a locked history table)
		 * would be retried as fast as the loop can run.  A shutdown request or
		 * a configuration reload sets the latch and ends the wait at once.
		 */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 (long) dbblue_backup_naptime * 1000L,
						 WAIT_EVENT_DBBLUE_BACKUP_LAUNCHER_MAIN);
		ResetLatch(MyLatch);
	}
	PG_exception_stack = &local_sigjmp_buf;

	for (;;)
	{
		long		sleep_ms;

		CHECK_FOR_INTERRUPTS();

		if (ShutdownRequestPending)
			break;

		/*
		 * Run a cycle right after a reload instead of sleeping until the
		 * wake-up computed from the old settings (up to the old naptime
		 * away), so that a newly listed database, a lower retention count or
		 * the retries reload_configuration() announces happen now.  A cycle
		 * still backs up only the databases that are due.
		 */
		if (ConfigReloadPending)
		{
			reload_configuration();
			next_run = 0;
		}

		if (dbblue_backup_enabled && !RecoveryInProgress())
		{
			if (!launcher_connected)
			{
				/*
				 * Connect outside the recovery block's reach: an ERROR while
				 * the connection is half set up must end the process (as it
				 * would in any backend) rather than be "recovered" from by
				 * looping round to connect again on a half-initialized
				 * backend.  With no exception stack, ERROR becomes FATAL.
				 */
				PG_exception_stack = NULL;
				BackgroundWorkerInitializeConnection(dbblue_backup_database,
													 NULL, 0);
				PG_exception_stack = &local_sigjmp_buf;

				/*
				 * Bound how long the launcher will wait for a lock while
				 * managing its own history table.  This never blocks on
				 * the pg_dump subprocess itself (dbblue_backup_timeout
				 * does that); it only protects the SPI bookkeeping.
				 */
				SetConfigOption("lock_timeout", "5s", PGC_SUSET, PGC_S_SESSION);

				/*
				 * Pin search_path.  Every query this worker runs executes as
				 * the bootstrap superuser, so no function, operator or table
				 * may ever be resolved from a user-writable schema such as
				 * public (otherwise e.g. a public.unnest(text[]) would be a
				 * better match than pg_catalog.unnest(anyarray) and run with
				 * superuser rights).  PGC_S_SESSION also overrides any
				 * ALTER DATABASE ... SET search_path, and is not reset by a
				 * SIGHUP reload of postgresql.conf.
				 */
				SetConfigOption("search_path", "pg_catalog, pg_temp",
								PGC_SUSET, PGC_S_SESSION);

				launcher_connected = true;
			}

			if (!environment_ready)
				environment_ready = resolve_environment();

			if (environment_ready && !schema_ready)
			{
				schema_ready = ensure_schema();
				if (schema_ready)
					warn_if_backup_database_listed();
			}

			if (environment_ready && schema_ready)
			{
				TimestampTz now = GetCurrentTimestamp();

				if (next_run == 0 || now >= next_run)
				{
					TimestampTz next_due;

					/* Checked every cycle; logged only on change, or daily. */
					report_unmanaged_backups();
					next_due = run_backup_cycle();

					/*
					 * Wake up after naptime, or as soon as the next database
					 * is due if that is sooner -- so that a backup starts when
					 * it is due, not up to a naptime later, which made each
					 * backup start a little later than the one before.
					 */
					next_run = GetCurrentTimestamp() +
						(int64) dbblue_backup_naptime * USECS_PER_SEC;
					if (next_due != 0 && next_due < next_run)
						next_run = next_due;

					/* This cycle's working memory, and its table statistics. */
					MemoryContextSwitchTo(TopMemoryContext);
					MemoryContextReset(launcher_cxt);
					MemoryContextSwitchTo(launcher_cxt);
					(void) pgstat_report_stat(false);
				}

				sleep_ms = (long) ((next_run - GetCurrentTimestamp()) / 1000);
				sleep_ms = Max(sleep_ms, 1000);
			}
			else
				sleep_ms = (long) dbblue_backup_naptime * 1000;
		}
		else
		{
			/* Disabled (or a standby): re-verify everything once re-enabled. */
			schema_ready = false;
			environment_ready = false;
			next_run = 0;
			last_report_count = -1;		/* report again once re-enabled */
			sleep_ms = 5000;
		}

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 sleep_ms,
						 WAIT_EVENT_DBBLUE_BACKUP_LAUNCHER_MAIN);
		ResetLatch(MyLatch);
	}

	ereport(LOG, (errmsg("dbblue backup launcher shutting down")));

	/*
	 * Exit non-zero on SIGTERM, matching the other dbblue workers: exiting
	 * 0 is treated by the postmaster as "terminate and forget", which
	 * would keep pg_terminate_backend() from this worker coming back until
	 * the next server restart even while the feature stays enabled.
	 */
	proc_exit(1);
}

/*
 * resolve_environment
 *		Verify the backup directory, locate the pg_dump binary installed
 *		alongside this postgres executable, and pick a local Unix socket
 *		directory for it to connect through.  Safe to retry on every wakeup
 *		until it succeeds.
 */
static bool
resolve_environment(void)
{
	int			ret;

	if (!check_backup_directory())
		return false;

	ret = find_other_exec(my_exec_path, "pg_dump",
						  DBBLUE_BACKUP_PGDUMP_VERSIONSTR, pg_dump_path);
	if (ret < 0)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: could not locate a matching \"pg_dump\" executable next to \"%s\"",
						my_exec_path),
				 errhint("This is a packaging problem: pg_dump must be installed in the same directory as postgres and be the same version.")));
		return false;
	}

	if (socket_dir == NULL)
	{
		const char *raw = GetConfigOption("unix_socket_directories", true, false);
		List	   *elemlist;

		if (raw != NULL && raw[0] != '\0')
		{
			char	   *rawstring = pstrdup(raw);

			if (SplitDirectoriesString(rawstring, ',', &elemlist) && elemlist != NIL)
				socket_dir = MemoryContextStrdup(TopMemoryContext,
												 (char *) linitial(elemlist));
			list_free(elemlist);
			pfree(rawstring);
		}
	}

	/*
	 * pg_dump connects through this server's own Unix socket.  With none
	 * configured it would fall back to libpq's compiled-in socket directory,
	 * where any socket -- not necessarily this server's -- may be listening.
	 */
	if (socket_dir == NULL)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: unix_socket_directories is empty, not backing up"),
				 errdetail("pg_dump connects to this server through its Unix socket."),
				 errhint("Set unix_socket_directories (changing it requires a server restart).")));
		return false;
	}

	ereport(LOG,
			(errmsg("dbblue backup launcher: ready (using pg_dump at \"%s\")",
					pg_dump_path)));

	return true;
}

/*
 * get_schema_owner
 *		Owner of schema nspname, or InvalidOid if there is no such schema.
 *		Caller must be connected to SPI.
 */
static Oid
get_schema_owner(const char *nspname)
{
	Oid			argtypes[1] = {TEXTOID};
	Datum		values[1];
	Datum		d;
	bool		isnull;

	values[0] = CStringGetTextDatum(nspname);

	/* read_only = false: must see a schema this transaction just created */
	if (SPI_execute_with_args("SELECT nspowner FROM pg_catalog.pg_namespace "
							  "WHERE nspname = $1",
							  1, argtypes, values, NULL, false, 0) != SPI_OK_SELECT ||
		SPI_processed != 1)
		return InvalidOid;

	d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	return isnull ? InvalidOid : DatumGetObjectId(d);
}

/*
 * lookup_relation
 *		Find nspname.relname in pg_class.  Returns false if there is no such
 *		relation; otherwise sets *relkind and *relowner and returns true.
 *		Caller must be connected to SPI.
 */
static bool
lookup_relation(const char *nspname, const char *relname,
				char *relkind, Oid *relowner)
{
	Oid			argtypes[2] = {TEXTOID, TEXTOID};
	Datum		values[2];
	Datum		d;
	bool		isnull;

	values[0] = CStringGetTextDatum(nspname);
	values[1] = CStringGetTextDatum(relname);

	/* read_only = false: must see a table this transaction just created */
	if (SPI_execute_with_args("SELECT c.relkind, c.relowner "
							  "FROM pg_catalog.pg_class c "
							  "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
							  "WHERE n.nspname = $1 AND c.relname = $2",
							  2, argtypes, values, NULL, false, 0) != SPI_OK_SELECT ||
		SPI_processed != 1)
		return false;

	d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	*relkind = isnull ? '\0' : DatumGetChar(d);
	d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
	*relowner = isnull ? InvalidOid : DatumGetObjectId(d);
	return true;
}

/*
 * create_history_guard
 *		Put a trigger on dbblue.dbblue_backup_history that refuses INSERT,
 *		UPDATE and DELETE by anyone but a superuser.
 *
 * The table's grants already allow non-superusers nothing but SELECT, but
 * members of pg_write_all_data bypass grants.  Retention deletes the dump
 * files of the rows it prunes, so such a member could add fake "success"
 * rows that push the real ones past dbblue_backup_retention_count -- and the
 * launcher itself would then delete every real backup from disk.  Triggers
 * are not bypassed by pg_write_all_data, and only the table's owner (the
 * bootstrap superuser) can disable or drop this one.
 *
 * Runs on every start, so existing installations get it too.  Needs
 * PL/pgSQL; without it, the guard is skipped with a WARNING rather than
 * stopping backups.
 */
static void
create_history_guard(void)
{
	if (SPI_execute("SELECT 1 FROM pg_catalog.pg_language WHERE lanname = 'plpgsql'",
					true, 0) != SPI_OK_SELECT || SPI_processed != 1)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: PL/pgSQL is not installed in database \"%s\", so dbblue.dbblue_backup_history is not protected against changes by pg_write_all_data members",
						dbblue_backup_database)));
		return;
	}

	if (SPI_execute("CREATE OR REPLACE FUNCTION dbblue.dbblue_backup_history_guard() "
					"RETURNS trigger LANGUAGE plpgsql "
					"SET search_path = pg_catalog, pg_temp AS $$ "
					"BEGIN "
					"  IF NOT (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user) THEN "
					"    RAISE EXCEPTION 'dbblue.dbblue_backup_history can only be changed by the backup launcher' "
					"      USING ERRCODE = 'insufficient_privilege'; "
					"  END IF; "
					"  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END; "
					"END $$",
					false, 0) != SPI_OK_UTILITY ||
		SPI_execute("CREATE OR REPLACE TRIGGER dbblue_backup_history_guard "
					"BEFORE INSERT OR UPDATE OR DELETE ON dbblue.dbblue_backup_history "
					"FOR EACH ROW EXECUTE FUNCTION dbblue.dbblue_backup_history_guard()",
					false, 0) != SPI_OK_UTILITY)
		ereport(WARNING,
				(errmsg("dbblue backup launcher: failed to create the guard trigger on dbblue.dbblue_backup_history")));
}

/*
 * create_history_objects
 *		The body of ensure_schema(); runs inside its transaction and SPI
 *		connection.  Returns false, after a WARNING, if the history table
 *		cannot be created or cannot be trusted.
 */
static bool
create_history_objects(void)
{
	char		relkind;
	Oid			relowner;

	/*
	 * The schema.  CREATE SCHEMA IF NOT EXISTS silently adopts a schema of
	 * that name created earlier by any role with CREATE on the database (the
	 * database owner, typically), and a schema's owner can drop -- and so
	 * replace -- any table inside it.  So whoever owns it must be verified,
	 * not assumed.  The dbblue audit log (pg_audit.c) creates this same
	 * schema as the same bootstrap superuser, so sharing it is fine.
	 */
	if (SPI_execute("CREATE SCHEMA IF NOT EXISTS dbblue", false, 0) != SPI_OK_UTILITY)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: failed to create schema \"dbblue\"")));
		return false;
	}

	if (get_schema_owner("dbblue") != BOOTSTRAP_SUPERUSERID)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("dbblue backup launcher: schema \"dbblue\" in database \"%s\" is not owned by the bootstrap superuser, refusing to use it",
						dbblue_backup_database),
				 errdetail("The schema already existed, created by another role; that role could drop and replace the backup history table, which the launcher writes to as superuser."),
				 errhint("Inspect the schema, then either drop it or make the bootstrap superuser its owner (ALTER SCHEMA dbblue OWNER TO %s).",
						 quote_identifier(backup_role))));
		return false;
	}

	/*
	 * One-time migration from earlier versions, which kept the table in
	 * public.  Moving it keeps the per-database cooldown and the retention
	 * history, so existing dump files are still pruned.  Only a table we can
	 * trust is moved: an ordinary table owned by the bootstrap superuser, and
	 * only while nothing of that name is in dbblue yet.  Anything else under
	 * that name in public is left alone and no longer used.  (Should the
	 * table be swapped between this check and the ALTER, the ownership check
	 * on dbblue.dbblue_backup_history below still refuses the result.)
	 */
	if (lookup_relation("public", "dbblue_backup_history", &relkind, &relowner))
	{
		char		new_relkind;
		Oid			new_relowner;

		if (lookup_relation("dbblue", "dbblue_backup_history",
							&new_relkind, &new_relowner))
		{
			/* already migrated (or created fresh); public's copy is unused */
		}
		else if (relkind == RELKIND_RELATION && relowner == BOOTSTRAP_SUPERUSERID)
		{
			if (SPI_execute("ALTER TABLE public.dbblue_backup_history SET SCHEMA dbblue",
							false, 0) != SPI_OK_UTILITY)
			{
				ereport(WARNING,
						(errmsg("dbblue backup launcher: failed to move public.dbblue_backup_history to schema \"dbblue\"")));
				return false;
			}
			ereport(LOG,
					(errmsg("dbblue backup launcher: moved public.dbblue_backup_history to schema \"dbblue\"")));
		}
		else
			ereport(WARNING,
					(errmsg("dbblue backup launcher: ignoring public.dbblue_backup_history in database \"%s\", which is not an ordinary table owned by the bootstrap superuser",
							dbblue_backup_database),
					 errdetail("Its backup history was not carried over to dbblue.dbblue_backup_history, so dump files it lists will not be pruned automatically.")));
	}

	if (SPI_execute("CREATE TABLE IF NOT EXISTS dbblue.dbblue_backup_history ("
					"  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
					"  database_name text NOT NULL,"
					"  backup_started_at timestamptz NOT NULL,"
					"  backup_finished_at timestamptz,"
					"  status text NOT NULL,"
					"  file_path text,"
					"  file_size_bytes bigint,"
					"  error_message text"
					")",
					false, 0) != SPI_OK_UTILITY)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: failed to create table dbblue.dbblue_backup_history")));
		return false;
	}

	/*
	 * The table, too, must be our own ordinary table before anything is
	 * indexed, granted or written: a view of the same name with an INSTEAD
	 * OF trigger, or a table owned by another role carrying its own
	 * triggers, would run that role's code as the superuser worker.
	 */
	if (!lookup_relation("dbblue", "dbblue_backup_history", &relkind, &relowner) ||
		relkind != RELKIND_RELATION || relowner != BOOTSTRAP_SUPERUSERID)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("dbblue backup launcher: \"dbblue.dbblue_backup_history\" in database \"%s\" is not an ordinary table owned by the bootstrap superuser, refusing to use it",
						dbblue_backup_database),
				 errhint("Drop the conflicting object so the launcher can create its own table.")));
		return false;
	}

	if (SPI_execute("CREATE INDEX IF NOT EXISTS dbblue_backup_history_db_status_idx "
					"ON dbblue.dbblue_backup_history (database_name, status, backup_finished_at)",
					false, 0) != SPI_OK_UTILITY)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: failed to create index on dbblue.dbblue_backup_history")));
		return false;
	}

	create_history_guard();

	/*
	 * Read-only to everyone, writable by no one but the owning worker.
	 * Strip any privilege PUBLIC might already hold (defensive, in case the
	 * table pre-existed), then grant back SELECT alone so a non-superuser
	 * pg_dump can still include it.  INSERT/UPDATE/DELETE/TRUNCATE are not
	 * granted; DROP is owner-only (and, the schema now being the bootstrap
	 * superuser's, schema-owner-only too), so non-owners get read access and
	 * nothing more.
	 *
	 * On the schema, PUBLIC gets USAGE and nothing else -- the same as the
	 * audit log grants -- which a non-superuser pg_dump needs to reach the
	 * table at all, while CREATE stays revoked so no other role can place
	 * objects next to it.
	 */
	if (SPI_execute("REVOKE ALL ON dbblue.dbblue_backup_history FROM PUBLIC",
					false, 0) != SPI_OK_UTILITY)
		ereport(WARNING,
				(errmsg("dbblue backup launcher: failed to revoke public privileges on dbblue.dbblue_backup_history")));

	if (SPI_execute("GRANT SELECT ON dbblue.dbblue_backup_history TO PUBLIC",
					false, 0) != SPI_OK_UTILITY)
		ereport(WARNING,
				(errmsg("dbblue backup launcher: failed to grant select on dbblue.dbblue_backup_history")));

	if (SPI_execute("REVOKE ALL ON SCHEMA dbblue FROM PUBLIC",
					false, 0) != SPI_OK_UTILITY ||
		SPI_execute("GRANT USAGE ON SCHEMA dbblue TO PUBLIC",
					false, 0) != SPI_OK_UTILITY)
		ereport(WARNING,
				(errmsg("dbblue backup launcher: failed to set privileges on schema \"dbblue\"")));

	return true;
}

/*
 * ensure_schema
 *		Create dbblue.dbblue_backup_history if it doesn't already exist,
 *		and verify that both it and its schema belong to the bootstrap
 *		superuser.  Safe to retry on every wakeup until it succeeds.
 *
 * Every statement this worker runs against the history table executes as the
 * bootstrap superuser, so the table must be one no other role controls.  It
 * therefore lives in the dedicated "dbblue" schema rather than in public:
 * since PostgreSQL 15 public is owned by the database owner, who could drop a
 * superuser-owned table there and put their own, with their own triggers, in
 * its place.
 *
 * A refusal is reported as a WARNING and a false return, not an ERROR, so
 * the main loop naps dbblue_backup_naptime before trying again rather than
 * re-entering at once through the error-recovery path.
 */
static bool
ensure_schema(void)
{
	bool		ok;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue backup launcher: ensuring schema");

	/*
	 * While we have catalog access, cache the bootstrap superuser's name for
	 * pg_dump's -U (see backup_role).  Done once; it never changes under us.
	 */
	if (backup_role == NULL)
	{
		char	   *name = GetUserNameFromId(BOOTSTRAP_SUPERUSERID, false);

		backup_role = MemoryContextStrdup(TopMemoryContext, name);
		pfree(name);
	}

	ok = create_history_objects();

	SPI_finish();
	PopActiveSnapshot();

	/* Keep nothing half-done from a refused or failed attempt. */
	if (ok)
		CommitTransactionCommand();
	else
		AbortCurrentTransaction();
	pgstat_report_activity(STATE_IDLE, NULL);

	if (ok)
		ereport(LOG,
				(errmsg("dbblue backup launcher: ready (dbblue.dbblue_backup_history verified in database \"%s\", backing up as role \"%s\")",
						dbblue_backup_database, backup_role)));

	return ok;
}

/*
 * parse_configured_databases
 *		Split dbblue_backup_databases into database names, the same way
 *		dbblue_check_backup_databases() does (case kept as written).  The
 *		returned list and its entries live in launcher_cxt.
 *
 *		A bad value can get here from postgresql.conf, which the check hook
 *		accepts (see dbblue_check_backup_databases()): a list that cannot be
 *		split backs up nothing, and a name that cannot be a folder name is
 *		skipped.  Either is warned about once per value, not every cycle.
 */
static List *
parse_configured_databases(void)
{
	static char *warned_value = NULL;
	char	   *rawstring;
	List	   *elemlist;
	List	   *result = NIL;
	ListCell   *lc;
	bool		warn;

	if (dbblue_backup_databases == NULL || dbblue_backup_databases[0] == '\0')
		return NIL;

	warn = (warned_value == NULL ||
			strcmp(warned_value, dbblue_backup_databases) != 0);
	if (warn)
	{
		if (warned_value != NULL)
			pfree(warned_value);
		warned_value = MemoryContextStrdup(TopMemoryContext,
										   dbblue_backup_databases);
	}

	rawstring = pstrdup(dbblue_backup_databases);
	if (!SplitGUCList(rawstring, ',', &elemlist))
	{
		if (warn)
			ereport(WARNING,
					(errmsg("dbblue backup launcher: dbblue_backup_databases \"%s\" is not a valid list, not backing up any database",
							dbblue_backup_databases),
					 errhint("List database names separated by commas, with double quotes around a name that contains a comma or a space.")));
		pfree(rawstring);
		list_free(elemlist);
		return NIL;
	}

	foreach(lc, elemlist)
	{
		char	   *item = (char *) lfirst(lc);
		BackupDbSpec *spec;
		ListCell   *lc2;
		bool		duplicate = false;
		const char *problem;

		problem = database_list_problem(NULL, item);
		if (problem != NULL)
		{
			if (warn)
				ereport(WARNING,
						(errmsg("dbblue backup launcher: skipping \"%s\" in dbblue_backup_databases",
								item),
						 errdetail("%s", problem),
						 errhint("A database named \".\" or \"..\", or whose name contains \"/\", cannot be listed.")));
			continue;
		}

		/*
		 * A repeated entry (e.g. a copy-paste mistake in the GUC) would
		 * otherwise turn into two candidates for the same database sharing
		 * one stale last_backup_at snapshot, letting the second back it up
		 * again immediately after the first commits, bypassing the
		 * cooldown.
		 */
		foreach(lc2, result)
		{
			BackupDbSpec *seen = (BackupDbSpec *) lfirst(lc2);

			if (strcmp(seen->dbname, item) == 0)
			{
				duplicate = true;
				break;
			}
		}
		if (duplicate)
			continue;

		spec = (BackupDbSpec *) palloc(sizeof(BackupDbSpec));
		spec->dbname = pstrdup(item);
		result = lappend(result, spec);
	}

	pfree(rawstring);
	list_free(elemlist);
	return result;
}

/*
 * load_backup_candidates
 *		One read-only SPI pass resolving every configured database name
 *		against pg_database and its last successful backup time.  The
 *		returned list and its entries live in launcher_cxt.
 */
static List *
load_backup_candidates(List *specs)
{
	List	   *candidates = NIL;
	int			n = list_length(specs);
	Datum	   *dbnames;
	ArrayType  *arr;
	Oid			argtypes[1] = {TEXTARRAYOID};
	Datum		values[1];
	int			i;
	int			ret;
	uint64		row;
	ListCell   *lc;

	if (n == 0)
		return NIL;

	dbnames = (Datum *) palloc(n * sizeof(Datum));
	i = 0;
	foreach(lc, specs)
	{
		BackupDbSpec *spec = (BackupDbSpec *) lfirst(lc);

		dbnames[i++] = CStringGetTextDatum(spec->dbname);
	}
	arr = construct_array_builtin(dbnames, n, TEXTOID);
	values[0] = PointerGetDatum(arr);

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue backup launcher: checking configured databases");

	ret = SPI_execute_with_args(
							   "WITH wanted AS ("
							   "  SELECT s AS dbname FROM pg_catalog.unnest($1::text[]) AS s"
							   ")"
							   "SELECT w.dbname, d.oid, d.datallowconn, d.datistemplate,"
							   "       (SELECT max(h.backup_started_at)"
							   "        FROM dbblue.dbblue_backup_history h"
							   "        WHERE h.database_name = w.dbname"
							   "          AND h.status = 'success') AS last_backup_at,"
							   "       (SELECT h.file_size_bytes"
							   "        FROM dbblue.dbblue_backup_history h"
							   "        WHERE h.database_name = w.dbname"
							   "          AND h.status = 'success'"
							   "        ORDER BY h.backup_started_at DESC LIMIT 1) AS last_backup_size,"
							   "       (SELECT h.file_path"
							   "        FROM dbblue.dbblue_backup_history h"
							   "        WHERE h.database_name = w.dbname"
							   "          AND h.status = 'success'"
							   "        ORDER BY h.backup_started_at DESC LIMIT 1) AS last_backup_path"
							   " FROM wanted w"
							   " LEFT JOIN pg_database d ON d.datname = w.dbname",
							   1, argtypes, values, NULL, false, 0);

	if (ret != SPI_OK_SELECT)
		ereport(WARNING,
				(errmsg("dbblue backup launcher: configured-database check query failed (SPI result %d)",
						ret)));
	else
	{
		for (row = 0; row < SPI_processed; row++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[row];
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			bool		isnull;
			Datum		d;
			BackupCandidate *cand;
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(launcher_cxt);

			cand = (BackupCandidate *) palloc0(sizeof(BackupCandidate));

			d = SPI_getbinval(tuple, tupdesc, 1, &isnull);
			cand->dbname = isnull ? NULL : TextDatumGetCString(d);

			if (cand->dbname == NULL)
			{
				MemoryContextSwitchTo(oldcxt);
				continue;
			}

			d = SPI_getbinval(tuple, tupdesc, 2, &isnull);
			cand->dboid = isnull ? InvalidOid : DatumGetObjectId(d);

			d = SPI_getbinval(tuple, tupdesc, 3, &isnull);
			cand->allowconn = isnull ? false : DatumGetBool(d);

			d = SPI_getbinval(tuple, tupdesc, 4, &isnull);
			cand->istemplate = isnull ? false : DatumGetBool(d);

			d = SPI_getbinval(tuple, tupdesc, 5, &isnull);
			cand->last_backup_at = isnull ? 0 : DatumGetTimestampTz(d);

			d = SPI_getbinval(tuple, tupdesc, 6, &isnull);
			cand->last_backup_size = isnull ? 0 : DatumGetInt64(d);

			d = SPI_getbinval(tuple, tupdesc, 7, &isnull);
			cand->last_backup_path = isnull ? NULL : TextDatumGetCString(d);

			candidates = lappend(candidates, cand);
			MemoryContextSwitchTo(oldcxt);
		}
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	return candidates;
}

/*
 * run_backup_cycle
 *		One full pass: resolve the current state of every configured
 *		database, then back up whichever ones exist, accept connections,
 *		are not templates, and are past their cooldown.
 *
 * Returns when the earliest of the databases it skipped as not yet due will
 * be due (0 if none), so that the caller can wake up then.  Its memory lives
 * in launcher_cxt, which the caller resets afterwards.
 */
static TimestampTz
run_backup_cycle(void)
{
	List	   *specs = parse_configured_databases();
	List	   *candidates;
	ListCell   *lc;
	TimestampTz next_due = 0;

	if (specs == NIL)
		return 0;

	if (!backup_directory_still_usable())
		return 0;

	candidates = load_backup_candidates(specs);

	foreach(lc, candidates)
	{
		BackupCandidate *cand = (BackupCandidate *) lfirst(lc);
		TimestampTz now;

		CHECK_FOR_INTERRUPTS();

		if (ShutdownRequestPending)
			break;

		if (ConfigReloadPending)
		{
			reload_configuration();

			/*
			 * If the feature was just switched off, stop rather than start
			 * backing up a database under a configuration that no longer
			 * wants this running at all.  The remaining databases in this
			 * cycle's candidate list are simply picked up again next time
			 * the feature is enabled.
			 */
			if (!dbblue_backup_enabled)
				break;
		}

		if (!OidIsValid(cand->dboid))
		{
			ereport(WARNING,
					(errmsg("dbblue backup launcher: configured database \"%s\" does not exist, skipping",
							cand->dbname)));
			continue;
		}

		/*
		 * Retention every cycle, not only right after a backup: a lowered
		 * dbblue_backup_retention_count then takes effect at the next cycle,
		 * not at the database's next successful backup (which, for a failing
		 * database, may never come), and failed attempts it has since
		 * recovered from are cleared.  Only for databases that still exist:
		 * the last backups of a dropped one may be the only copy left, and
		 * are not this worker's to delete.
		 */
		apply_retention(cand->dbname);

		if (!cand->allowconn || cand->istemplate)
		{
			ereport(WARNING,
					(errmsg("dbblue backup launcher: configured database \"%s\" does not accept connections or is a template, skipping",
							cand->dbname)));
			continue;
		}

		now = GetCurrentTimestamp();
		if (cand->last_backup_at != 0 &&
			now < cand->last_backup_at +
			(int64) dbblue_backup_interval * USECS_PER_SEC)
		{
			TimestampTz due = cand->last_backup_at +
				(int64) dbblue_backup_interval * USECS_PER_SEC;

			if (next_due == 0 || due < next_due)
				next_due = due;
			continue;
		}

		/* Failing lately?  Then wait out its retry delay (see BackupRetryState). */
		if (retry_state != NULL)
		{
			BackupRetryState *rs = hash_search(retry_state, cand->dbname,
											   HASH_FIND, NULL);

			if (rs != NULL &&
				now < rs->last_failure_at +
				retry_delay_secs(rs->failures) * USECS_PER_SEC)
			{
				TimestampTz due = rs->last_failure_at +
					retry_delay_secs(rs->failures) * USECS_PER_SEC;

				if (next_due == 0 || due < next_due)
					next_due = due;
				ereport(DEBUG1,
						(errmsg_internal("dbblue backup launcher: database \"%s\" is waiting out its retry delay after %d failed attempts",
										 cand->dbname, rs->failures)));
				continue;
			}
		}

		/*
		 * The last dump gone is how a backup disk that has been unmounted
		 * shows up: its mount point is then an empty folder on another disk,
		 * which this backup would silently be written to.  Warn only -- old
		 * dumps deleted by hand must not stop backups -- and only just before
		 * backing up, so it is said once per attempt, not every cycle.
		 */
		if (cand->last_backup_path != NULL)
		{
			struct stat st;

			if (stat(cand->last_backup_path, &st) != 0 && errno == ENOENT)
				ereport(WARNING,
						(errmsg("dbblue backup launcher: the last backup of database \"%s\", \"%s\", is missing",
								cand->dbname, cand->last_backup_path),
						 errhint("If the backup disk is not mounted, this backup is being written to another disk.")));
		}

		note_backup_result(cand->dbname,
						   backup_one_database(cand->dbname,
											   cand->last_backup_size),
						   cand->last_backup_at);
	}

	return next_due;
}

/*
 * warn_if_backup_database_listed
 *		Warn when dbblue_backup_database, the database this worker stays
 *		connected to, is also one of the databases it backs up.
 *
 * Such a database is typically an application database (an Odoo database,
 * say), and the worker's connection, which it never closes, stops anyone
 * from dropping, renaming or copying it: those need every other session
 * gone, and a non-superuser such as Odoo's role cannot terminate the
 * worker's superuser session.  'postgres' (the default) avoids that.
 */
static void
warn_if_backup_database_listed(void)
{
	List	   *specs = parse_configured_databases();
	ListCell   *lc;

	foreach(lc, specs)
	{
		BackupDbSpec *spec = (BackupDbSpec *) lfirst(lc);

		if (strcmp(spec->dbname, dbblue_backup_database) == 0)
		{
			ereport(WARNING,
					(errmsg("dbblue backup launcher: dbblue_backup_database \"%s\" is also one of the databases being backed up",
							dbblue_backup_database),
					 errdetail("The backup launcher stays connected to it, so dropping, renaming or copying it (Odoo's Drop, Rename and Duplicate) fails with \"being accessed by other users\"."),
					 errhint("Set dbblue_backup_database to 'postgres' (changing it requires a server restart).")));
			break;
		}
	}
	list_free_deep(specs);
}

/*
 * reload_configuration
 *		Process a pending SIGHUP, and end every failing database's current
 *		retry wait so that it is tried again at the next cycle (see
 *		BackupRetryState).  The failure counts are kept.
 */
static void
reload_configuration(void)
{
	ConfigReloadPending = false;
	ProcessConfigFile(PGC_SIGHUP);

	/* dbblue_backup_databases may have changed. */
	if (dbblue_backup_enabled && launcher_connected)
		warn_if_backup_database_listed();

	if (retry_state != NULL)
	{
		HASH_SEQ_STATUS status;
		BackupRetryState *rs;
		long		n = 0;

		hash_seq_init(&status, retry_state);
		while ((rs = hash_seq_search(&status)) != NULL)
		{
			rs->last_failure_at = 0;
			n++;
		}

		if (n > 0)
			ereport(LOG,
					(errmsg_plural("dbblue backup launcher: configuration reloaded, retrying %ld failing database at the next cycle",
								   "dbblue backup launcher: configuration reloaded, retrying %ld failing databases at the next cycle",
								   n, n)));
	}
}

/*
 * retry_delay_secs
 *		Seconds to wait after the failures-th consecutive failed backup of a
 *		database: dbblue_backup_naptime doubled per failure after the first,
 *		capped at dbblue_backup_interval.
 */
static int64
retry_delay_secs(int failures)
{
	int64		delay;

	/* naptime <= INT_MAX / 1000, so a shift of up to 30 cannot overflow */
	delay = (int64) dbblue_backup_naptime << Min(Max(failures - 1, 0), 30);

	return Min(delay, (int64) dbblue_backup_interval);
}

/*
 * note_backup_result
 *		Update dbname's retry state after a backup attempt, and warn once its
 *		backups keep timing out.  last_backup_at is the start of its last
 *		successful backup (0 if none), for that warning.
 */
static void
note_backup_result(const char *dbname, bool ok, TimestampTz last_backup_at)
{
	BackupRetryState *rs;
	bool		found;

	if (ok)
	{
		if (retry_state == NULL)
			return;

		rs = hash_search(retry_state, dbname, HASH_FIND, NULL);
		if (rs == NULL)
			return;

		ereport(LOG,
				(errmsg_plural("dbblue backup launcher: backed up database \"%s\" after %d failed attempt",
							   "dbblue backup launcher: backed up database \"%s\" after %d failed attempts",
							   rs->failures, dbname, rs->failures)));
		(void) hash_search(retry_state, dbname, HASH_REMOVE, NULL);
		return;
	}

	if (retry_state == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = NAMEDATALEN;
		ctl.entrysize = sizeof(BackupRetryState);
		ctl.hcxt = TopMemoryContext;
		retry_state = hash_create("dbblue backup retry state", 16, &ctl,
								  HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	}

	rs = hash_search(retry_state, dbname, HASH_ENTER, &found);
	if (!found)
	{
		rs->failures = 0;
		rs->timeouts = 0;
	}
	if (rs->failures < INT_MAX)
		rs->failures++;
	rs->last_failure_at = GetCurrentTimestamp();

	/* Timeouts in a row; any other failure ends the run. */
	if (!last_attempt_timed_out)
		rs->timeouts = 0;
	else if (rs->timeouts < INT_MAX)
		rs->timeouts++;

	if (rs->timeouts >= DBBLUE_BACKUP_TIMEOUT_WARN_AFTER)
		ereport(WARNING,
				(errmsg("dbblue backup launcher: backups of database \"%s\" have timed out %d times in a row; %s",
						dbname, rs->timeouts,
						last_backup_at != 0 ?
						psprintf("its newest backup is from %s", timestamptz_to_str(last_backup_at)) :
						"it has never been backed up"),
				 errdetail("Each attempt was stopped after dbblue_backup_timeout (%d s). The database has probably grown too large to dump within that time.",
						   dbblue_backup_timeout),
				 errhint("Increase dbblue_backup_timeout, or set it to 0 for no limit.")));

	ereport(LOG,
			(errmsg_plural("dbblue backup launcher: backup of database \"%s\" has failed %d time in a row, next attempt in %lld s",
						   "dbblue backup launcher: backup of database \"%s\" has failed %d times in a row, next attempt in %lld s",
						   rs->failures, dbname, rs->failures,
						   (long long) retry_delay_secs(rs->failures)),
			 errhint("Once the cause is fixed, a configuration reload (SELECT pg_reload_conf()) retries it at the next cycle.")));
}

/*
 * run_backup_command
 *		Run one backup shell command as a child process, staying responsive to
 *		a shutdown request while it runs.
 *
 * Returns the child's wait status in the same encoding system() uses -- and
 * that wait_result_to_str() decodes -- so the caller handles the result exactly
 * as it handled system()'s return: 0 means a clean exit, anything else a
 * failure.  The difference from system() is that this does not block the worker
 * for the whole (possibly hour-long) dump: it polls the child once a second and
 * wakes immediately on its latch, so a SIGTERM (fast shutdown) is acted on at
 * once -- the child's whole process group is signalled and reaped, and the
 * non-zero status makes the backup record as failed rather than succeed.
 *
 * The same once-a-second poll checks the free space on the file system holding
 * space_path, and stops the dump the same way if it falls below
 * dbblue_backup_min_free_space.  A cancel request (pg_cancel_backend() on this
 * worker) stops it the same way too, instead of raising an ERROR here.  When
 * the dump is stopped for either reason, *stop_reason says why; otherwise it
 * is NULL.
 *
 * pg_dump never outlives this function: if an ERROR escapes the poll loop
 * anyway, the process group is killed and reaped before it is re-thrown.
 */
static int
run_backup_command(const char *cmd, const char *space_path,
				   const char **stop_reason)
{
	pid_t		pid;
	int			status = -1;
	sigset_t	save_mask;

	*stop_reason = NULL;

	/* A shutdown already asked for: don't start a dump only to kill it. */
	if (ShutdownRequestPending)
		return -1;

	/* Flush our own buffers before forking, as system() does. */
	fflush(NULL);

	/*
	 * Block signals across fork(), so that none is handled in the child by
	 * this worker's own handlers (which only set flags in our memory, so a
	 * SIGTERM sent to the new process group would be swallowed and pg_dump
	 * would start regardless).  The child resets them before exec.
	 */
	sigprocmask(SIG_SETMASK, &BlockSig, &save_mask);
	pid = fork();
	if (pid < 0)
	{
		sigprocmask(SIG_SETMASK, &save_mask, NULL);
		ereport(WARNING,
				(errmsg("dbblue backup launcher: could not fork to run pg_dump: %m")));
		return -1;
	}
	if (pid == 0)
	{
		/*
		 * Child: start a new process group so the whole pipeline (the shell
		 * and the pg_dump beneath it) can be signalled together, put the
		 * signals back to their defaults -- a signal already sent to the group
		 * then acts on it as soon as they are unblocked -- and run exactly the
		 * command the caller built, via the shell as system() would.
		 */
		setpgid(0, 0);
		signal(SIGTERM, SIG_DFL);
		signal(SIGINT, SIG_DFL);
		signal(SIGQUIT, SIG_DFL);
		signal(SIGHUP, SIG_DFL);
		signal(SIGUSR1, SIG_DFL);
		signal(SIGUSR2, SIG_DFL);
		sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);
		execl("/bin/sh", "sh", "-c", cmd, (char *) NULL);
		_exit(127);				/* only reached if exec itself failed */
	}

	/* Parent: set the group too, race-free (harmless if the child already did). */
	setpgid(pid, pid);
	dump_pgid = pid;			/* for backup_launcher_quickdie() */
	sigprocmask(SIG_SETMASK, &save_mask, NULL);

	/*
	 * A cancel must not raise an ERROR in the loop below: that would leave
	 * pg_dump running with nobody watching it.  Hold it, and handle it in the
	 * loop as "stop this dump".
	 */
	HOLD_CANCEL_INTERRUPTS();

	PG_TRY();
	{
		/* A monotonic clock: a change of the system time must not end a dump. */
		double		start = monotonic_seconds();
		bool		term_sent = false;	/* SIGTERM (timeout/shutdown) sent? */
		double		term_at = 0;		/* when, so SIGKILL can follow after grace */

		for (;;)
		{
			pid_t		w = waitpid(pid, &status, WNOHANG);

			if (w == pid)
				break;			/* finished; 'status' holds the wait result */
			if (w < 0 && errno != EINTR)
			{
				ereport(WARNING,
						(errmsg("dbblue backup launcher: waitpid for pg_dump failed: %m")));
				/* Can't watch it any more: don't leave it running. */
				kill(-pid, SIGKILL);
				status = -1;
				break;
			}

			/* Still running: nap on the latch so a signal wakes us at once. */
			{
				int			wl = WaitLatch(MyLatch,
										   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
										   1000L,
										   WAIT_EVENT_DBBLUE_BACKUP_LAUNCHER_MAIN);

				ResetLatch(MyLatch);

				/* Postmaster gone: kill the dump's group hard and exit. */
				if (wl & WL_POSTMASTER_DEATH)
				{
					kill(-pid, SIGKILL);
					(void) waitpid(pid, &status, 0);
					proc_exit(1);
				}
			}

			CHECK_FOR_INTERRUPTS();

			/* Cancel requested: stop the dump, like a timeout. */
			if (QueryCancelPending)
			{
				QueryCancelPending = false;
				if (!term_sent)
				{
					ereport(WARNING,
							(errmsg("dbblue backup launcher: backup cancelled by request, stopping pg_dump")));
					kill(-pid, SIGTERM);
					term_sent = true;
					term_at = monotonic_seconds();
					*stop_reason = "stopped: cancelled by pg_cancel_backend()";
					continue;
				}
			}

			/*
			 * Fast shutdown requested: stop the in-flight dump promptly instead
			 * of blocking until it finishes.  Signal the whole process group so
			 * pg_dump (not just the shell) is terminated.
			 */
			if (ShutdownRequestPending && !term_sent)
			{
				kill(-pid, SIGTERM);
				term_sent = true;
				term_at = monotonic_seconds();
				continue;
			}

			/*
			 * Timeout enforcement (formerly the external "timeout" wrapper):
			 * after dbblue_backup_timeout seconds send SIGTERM to the group,
			 * and if it has not died within the grace period, SIGKILL it.  A
			 * timeout of 0 disables the limit.
			 */
			if (!term_sent && dbblue_backup_timeout > 0 &&
				monotonic_seconds() - start >= dbblue_backup_timeout)
			{
				ereport(WARNING,
						(errmsg("dbblue backup launcher: pg_dump exceeded dbblue_backup_timeout (%d s), terminating",
								dbblue_backup_timeout)));
				kill(-pid, SIGTERM);
				term_sent = true;
				term_at = monotonic_seconds();
				*stop_reason = timed_out_reason;
				continue;
			}

			/*
			 * Free-space guard: stop the dump before it fills the file system.
			 * The check before the dump started used an estimate; this one
			 * also catches a dump larger than its last one, and other writers
			 * on the same file system -- the database itself among them, when
			 * it is shared.  It runs once a second, so the margin must exceed
			 * what pg_dump (and everything else) writes in a second, which
			 * dbblue_backup_min_free_space's default of 2 GB does by far.
			 */
			if (!term_sent && min_free_space_bytes() > 0)
			{
				int64		avail = get_free_space(space_path);

				if (avail >= 0 && avail < min_free_space_bytes())
				{
					ereport(WARNING,
							(errmsg("dbblue backup launcher: free space on the file system of \"%s\" fell to %lld bytes, below dbblue_backup_min_free_space (%d MB), stopping pg_dump",
									space_path, (long long) avail,
									dbblue_backup_min_free_space)));
					kill(-pid, SIGTERM);
					term_sent = true;
					term_at = monotonic_seconds();
					*stop_reason = "stopped: free disk space fell below dbblue_backup_min_free_space";
					continue;
				}
			}

			/* Escalate to SIGKILL if the group ignored SIGTERM past the grace. */
			if (term_sent && monotonic_seconds() - term_at >= DBBLUE_BACKUP_KILL_GRACE_S)
				kill(-pid, SIGKILL);
		}
	}
	PG_CATCH();
	{
		/* Never leave pg_dump running unwatched: kill and reap it first. */
		kill(-pid, SIGKILL);
		(void) waitpid(pid, NULL, 0);
		dump_pgid = 0;
		PG_RE_THROW();
	}
	PG_END_TRY();

	dump_pgid = 0;
	RESUME_CANCEL_INTERRUPTS();

	/*
	 * The poll loop above called ResetLatch, which consumed the latch the
	 * shutdown signal set.  Re-arm it so the caller's own WaitLatch returns at
	 * once and the worker exits promptly, rather than napping out the backup
	 * interval before it notices the pending shutdown.
	 */
	if (ShutdownRequestPending)
		SetLatch(MyLatch);

	return status;
}

/*
 * read_dump_errors
 *		What the last pg_dump wrote to its error output, or NULL if nothing,
 *		cut to DUMP_ERRORS_KEEP bytes.
 *
 * pg_dump's own error is the last line starting "pg_dump: error:", after any
 * warnings, but it can be followed by a detail line far longer than what is
 * kept: on a lock timeout, "Query was: LOCK TABLE" with every table, several
 * kB on an Odoo database.  Keeping just the end of the output then kept a
 * piece of that table list, or nothing, and lost the error itself.  So keep
 * the text from that error line on; only output with no such line (e.g. the
 * shell's own "not found") falls back to its last DUMP_ERRORS_KEEP bytes.
 */
#define DUMP_ERRORS_SCAN	(64 * 1024)
#define DUMP_ERRORS_KEEP	1024

static char *
read_dump_errors(void)
{
	char	   *buf;
	char	   *text = NULL;
	char	   *p;
	int			fd;
	off_t		size;
	off_t		start;
	ssize_t		len;
	size_t		textlen;

	fd = open(DBBLUE_BACKUP_STDERR_FILE, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		return NULL;

	buf = palloc(DUMP_ERRORS_SCAN + 1);
	size = lseek(fd, 0, SEEK_END);
	start = size > DUMP_ERRORS_SCAN ? size - DUMP_ERRORS_SCAN : 0;
	len = size > 0 ? pg_pread(fd, buf, size - start, start) : 0;
	close(fd);
	if (len <= 0)
	{
		pfree(buf);
		return NULL;
	}
	buf[len] = '\0';

	/* The last "pg_dump: error:" that begins a line. */
	for (p = buf; (p = strstr(p, "pg_dump: error:")) != NULL; p++)
	{
		if (p == buf ? start == 0 : p[-1] == '\n')
			text = p;
	}

	if (text == NULL)
	{
		/* The last DUMP_ERRORS_KEEP bytes, from a line start if one follows. */
		text = buf + (len > DUMP_ERRORS_KEEP ? len - DUMP_ERRORS_KEEP : 0);
		if (text > buf || start > 0)
		{
			char	   *nl = strchr(text, '\n');

			if (nl != NULL && nl[1] != '\0')
				text = nl + 1;
		}
	}

	/* Cut to size, marking the cut, and drop the trailing newline(s). */
	textlen = strlen(text);
	if (textlen > DUMP_ERRORS_KEEP)
	{
		textlen = DUMP_ERRORS_KEEP;
		memcpy(text + textlen - 3, "...", 3);
		text[textlen] = '\0';
	}
	while (textlen > 0 && (text[textlen - 1] == '\n' || text[textlen - 1] == '\r'))
		text[--textlen] = '\0';

	if (text[0] == '\0')
	{
		pfree(buf);
		return NULL;
	}

	/*
	 * It goes into a text column, so it must be valid in the database's
	 * encoding: pg_dump writes in the OS locale's, and the text can start or
	 * be cut in the middle of a multibyte character.  Replace each byte that
	 * does not begin a valid character with '?'.
	 */
	{
		char	   *out = pstrdup(text);
		int			enc = GetDatabaseEncoding();
		int			left = strlen(out);
		char	   *q = out;

		pfree(buf);
		while (left > 0)
		{
			int			l = pg_encoding_verifymbchar(enc, q, left);

			if (l <= 0)
			{
				*q = '?';
				l = 1;
			}
			q += l;
			left -= l;
		}
		return out;
	}
}

/*
 * remove_partial_dumps
 *		Delete the <dbname>_*.dump.partial files left in dbdir by dumps that
 *		never finished.
 *
 * A dump that fails while this worker is alive cleans up after itself; this
 * catches the ones that could not, because the worker itself was killed
 * mid-dump: an immediate shutdown, a crash-restart after any backend crashed,
 * kill -9, the OOM killer, postmaster death, or power loss.  Such a file has
 * no history row, so retention would never remove it.
 *
 * Called just before dbname is dumped again.  Only this worker writes these
 * files, and it dumps one database at a time, so none of them can belong to
 * a dump still in progress.
 */
static void
remove_partial_dumps(const char *dbdir, const char *dbname)
{
	static const char suffix[] = ".dump" DBBLUE_BACKUP_PARTIAL_SUFFIX;
	size_t		namelen = strlen(dbname);
	size_t		suffixlen = strlen(suffix);
	DIR		   *dir;
	struct dirent *de;

	dir = AllocateDir(dbdir);
	while ((de = ReadDirExtended(dir, dbdir, WARNING)) != NULL)
	{
		size_t		len = strlen(de->d_name);
		char		path[MAXPGPATH];

		/* Only <dbname>_<something>.dump.partial, as backup_one_database names it */
		if (len <= namelen + 1 + suffixlen ||
			strncmp(de->d_name, dbname, namelen) != 0 ||
			de->d_name[namelen] != '_' ||
			strcmp(de->d_name + len - suffixlen, suffix) != 0)
			continue;

		if (snprintf(path, sizeof(path), "%s/%s", dbdir, de->d_name) >= MAXPGPATH)
			continue;

		if (unlink(path) == 0)
			ereport(LOG,
					(errmsg("dbblue backup launcher: removed \"%s\", left by a backup of database \"%s\" that did not finish",
							path, dbname)));
		else if (errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("dbblue backup launcher: could not remove unfinished backup \"%s\": %m",
							path)));
	}
	FreeDir(dir);
}

/*
 * backup_one_database
 *		pg_dump (custom format) one database to its own subdirectory
 *		under dbblue_backup_directory, record the outcome in
 *		dbblue_backup_history, and prune old backups on success.  A
 *		failure here is caught and logged; it does not abort the rest of
 *		the cycle or the worker.
 *
 * The dump is written to <outfile>.partial and renamed to outfile only after
 * pg_dump has exited successfully and the file has been fsync'd, so that a
 * file ending in .dump is always a complete, durable backup, and a "success"
 * row is never recorded for one that is not.
 *
 * last_backup_size, the size of the database's last successful dump (0 if
 * none), estimates the size of this one for the free-space check before it
 * starts (see dbblue_backup_min_free_space).
 */
static bool
backup_one_database(const char *dbname, int64 last_backup_size)
{
	char		dbdir[MAXPGPATH];
	char		outfile[MAXPGPATH];
	char		partfile[MAXPGPATH];
	char	   *dump_errors;
	char		timebuf[32];
	time_t		now_t;
	struct tm	lt;
	TimestampTz started_at = GetCurrentTimestamp();
	StringInfoData cmd;
	int			rc;
	bool		ok = true;
	const char *stop_reason;

	last_attempt_timed_out = false;

	if (dbblue_backup_directory == NULL || dbblue_backup_directory[0] == '\0')
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: dbblue_backup_directory is not set, skipping backup of \"%s\"",
						dbname)));
		record_backup_history(dbname, started_at, "failed", NULL, 0,
							  "dbblue_backup_directory is not set");
		apply_retention(dbname);
		return false;
	}

	now_t = time(NULL);
	(void) localtime_r(&now_t, &lt);
	strftime(timebuf, sizeof(timebuf), "%Y%m%d_%H%M%S", &lt);

	/*
	 * Every path this backup uses, checked for length before anything is
	 * created on disk -- a truncated path would name some other folder.
	 */
	if (snprintf(dbdir, sizeof(dbdir), "%s/%s",
				 dbblue_backup_directory, dbname) >= MAXPGPATH ||
		snprintf(outfile, sizeof(outfile), "%s/%s_%s.dump",
				 dbdir, dbname, timebuf) >= MAXPGPATH ||
		snprintf(partfile, sizeof(partfile), "%s%s",
				 outfile, DBBLUE_BACKUP_PARTIAL_SUFFIX) >= MAXPGPATH)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: backup file path for database \"%s\" is too long, skipping",
						dbname)));
		record_backup_history(dbname, started_at, "failed", NULL, 0,
							  "backup file path is too long");
		apply_retention(dbname);
		return false;
	}

	/*
	 * The database's own directory must not be inside the data directory
	 * either.  check_backup_directory() has ruled that out for the backup
	 * directory, but with the data directory inside the backup directory, a
	 * database named after the next component of its path would be: with
	 * dbblue_backup_directory = '/srv' and the data directory /srv/pgdata, a
	 * database named "pgdata" would be dumped into the data directory.  An
	 * existing <dbname> directory may also be a symlink pointing into it.
	 */
	{
		char		dbdir_real[MAXPGPATH];
		struct stat st;

		if (lstat(dbdir, &st) != 0 || !get_real_path(dbdir, dbdir_real))
			snprintf(dbdir_real, sizeof(dbdir_real), "%s/%s",
					 backup_dir_real, dbname);

		if (path_is_within(dbdir_real, data_dir_real))
		{
			ereport(WARNING,
					(errmsg("dbblue backup launcher: backup directory \"%s\" of database \"%s\" is the data directory or inside it, skipping",
							dbdir, dbname),
					 errhint("Set dbblue_backup_directory to a directory that does not contain the data directory \"%s\".",
							 data_dir_real)));
			record_backup_history(dbname, started_at, "failed", NULL, 0,
								  "backup directory is inside the data directory");
			apply_retention(dbname);
			return false;
		}
	}

	/*
	 * Only the database's own folder: the backup directory itself must
	 * already exist (see backup_directory_still_usable()), and re-creating it
	 * on the root disk while a backup disk is unmounted would hide that.
	 */
	if (mkdir(dbdir, DBBLUE_BACKUP_DIR_MODE) != 0 && errno != EEXIST)
	{
		int			save_errno = errno;

		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue backup launcher: could not create directory \"%s\": %s",
						dbdir, strerror(save_errno))));
		record_backup_history(dbname, started_at, "failed", NULL, 0,
							  psprintf("could not create backup directory \"%s\": %s",
									   dbdir, strerror(save_errno)));
		apply_retention(dbname);
		return false;
	}

	/*
	 * The database's own folder must be ours alone as well, and a real
	 * folder: someone able to write in the backup directory before it was
	 * secured could have created it, or put a symlink in its place.
	 */
	{
		const char *reason = folder_unsafe_reason(dbdir, false);

		if (reason != NULL)
		{
			ereport(WARNING,
					(errmsg("dbblue backup launcher: backup directory \"%s\" of database \"%s\" %s, skipping",
							dbdir, dbname, reason),
					 errhint("Make it a directory owned by the operating-system user the server runs as, and not writable by group or others (chmod go-w).")));
			record_backup_history(dbname, started_at, "failed", NULL, 0,
								  psprintf("backup directory \"%s\" %s", dbdir, reason));
			/* No apply_retention(): it refuses this folder too, and says so. */
			return false;
		}
	}

	remove_partial_dumps(dbdir, dbname);

	/*
	 * Don't start a dump that would not fit: it needs about as much space as
	 * its last one, and dbblue_backup_min_free_space must be left over.
	 * (Older dumps are pruned only after this one succeeds, so they cannot
	 * make room for it.)  Done after remove_partial_dumps(), which may have
	 * just freed space.  The estimate can be low; run_backup_command() keeps
	 * checking while the dump runs.  If free space cannot be read at all,
	 * the dump goes ahead: better a backup than none.
	 */
	if (min_free_space_bytes() > 0)
	{
		int64		avail = get_free_space(dbdir);
		int64		needed = min_free_space_bytes() + last_backup_size;

		if (avail < 0)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("dbblue backup launcher: could not determine the free space on the file system of \"%s\": %m",
							dbdir)));
		else if (avail < needed)
		{
			ereport(WARNING,
					(errmsg("dbblue backup launcher: not enough free disk space to back up database \"%s\": %lld bytes free on the file system of \"%s\", %lld needed",
							dbname, (long long) avail, dbdir, (long long) needed),
					 last_backup_size > 0 ?
					 errdetail("That is the size of its last backup, %lld bytes, plus dbblue_backup_min_free_space (%d MB).",
							   (long long) last_backup_size,
							   dbblue_backup_min_free_space) :
					 errdetail("That is dbblue_backup_min_free_space (%d MB); the database has no earlier backup to estimate this one's size from.",
							   dbblue_backup_min_free_space),
					 errhint("Free some space on that file system, or lower dbblue_backup_min_free_space.")));
			record_backup_history(dbname, started_at, "failed", NULL, 0,
								  psprintf("not enough free disk space: %lld bytes free, %lld needed",
										   (long long) avail, (long long) needed));
			apply_retention(dbname);
			return false;
		}
	}

	initStringInfo(&cmd);

	/*
	 * The dbblue_backup_timeout is enforced by run_backup_command() below, not
	 * by wrapping the command in the external "timeout" binary: that wrapper
	 * put pg_dump in a process group of its own, so a kill of the shell's group
	 * on shutdown left pg_dump orphaned.  Running pg_dump directly under the
	 * shell keeps it in the child's own group, where run_backup_command() can
	 * signal it for both the timeout and a fast-shutdown.
	 */
	ok &= append_shell_quoted(&cmd, pg_dump_path);
	appendStringInfoString(&cmd, " -Fc --no-password --lock-wait-timeout="
						   DBBLUE_BACKUP_LOCK_WAIT_TIMEOUT);

	/*
	 * Connect as the bootstrap superuser rather than letting pg_dump default
	 * to the OS user name, which need not be a valid or privileged role.
	 */
	if (backup_role != NULL)
	{
		appendStringInfoString(&cmd, " -U ");
		ok &= append_shell_quoted(&cmd, backup_role);
	}

	if (socket_dir != NULL)
	{
		appendStringInfoString(&cmd, " -h ");
		ok &= append_shell_quoted(&cmd, socket_dir);
	}

	appendStringInfo(&cmd, " -p %d -f ", PostPortNumber);
	ok &= append_shell_quoted(&cmd, partfile);
	/*
	 * The database, as the connection string dbname='<name>' rather than the
	 * bare name: pg_dump hands its dbname argument to libpq, which reads any
	 * value containing '=' (or starting with postgresql://) as a connection
	 * string.  A database named "dbname=other" would otherwise be dumped as
	 * database "other" -- recorded as a successful backup of the wrong
	 * database -- and one named "host=..." would send this superuser
	 * connection elsewhere.  Quoted, the name is always taken literally.
	 *
	 * "--" still ends pg_dump's options first; shell-quoting keeps the shell
	 * from misreading the argument.
	 */
	{
		StringInfoData conninfo;

		initStringInfo(&conninfo);
		append_conninfo_dbname(&conninfo, dbname);
		appendStringInfoString(&cmd, " -- ");
		ok &= append_shell_quoted(&cmd, conninfo.data);
		pfree(conninfo.data);
	}

	/* pg_dump's error output goes to a file, read back if it fails. */
	appendStringInfoString(&cmd, " 2> ");
	ok &= append_shell_quoted(&cmd, DBBLUE_BACKUP_STDERR_FILE);

	if (!ok)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: database name or path for \"%s\" contains a newline or carriage return, skipping",
						dbname)));
		record_backup_history(dbname, started_at, "failed", NULL, 0,
							  "database name or path contains an invalid character");
		apply_retention(dbname);
		pfree(cmd.data);
		return false;
	}

	pgstat_report_activity(STATE_RUNNING,
						   psprintf("dbblue backup launcher: backing up %s", dbname));

	ereport(DEBUG3,
			(errmsg_internal("dbblue backup launcher: executing \"%s\"", cmd.data)));

	rc = run_backup_command(cmd.data, dbdir, &stop_reason);
	pfree(cmd.data);
	last_attempt_timed_out = (stop_reason == timed_out_reason);

	dump_errors = read_dump_errors();
	unlink(DBBLUE_BACKUP_STDERR_FILE);

	pgstat_report_activity(STATE_IDLE, NULL);

	if (rc == 0 && stop_reason == NULL)
	{
		struct stat statbuf;

		if (stat(partfile, &statbuf) != 0 || statbuf.st_size == 0)
		{
			ereport(WARNING,
					(errmsg("dbblue backup launcher: pg_dump for \"%s\" exited successfully but \"%s\" is missing or empty",
							dbname, partfile)));
			unlink(partfile);
			record_backup_history(dbname, started_at, "failed", NULL, 0,
								  "pg_dump exited successfully but produced no output file");
			apply_retention(dbname);
			return false;
		}

		/*
		 * Give the dump its final name only once it is on disk, and make the
		 * rename itself durable, before the "success" row is committed: else
		 * a power loss could leave a recorded backup truncated, or still
		 * named .partial (and so deleted by remove_partial_dumps()).
		 * durable_rename() fsyncs the file, renames it, and fsyncs it and
		 * dbdir; dbdir's own entry, new on a database's first backup, needs
		 * the backup directory synced too.  WARNING, not data_sync_elevel():
		 * a failed fsync of a dump calls for discarding that dump, not for
		 * PANICking the server.
		 */
		if (durable_rename(partfile, outfile, WARNING) != 0 ||
			fsync_fname_ext(dbblue_backup_directory, true, false, WARNING) != 0)
		{
			struct stat st;

			/*
			 * Still under its .partial name means the rename never happened,
			 * and outfile, if it exists, is not this dump.
			 */
			if (lstat(partfile, &st) == 0)
				unlink(partfile);
			else
				unlink(outfile);

			record_backup_history(dbname, started_at, "failed", NULL, 0,
								  "could not write the backup file to disk durably");
			apply_retention(dbname);
			return false;
		}

		/*
		 * A dump the history does not list would never be pruned, nor show
		 * up anywhere: if its row cannot be recorded (e.g. lock_timeout on
		 * the history table, or a full disk), remove it again, and count the
		 * attempt as failed so it is retried.
		 */
		if (!record_backup_history(dbname, started_at, "success", outfile,
								   (int64) statbuf.st_size, NULL))
		{
			ereport(WARNING,
					(errmsg("dbblue backup launcher: removing backup \"%s\" of database \"%s\", which could not be recorded in dbblue.dbblue_backup_history",
							outfile, dbname),
					 errdetail("A backup the history does not list would never be removed by retention.")));
			unlink(outfile);
			(void) record_backup_history(dbname, started_at, "failed", NULL, 0,
										 "could not record the backup in dbblue_backup_history");
			return false;
		}

		ereport(LOG,
				(errmsg("dbblue backup launcher: backed up database \"%s\" to \"%s\" (%lld bytes)",
						dbname, outfile, (long long) statbuf.st_size)));
		apply_retention(dbname);
		return true;
	}

	/*
	 * Stopped by this worker (low disk space, or a cancel): the dump may well
	 * have exited 0 if it had just finished, but it was signalled, so it is
	 * discarded like any other failure.
	 */
	{
		/*
		 * Otherwise pg_dump's own error message says why it failed
		 * ("fe_sendauth: no password supplied", ...); its exit status only
		 * says that it did.
		 */
		const char *reason = stop_reason != NULL ? stop_reason :
			dump_errors != NULL ? dump_errors :
			wait_result_to_str(rc);

		ereport(WARNING,
				(errmsg("dbblue backup launcher: backing up database \"%s\" failed: %s",
						dbname, reason)));
		unlink(partfile);
		record_backup_history(dbname, started_at, "failed", NULL, 0, reason);
		apply_retention(dbname);
		return false;
	}
}

/*
 * report_caught_error
 *		Recover from an error caught by PG_CATCH() in this worker: report it
 *		as a WARNING prefixed with what was being done, clear it, and abort
 *		the transaction it left open.  Leaves cxt as the current memory
 *		context; cxt must not be a transaction's.
 */
static void
report_caught_error(MemoryContext cxt, const char *what)
{
	ErrorData  *edata;

	MemoryContextSwitchTo(cxt);
	edata = CopyErrorData();
	FlushErrorState();
	AbortCurrentTransaction();
	MemoryContextSwitchTo(cxt);

	ereport(WARNING,
			(errmsg("dbblue backup launcher: %s: %s", what, edata->message)));
	FreeErrorData(edata);
}

/*
 * apply_retention
 *		All of dbname's retention: successful backups beyond
 *		dbblue_backup_retention_count (files and rows), failed attempts
 *		beyond it, and failed attempts older than its last success.
 *
 * Runs every cycle for every configured database that exists, as well as
 * after each successful backup, so it must never stop a cycle: an error (e.g.
 * lock_timeout on the history table) is reported as a WARNING and the cycle
 * goes on.
 */
static void
apply_retention(const char *dbname)
{
	MemoryContext oldcxt = CurrentMemoryContext;
	char		what[NAMEDATALEN + 64];

	snprintf(what, sizeof(what),
			 "could not apply retention to the backups of database \"%s\"", dbname);

	/*
	 * Not through a database folder that is a symlink or not ours alone (see
	 * folder_unsafe_reason()): retention removes <folder>/<file> by name,
	 * which would follow the link and delete a file of that name wherever it
	 * points, then drop the history row of the real dump.  A folder that is
	 * missing altogether is fine; see dump_is_gone().
	 */
	if (dbblue_backup_directory != NULL && dbblue_backup_directory[0] != '\0')
	{
		char		dbdir[MAXPGPATH];
		struct stat st;

		if (snprintf(dbdir, sizeof(dbdir), "%s/%s",
					 dbblue_backup_directory, dbname) < MAXPGPATH &&
			(lstat(dbdir, &st) == 0 || errno != ENOENT))
		{
			const char *reason = folder_unsafe_reason(dbdir, false);

			if (reason != NULL)
			{
				ereport(WARNING,
						(errmsg("dbblue backup launcher: not applying retention to the backups of database \"%s\": backup directory \"%s\" %s",
								dbname, dbdir, reason),
						 errhint("Make it a directory owned by the operating-system user the server runs as, and not writable by group or others (chmod go-w).")));
				return;
			}
		}
	}

	PG_TRY();
	{
		fix_future_dated_rows(dbname);	/* first: the others rely on the times */
		enforce_retention(dbname, "success");
		enforce_retention(dbname, "failed");
		prune_resolved_failures(dbname);
	}
	PG_CATCH();
	{
		report_caught_error(oldcxt, what);
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcxt);
}

/*
 * fix_future_dated_rows
 *		Reset to now the times of dbname's history rows that are dated in the
 *		future, and say so.
 *
 * A row gets a future time when the server's clock was ahead while it was
 * recorded -- e.g. a hardware clock kept in local time but read as UTC until
 * NTP corrects it, or a wrong date typed in.  Left as is, such a row would
 * make the waiting period (dbblue_backup_interval, counted from the last
 * backup's start) run from that future time, skipping the database with no
 * message until then, and would sort as the newest in retention, crowding out
 * real backups.  Now is the best estimate of when the row was really written:
 * the clock has only just been seen to be right again.  A minute of slack
 * keeps a small clock step back from being reported.
 */
static void
fix_future_dated_rows(const char *dbname)
{
	static const char *sql =
		"UPDATE dbblue.dbblue_backup_history "
		"SET backup_started_at = least(backup_started_at, now()), "
		"    backup_finished_at = least(backup_finished_at, now()) "
		"WHERE database_name = $1 "
		"  AND (backup_started_at > now() + interval '1 minute' "
		"       OR backup_finished_at > now() + interval '1 minute')";
	Oid			argtypes[1] = {TEXTOID};
	Datum		values[1];

	values[0] = CStringGetTextDatum(dbname);

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_execute_with_args(sql, 1, argtypes, values, NULL, false, 0) == SPI_OK_UPDATE &&
		SPI_processed > 0)
		ereport(WARNING,
				(errmsg("dbblue backup launcher: %llu backup history rows of database \"%s\" were dated in the future, and have been reset to the current time",
						(unsigned long long) SPI_processed, dbname),
				 errdetail("The server's clock was probably ahead when they were recorded. Left as they were, backups of this database would have been skipped until that time.")));

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
}

/*
 * prune_resolved_failures
 *		Delete dbname's failed attempts older than its last successful
 *		backup.
 *
 * Retention of failed rows otherwise runs only when there are failed rows to
 * keep in check, so the failures a database has recovered from would stay
 * forever.  They carry no file (a failed attempt removes its dump), so only
 * the row goes.
 */
static void
prune_resolved_failures(const char *dbname)
{
	static const char *sql =
		"DELETE FROM dbblue.dbblue_backup_history "
		"WHERE database_name = $1 AND status = 'failed' "
		"  AND backup_started_at < "
		"      (SELECT max(backup_started_at) FROM dbblue.dbblue_backup_history "
		"       WHERE database_name = $1 AND status = 'success')";
	Oid			argtypes[1] = {TEXTOID};
	Datum		values[1];

	values[0] = CStringGetTextDatum(dbname);

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_execute_with_args(sql, 1, argtypes, values, NULL, false, 0) == SPI_OK_DELETE &&
		SPI_processed > 0)
		ereport(DEBUG1,
				(errmsg_internal("dbblue backup launcher: removed %llu failed attempts of database \"%s\" older than its last successful backup",
								 (unsigned long long) SPI_processed, dbname)));

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
}

/*
 * record_backup_history
 *		Insert one row describing the outcome of a backup attempt.
 *		Append-only -- one row per attempt, not one row per database --
 *		so retention (enforce_retention) is what keeps this table from
 *		growing without bound.
 *
 * Returns false, after a WARNING, if the row could not be recorded -- an
 * error included, which is caught here rather than left to abort the cycle,
 * so that the caller can deal with a dump the history will not list.
 */
static bool
record_backup_history(const char *dbname, TimestampTz started_at,
					  const char *status, const char *file_path,
					  int64 file_size_bytes, const char *error_message)
{
	MemoryContext oldcxt = CurrentMemoryContext;
	volatile bool ok = true;
	char		what[NAMEDATALEN + 64];
	/*
	 * The id is picked here rather than taken from the identity sequence:
	 * pg_write_all_data grants UPDATE on every sequence, which no REVOKE
	 * removes, so a member could setval() it to its maximum and make every
	 * insert fail -- and with it every backup, whose dump is removed when its
	 * row cannot be saved.  The guard trigger lets no one but this worker
	 * insert, so max(id) + 1 cannot collide.
	 */
	static const char *sql =
		"INSERT INTO dbblue.dbblue_backup_history "
		"(id, database_name, backup_started_at, backup_finished_at, status, "
		" file_path, file_size_bytes, error_message) "
		"OVERRIDING SYSTEM VALUE "
		"VALUES ((SELECT coalesce(max(id), 0) + 1 FROM dbblue.dbblue_backup_history), "
		"        $1, $2, now(), $3, $4, $5, $6)";
	static const Oid argtypes[6] = {TEXTOID, TIMESTAMPTZOID, TEXTOID, TEXTOID, INT8OID, TEXTOID};
	Datum		values[6];
	char		nulls[7] = "      ";	/* SPI convention: ' ' = not null, 'n' = null */

	values[0] = CStringGetTextDatum(dbname);
	values[1] = TimestampTzGetDatum(started_at);
	values[2] = CStringGetTextDatum(status);

	if (file_path != NULL)
		values[3] = CStringGetTextDatum(file_path);
	else
		nulls[3] = 'n';

	if (file_size_bytes > 0)
		values[4] = Int64GetDatum(file_size_bytes);
	else
		nulls[4] = 'n';

	if (error_message != NULL)
		values[5] = CStringGetTextDatum(error_message);
	else
		nulls[5] = 'n';

	/*
	 * backup_finished_at is now(), i.e. this transaction's start time, which
	 * StartTransactionCommand() copies from the statement start time.  A
	 * background worker must set that itself, so set it here: otherwise it is
	 * still whatever it was when the cycle began (load_backup_candidates()),
	 * and every row of a cycle would get the cycle's start as its finish time
	 * -- before the backup even started.
	 */
	snprintf(what, sizeof(what),
			 "could not record backup history for database \"%s\"", dbname);

	PG_TRY();
	{
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		if (SPI_execute_with_args(sql, 6, (Oid *) argtypes, values, nulls, false, 0) < 0)
		{
			ereport(WARNING,
					(errmsg("dbblue backup launcher: %s", what)));
			ok = false;
		}

		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		report_caught_error(oldcxt, what);
		ok = false;
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcxt);
	return ok;
}

/*
 * path_has_parent_component
 *		True if any '/'-separated component of path is exactly "..".
 */
static bool
path_has_parent_component(const char *path)
{
	const char *p = path;

	for (;;)
	{
		const char *end = strchr(p, '/');
		size_t		len = end ? (size_t) (end - p) : strlen(p);

		if (len == 2 && p[0] == '.' && p[1] == '.')
			return true;
		if (end == NULL)
			return false;
		p = end + 1;
	}
}

/*
 * get_prunable_backup_path
 *		Decide whether file_path, read from a history row, names a dump this
 *		launcher could itself have written for dbname, i.e. a file
 *		<dbblue_backup_directory>/<dbname>/<dbname>_<suffix>.dump with no
 *		further '/' in <suffix>.  If so, copy its canonical form into
 *		result (MAXPGPATH bytes) and return true.
 *
 *		The history table is read-only to PUBLIC, but members of
 *		pg_write_all_data bypass that ACL and can rewrite file_path, and
 *		retention unlinks as the server's OS user.  Without this check any
 *		such role could have the server delete any file it can reach --
 *		pg_control, WAL, relation files, TDE keys.  A ".." component is
 *		refused outright rather than collapsed: canonicalize_path() resolves
 *		it lexically, which disagrees with the kernel when an earlier
 *		component is a symlink.
 */
static bool
get_prunable_backup_path(const char *dbname, const char *file_path,
						 char *result)
{
	char		dbdir[MAXPGPATH];
	size_t		dirlen;
	size_t		namelen;
	const char *rest;
	const char *suffix;
	size_t		suffixlen;

	if (dbblue_backup_directory == NULL || dbblue_backup_directory[0] == '\0')
		return false;

	if (path_has_parent_component(file_path) ||
		path_has_parent_component(dbblue_backup_directory) ||
		path_has_parent_component(dbname))
		return false;

	if (snprintf(dbdir, MAXPGPATH, "%s/%s",
				 dbblue_backup_directory, dbname) >= MAXPGPATH ||
		strlcpy(result, file_path, MAXPGPATH) >= MAXPGPATH)
		return false;

	canonicalize_path(dbdir);
	canonicalize_path(result);

	/* Directly inside <backup directory>/<dbname>/ ... */
	dirlen = strlen(dbdir);
	if (strncmp(result, dbdir, dirlen) != 0 || result[dirlen] != '/')
		return false;

	/* ... named <dbname>_<suffix>.dump, as backup_one_database() names it. */
	rest = result + dirlen + 1;
	namelen = strlen(dbname);
	if (strncmp(rest, dbname, namelen) != 0 || rest[namelen] != '_')
		return false;

	suffix = rest + namelen + 1;
	suffixlen = strlen(suffix);
	if (strchr(suffix, '/') != NULL ||
		suffixlen <= strlen(".dump") ||
		strcmp(suffix + suffixlen - strlen(".dump"), ".dump") != 0)
		return false;

	return true;
}

/*
 * looks_like_our_dump
 *		True if path ends in "<dbname>/<dbname>_<something>.dump", the way
 *		backup_one_database() names a dump -- in whatever backup directory.
 *
 *		Used for dumps outside the current dbblue_backup_directory, which
 *		are kept and reported rather than removed.  The check keeps a
 *		file_path rewritten to point elsewhere (e.g. at /etc/passwd) out of
 *		that report, where it would be listed as a file to delete.
 */
static bool
looks_like_our_dump(const char *path, const char *dbname)
{
	char		folder[MAXPGPATH];
	const char *file_name = last_dir_separator(path);
	const char *folder_name;
	size_t		len = strlen(dbname);

	if (file_name == NULL || path_has_parent_component(path))
		return false;
	file_name++;

	/* The file is named <dbname>_<something>.dump ... */
	if (strncmp(file_name, dbname, len) != 0 || file_name[len] != '_' ||
		!pg_str_endswith(file_name, ".dump"))
		return false;

	/* ... and sits in a folder named <dbname>. */
	strlcpy(folder, path, sizeof(folder));
	get_parent_directory(folder);
	folder_name = last_dir_separator(folder);
	return folder_name != NULL && strcmp(folder_name + 1, dbname) == 0;
}

/*
 * dump_is_gone
 *		True only if path no longer exists while the folder it was in still
 *		does, i.e. the file was deleted.  False when the folder is missing
 *		too -- a backup disk that is not mounted looks just like that -- and
 *		when the file cannot be checked at all (permissions, I/O errors).
 *
 *		Retention deletes a history row only once its file is gone, so that
 *		a dump that is merely out of reach for now never loses its row.
 */
static bool
dump_is_gone(const char *path)
{
	char		folder[MAXPGPATH];
	struct stat st;

	if (lstat(path, &st) == 0 || errno != ENOENT)
		return false;			/* still there, or can't tell */

	strlcpy(folder, path, sizeof(folder));
	get_parent_directory(folder);
	return stat(folder, &st) == 0;	/* folder there, file not: deleted */
}

/*
 * enforce_retention
 *		Delete backups of dbname with the given status beyond
 *		dbblue_backup_retention_count, both the dump file (if any) and the
 *		history row.  Called right after a new row of that status is
 *		recorded, so the newest is always kept even when the count is
 *		exceeded.  Applied to both "success" and "failed" rows -- without
 *		this, a persistently misconfigured target (bad directory, auth
 *		failure, etc.) would insert one more "failed" row every naptime
 *		cycle forever, since only successes used to be pruned.
 */
static void
enforce_retention(const char *dbname, const char *status)
{
	static const char *select_sql =
		"SELECT id, file_path FROM dbblue.dbblue_backup_history "
		"WHERE database_name = $1 AND status = $2 "
		"ORDER BY backup_finished_at DESC "
		"OFFSET $3";
	static const Oid select_argtypes[3] = {TEXTOID, TEXTOID, INT8OID};
	Datum		select_values[3];
	int			ret;
	Datum	   *ids_to_delete;
	int			n_to_delete = 0;
	uint64		row;

	select_values[0] = CStringGetTextDatum(dbname);
	select_values[1] = CStringGetTextDatum(status);

	/*
	 * Keep at least the newest backup.  dbblue_backup_retention_count is
	 * documented as "0 keeps only the newest", and this OFFSET selects the
	 * rows to delete -- the ones past the first N -- so a literal OFFSET 0
	 * would match and delete every row, including the backup just recorded a
	 * moment ago.  Floor the offset at 1 so retention 0 keeps exactly the
	 * newest, as documented, and it is never possible to prune away the last
	 * remaining backup.
	 */
	select_values[2] = Int64GetDatum((int64) Max(dbblue_backup_retention_count, 1));

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute_with_args(select_sql, 3, (Oid *) select_argtypes,
								select_values, NULL, false, 0);

	if (ret != SPI_OK_SELECT)
	{
		ereport(WARNING,
				(errmsg("dbblue backup launcher: retention lookup for \"%s\" failed (SPI result %d)",
						dbname, ret)));
		ids_to_delete = NULL;
	}
	else
	{
		ids_to_delete = (Datum *) palloc(SPI_processed * sizeof(Datum));

		for (row = 0; row < SPI_processed; row++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[row];
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			bool		isnull;
			Datum		id;
			Datum		file_path_datum;
			char	   *file_path;

			id = SPI_getbinval(tuple, tupdesc, 1, &isnull);
			if (isnull)
				continue;

			file_path_datum = SPI_getbinval(tuple, tupdesc, 2, &isnull);
			file_path = isnull ? NULL : TextDatumGetCString(file_path_datum);

			if (file_path != NULL)
			{
				char		safe_path[MAXPGPATH];
				bool		prunable = get_prunable_backup_path(dbname, file_path,
																safe_path);

				/*
				 * Only remove a file in the current backup directory.
				 *
				 * Our dump in an earlier backup directory is kept, and so is
				 * its row: the row is the only record of where the dump is,
				 * and report_unmanaged_backups() reports it from there.  Once
				 * the file has been deleted by hand, the row goes too -- but
				 * only then (see dump_is_gone()), not while its disk is
				 * unmounted or the file cannot be checked.
				 *
				 * Any other path is refused, and its row deleted.
				 *
				 * A row is deleted only once its file is known to be gone:
				 * a dump that could not be removed keeps its row, so that it
				 * is retried next cycle rather than left on disk untracked.
				 */
				if (!prunable && looks_like_our_dump(file_path, dbname))
				{
					if (!dump_is_gone(file_path))
						continue;	/* keep row and file */
				}
				else if (!prunable)
					ereport(WARNING,
							(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							 errmsg("dbblue backup launcher: not removing \"%s\" named in backup history row %lld, which is not a backup file of database \"%s\"",
									file_path, (long long) DatumGetInt64(id),
									dbname),
							 errdetail("Retention removes only files named \"%s/%s/%s_*.dump\". The row has been deleted; the file was left in place.",
									   dbblue_backup_directory, dbname, dbname)));
				else if (unlink(safe_path) != 0)
				{
					int			save_errno = errno;

					if (!dump_is_gone(safe_path))
					{
						errno = save_errno;
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("dbblue backup launcher: could not remove old backup \"%s\": %m",
										safe_path),
								 save_errno == ENOENT ?
								 errhint("Its folder is missing too; is the backup disk mounted? It will be retried.") :
								 errhint("It will be retried at the next cycle.")));
						continue;	/* keep the row: retry next cycle */
					}
				}
			}

			ids_to_delete[n_to_delete++] = id;
		}
	}

	if (n_to_delete > 0)
	{
		static const char *delete_sql =
			"DELETE FROM dbblue.dbblue_backup_history WHERE id = ANY($1::bigint[])";
		static const Oid delete_argtypes[1] = {INT8ARRAYOID};
		Datum		delete_values[1];
		ArrayType  *idarr;

		idarr = construct_array_builtin(ids_to_delete, n_to_delete, INT8OID);
		delete_values[0] = PointerGetDatum(idarr);

		if (SPI_execute_with_args(delete_sql, 1, (Oid *) delete_argtypes,
								  delete_values, NULL, false, 0) < 0)
			ereport(WARNING,
					(errmsg("dbblue backup launcher: failed to prune backup history for \"%s\"",
							dbname)));
	}

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
}

/*
 * report_unmanaged_backups
 *		Log the dumps this launcher keeps but will never remove, so that an
 *		administrator can delete them by hand:
 *
 *	- dumps of a database that was dropped, or removed from
 *	  dbblue_backup_databases (retention leaves those alone: they may be
 *	  the only copy left);
 *	- dumps in an earlier dbblue_backup_directory (retention only removes
 *	  files in the current one).
 *
 * Checked every cycle, but logged only when the list has changed since it was
 * last logged (so also right after every start), or a day after that, as a
 * reminder -- not the same message every naptime.
 * An error here is logged as a WARNING and never stops backups.
 */
static void
report_unmanaged_backups(void)
{
	/*
	 * Every successful backup, and whether its database is still backed up
	 * (exists and is configured).  Which of them are leftovers is decided in
	 * C below, with the same checks retention uses, so that the two always
	 * agree.
	 */
	static const char *sql =
		"SELECT h.database_name, h.file_path,"
		"       (d.oid IS NOT NULL AND h.database_name = ANY ($1)) AS managed"
		" FROM dbblue.dbblue_backup_history h"
		"      LEFT JOIN pg_catalog.pg_database d ON d.datname = h.database_name"
		" WHERE h.status = 'success' AND h.file_path IS NOT NULL"
		" ORDER BY h.file_path";
	MemoryContext oldcxt = CurrentMemoryContext;
	List	   *configured = parse_configured_databases();
	Datum	   *names = palloc((list_length(configured) + 1) * sizeof(Datum));
	Oid			argtypes[1] = {TEXTARRAYOID};
	Datum		values[1];
	int			n = 0;
	ListCell   *lc;

	foreach(lc, configured)
		names[n++] = CStringGetTextDatum(((BackupDbSpec *) lfirst(lc))->dbname);
	values[0] = PointerGetDatum(construct_array_builtin(names, n, TEXTOID));

	PG_TRY();
	{
		StringInfoData files;
		int			count = 0;
		int64		bytes = 0;
		uint64		row;
		bool		changed;
		bool		reminder_due;

		initStringInfo(&files);

		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		if (SPI_execute_with_args(sql, 1, argtypes, values, NULL, true, 0) != SPI_OK_SELECT)
			elog(ERROR, "could not read dbblue.dbblue_backup_history");

		/* One line per leftover dump: its path, size, and why. */
		for (row = 0; row < SPI_processed; row++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[row];
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			bool		isnull;
			char	   *dbname = SPI_getvalue(tuple, tupdesc, 1);
			char	   *path = SPI_getvalue(tuple, tupdesc, 2);
			bool		managed = DatumGetBool(SPI_getbinval(tuple, tupdesc, 3, &isnull));
			char		safe_path[MAXPGPATH];
			struct stat st;
			bool		in_current_dir;

			/*
			 * In the current backup directory?  Decided as retention decides
			 * it -- on normalized paths, so "/b" and "/b/" are the same
			 * directory -- so that a dump retention still manages is never
			 * reported as one to delete by hand.
			 */
			in_current_dir = get_prunable_backup_path(dbname, path, safe_path);

			if (in_current_dir && managed)
				continue;		/* a managed backup, not a leftover */

			/* Only our own dumps (see looks_like_our_dump()). */
			if (!looks_like_our_dump(path, dbname))
				continue;

			/*
			 * Gone, or not checkable (permissions, I/O): skip just this one,
			 * rather than failing the whole report as an ERROR would.
			 */
			if (lstat(path, &st) != 0 || !S_ISREG(st.st_mode))
				continue;

			appendStringInfo(&files, "%s%s (%lld bytes): %s",
							 count > 0 ? "\n" : "", path, (long long) st.st_size,
							 in_current_dir ? "database no longer backed up"
							 : "in an earlier dbblue_backup_directory");
			count++;
			bytes += st.st_size;
		}

		/* Log only if the list changed, or the daily reminder is due. */
		changed = (count != last_report_count || bytes != last_report_bytes);
		reminder_due = GetCurrentTimestamp() >=
			last_report_at + (int64) SECS_PER_DAY * USECS_PER_SEC;

		last_report_count = count;
		last_report_bytes = bytes;

		if (count > 0 && (changed || reminder_due))
		{
			last_report_at = GetCurrentTimestamp();
			ereport(LOG,
					(errmsg("dbblue backup launcher: keeping %d backups (%lld bytes) that it no longer manages",
							count, (long long) bytes),
					 errdetail_internal("%s", files.data),
					 errhint("They are never removed automatically. Delete them by hand once they are no longer needed.")));
		}

		PopActiveSnapshot();
		SPI_finish();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		report_caught_error(oldcxt, "could not check for backups it no longer manages");
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcxt);
}

/*
 * append_conninfo_dbname
 *		Append the libpq connection string dbname='<dbname>' to buf, with any
 *		single quote or backslash in the name escaped by a backslash, so that
 *		libpq takes the whole name literally -- the same quoting as
 *		appendConnStrVal() in fe_utils/string_utils.c, which is not linked
 *		into the backend.
 */
static void
append_conninfo_dbname(StringInfo buf, const char *dbname)
{
	const char *p;

	appendStringInfoString(buf, "dbname='");
	for (p = dbname; *p; p++)
	{
		if (*p == '\'' || *p == '\\')
			appendStringInfoChar(buf, '\\');
		appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 * append_shell_quoted
 *		Append str to buf, single-quoted for /bin/sh unless it is already
 *		composed only of characters that are never special to a shell.
 *		Ported from fe_utils/string_utils.c's appendShellStringNoError(),
 *		since fe_utils is not linked into the backend.  Returns false (and
 *		leaves *some* partial output in buf) if str contains a newline or
 *		carriage return, which cannot be represented as a single shell
 *		argument; callers must treat that as a hard error rather than run
 *		the resulting command.
 */
static bool
append_shell_quoted(StringInfo buf, const char *str)
{
	bool		ok = true;
	const char *p;

	if (*str != '\0' &&
		strspn(str, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./:") == strlen(str))
	{
		appendStringInfoString(buf, str);
		return ok;
	}

	appendStringInfoChar(buf, '\'');
	for (p = str; *p; p++)
	{
		if (*p == '\n' || *p == '\r')
		{
			ok = false;
			continue;
		}

		if (*p == '\'')
			appendStringInfoString(buf, "'\"'\"'");
		else
			appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '\'');

	return ok;
}
