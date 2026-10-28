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
 *      pg_basebackup -R against the local primary, together with the
 *      physical replication slot "dbblue_standby" it streams through.
 *   2. A free TCP port is picked by probing PostPortNumber+1, +2, ...
 *      and written, along with the primary's unix_socket_directories,
 *      hot_standby_feedback = on and dbblue_create_standby = off, to the
 *      standby's postgresql.auto.conf.
 *   3. The standby is started with pg_ctl on the primary's configuration
 *      files (which need not live in the copied data directory), and the
 *      chosen port is reported in the server log and written to
 *      <DataDir>/dbblue_standby.port so tooling (e.g. an odoo.conf
 *      generator setting db_replica_port) can pick it up.
 *
 * The worker then babysits the standby: every tick it checks the standby
 * postmaster is still alive and restarts it if not.  It also checks the
 * standby is still usable: if our replication slot has been invalidated,
 * the standby can never catch up and is rebuilt; if the directory holds a
 * promoted copy of us or a different cluster altogether, it is left alone,
 * and neither started nor reported.  The standby is left
 * running when the primary shuts down — it remains usable for read-only
 * traffic and simply resumes streaming when the primary returns.
 *
 * The standby must not spawn standbys of its own, neither while it is one
 * nor once promoted, so its copy of dbblue_create_standby is turned off.
 * The worker also never runs on a cluster that is itself in recovery, which
 * covers standbys built by other means that still carry the setting.
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
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_control.h"
#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/dbblue_create_standby.h"
#include "postmaster/interrupt.h"
#include "port/pg_crc32c.h"
#include "postmaster/postmaster.h"
#include "replication/slot.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/guc_hooks.h"
#include "utils/pidfile.h"
#include "utils/syscache.h"
#include "utils/varlena.h"
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

/*
 * How many lines of a failed command's output run_shell() quotes, how long
 * each may be, and how much of the end of that output it reads to find them.
 */
#define DBBLUE_STANDBY_OUTPUT_LINES		5
#define DBBLUE_STANDBY_OUTPUT_LINE_MAX	512
#define DBBLUE_STANDBY_OUTPUT_SCAN		65536

/*
 * pg_tde layout, as in src/include/tde/pg_tde.h.  Spelled out here rather
 * than included because that header drags in pg_tde's own definitions,
 * which the core backend does not otherwise need.
 */
#define DBBLUE_TDE_DIR				"pg_tde"
#define DBBLUE_TDE_WAL_KEYS			"wal_keys"
#define DBBLUE_TDE_GLOBAL_PROVIDERS	"1664_providers"

/*
 * Physical replication slot the managed standby streams through, so that we
 * never recycle WAL it has not received yet.  pg_basebackup -C creates it and
 * -R writes it into the standby's primary_slot_name.  How much WAL it may hold
 * back is capped by max_slot_wal_keep_size, which initdb's auto-tune sets.
 */
#define DBBLUE_STANDBY_SLOT			"dbblue_standby"

typedef enum
{
	STANDBY_SLOT_ABSENT,
	STANDBY_SLOT_IDLE,
	STANDBY_SLOT_ACTIVE,
	STANDBY_SLOT_INVALIDATED,	/* WAL it needed is gone; see
								 * vet_existing_standby() */
} StandbySlotState;

/* What an existing standby directory holds; see standby_identity(). */
typedef enum
{
	STANDBY_OURS,				/* a standby of this cluster */
	STANDBY_PROMOTED,			/* a copy of this cluster, since promoted */
	STANDBY_FOREIGN,			/* a different cluster altogether */
	STANDBY_UNREADABLE,			/* cannot tell; pg_control unreadable */
} StandbyIdentity;

static int	consecutive_failures = 0;

/* Whether warn_unlimited_slot_wal() has spoken since the last reload. */
static bool slot_wal_limit_warned = false;

/*
 * standby_directory_path
 *		Return a palloc'd absolute path for the standby data directory named
 *		by the setting value dir.
 *
 * An empty value means "<DataDir>_standby".  Relative paths are interpreted
 * against the parent directory of DataDir, so the standby never ends up
 * inside the primary's data directory (which would bloat every future base
 * backup).
 */
static char *
standby_directory_path(const char *dir)
{
	char		parent[MAXPGPATH];
	char	   *result;

	if (dir == NULL || dir[0] == '\0')
		result = psprintf("%s_standby", DataDir);
	else if (is_absolute_path(dir))
		result = pstrdup(dir);
	else
	{
		strlcpy(parent, DataDir, sizeof(parent));
		get_parent_directory(parent);
		result = psprintf("%s/%s", parent, dir);
	}

	canonicalize_path(result);
	return result;
}

/*
 * resolve_standby_directory
 *		Return a palloc'd absolute path for the standby data directory.
 */
static char *
resolve_standby_directory(void)
{
	char	   *result = standby_directory_path(dbblue_standby_directory);

	/*
	 * Validate path length: must leave room for suffixes like "/PG_VERSION",
	 * "/postgresql.auto.conf", "/postmaster.pid", etc. Reserve 32 bytes.
	 */
	if (strlen(result) >= MAXPGPATH - 32)
	{
		ereport(WARNING,
				(errmsg("dbblue create standby: directory path is too long (exceeds %d bytes)",
						MAXPGPATH - 32),
				 errdetail("Path: %s", result)));
		pfree(result);
		return NULL;
	}

	/*
	 * check_dbblue_standby_directory() refuses this for ALTER SYSTEM, but
	 * not for a value read from the configuration file.
	 */
	if (path_is_prefix_of_path(DataDir, result))
	{
		ereport(WARNING,
				(errmsg("dbblue create standby: directory \"%s\" is the data directory of this server or lies inside it",
						result),
				 errhint("Point dbblue_standby_directory elsewhere.")));
		pfree(result);
		return NULL;
	}

	return result;
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
 * standby_dir_is_empty
 *		Set *empty to whether dir, which exists, holds nothing pg_basebackup
 *		would refuse to write over.  Returns false, with errno set, if dir
 *		cannot be read.
 *
 * A lone pg_tde directory counts as empty: it is our own seed from an
 * attempt that died before pg_basebackup could clean up, and
 * seed_standby_tde_dir() replaces it.
 */
static bool
standby_dir_is_empty(const char *dir, bool *empty)
{
	DIR		   *d;
	struct dirent *de;

	d = opendir(dir);
	if (d == NULL)
		return false;

	*empty = true;
	while ((de = readdir(d)) != NULL)
	{
		if (strcmp(de->d_name, ".") != 0 &&
			strcmp(de->d_name, "..") != 0 &&
			strcmp(de->d_name, DBBLUE_TDE_DIR) != 0)
		{
			*empty = false;
			break;
		}
	}
	closedir(d);
	return true;
}

/*
 * read_system_identifier
 *		Read the system identifier from dir's pg_control.  Reports a WARNING
 *		and returns false if that cannot be done.
 *
 * get_controlfile() would do, but raises an ERROR on any problem, which
 * would cost us the worker rather than one tick.  Like its frontend flavor,
 * retry on a bad CRC: the standby may be rewriting the file as we read it.
 */
static bool
read_system_identifier(const char *dir, uint64 *sysid)
{
	char		path[MAXPGPATH];
	ControlFileData control;
	pg_crc32c	crc;

	snprintf(path, sizeof(path), "%s/%s", dir, XLOG_CONTROL_FILE);

	for (int attempt = 0; attempt < 10; attempt++)
	{
		int			fd;
		ssize_t		nread;

		fd = open(path, O_RDONLY | PG_BINARY, 0);
		if (fd < 0)
		{
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("dbblue create standby: could not open \"%s\": %m",
							path)));
			return false;
		}
		nread = read(fd, &control, sizeof(control));
		close(fd);
		if (nread != sizeof(control))
		{
			ereport(WARNING,
					(errmsg("dbblue create standby: could not read \"%s\"",
							path)));
			return false;
		}

		INIT_CRC32C(crc);
		COMP_CRC32C(crc, &control, offsetof(ControlFileData, crc));
		FIN_CRC32C(crc);
		if (EQ_CRC32C(crc, control.crc))
		{
			*sysid = control.system_identifier;
			return true;
		}

		pg_usleep(10000L);
	}

	ereport(WARNING,
			(errmsg("dbblue create standby: \"%s\" has an invalid checksum",
					path)));
	return false;
}

/*
 * standby_identity
 *		Is the cluster in dir really a standby of ours?
 *
 * Its PG_VERSION only says it is some cluster.  It may also be a copy of us
 * that has since been promoted -- leaving us, most likely, the old primary
 * of that failover -- or a different cluster entirely, typically the
 * standby of an earlier cluster that lived in our data directory before
 * this one was initialized.  Neither can stream from us, and neither is
 * ours to start, report, or delete.
 *
 * On STANDBY_FOREIGN, *their_sysid is set to the identifier found.
 */
static StandbyIdentity
standby_identity(const char *dir, uint64 *their_sysid)
{
	char		path[MAXPGPATH];
	struct stat st;

	if (!read_system_identifier(dir, their_sysid))
		return STANDBY_UNREADABLE;
	if (*their_sysid != GetSystemIdentifier())
		return STANDBY_FOREIGN;

	snprintf(path, sizeof(path), "%s/%s", dir, STANDBY_SIGNAL_FILE);
	if (stat(path, &st) != 0)
		return STANDBY_PROMOTED;

	return STANDBY_OURS;
}

/*
 * forget_standby_port
 *		Remove <DataDir>/dbblue_standby.port, so that tooling reading it does
 *		not keep sending read-only traffic to a server that is no longer our
 *		standby.
 */
static void
forget_standby_port(void)
{
	char		path[MAXPGPATH];

	snprintf(path, sizeof(path), "%s/dbblue_standby.port", DataDir);
	if (unlink(path) != 0 && errno != ENOENT)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not remove \"%s\": %m",
						path)));
}

/*
 * primary_uses_wal_encryption
 *		True if this cluster has pg_tde WAL keys, i.e. its WAL on disk is
 *		encrypted and a base backup of it must be taken with -E.
 */
static bool
primary_uses_wal_encryption(void)
{
	char		path[MAXPGPATH];
	struct stat st;

	snprintf(path, sizeof(path), "%s/%s/%s",
			 DataDir, DBBLUE_TDE_DIR, DBBLUE_TDE_WAL_KEYS);
	return (stat(path, &st) == 0 && S_ISREG(st.st_mode));
}

/*
 * copy_tde_file
 *		Copy one small pg_tde file.  Reports a WARNING and returns false on
 *		failure instead of raising an ERROR, so a transient problem costs one
 *		failed tick rather than the worker itself.
 */
static bool
copy_tde_file(const char *from, const char *to)
{
	char		buf[8192];
	int			srcfd;
	int			dstfd;
	ssize_t		nread;
	bool		ok = true;

	srcfd = open(from, O_RDONLY | PG_BINARY, 0);
	if (srcfd < 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not open \"%s\": %m", from)));
		return false;
	}

	dstfd = open(to, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY, S_IRUSR | S_IWUSR);
	if (dstfd < 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not create \"%s\": %m", to)));
		close(srcfd);
		return false;
	}

	while ((nread = read(srcfd, buf, sizeof(buf))) > 0)
	{
		if (write(dstfd, buf, nread) != nread)
		{
			if (errno == 0)
				errno = ENOSPC;
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("dbblue create standby: could not write \"%s\": %m", to)));
			ok = false;
			break;
		}
	}
	if (nread < 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not read \"%s\": %m", from)));
		ok = false;
	}

	close(srcfd);
	if (close(dstfd) != 0 && ok)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not close \"%s\": %m", to)));
		ok = false;
	}
	return ok;
}

/*
 * seed_standby_tde_dir
 *		Prepare dir for "pg_basebackup -E".
 *
 * -E re-encrypts the WAL streamed during the copy with a fresh WAL key, and
 * wraps that key with the server principal key.  It finds the server key
 * through <dir>/pg_tde before the backup starts, so the two files it reads
 * there must already be in place: wal_keys (whose header records the server
 * key) and 1664_providers (which says where the keyring is).  Every other
 * pg_tde file arrives with the backup itself and overwrites whatever is
 * seeded here, and pg_basebackup skips the server's wal_keys under -E and
 * writes its own, so it does not matter if a key file changes while we copy.
 *
 * A pg_tde directory left behind by an earlier failed attempt is replaced.
 */
static bool
seed_standby_tde_dir(const char *dir)
{
	char		dirbuf[MAXPGPATH];
	char		tdedir[MAXPGPATH];
	char		from[MAXPGPATH];
	char		to[MAXPGPATH];
	struct stat st;
	static const char *const files[] = {
		DBBLUE_TDE_WAL_KEYS,
		DBBLUE_TDE_GLOBAL_PROVIDERS,
	};

	/* pg_mkdir_p scribbles on its argument */
	strlcpy(dirbuf, dir, sizeof(dirbuf));
	if (pg_mkdir_p(dirbuf, S_IRWXU) != 0 && errno != EEXIST)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not create directory \"%s\": %m",
						dir)));
		return false;
	}
	/* pg_basebackup refuses a target directory with group/other access */
	if (chmod(dir, S_IRWXU) != 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not set permissions on \"%s\": %m",
						dir)));
		return false;
	}

	snprintf(tdedir, sizeof(tdedir), "%s/%s", dir, DBBLUE_TDE_DIR);
	if (stat(tdedir, &st) == 0 && !rmtree(tdedir, true))
	{
		ereport(WARNING,
				(errmsg("dbblue create standby: could not remove stale \"%s\"",
						tdedir)));
		return false;
	}
	if (mkdir(tdedir, S_IRWXU) != 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not create directory \"%s\": %m",
						tdedir)));
		return false;
	}

	for (int i = 0; i < lengthof(files); i++)
	{
		snprintf(from, sizeof(from), "%s/%s/%s", DataDir, DBBLUE_TDE_DIR, files[i]);
		snprintf(to, sizeof(to), "%s/%s", tdedir, files[i]);
		if (!copy_tde_file(from, to))
			return false;
	}

	return true;
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
 * first_socket_directory
 *		Our first Unix socket directory, palloc'd, or NULL if we have none.
 *		The standby is given the same ones; see write_standby_settings().
 */
static char *
first_socket_directory(void)
{
	char	   *dir = NULL;
	char	   *rawstring;
	List	   *elemlist;

	if (Unix_socket_directories == NULL || Unix_socket_directories[0] == '\0')
		return NULL;

	rawstring = pstrdup(Unix_socket_directories);
	if (SplitDirectoriesString(rawstring, ',', &elemlist) && elemlist != NIL)
		dir = pstrdup((char *) linitial(elemlist));
	list_free_deep(elemlist);
	pfree(rawstring);
	return dir;
}

/*
 * socket_port_in_use
 *		Does a live server hold the Unix socket for port in one of our socket
 *		directories, which the standby is given too?
 *
 * A server uses its port number twice: for TCP, and to name its socket,
 * "<dir>/.s.PGSQL.<port>", guarded by a ".lock" file beside it.  One that
 * listens on the socket alone, or on addresses other than the loopback one,
 * leaves the port free to bind on 127.0.0.1 but holds the lock, and a
 * standby started on that port fails with "lock file ... already exists" --
 * on every retry, as the TCP probe alone keeps choosing the same port.  As
 * PostgreSQL itself does, a lock whose process is gone counts as stale, and
 * the port as free.
 */
static bool
socket_port_in_use(int port)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *lc;
	bool		in_use = false;

	if (Unix_socket_directories == NULL || Unix_socket_directories[0] == '\0')
		return false;

	rawstring = pstrdup(Unix_socket_directories);
	if (SplitDirectoriesString(rawstring, ',', &elemlist))
	{
		foreach(lc, elemlist)
		{
			const char *dir = (const char *) lfirst(lc);
			char		path[MAXPGPATH];
			FILE	   *fp;
			long		pid = 0;

			/* an abstract socket (Linux "@name") has no lock file */
			if (dir[0] == '@')
				continue;

			snprintf(path, sizeof(path), "%s/.s.PGSQL.%d.lock", dir, port);
			fp = fopen(path, "r");
			if (fp == NULL)
				continue;
			if (fscanf(fp, "%ld", &pid) != 1)
				pid = 0;
			fclose(fp);

			/* a standalone backend writes its PID negated */
			if (pid < 0)
				pid = -pid;
			if (pid > 1 && (kill((pid_t) pid, 0) == 0 || errno == EPERM))
			{
				in_use = true;
				break;
			}
		}
	}
	list_free_deep(elemlist);
	pfree(rawstring);
	return in_use;
}

/*
 * find_free_port
 *		Return the first port, starting just above the primary's own, that is
 *		free both over TCP on the loopback interface and for the Unix socket
 *		in each of our socket directories, or -1.
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

		/*
		 * Validate port is within valid TCP port range (1-65535).
		 * Prevents integer overflow when casting to uint16.
		 */
		if (port > 65535)
		{
			ereport(WARNING,
					(errmsg("dbblue create standby: port %d exceeds maximum port 65535; cannot search further",
							port)));
			return -1;
		}

		sock = socket(AF_INET, SOCK_STREAM, 0);
		if (sock < 0)
		{
			ereport(DEBUG2,
					(errmsg("dbblue create standby: socket creation failed for port %d: %m",
							port)));
			continue;	/* Try next port */
		}

		memset(&addr, 0, sizeof(addr));
		addr.sin_family = AF_INET;
		addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		addr.sin_port = htons((uint16) port);

		free_port = (bind(sock, (struct sockaddr *) &addr, sizeof(addr)) == 0);
		close(sock);

		if (free_port && socket_port_in_use(port))
		{
			ereport(LOG,
					(errmsg("dbblue create standby: port %d is free over TCP, but another server holds its Unix socket; trying the next port",
							port)));
			free_port = false;
		}

		if (free_port)
			return port;
	}

	return -1;
}

/*
 * conf_line_sets
 *		Does this postgresql.auto.conf line assign the named parameter?
 */
static bool
conf_line_sets(const char *line, const char *name)
{
	size_t		len = strlen(name);

	while (*line == ' ' || *line == '\t')
		line++;
	if (pg_strncasecmp(line, name, len) != 0)
		return false;
	line += len;
	while (*line == ' ' || *line == '\t')
		line++;
	return *line == '=';
}

/*
 * next_conf_line
 *		Return the start of the line after the one at line, or the end of the
 *		string if line is the last one.
 */
static const char *
next_conf_line(const char *line)
{
	const char *nl = strchr(line, '\n');

	return nl ? nl + 1 : line + strlen(line);
}

/*
 * write_standby_settings
 *		Rewrite the standby's postgresql.auto.conf -- our own copy, which
 *		pg_basebackup -R then appended primary_conninfo and primary_slot_name
 *		to -- so that it runs as our standby rather than as a second us.
 *
 * port and unix_socket_directories are set for the standby.  The latter is
 * copied from the primary's live value: the primary may have got it from the
 * command line (pg_ctl -o "-k /tmp"), which pg_basebackup cannot copy, and
 * the compiled-in default directory is often not writable by the user the
 * cluster runs as.
 *
 * hot_standby_feedback is turned on.  Odoo sends its read-only cursors
 * (db_replica_host/db_replica_port) here, and reports over large tables can
 * run long; without feedback, VACUUM on the primary removes rows they still
 * see and they are cancelled with a recovery conflict.  The xmin it holds
 * back is kept in our slot, which max_slot_wal_keep_size eventually
 * invalidates if the standby dies, so a dead standby does not hold back
 * VACUUM for good.
 *
 * dbblue_create_standby is turned off.  The copied value is ours, on, and
 * the standby only ignores it while in recovery: promoted, it would at once
 * build a standby of its own, and that one another after its own promotion.
 * Whether a promoted standby should get a replica is for the operator to
 * decide, with ALTER SYSTEM.
 *
 * Only the last primary_conninfo and primary_slot_name are kept, which are
 * the ones -R just appended.  Any earlier ones came with the copy -- we were
 * a standby once ourselves, and promoted -- and point at our old primary.
 * They are harmless, since the last assignment wins, but would otherwise
 * pile up one more pair with every link of a chain of promotions.
 *
 * Everything else is kept.  The standby needs settings such as
 * pg_tde.wal_encrypt and default_table_access_method to stay encrypted, and
 * max_connections and friends at least as high as ours to start at all.
 *
 * This runs before every start, not just after the build, so it drops
 * earlier assignments of what it sets instead of adding to them.
 */
static bool
write_standby_settings(const char *dir, int port)
{
	char		path[MAXPGPATH];
	char		tmppath[MAXPGPATH];
	StringInfoData old;
	StringInfoData buf;
	FILE	   *fp;
	char		chunk[1024];
	size_t		nread;
	const char *line;
	const char *last_conninfo = NULL;
	const char *last_slot_name = NULL;
	const char *s;

	snprintf(path, sizeof(path), "%s/postgresql.auto.conf", dir);
	snprintf(tmppath, sizeof(tmppath), "%s.tmp", path);

	initStringInfo(&old);
	initStringInfo(&buf);

	fp = fopen(path, "r");
	if (fp == NULL && errno != ENOENT)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not open \"%s\": %m",
						path)));
		pfree(old.data);
		pfree(buf.data);
		return false;
	}
	if (fp != NULL)
	{
		while ((nread = fread(chunk, 1, sizeof(chunk), fp)) > 0)
			appendBinaryStringInfo(&old, chunk, nread);
		if (ferror(fp))
		{
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("dbblue create standby: could not read \"%s\": %m",
							path)));
			fclose(fp);
			pfree(old.data);
			pfree(buf.data);
			return false;
		}
		fclose(fp);
	}

	for (line = old.data; *line; line = next_conf_line(line))
	{
		if (conf_line_sets(line, "primary_conninfo"))
			last_conninfo = line;
		else if (conf_line_sets(line, "primary_slot_name"))
			last_slot_name = line;
	}

	for (line = old.data; *line; line = next_conf_line(line))
	{
		bool		keep;

		if (conf_line_sets(line, "port") ||
			conf_line_sets(line, "unix_socket_directories") ||
			conf_line_sets(line, "hot_standby_feedback") ||
			conf_line_sets(line, "dbblue_create_standby"))
			keep = false;
		else if (conf_line_sets(line, "primary_conninfo"))
			keep = (line == last_conninfo);
		else if (conf_line_sets(line, "primary_slot_name"))
			keep = (line == last_slot_name);
		else
			keep = true;

		if (keep)
			appendBinaryStringInfo(&buf, line, next_conf_line(line) - line);
	}
	pfree(old.data);

	if (buf.len > 0 && buf.data[buf.len - 1] != '\n')
		appendStringInfoChar(&buf, '\n');

	appendStringInfo(&buf, "port = %d\n", port);
	appendStringInfoString(&buf, "unix_socket_directories = '");
	for (s = Unix_socket_directories ? Unix_socket_directories : ""; *s; s++)
	{
		if (*s == '\'')
			appendStringInfoChar(&buf, '\'');
		appendStringInfoChar(&buf, *s);
	}
	appendStringInfoString(&buf, "'\n");
	appendStringInfoString(&buf, "hot_standby_feedback = on\n");
	appendStringInfoString(&buf, "dbblue_create_standby = off\n");

	fp = fopen(tmppath, "w");
	if (fp == NULL)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not create \"%s\": %m",
						tmppath)));
		pfree(buf.data);
		return false;
	}
	if (fwrite(buf.data, 1, buf.len, fp) != (size_t) buf.len ||
		fclose(fp) != 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not write \"%s\": %m",
						tmppath)));
		unlink(tmppath);
		pfree(buf.data);
		return false;
	}
	pfree(buf.data);

	if (rename(tmppath, path) != 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not rename \"%s\" to \"%s\": %m",
						tmppath, path)));
		unlink(tmppath);
		return false;
	}
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
	char		tmppath[MAXPGPATH];
	FILE	   *fp;
	bool		ok;
	bool		tcp = (ListenAddresses != NULL && ListenAddresses[0] != '\0');
	char	   *socketdir = first_socket_directory();

	/*
	 * The standby takes the same port number over TCP and for its Unix
	 * socket, and the listen_addresses and socket directories of ours.  Name
	 * whichever of the two it has: Odoo's replica should be reached the way
	 * its db_host reaches us, as the standby shares our pg_hba.conf, and on
	 * Debian a TCP connection there asks for the password a socket one does
	 * not.
	 */
	if (tcp && socketdir)
		ereport(LOG,
				(errmsg("dbblue create standby: standby is running on port %d (data directory \"%s\")",
						port, dir),
				 errhint("For Odoo, set db_replica_port = %d, and db_replica_host the way db_host is set: localhost to connect over TCP, or \"%s\" over the Unix socket.",
						 port, socketdir)));
	else if (tcp)
		ereport(LOG,
				(errmsg("dbblue create standby: standby is running on port %d (data directory \"%s\")",
						port, dir),
				 errhint("For Odoo, set db_replica_host = localhost and db_replica_port = %d.",
						 port)));
	else if (socketdir)
		ereport(LOG,
				(errmsg("dbblue create standby: standby is running on port %d (data directory \"%s\")",
						port, dir),
				 errhint("For Odoo, set db_replica_host = \"%s\" and db_replica_port = %d.",
						 socketdir, port)));
	else
		ereport(LOG,
				(errmsg("dbblue create standby: standby is running on port %d (data directory \"%s\")",
						port, dir)));

	if (socketdir)
		pfree(socketdir);

	snprintf(path, sizeof(path), "%s/dbblue_standby.port", DataDir);
	snprintf(tmppath, sizeof(tmppath), "%s.tmp", path);

	/*
	 * Write a temporary file and rename it into place, so that tooling reading
	 * the file sees the old port or the new one, never an empty file.
	 * fopen(path, "w") would empty it at once, and leave it empty if the
	 * write then failed.  syslogger.c writes current_logfiles the same way.
	 */
	fp = fopen(tmppath, "w");
	if (fp == NULL)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not create \"%s\": %m",
						tmppath)));
		return;
	}
	ok = (fprintf(fp, "%d\n", port) >= 0);
	if (fclose(fp) != 0)
		ok = false;
	if (!ok)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not write \"%s\": %m",
						tmppath)));
		unlink(tmppath);
		return;
	}

	if (rename(tmppath, path) != 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not rename \"%s\" to \"%s\": %m",
						tmppath, path)));
		unlink(tmppath);
	}
}

/*
 * reported_standby_port
 *		The port <DataDir>/dbblue_standby.port names, or -1 if it is missing
 *		or holds no number.
 */
static int
reported_standby_port(void)
{
	char		path[MAXPGPATH];
	FILE	   *fp;
	int			port;

	snprintf(path, sizeof(path), "%s/dbblue_standby.port", DataDir);
	fp = fopen(path, "r");
	if (fp == NULL)
		return -1;
	if (fscanf(fp, "%d", &port) != 1)
		port = -1;
	fclose(fp);
	return port;
}

/*
 * standby_pidfile_port
 *		Read, from the postmaster.pid of the standby in dir, the port it
 *		listens on and whether it accepts connections yet.  Returns false if
 *		the file cannot be read or is not complete, as it is for a moment
 *		while the standby starts.
 */
static bool
standby_pidfile_port(const char *dir, int *port, bool *ready)
{
	char		path[MAXPGPATH];
	char		line[MAXPGPATH];
	FILE	   *fp;
	int			lineno = 0;
	bool		have_port = false;
	bool		have_status = false;

	snprintf(path, sizeof(path), "%s/postmaster.pid", dir);
	fp = fopen(path, "r");
	if (fp == NULL)
		return false;

	while (fgets(line, sizeof(line), fp) != NULL)
	{
		lineno++;
		if (lineno == LOCK_FILE_LINE_PORT)
		{
			*port = atoi(line);
			have_port = (*port > 0 && *port <= 65535);
		}
		else if (lineno == LOCK_FILE_LINE_PM_STATUS)
		{
			*ready = (strncmp(line, PM_STATUS_READY, strlen(PM_STATUS_READY)) == 0 ||
					  strncmp(line, PM_STATUS_STANDBY, strlen(PM_STATUS_STANDBY)) == 0);
			have_status = true;
		}
	}
	fclose(fp);

	return have_port && have_status;
}

/*
 * sync_standby_port
 *		Make <DataDir>/dbblue_standby.port name the port the running standby
 *		in dir listens on, once it accepts connections.
 *
 * report_standby_port() runs after a pg_ctl start that succeeds.  But pg_ctl
 * stops waiting after 60 seconds, while a standby with much WAL to replay
 * may take longer, and then comes up with nobody to report it.  The file may
 * also have been removed since, or restored without it.  So every tick that
 * finds the standby running compares the file with the port the standby's
 * own postmaster.pid records, and rewrites it if they differ.
 */
static void
sync_standby_port(const char *dir)
{
	int			port = 0;
	bool		ready = false;

	if (!standby_pidfile_port(dir, &port, &ready) || !ready)
		return;

	if (reported_standby_port() != port)
		report_standby_port(dir, port);
}

/*
 * file_size
 *		Size of the file at path, or 0 if it does not exist.
 */
static off_t
file_size(const char *path)
{
	struct stat st;

	return (stat(path, &st) == 0) ? st.st_size : 0;
}

/*
 * line_has_marker
 *		Does line contain any of the given markers?
 */
static bool
line_has_marker(const char *line, const char *const *markers, int nmarkers)
{
	for (int i = 0; i < nmarkers; i++)
	{
		if (strstr(line, markers[i]) != NULL)
			return true;
	}
	return false;
}

/*
 * remember_line
 *		Append line to ring, which holds the last DBBLUE_STANDBY_OUTPUT_LINES
 *		lines given to it.
 */
static void
remember_line(const char **ring, int *n, const char *line)
{
	if (*n == DBBLUE_STANDBY_OUTPUT_LINES)
	{
		memmove(ring, ring + 1, (DBBLUE_STANDBY_OUTPUT_LINES - 1) * sizeof(*ring));
		ring[DBBLUE_STANDBY_OUTPUT_LINES - 1] = line;
	}
	else
		ring[(*n)++] = line;
}

/*
 * ring_contains
 *		Is this very line already in ring?
 */
static bool
ring_contains(const char **ring, int n, const char *line)
{
	for (int i = 0; i < n; i++)
	{
		if (ring[i] == line)
			return true;
	}
	return false;
}

/*
 * line_pid
 *		The process ID a server log line names, as "[12345]" in the default
 *		log_line_prefix, or 0 if it names none.
 */
static long
line_pid(const char *line)
{
	const char *p = line;

	while ((p = strchr(p, '[')) != NULL)
	{
		char	   *end;
		long		pid;

		p++;
		if (!isdigit((unsigned char) *p))
			continue;
		pid = strtol(p, &end, 10);
		if (*end == ']')
			return pid;
	}
	return 0;
}

/*
 * command_output_excerpt
 *		Return the lines of logfile past offset start that say why a command
 *		failed, palloc'd and joined by newlines, or NULL if there are none.
 *
 * Those are its last error lines -- "pg_basebackup: error: ..." from a client
 * program, "FATAL:  ..." from the standby's own server log, which pg_ctl
 * start sends to the same file -- each with any detail and hint lines that
 * follow it.  Without any, they are its last lines.  The last lines alone
 * would not do: pg_ctl start ends with "Examine the log output." after the
 * standby's FATAL.  Only the end of a long output is read.
 *
 * A server that fails to start often logs the cause at a lower level first,
 * and a FATAL that only sums it up last: each bad setting as "invalid value
 * for parameter", then "configuration file ... contains errors"; "could not
 * bind", then "could not create any TCP/IP sockets".  So a FATAL or PANIC
 * comes with the lines just before it from the same process, as the "[pid]"
 * of the default log_line_prefix tells.  Under a prefix without one, it
 * comes alone.
 */
static char *
command_output_excerpt(const char *logfile, off_t start)
{
	static const char *const error_markers[] = {
		"error:", "ERROR:", "FATAL:", "PANIC:",
	};
	static const char *const followup_markers[] = {
		"detail:", "hint:", "DETAIL:", "HINT:",
	};
	static const char *const fatal_markers[] = {
		"FATAL:", "PANIC:",
	};
	const char *errors[DBBLUE_STANDBY_OUTPUT_LINES];
	const char *last[DBBLUE_STANDBY_OUTPUT_LINES];
	int			nerrors = 0;
	int			nlast = 0;
	bool		prev_error = false;
	const char **pick;
	int			npick;
	int			fd;
	struct stat st;
	off_t		from;
	ssize_t		nread;
	char	   *buf;
	char	   *line;
	StringInfoData out;

	fd = open(logfile, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		return NULL;
	if (fstat(fd, &st) != 0 || st.st_size <= start)
	{
		close(fd);
		return NULL;
	}
	from = Max(start, st.st_size - DBBLUE_STANDBY_OUTPUT_SCAN);
	buf = palloc(st.st_size - from + 1);
	nread = pg_pread(fd, buf, st.st_size - from, from);
	close(fd);
	if (nread <= 0)
	{
		pfree(buf);
		return NULL;
	}
	buf[nread] = '\0';

	line = buf;
	/* a scan that starts after the output does starts mid-line */
	if (from > start)
	{
		char	   *nl = strchr(line, '\n');

		line = nl ? nl + 1 : line + strlen(line);
	}

	while (*line)
	{
		char	   *nl = strchr(line, '\n');
		char	   *next = nl ? nl + 1 : line + strlen(line);

		if (nl)
			*nl = '\0';

		/* the cause this FATAL sums up, logged just before it */
		if (line_has_marker(line, fatal_markers, lengthof(fatal_markers)))
		{
			long		pid = line_pid(line);
			int			first = nlast;

			while (pid != 0 && first > 0 &&
				   line_pid(last[first - 1]) == pid &&
				   !ring_contains(errors, nerrors, last[first - 1]))
				first--;
			for (int i = first; i < nlast; i++)
				remember_line(errors, &nerrors, last[i]);
		}

		if (line_has_marker(line, error_markers, lengthof(error_markers)) ||
			(prev_error &&
			 line_has_marker(line, followup_markers, lengthof(followup_markers))))
		{
			remember_line(errors, &nerrors, line);
			prev_error = true;
		}
		else
			prev_error = false;

		if (strspn(line, " \t\r") < strlen(line))
			remember_line(last, &nlast, line);

		line = next;
	}

	pick = nerrors > 0 ? errors : last;
	npick = nerrors > 0 ? nerrors : nlast;
	if (npick == 0)
	{
		pfree(buf);
		return NULL;
	}

	initStringInfo(&out);
	for (int i = 0; i < npick; i++)
	{
		if (i > 0)
			appendStringInfoChar(&out, '\n');
		appendBinaryStringInfo(&out, pick[i],
							   Min(strlen(pick[i]), DBBLUE_STANDBY_OUTPUT_LINE_MAX));
	}
	pfree(buf);
	return out.data;
}

/*
 * run_shell
 *		Run a command via system(), appending its output to logfile.
 *		Returns true on exit status 0.
 *
 * On failure, the warning quotes what the command said about it, so that
 * the reason -- a role that does not exist, a pg_hba.conf that refuses it,
 * a full disk -- shows in the server log, and not only in logfile.
 *
 * Once shutdown has been requested, nothing new is started, and false is
 * returned.  The postmaster cannot finish shutting down until we exit.  It
 * signals our whole process group, so a command already running when the
 * shutdown begins is killed along with us; but one started after that is
 * never signalled, and would run to completion, a whole base backup perhaps,
 * with the shutdown waiting on it.
 */
static bool
run_shell(const char *cmd, const char *logfile, const char *what)
{
	char	   *full;
	char	   *qlog;
	int			rc;
	struct stat st;
	off_t		logsize;

	if (ShutdownRequestPending)
		return false;

	qlog = shell_quote(logfile);
	full = psprintf("%s >> %s 2>&1", cmd, qlog);

	/* the command's output is what it appends to logfile from here on */
	logsize = file_size(logfile);

	fflush(NULL);
	pgstat_report_wait_start(WAIT_EVENT_DBBLUE_CREATE_STANDBY_MAIN);
	rc = system(full);
	pgstat_report_wait_end();

	/* killed by the shutdown signal, which is no failure worth a warning */
	if (rc != 0 && ShutdownRequestPending)
	{
		ereport(LOG,
				(errmsg("dbblue create standby: %s interrupted by shutdown",
						what)));
		pfree(qlog);
		pfree(full);
		return false;
	}

	if (rc != 0)
	{
		char	   *reason = wait_result_to_str(rc);
		char	   *output = command_output_excerpt(logfile, logsize);

		if (output != NULL)
			ereport(WARNING,
					(errmsg("dbblue create standby: %s failed: %s",
							what, reason),
					 errdetail("%s\nCommand: %s", output, full),
					 errhint("See \"%s\" for the full output.", logfile)));
		else if (stat(logfile, &st) != 0)
			ereport(WARNING,
					(errmsg("dbblue create standby: %s failed: %s",
							what, reason),
					 errdetail("Command: %s", full),
					 errhint("Its output could not be written to \"%s\". Check that the directory exists and that the server may write to it.",
							 logfile)));
		else
			ereport(WARNING,
					(errmsg("dbblue create standby: %s failed: %s",
							what, reason),
					 errdetail("Command: %s", full),
					 errhint("See \"%s\" for details.", logfile)));

		if (output)
			pfree(output);
		pfree(reason);
		pfree(qlog);
		pfree(full);
		return false;
	}

	pfree(qlog);
	pfree(full);
	return true;
}

/*
 * replication_host
 *		Host the client programs we run against ourselves connect to: our
 *		first Unix socket directory, or localhost if we have none.
 *
 * The socket gets the "local replication" entry of pg_hba.conf, which is
 * peer both with initdb's defaults and with Debian's pg_createcluster.  The
 * latter makes TCP replication connections ask for a password, which -w
 * cannot supply.  pg_basebackup -R copies this host into the standby's
 * primary_conninfo, so the standby streams over the same socket.  Result is
 * palloc'd.
 */
static char *
replication_host(void)
{
	char	   *host = first_socket_directory();

	return host ? host : pstrdup("localhost");
}

/*
 * replication_conn_opts
 *		Connection options for the client programs we run against ourselves
 *		(pg_basebackup, pg_receivewal).  An empty dbblue_standby_user means
 *		"connect as the OS user", which works out of the box with initdb's
 *		default replication pg_hba entries.  Result is palloc'd.
 */
static char *
replication_conn_opts(void)
{
	char	   *host = replication_host();
	char	   *qhost = shell_quote(host);
	char	   *quser;
	char	   *opts;

	if (dbblue_standby_user == NULL || dbblue_standby_user[0] == '\0')
		opts = psprintf("-h %s -p %d", qhost, PostPortNumber);
	else
	{
		quser = shell_quote(dbblue_standby_user);
		opts = psprintf("-h %s -p %d -U %s", qhost, PostPortNumber, quser);
		pfree(quser);
	}

	pfree(qhost);
	pfree(host);
	return opts;
}

/*
 * standby_server_opts
 *		Server options pg_ctl passes to the standby's postgres, already quoted
 *		for the shell pg_ctl starts it through.  Result is palloc'd.
 *
 * pg_basebackup copies only the data directory, so the standby gets our
 * configuration files only where initdb leaves them, inside it.  Debian's
 * pg_createcluster moves them to /etc/postgresql/<version>/<cluster>, and
 * a standby started on the bare copy fails for want of postgresql.conf.  So
 * the standby is pointed at our own files, which also keeps the settings it
 * must match, such as max_connections, in step with ours.  What it sets
 * differently, port and the like, is in its postgresql.auto.conf, which
 * overrides postgresql.conf; see write_standby_settings().
 *
 * Such a postgresql.conf names our data directory in data_directory, and our
 * PID file in external_pid_file.  Both are overridden here, as options on the
 * command line take precedence over the file.  The standby's PID file goes
 * next to its log, "<dir>.pid"; it cannot be cleared instead, as postgres
 * tries to write even an empty external_pid_file.
 */
static char *
standby_server_opts(const char *dir)
{
	StringInfoData buf;
	char	   *pidfile = NULL;

	if (external_pid_file != NULL && external_pid_file[0] != '\0')
		pidfile = psprintf("%s.pid", dir);

	{
		const char *const settings[][2] = {
			{"config_file", ConfigFileName},
			{"hba_file", HbaFileName},
			{"ident_file", IdentFileName},
			{"data_directory", dir},
			{"external_pid_file", pidfile},
		};

		initStringInfo(&buf);
		for (int i = 0; i < lengthof(settings); i++)
		{
			char	   *qvalue;

			/* nothing to override */
			if (settings[i][1] == NULL)
				continue;

			qvalue = shell_quote(settings[i][1]);
			appendStringInfo(&buf, "%s-c %s=%s", buf.len > 0 ? " " : "",
							 settings[i][0], qvalue);
			pfree(qvalue);
		}
	}

	if (pidfile)
		pfree(pidfile);
	return buf.data;
}

/*
 * standby_slot_state
 *		Does our replication slot exist, is it still usable, and is a process
 *		streaming from it?
 */
static StandbySlotState
standby_slot_state(void)
{
	ReplicationSlot *slot;
	StandbySlotState state = STANDBY_SLOT_ABSENT;

	LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
	slot = SearchNamedReplicationSlot(DBBLUE_STANDBY_SLOT, false);
	if (slot != NULL)
	{
		SpinLockAcquire(&slot->mutex);
		if (slot->data.invalidated != RS_INVAL_NONE)
			state = STANDBY_SLOT_INVALIDATED;
		else if (slot->active_proc != INVALID_PROC_NUMBER)
			state = STANDBY_SLOT_ACTIVE;
		else
			state = STANDBY_SLOT_IDLE;
		SpinLockRelease(&slot->mutex);
	}
	LWLockRelease(ReplicationSlotControlLock);

	return state;
}

/*
 * drop_standby_slot
 *		Drop our replication slot if it exists.  Returns true if, afterwards,
 *		there is no such slot.
 *
 * This is only called when we are about to build a standby from scratch, or
 * have just failed to, so an existing slot belongs to a standby that is gone:
 * its folder was removed, or an earlier attempt failed after pg_basebackup -C
 * had created the slot.  Left in place it would hold back WAL for nobody,
 * and it would make the next pg_basebackup -C fail with "already exists".
 *
 * A slot something is still streaming from is not ours to drop -- most
 * likely a standby still running although its folder was deleted under it,
 * or another standby configured with the same slot name.  Refuse, and let
 * the operator sort it out.
 */
static bool
drop_standby_slot(const char *logfile)
{
	char		binpath[MAXPGPATH];
	char	   *qbin;
	char	   *connopts;
	char	   *cmd;
	bool		ok;

	/*
	 * run_shell() would not run pg_receivewal anyway, and the slot can wait:
	 * the next build drops it.
	 */
	if (ShutdownRequestPending)
		return false;

	switch (standby_slot_state())
	{
		case STANDBY_SLOT_ABSENT:
			return true;
		case STANDBY_SLOT_ACTIVE:
			ereport(WARNING,
					(errmsg("dbblue create standby: replication slot \"%s\" is in use by another process",
							DBBLUE_STANDBY_SLOT),
					 errhint("Stop whatever is streaming from it, or drop it with pg_drop_replication_slot('%s').",
							 DBBLUE_STANDBY_SLOT)));
			return false;
		case STANDBY_SLOT_IDLE:
		case STANDBY_SLOT_INVALIDATED:
			break;
	}

	ereport(LOG,
			(errmsg("dbblue create standby: dropping stale replication slot \"%s\"",
					DBBLUE_STANDBY_SLOT)));

	resolve_bin_path("pg_receivewal", binpath, sizeof(binpath));
	qbin = shell_quote(binpath);
	connopts = replication_conn_opts();
	cmd = psprintf("%s %s --drop-slot -S %s -w",
				   qbin, connopts, DBBLUE_STANDBY_SLOT);

	ok = run_shell(cmd, logfile, "pg_receivewal --drop-slot");

	pfree(cmd);
	pfree(connopts);
	pfree(qbin);
	return ok;
}

/*
 * standby_parent_usable
 *		Check that the parent directory of the standby directory dir exists
 *		and that we may write to it, and say why not if not.
 *
 * The standby is built there, and the log of every command we run, which
 * the shell opens before the command starts, is "<dir>.log" next to it.
 * Without this check, a parent we cannot write to shows up as whichever of
 * pg_receivewal, pg_basebackup or pg_ctl happens to run first failing.
 *
 * A missing parent is refused, not created.  ALTER SYSTEM refuses a standby
 * directory that does not exist (see check_dbblue_standby_directory()), and
 * the same must hold for what that check never sees: a value read from the
 * configuration file, the default "<DataDir>_standby", or a directory
 * removed since.  A typo must not quietly grow a new directory tree and a
 * full copy of the database -- which pg_mkdir_p() in seed_standby_tde_dir()
 * would otherwise do, but only when WAL is encrypted.
 */
static bool
standby_parent_usable(const char *dir)
{
	char		parent[MAXPGPATH];
	struct stat st;

	strlcpy(parent, dir, sizeof(parent));
	get_parent_directory(parent);

	if (stat(parent, &st) != 0)
	{
		if (errno == ENOENT)
			ereport(WARNING,
					(errmsg("dbblue create standby: cannot use standby directory \"%s\": parent directory \"%s\" does not exist",
							dir, parent),
					 errhint("Create it, or point dbblue_standby_directory elsewhere.")));
		else
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("dbblue create standby: could not access directory \"%s\": %m",
							parent)));
		return false;
	}

	if (!S_ISDIR(st.st_mode))
	{
		ereport(WARNING,
				(errmsg("dbblue create standby: cannot use standby directory \"%s\": \"%s\" is not a directory",
						dir, parent)));
		return false;
	}

	if (access(parent, W_OK | X_OK) != 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: cannot use standby directory \"%s\": the server cannot write to its parent directory \"%s\": %m",
						dir, parent),
				 errhint("The standby's log file, \"%s.log\", is written there as well.",
						 dir)));
		return false;
	}

	return true;
}

/*
 * stop_standby
 *		Stop the standby postmaster in dir, if it is running.
 */
static bool
stop_standby(const char *dir, const char *logfile)
{
	char		binpath[MAXPGPATH];
	char	   *qbin;
	char	   *qdir;
	char	   *cmd;
	bool		ok;

	if (!standby_is_running(dir))
		return true;

	resolve_bin_path("pg_ctl", binpath, sizeof(binpath));
	qbin = shell_quote(binpath);
	qdir = shell_quote(dir);
	cmd = psprintf("%s -D %s -m fast -w -t 60 stop", qbin, qdir);

	ok = run_shell(cmd, logfile, "pg_ctl stop");

	pfree(cmd);
	pfree(qdir);
	pfree(qbin);
	return ok;
}

/*
 * vet_existing_standby
 *		Check that the cluster in dir is a standby we can keep using.
 *
 * Returns true if it is, or if it was not but has been cleared away for
 * run_standby_tick() to rebuild; false if it is unusable and must be left
 * for the operator, after saying why.
 *
 * Only a standby of ours is ever stopped or deleted here, and only once our
 * replication slot has been invalidated: it fell more than
 * max_slot_wal_keep_size behind, the WAL it needs is gone, and it would
 * otherwise sit there forever -- alive, so never restarted, serving ever
 * staler reads.  Everything it holds is also here, so nothing is lost by
 * building it again.
 *
 * A directory that is not our standby is not ours to delete, whatever it
 * holds: a different cluster may be the last copy of a primary that is gone,
 * and a promoted one is somebody's primary now.  We neither start nor report
 * either, which would send read-only traffic to the wrong server.
 */
static bool
vet_existing_standby(const char *dir, const char *logfile)
{
	uint64		their_sysid = 0;

	switch (standby_identity(dir, &their_sysid))
	{
		case STANDBY_OURS:
			break;
		case STANDBY_PROMOTED:
			forget_standby_port();
			ereport(WARNING,
					(errmsg("dbblue create standby: \"%s\" is no longer a standby of this server",
							dir),
					 errdetail("It has been promoted, and is now a primary in its own right; this server may be the old primary of that failover."),
					 errhint("If this server is to stay the primary, remove \"%s\" to have a new standby built. Otherwise turn dbblue_create_standby off here, and make this server a standby of the new primary.",
							 dir)));
			return false;
		case STANDBY_FOREIGN:
			forget_standby_port();
			ereport(WARNING,
					(errmsg("dbblue create standby: \"%s\" holds a different database cluster, not a standby of this server",
							dir),
					 errdetail("Its system identifier is %" PRIu64 ", this server's is %" PRIu64 ".",
							   their_sysid, GetSystemIdentifier()),
					 errhint("It is most likely the standby of a cluster that this data directory held before. Remove it, or point dbblue_standby_directory elsewhere, to have a standby of this server built.")));
			return false;
		case STANDBY_UNREADABLE:
			/* read_system_identifier() has said why */
			return false;
	}

	if (standby_slot_state() != STANDBY_SLOT_INVALIDATED)
		return true;

	forget_standby_port();
	ereport(WARNING,
			(errmsg("dbblue create standby: replication slot \"%s\" has been invalidated, rebuilding the standby in \"%s\"",
					DBBLUE_STANDBY_SLOT, dir),
			 errdetail("The standby fell more than max_slot_wal_keep_size behind, and the WAL it still needed has been removed.")));

	if (!stop_standby(dir, logfile))
		return false;

	/*
	 * Removing a whole standby takes a while, and the rebuild cannot start
	 * before the next startup anyway; it finds the slot still invalidated.
	 */
	if (ShutdownRequestPending)
		return false;

	/* the directory itself may be the operator's, with its permissions */
	if (!rmtree(dir, false))
	{
		ereport(WARNING,
				(errmsg("dbblue create standby: could not remove the contents of \"%s\"",
						dir)));
		return false;
	}

	/* the build drops the invalidated slot before making a new one */
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
	bool		encrypt_wal;
	char	   *connopts;
	off_t		logsize;

	dir = resolve_standby_directory();
	if (dir == NULL)
	{
		/* Path validation error already reported */
		return false;
	}

	logfile = psprintf("%s.log", dir);

	/*
	 * A standby that exists is only fine if it is ours and can still stream.
	 * Vetting it may also clear it out for a rebuild, below.
	 */
	if (standby_has_data(dir) && !vet_existing_standby(dir, logfile))
	{
		pfree(logfile);
		pfree(dir);
		return false;
	}

	/*
	 * Nothing to do while the standby postmaster is alive, but to make sure
	 * its port is reported.
	 */
	if (standby_is_running(dir))
	{
		sync_standby_port(dir);
		pfree(logfile);
		pfree(dir);
		return true;
	}

	/* Building or starting it, from here on, needs its parent directory. */
	if (!standby_parent_usable(dir))
	{
		pfree(logfile);
		pfree(dir);
		return false;
	}

	if (!standby_has_data(dir))
	{
		struct stat st;

		/*
		 * Refuse to touch an existing directory that is not a cluster:
		 * pg_basebackup would fail on it anyway if non-empty, and we must
		 * never guess about deleting an operator's data.
		 */
		if (stat(dir, &st) == 0)
		{
			/* an empty directory is fine, pg_basebackup accepts it */
			bool		empty;

			if (!standby_dir_is_empty(dir, &empty))
			{
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("dbblue create standby: could not read directory \"%s\": %m",
								dir)));
				pfree(logfile);
				pfree(dir);
				return false;
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

			/*
			 * pg_basebackup keeps the permissions of a directory that already
			 * exists, and the standby then refuses to start in one that group
			 * members may write or others may access -- as one made by hand
			 * with mkdir usually is.
			 */
			if ((st.st_mode & (S_IWGRP | S_IRWXO)) != 0 &&
				chmod(dir, S_IRWXU) != 0)
			{
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("dbblue create standby: could not set permissions on \"%s\": %m",
								dir)));
				pfree(logfile);
				pfree(dir);
				return false;
			}
		}

		/*
		 * With pg_tde WAL encryption on, the walsender hands pg_basebackup
		 * plaintext WAL while the copied pg_tde/wal_keys says that range is
		 * encrypted.  Without -E the standby's own WAL from the copy window
		 * would therefore be plaintext on disk and unreadable to it, and it
		 * could only start by re-fetching that WAL from us -- impossible
		 * once we have recycled it.  -E re-encrypts that WAL with a fresh
		 * key and writes a matching wal_keys, but needs pg_tde seeded first.
		 */
		/*
		 * A fresh standby gets a fresh slot.  Any slot of that name left
		 * behind belongs to a standby that no longer exists; see
		 * drop_standby_slot().
		 */
		if (!drop_standby_slot(logfile))
		{
			pfree(logfile);
			pfree(dir);
			return false;
		}

		encrypt_wal = primary_uses_wal_encryption();
		if (encrypt_wal && !seed_standby_tde_dir(dir))
		{
			rmtree(dir, false);
			pfree(logfile);
			pfree(dir);
			return false;
		}

		resolve_bin_path("pg_basebackup", binpath, sizeof(binpath));
		qbin = shell_quote(binpath);
		qdir = shell_quote(dir);
		connopts = replication_conn_opts();

		/*
		 * -R writes standby.signal and primary_conninfo, and with -S also
		 * primary_slot_name; -X stream keeps up with WAL during the copy;
		 * -c fast avoids waiting for a checkpoint; -w forbids password
		 * prompts (there is no terminal to prompt on); -E encrypts the
		 * streamed WAL, see above.
		 *
		 * -C -S creates the replication slot before the copy starts and
		 * streams the copy-window WAL through it.  From then on we keep every
		 * WAL file the standby has not received, so one that was down across
		 * a checkpoint can still catch up instead of looping forever on
		 * "requested WAL segment has already been removed".
		 */
		cmd = psprintf("%s %s -D %s -X stream -c fast%s -C -S %s -R -w",
					   qbin, connopts, qdir,
					   encrypt_wal ? " -E" : "",
					   DBBLUE_STANDBY_SLOT);
		pfree(connopts);

		ereport(LOG,
				(errmsg("dbblue create standby: creating standby in \"%s\" from primary on port %d",
						dir, PostPortNumber)));

		ok = run_shell(cmd, logfile, "pg_basebackup");
		pfree(cmd);
		pfree(qbin);
		pfree(qdir);

		if (!ok)
		{
			/*
			 * pg_basebackup removes what it wrote, but a crash or kill can
			 * leave a partial copy, and the seeded pg_tde must not survive
			 * either way.  The directory was empty, or held only our seed,
			 * when we started, so clearing its contents loses nothing.
			 */
			if (stat(dir, &st) == 0)
				rmtree(dir, false);

			/*
			 * -C creates the slot before the copy starts, so a failure after
			 * that point leaves it behind, holding WAL for a standby that
			 * does not exist.
			 */
			(void) drop_standby_slot(logfile);
			pfree(logfile);
			pfree(dir);
			return false;
		}
	}

	/*
	 * Pick a free port now, just before starting: 5433 may have been free
	 * when the standby was first created but taken since.
	 *
	 * Something may still take it before the standby binds it.  No probe
	 * can rule that out, as the port is free until somebody binds it; the
	 * standby then fails to start, and the next tick picks another port.
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

	if (!write_standby_settings(dir, port))
	{
		pfree(logfile);
		pfree(dir);
		return false;
	}

	/*
	 * The port may differ from the last start's, and until the standby is up
	 * there is none to report: tooling must not be sent to the old one.
	 */
	forget_standby_port();

	resolve_bin_path("pg_ctl", binpath, sizeof(binpath));
	qbin = shell_quote(binpath);
	qdir = shell_quote(dir);
	{
		char	   *qlog = shell_quote(logfile);
		char	   *opts = standby_server_opts(dir);
		char	   *qopts = shell_quote(opts);

		cmd = psprintf("%s -D %s -l %s -o %s -w -t 60 start",
					   qbin, qdir, qlog, qopts);
		pfree(qopts);
		pfree(opts);
		pfree(qlog);
	}

	logsize = file_size(logfile);
	ok = run_shell(cmd, logfile, "pg_ctl start");
	pfree(cmd);
	pfree(qbin);
	pfree(qdir);

	/*
	 * pg_ctl start also succeeds when the server exits during startup while
	 * its control file says it was shut down in recovery, taking that for a
	 * stop at a recovery target.  But that is what a standby we stopped
	 * cleanly says, whatever then kept it from starting: a configuration
	 * error, or a port taken since we picked it.  So check it is up.
	 */
	if (ok && !standby_is_running(dir))
	{
		char	   *output = command_output_excerpt(logfile, logsize);

		ereport(WARNING,
				(errmsg("dbblue create standby: standby in \"%s\" exited during startup",
						dir),
				 output ? errdetail("%s", output) : 0,
				 errhint("See \"%s\" for the full output.", logfile)));
		if (output)
			pfree(output);
		ok = false;
	}

	/*
	 * pg_ctl also fails when it stops waiting after 60 seconds, but the
	 * standby goes on starting, and sync_standby_port() reports its port
	 * once it is up.  Say so, lest the warning read as a dead standby.
	 */
	if (!ok && !ShutdownRequestPending && standby_is_running(dir))
		ereport(LOG,
				(errmsg("dbblue create standby: standby in \"%s\" is still starting; its port will be reported once it accepts connections",
						dir)));

	if (ok)
		report_standby_port(dir, port);

	pfree(logfile);
	pfree(dir);
	return ok;
}

/*
 * check_dbblue_standby_directory
 *		GUC check hook for dbblue_standby_directory.
 *
 * The path is always canonicalized, so that a trailing slash does not put the
 * standby's "<dir>.log" and "<dir>.pid" inside the standby directory, where
 * pg_basebackup would find it not empty.
 *
 * When the value is set with ALTER SYSTEM, it must also not be our data
 * directory or lie inside it, and the directory must already exist, belong
 * to us, and be empty or hold a cluster.  That is only checked inside a
 * transaction.  The configuration file is read outside one, at startup and
 * on every reload, and no server must refuse to start over this setting:
 * the standby's directory may be missing because the operator removed it to
 * have the standby rebuilt, as the hints in vet_existing_standby() suggest,
 * and every copy of this cluster, the standby included, carries the value in
 * its postgresql.auto.conf, where it names that copy's own data directory.
 * run_standby_tick() repeats the checks that matter at the time it builds.
 * An empty value, "<DataDir>_standby", is created by pg_basebackup and need
 * not exist.
 */
bool
check_dbblue_standby_directory(char **newval, void **extra, GucSource source)
{
	char	   *dir;
	struct stat st;
	bool		empty;
	bool		ok = false;

	if (*newval == NULL || (*newval)[0] == '\0')
		return true;

	/* canonicalize_path never enlarges the string, so modify it in place */
	canonicalize_path(*newval);

	if (!IsTransactionState())
		return true;

	dir = standby_directory_path(*newval);

	if (strlen(dir) >= MAXPGPATH - 32)
		GUC_check_errdetail("Path \"%s\" is too long (maximum %d bytes).",
							dir, MAXPGPATH - 33);
	else if (path_is_prefix_of_path(DataDir, dir))
	{
		GUC_check_errdetail("Path \"%s\" is the data directory of this server or lies inside it.",
							dir);
		GUC_check_errhint("Every base backup would then copy the standby into itself.");
	}
	else if (stat(dir, &st) != 0)
	{
		if (errno == ENOENT)
		{
			GUC_check_errdetail("Directory \"%s\" does not exist.", dir);
			GUC_check_errhint("Create it first, as the operating-system user the server runs as.");
		}
		else
			GUC_check_errdetail("Could not access directory \"%s\": %m.", dir);
	}
	else if (!S_ISDIR(st.st_mode))
		GUC_check_errdetail("\"%s\" is not a directory.", dir);
	else if (st.st_uid != geteuid())
	{
		GUC_check_errdetail("Directory \"%s\" is not owned by the operating-system user the server runs as.",
							dir);
		GUC_check_errhint("A data directory must be owned by that user, or the standby cannot start in it.");
	}
	else if (access(dir, R_OK | W_OK | X_OK) != 0)
		GUC_check_errdetail("The server cannot write to directory \"%s\": %m.", dir);
	else if (standby_has_data(dir))
		ok = true;
	else if (!standby_dir_is_empty(dir, &empty))
		GUC_check_errdetail("Could not read directory \"%s\": %m.", dir);
	else if (!empty)
	{
		GUC_check_errdetail("Directory \"%s\" is not empty, and does not hold a PostgreSQL cluster.",
							dir);
		GUC_check_errhint("Use an empty directory, or the data directory of an existing standby of this server.");
	}
	else
		ok = true;

	pfree(dir);
	return ok;
}

/*
 * check_dbblue_standby_user
 *		GUC check hook for dbblue_standby_user.
 *
 * pg_basebackup can only use a role that exists, may log in, and has
 * REPLICATION or is a superuser.  As for default_tablespace, that can only be
 * verified inside a transaction, which is to say for ALTER SYSTEM.  A value
 * read from the configuration file is taken on faith, and a bad one shows up
 * as a pg_basebackup failure.  So does a pg_hba.conf that lets the role in
 * only with a password, which no check short of connecting can tell.
 */
bool
check_dbblue_standby_user(char **newval, void **extra, GucSource source)
{
	HeapTuple	roletup;
	Form_pg_authid roleform;
	bool		ok = false;

	/* empty means the operating-system user */
	if (*newval == NULL || (*newval)[0] == '\0')
		return true;

	if (strlen(*newval) >= NAMEDATALEN)
	{
		GUC_check_errdetail("Role names can be at most %d bytes long.",
							NAMEDATALEN - 1);
		return false;
	}

	if (!IsTransactionState() || !OidIsValid(MyDatabaseId))
		return true;

	roletup = SearchSysCache1(AUTHNAME, CStringGetDatum(*newval));
	if (!HeapTupleIsValid(roletup))
	{
		GUC_check_errdetail("Role \"%s\" does not exist.", *newval);
		return false;
	}
	roleform = (Form_pg_authid) GETSTRUCT(roletup);

	if (!roleform->rolcanlogin)
	{
		GUC_check_errdetail("Role \"%s\" is not permitted to log in.", *newval);
		GUC_check_errhint("Grant it with ALTER ROLE %s LOGIN.",
						  quote_identifier(*newval));
	}
	else if (!roleform->rolreplication && !roleform->rolsuper)
	{
		GUC_check_errdetail("Role \"%s\" has neither the REPLICATION nor the SUPERUSER attribute.",
							*newval);
		GUC_check_errhint("Grant it with ALTER ROLE %s REPLICATION.",
						  quote_identifier(*newval));
	}
	else
		ok = true;

	ReleaseSysCache(roletup);
	return ok;
}

/*
 * warn_unlimited_slot_wal
 *		Warn, once per configuration load, if max_slot_wal_keep_size leaves
 *		our replication slot free to retain WAL without limit.
 *
 * initdb's auto-tune sets a limit, but a cluster made without it, or before
 * it did so, has PostgreSQL's default of -1.  A standby that stops streaming
 * then holds back WAL until the disk fills and the primary stops, and as the
 * slot is never invalidated, vet_existing_standby() never rebuilds it.  It is
 * only a warning: some run without a limit on purpose, watching disk space
 * by other means.
 */
static void
warn_unlimited_slot_wal(void)
{
	if (max_slot_wal_keep_size_mb >= 0 || slot_wal_limit_warned)
		return;

	ereport(WARNING,
			(errmsg("dbblue create standby: max_slot_wal_keep_size is -1, so replication slot \"%s\" may retain WAL without limit",
					DBBLUE_STANDBY_SLOT),
			 errdetail("If the standby stops streaming, WAL accumulates in pg_wal until the disk is full and this server stops; the slot is never invalidated, so the standby is never rebuilt either."),
			 errhint("Set a limit, such as twice max_wal_size: ALTER SYSTEM SET max_slot_wal_keep_size = '%" PRId64 "MB'; SELECT pg_reload_conf();",
					 (int64) max_wal_size_mb * 2)));
	slot_wal_limit_warned = true;
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
			/* and repeat the warning if the limit is still missing */
			slot_wal_limit_warned = false;
		}

		/*
		 * Never act while in recovery: a standby must not spawn standbys of
		 * its own just because it inherited dbblue_create_standby=on from
		 * the primary's configuration.
		 */
		if (dbblue_create_standby && !RecoveryInProgress())
		{
			/* even once we have given up, as the slot then holds back WAL */
			warn_unlimited_slot_wal();

			if (consecutive_failures < DBBLUE_STANDBY_MAX_FAILURES)
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
