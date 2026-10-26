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

#include "access/xlog.h"
#include "access/xlog_internal.h"
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
#include "utils/guc.h"
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

	/*
	 * Validate path length: must leave room for suffixes like "/PG_VERSION",
	 * "/postgresql.auto.conf", "/postmaster.pid", etc. Reserve 32 bytes.
	 */
	if (strlen(result) >= MAXPGPATH - 32)
	{
		ereport(ERROR,
				(errmsg("dbblue create standby: directory path is too long (exceeds %d bytes)",
						MAXPGPATH - 32),
				 errdetail("Path: %s", result)));
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
	if (fclose(fp) != 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("dbblue create standby: could not write \"%s\": %m",
						path)));
	}
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
	char	   *host = NULL;

	if (Unix_socket_directories != NULL && Unix_socket_directories[0] != '\0')
	{
		char	   *rawstring = pstrdup(Unix_socket_directories);
		List	   *elemlist;

		if (SplitDirectoriesString(rawstring, ',', &elemlist) &&
			elemlist != NIL)
			host = pstrdup((char *) linitial(elemlist));
		list_free_deep(elemlist);
		pfree(rawstring);
	}

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
 * verify_port_still_free
 *		Double-check that a port is still available to bind after discovery.
 *		Returns true if the port is free, false otherwise.
 *		Used to detect TOCTOU race: port may have been claimed between
 *		find_free_port() and starting the standby.
 */
static bool
verify_port_still_free(int port)
{
	int			sock;
	struct sockaddr_in addr;
	bool		free_port;

	/*
	 * Validate port is within valid TCP port range (1-65535).
	 * Prevents integer overflow when casting to uint16.
	 */
	if (port < 1 || port > 65535)
		return false;

	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0)
		return false;

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = htons((uint16) port);

	free_port = (bind(sock, (struct sockaddr *) &addr, sizeof(addr)) == 0);
	close(sock);

	return free_port;
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

	/* Nothing to do while the standby postmaster is alive. */
	if (standby_is_running(dir))
	{
		pfree(logfile);
		pfree(dir);
		return true;
	}

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
					/*
					 * A lone pg_tde directory is our own seed from an
					 * attempt that died before pg_basebackup could clean
					 * up; seed_standby_tde_dir() replaces it.
					 */
					if (strcmp(de->d_name, ".") != 0 &&
						strcmp(de->d_name, "..") != 0 &&
						strcmp(de->d_name, DBBLUE_TDE_DIR) != 0)
					{
						empty = false;
						break;
					}
				}
				closedir(d);
			}
			else
			{
				/* opendir failed - report the error clearly */
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
	 * Verify the port is still free: it may have been claimed since we
	 * discovered it. If not, don't penalize with a failure count; just
	 * return false to retry on the next tick.
	 */
	if (!verify_port_still_free(port))
	{
		ereport(DEBUG1,
				(errmsg("dbblue create standby: port %d was claimed after discovery; will retry",
						port)));
		pfree(logfile);
		pfree(dir);
		return false;
	}

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
