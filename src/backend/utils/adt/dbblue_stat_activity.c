/*-------------------------------------------------------------------------
 *
 * dbblue_stat_activity.c
 *	  Extended per-backend activity statistics for dbblue.
 *
 * This file implements dbblue_stat_get_backend_resources(), a set-returning
 * function that reports, for every reportable backend, its accumulated CPU
 * time, current resident memory, and cumulative per-backend I/O (split into
 * I/O against shared relations vs. node-local temporary relations).
 *
 * It is the data source behind the dbblue_stat_activity system view, which
 * LEFT JOINs pg_stat_activity to this function on pid.  The design mirrors
 * PolarDB's polar_stat_activity, which surfaces the same class of
 * per-connection resource data alongside the standard activity columns.
 *
 * CPU and RSS are read from /proc/<pid>/stat on Linux; on other platforms
 * those columns are reported as NULL.  I/O counters are read from the core
 * per-backend pgstats infrastructure (PGSTAT_KIND_BACKEND), so they are
 * available on every platform and require no extra sampling.
 *
 * Portions Copyright (c) 2026, dbblue / Cybrosys Technologies.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/dbblue_stat_activity.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_authid_d.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"

#ifdef __linux__
#include <unistd.h>
#endif

/*
 * Same visibility rule pg_stat_get_activity() uses for its sensitive columns:
 * the caller must be a member of the backend's role or of pg_read_all_stats.
 */
#define HAS_PGSTAT_PERMISSIONS(role)	\
	(has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS) || \
	 has_privs_of_role(GetUserId(), role))

#ifdef __linux__
/*
 * Read accumulated CPU time (user and system, in milliseconds) and current
 * resident set size (in kilobytes) for the given pid from /proc/<pid>/stat.
 *
 * Returns true on success.  The comm field (field 2) may itself contain
 * spaces and parentheses, so we scan past the final ')' before tokenizing
 * the remaining space-separated fields.  Relative to that point, field 3
 * (state) is token 0, hence utime (field 14) is token 11, stime (field 15)
 * is token 12, and rss in pages (field 24) is token 21.
 */
static bool
read_proc_resources(int pid, int64 *cpu_user_ms, int64 *cpu_sys_ms,
					int64 *rss_kb)
{
	char		path[64];
	char		buf[1024];
	FILE	   *fp;
	char	   *p;
	size_t		len;
	long		clk_tck;
	long		page_kb;
	unsigned long utime = 0;
	unsigned long stime = 0;
	long		rss_pages = 0;
	char	   *tok;
	int			field;

	snprintf(path, sizeof(path), "/proc/%d/stat", pid);
	fp = AllocateFile(path, "r");
	if (fp == NULL)
		return false;

	len = fread(buf, 1, sizeof(buf) - 1, fp);
	FreeFile(fp);
	if (len == 0)
		return false;
	buf[len] = '\0';

	/* Skip to just after the final ')' that closes the comm field. */
	p = strrchr(buf, ')');
	if (p == NULL || *(p + 1) == '\0')
		return false;
	p++;

	/*
	 * Tokenize the remainder.  token 0 == field 3 (state).  We need token 11
	 * (utime), token 12 (stime) and token 21 (rss, in pages).
	 */
	field = 0;
	for (tok = strtok(p, " \t\n"); tok != NULL; tok = strtok(NULL, " \t\n"))
	{
		switch (field)
		{
			case 11:
				utime = strtoul(tok, NULL, 10);
				break;
			case 12:
				stime = strtoul(tok, NULL, 10);
				break;
			case 21:
				rss_pages = strtol(tok, NULL, 10);
				break;
			default:
				break;
		}
		field++;
		if (field > 21)
			break;
	}
	if (field <= 21)
		return false;

	clk_tck = sysconf(_SC_CLK_TCK);
	if (clk_tck <= 0)
		clk_tck = 100;			/* sane fallback */
	page_kb = sysconf(_SC_PAGESIZE) / 1024;
	if (page_kb <= 0)
		page_kb = 4;

	*cpu_user_ms = (int64) utime * 1000 / clk_tck;
	*cpu_sys_ms = (int64) stime * 1000 / clk_tck;
	*rss_kb = (int64) rss_pages * page_kb;
	return true;
}
#endif							/* __linux__ */

/*
 * Sum the byte counters of one I/O object (across all I/O contexts) for a
 * given I/O operation.
 */
static uint64
sum_io_bytes(const PgStat_BktypeIO *io, IOObject io_object, IOOp io_op)
{
	uint64		total = 0;
	int			ctx;

	for (ctx = 0; ctx < IOCONTEXT_NUM_TYPES; ctx++)
		total += io->bytes[io_object][ctx][io_op];

	return total;
}

/*
 * dbblue_stat_get_backend_resources
 *
 * Returns one row per reportable backend with its pid and extended resource
 * usage.  Consumed by the dbblue_stat_activity view.
 */
Datum
dbblue_stat_get_backend_resources(PG_FUNCTION_ARGS)
{
#define DBBLUE_BACKEND_RESOURCE_COLS	8
	int			num_backends = pgstat_fetch_stat_numbackends();
	int			curr_backend;

	InitMaterializedSRF(fcinfo, 0);

	/* 1-based index, matching pg_stat_get_activity() */
	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
		Datum		values[DBBLUE_BACKEND_RESOURCE_COLS] = {0};
		bool		nulls[DBBLUE_BACKEND_RESOURCE_COLS] = {0};
		LocalPgBackendStatus *local_beentry;
		PgBackendStatus *beentry;
		ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

		local_beentry = pgstat_get_local_beentry_by_index(curr_backend);
		beentry = &local_beentry->backendStatus;

		/* pid is available to everyone, like in pg_stat_activity */
		values[0] = Int32GetDatum(beentry->st_procpid);

		/* Everything else is gated on the same permission rule. */
		if (HAS_PGSTAT_PERMISSIONS(beentry->st_userid))
		{
			PgStat_Backend *backend_stats;

			/* CPU and RSS (Linux only). */
#ifdef __linux__
			{
				int64		cpu_user_ms = 0;
				int64		cpu_sys_ms = 0;
				int64		rss_kb = 0;

				if (read_proc_resources(beentry->st_procpid, &cpu_user_ms,
										&cpu_sys_ms, &rss_kb))
				{
					values[1] = Int64GetDatum(cpu_user_ms);
					values[2] = Int64GetDatum(cpu_sys_ms);
					values[3] = Int64GetDatum(rss_kb);
				}
				else
				{
					nulls[1] = nulls[2] = nulls[3] = true;
				}
			}
#else
			nulls[1] = nulls[2] = nulls[3] = true;
#endif

			/*
			 * Per-backend cumulative I/O.  shared_* is I/O against shared
			 * relations, local_* is I/O against node-local temp relations.
			 * Writes fold in EXTEND, since growing a relation also emits
			 * bytes to storage.  A NULL fetch means this backend type does
			 * not track I/O stats.
			 */
			backend_stats = pgstat_fetch_stat_backend(local_beentry->proc_number);
			if (backend_stats != NULL)
			{
				const PgStat_BktypeIO *io = &backend_stats->io_stats;

				values[4] = Int64GetDatum((int64)
										  sum_io_bytes(io, IOOBJECT_RELATION, IOOP_READ));
				values[5] = Int64GetDatum((int64)
										  (sum_io_bytes(io, IOOBJECT_RELATION, IOOP_WRITE) +
										   sum_io_bytes(io, IOOBJECT_RELATION, IOOP_EXTEND)));
				values[6] = Int64GetDatum((int64)
										  sum_io_bytes(io, IOOBJECT_TEMP_RELATION, IOOP_READ));
				values[7] = Int64GetDatum((int64)
										  (sum_io_bytes(io, IOOBJECT_TEMP_RELATION, IOOP_WRITE) +
										   sum_io_bytes(io, IOOBJECT_TEMP_RELATION, IOOP_EXTEND)));
			}
			else
			{
				nulls[4] = nulls[5] = nulls[6] = nulls[7] = true;
			}
		}
		else
		{
			/* Not permitted to see this backend's resources. */
			nulls[1] = nulls[2] = nulls[3] = true;
			nulls[4] = nulls[5] = nulls[6] = nulls[7] = true;
		}

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
}
