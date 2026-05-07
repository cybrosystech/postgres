/*-------------------------------------------------------------------------
 *
 * auto_tune.c
 *	  Hardware detection and tuning formulas for initdb's auto-tune feature.
 *
 * Detection covers Linux/macOS (POSIX sysconf) and Windows
 * (GlobalMemoryStatusEx / GetSystemInfo).  Formulas follow the well-known
 * PGTune profile, parameterised by workload.  Memory values are produced
 * in kilobytes so initdb can format them with a "kB"/"MB"/"GB" suffix.
 *
 * src/bin/initdb/auto_tune.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <limits.h>
#include <string.h>
#include <unistd.h>
#include "utils/elog.h"

#ifdef WIN32
#include <windows.h>
#endif

#include "auto_tune.h"

#define KB_PER_MB			1024
#define KB_PER_GB			(1024 * 1024)

bool
auto_tune_parse_workload(const char *name, WorkloadProfile *out)
{
	if (name == NULL || *name == '\0')
		return false;

	if (pg_strcasecmp(name, "mixed") == 0)
	{
		ereport(LOG,errmsg("workload : mixed"));
		*out = WORKLOAD_MIXED;
	}
	else if (pg_strcasecmp(name, "oltp") == 0)
	{
		ereport(LOG,errmsg("workload : oltp"));
		*out = WORKLOAD_OLTP;
	}
	else if (pg_strcasecmp(name, "olap") == 0 ||
			 pg_strcasecmp(name, "dw") == 0 ||
			 pg_strcasecmp(name, "warehouse") == 0)
	{
		ereport(LOG,errmsg("workload : olap"));
		*out = WORKLOAD_OLAP;
	}	
	else if (pg_strcasecmp(name, "web") == 0)
	{
		ereport(LOG,errmsg("workload : web"));
		*out = WORKLOAD_WEB;
	}
	else if (pg_strcasecmp(name, "desktop") == 0)
	{
		ereport(LOG,errmsg("workload : desktop"));
		*out = WORKLOAD_DESKTOP;
	}
	else
		return false;

	return true;
}

/*
 * Detect total physical RAM.  Returns 0 on failure.
 */
static uint64
detect_total_ram(void)
{
#ifdef WIN32
	MEMORYSTATUSEX status;

	status.dwLength = sizeof(status);
	if (GlobalMemoryStatusEx(&status))
		return (uint64) status.ullTotalPhys;
	return 0;
#else
#if defined(_SC_PHYS_PAGES) && defined(_SC_PAGE_SIZE)
	long		pages = sysconf(_SC_PHYS_PAGES);
	long		page_size = sysconf(_SC_PAGE_SIZE);

	if (pages > 0 && page_size > 0)
		
		return (uint64) pages * (uint64) page_size;
		ereport(LOG,errmsg("total ram : %d",pages * page_size));
#endif
	return 0;
#endif
}

/*
 * Detect online CPU count.  Returns 0 on failure.
 */
static int
detect_cpu_count(void)
{
#ifdef WIN32
	SYSTEM_INFO si;

	GetSystemInfo(&si);
	if (si.dwNumberOfProcessors > 0)
		return (int) si.dwNumberOfProcessors;
	return 0;
#else
#ifdef _SC_NPROCESSORS_ONLN
	long		n = sysconf(_SC_NPROCESSORS_ONLN);
	ereport(LOG,errmsg("cpu count : %d",n));
	if (n > 0)
		return (int) n;
#endif
	return 0;
#endif
}

/*
 * Best-effort SSD detection.  On Linux, /sys/block/<dev>/queue/rotational
 * exposes 0 for SSD/NVMe and 1 for spinning disks.  Anywhere we cannot
 * determine the answer, we assume SSD: that is the right default for the
 * hardware most clusters now run on, and the cost of being wrong is just
 * a slightly aggressive random_page_cost.
 */
static bool
detect_ssd(void)
{
#if defined(__linux__)
	const char *candidates[] = {
		"/sys/block/nvme0n1/queue/rotational",
		"/sys/block/sda/queue/rotational",
		"/sys/block/vda/queue/rotational",
		"/sys/block/xvda/queue/rotational",
		NULL
	};
	int			i;

	for (i = 0; candidates[i] != NULL; i++)
	{
		FILE	   *fp = fopen(candidates[i], "r");
		int			val;

		if (fp == NULL)
			continue;
		if (fscanf(fp, "%d", &val) == 1)
		{
			fclose(fp);
			return val == 0;
		}
		fclose(fp);
	}
#endif
	return true;
}

/*
 * Clamp helper.
 */
static int
clamp_int(int v, int lo, int hi)
{
	ereport(LOG,errmsg("clamp : %d : %d : %d",v,lo,hi));
	if (v < lo)
		return lo;
	if (v > hi)
		return hi;
	return v;
}

/*
 * Round a kB value down to the nearest 8 kB boundary so that shared_buffers
 * (which must be a multiple of BLCKSZ, typically 8 kB) is always valid.
 */
static int
round_down_8kb(int kb)
{
	return (kb / 8) * 8;
}

AutoTuneSettings
auto_tune_compute(WorkloadProfile workload, int max_connections)
{
	AutoTuneSettings s;
	uint64		ram_bytes;
	int			cpus;
	bool		ssd;
	int			ram_kb;
	int			shared_kb;
	int			work_mem_base_kb;
	int			parallel_per_gather;

	memset(&s, 0, sizeof(s));
	s.workload = workload;
	s.wal_buffers_kb = -1;		/* let the server auto-derive */

	ram_bytes = detect_total_ram();
	cpus = detect_cpu_count();
	ssd = detect_ssd();

	if (ram_bytes == 0 || cpus <= 0 || max_connections <= 0)
	{
		s.valid = false;
		return s;
	}

	s.total_ram_bytes = ram_bytes;
	s.cpu_count = cpus;
	s.ssd_storage = ssd;

	/*
	 * Convert RAM to kB up front.  Cap at INT_MAX kB (~2 TB) so the int
	 * arithmetic below cannot overflow on enormous machines; the formulas
	 * still produce sensible values at the cap.
	 */
	if (ram_bytes / 1024 > (uint64) INT_MAX)
		ram_kb = INT_MAX;
	else
		ram_kb = (int) (ram_bytes / 1024);

	/*
	 * shared_buffers: 25% of RAM for server workloads, 6.25% for desktop.
	 * No upper cap — the server's own limits will reject anything truly
	 * unreasonable, and initdb re-probes the value before committing to it.
	 */
	if (workload == WORKLOAD_DESKTOP)
		shared_kb = ram_kb / 16;
	else
		shared_kb = ram_kb / 4;

	/* Floor at 128 MB (the upstream default) so we never tune *down* a
	 * tiny VM into uselessness. */
	if (shared_kb < 128 * KB_PER_MB)
		shared_kb = 128 * KB_PER_MB;
	s.shared_buffers_kb = round_down_8kb(shared_kb);

	/* effective_cache_size: planner hint about OS + PG cache combined. */
	if (workload == WORKLOAD_DESKTOP)
		s.effective_cache_size_kb = ram_kb / 4;
	else
		s.effective_cache_size_kb = (int) ((int64) ram_kb * 3 / 4);

	/* maintenance_work_mem: bigger for analytics, capped to avoid waste. */
	{
		int			mwm_kb;
		int			cap_kb;

		if (workload == WORKLOAD_OLAP)
		{
			mwm_kb = ram_kb / 8;
			cap_kb = 4 * KB_PER_GB;
		}
		else
		{
			mwm_kb = ram_kb / 16;
			cap_kb = 2 * KB_PER_GB;
		}
		s.maintenance_work_mem_kb = clamp_int(mwm_kb, 64, cap_kb);
	}

	/*
	 * Parallelism: leave room for non-parallel workers and the leader.
	 * On a 1-CPU host we still allow per_gather = 1 (a single parallel
	 * worker beats serial for big sequential scans).
	 */
	s.max_worker_processes = clamp_int(cpus, 8, 1024);
	s.max_parallel_workers = clamp_int(cpus, 8, 1024);
	parallel_per_gather = cpus / 2;
	if (parallel_per_gather < 2)
		parallel_per_gather = (cpus >= 2) ? 2 : 1;
	s.max_parallel_workers_per_gather = parallel_per_gather;
	s.max_parallel_maintenance_workers = clamp_int(cpus / 2, 2, 16);

	/*
	 * work_mem: divide the RAM not reserved for shared_buffers among the
	 * concurrent operations a server might run (max_connections * per-conn
	 * factor * parallel workers).  Then scale by workload — analytical
	 * workloads run fewer-but-bigger sorts, web workloads run many small
	 * ones.  PGTune uses similar multipliers.
	 */
	{
		int			divisor;
		int			work_mem_kb;

		divisor = max_connections * 3 * parallel_per_gather;
		if (divisor < 1)
			divisor = 1;
		work_mem_base_kb = (ram_kb - s.shared_buffers_kb) / divisor;
		if (work_mem_base_kb < 64)
			work_mem_base_kb = 64;	/* minimum supported by the GUC */

		switch (workload)
		{
			case WORKLOAD_WEB:
			case WORKLOAD_OLTP:
				work_mem_kb = work_mem_base_kb;
				break;
			case WORKLOAD_MIXED:
			case WORKLOAD_OLAP:
				work_mem_kb = work_mem_base_kb / 2;
				break;
			case WORKLOAD_DESKTOP:
				work_mem_kb = work_mem_base_kb / 6;
				break;
			default:
				work_mem_kb = work_mem_base_kb / 2;
				break;
		}
		if (work_mem_kb < 64)
			work_mem_kb = 64;
		s.work_mem_kb = work_mem_kb;
	}

	/* Storage cost model. */
	s.random_page_cost = ssd ? 1.1 : 4.0;
	s.effective_io_concurrency = ssd ? 200 : 2;

	/* Smooth checkpoints over the full interval. */
	s.checkpoint_completion_target = 0.9;

	/* WAL sizing follows workload write intensity. */
	switch (workload)
	{
		case WORKLOAD_WEB:
			s.min_wal_size_mb = 1024;
			s.max_wal_size_mb = 4096;
			break;
		case WORKLOAD_OLTP:
			s.min_wal_size_mb = 2048;
			s.max_wal_size_mb = 8192;
			break;
		case WORKLOAD_OLAP:
			s.min_wal_size_mb = 4096;
			s.max_wal_size_mb = 16384;
			break;
		case WORKLOAD_DESKTOP:
			s.min_wal_size_mb = 100;
			s.max_wal_size_mb = 2048;
			break;
		case WORKLOAD_MIXED:
		default:
			s.min_wal_size_mb = 1024;
			s.max_wal_size_mb = 4096;
			break;
	}

	/* More histogram buckets help analytical query plans. */
	s.default_statistics_target = (workload == WORKLOAD_OLAP) ? 500 : 100;

	s.valid = true;
	return s;
}
