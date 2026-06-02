/*-------------------------------------------------------------------------
 *
 * auto_tune.c
 *	  Hardware detection and tuning formulas for initdb's auto-tune feature.
 *
 * Detection covers Linux/macOS (POSIX sysconf) and Windows
 * (GlobalMemoryStatusEx / GetSystemInfo).  Formulas are tuned for Odoo
 * workloads: many concurrent short transactions from the ORM, with the
 * occasional heavier report.  Memory values are produced in kilobytes so
 * initdb can format them with a "kB"/"MB"/"GB" suffix.
 *
 * src/bin/initdb/auto_tune.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <limits.h>
#include <string.h>
#include <unistd.h>

#ifdef WIN32
#include <windows.h>
#endif

#include "auto_tune.h"

#define KB_PER_MB			1024
#define KB_PER_GB			(1024 * 1024)

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
auto_tune_compute(int max_connections)
{
	AutoTuneSettings s;
	uint64		ram_bytes;
	int			cpus;
	bool		ssd;
	int			ram_kb;
	int			shared_kb;
	int			parallel_per_gather;

	memset(&s, 0, sizeof(s));

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
	 * shared_buffers: 25% of RAM is the long-standing rule of thumb and
	 * works well for Odoo, whose hot tables (res_partner, sale_order, ...)
	 * benefit from staying resident.  Floor at the upstream default so a
	 * tiny VM is never tuned *down*.
	 */
	shared_kb = ram_kb / 4;
	if (shared_kb < 128 * KB_PER_MB)
		shared_kb = 128 * KB_PER_MB;
	s.shared_buffers_kb = round_down_8kb(shared_kb);

	/* Planner hint: combined OS + PG cache. */
	s.effective_cache_size_kb = (int) ((int64) ram_kb * 3 / 4);

	/*
	 * maintenance_work_mem: governs VACUUM, CREATE INDEX, ALTER TABLE.
	 * Odoo's module updates create/rebuild many indexes, so be generous.
	 * RAM/16 capped at 2 GB matches PGTune.
	 */
	s.maintenance_work_mem_kb = clamp_int(ram_kb / 16, 64, 2 * KB_PER_GB);

	/*
	 * Parallelism.  Odoo rarely benefits from very wide parallel workers
	 * on a single query (most queries are short OLTP), but reports do —
	 * so we allow up to CPU/2 per gather, capped at 4 to keep one big
	 * report from starving everyone else.
	 */
	s.max_worker_processes = clamp_int(cpus, 8, 1024);
	s.max_parallel_workers = clamp_int(cpus, 8, 1024);
	parallel_per_gather = cpus / 2;
	if (parallel_per_gather < 2)
		parallel_per_gather = (cpus >= 2) ? 2 : 1;
	if (parallel_per_gather > 4)
		parallel_per_gather = 4;
	s.max_parallel_workers_per_gather = parallel_per_gather;
	s.max_parallel_maintenance_workers = clamp_int(cpus / 2, 2, 4);

	/*
	 * work_mem: per-operation sort/hash budget.  Odoo's ORM emits joins
	 * with multiple ORDER BY / GROUP BY clauses, so it benefits from a
	 * roomier work_mem than the upstream 4 MB.  We compute the maximum
	 * each operation could safely take if every connection ran in
	 * parallel, then keep the full value (no /2 mixed-workload haircut)
	 * because Odoo's typical concurrency is well below max_connections.
	 */
	{
		int			divisor;
		int			work_mem_kb;

		/*
		 * Use parallel_per_gather as the parallel factor in the divisor —
		 * the budget is shared across the leader and its workers.
		 */
		divisor = max_connections * 3 * parallel_per_gather;
		if (divisor < 1)
			divisor = 1;
		work_mem_kb = (ram_kb - s.shared_buffers_kb) / divisor;
		if (work_mem_kb < 4 * KB_PER_MB)
			work_mem_kb = 4 * KB_PER_MB;	/* never below upstream default */
		s.work_mem_kb = work_mem_kb;
	}

	/* Storage cost model. */
	s.random_page_cost = ssd ? 1.1 : 4.0;
	s.effective_io_concurrency = ssd ? 200 : 2;

	/* Smooth checkpoints over the full interval. */
	s.checkpoint_completion_target = 0.9;

	/*
	 * WAL sizing: Odoo writes constantly (every business action is a
	 * transaction) so size for sustained throughput without forcing
	 * frequent checkpoints.
	 */
	s.min_wal_size_mb = 2048;
	s.max_wal_size_mb = 8192;

	/*
	 * Odoo joins many tables (res_partner, res_users, account_move_line, ...)
	 * with selective filters where better histograms pay off.  Bump from
	 * the upstream default of 100.
	 */
	s.default_statistics_target = 200;

	s.valid = true;
	return s;
}
