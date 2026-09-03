/*-------------------------------------------------------------------------
 *
 * auto_tune.c
 *	  Hardware detection and tuning formulas for initdb's auto-tune feature.
 *
 * Detection covers Linux/macOS (POSIX sysconf) and Windows
 * (GlobalMemoryStatusEx / GetSystemInfo), and on Linux honours a cgroup
 * memory limit when one confines us -- initdb is very often run inside a
 * container, where the host's physical RAM is the wrong number to size a
 * cluster from.  Formulas are tuned for Odoo
 * workloads: many concurrent short transactions from the ORM, with the
 * occasional heavier report.  Memory values are produced in kilobytes,
 * rounded to a whole megabyte, so initdb can format them with an "MB"/"GB"
 * suffix.
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
detect_physical_ram(void)
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

#if defined(__linux__)

/*
 * Read a byte count out of a cgroup control file.  Returns false if the
 * file is absent, unreadable, or does not start with a plain number --
 * which is how cgroup v2 spells "no limit" (the literal string "max").
 */
static bool
read_cgroup_limit(const char *path, uint64 *limit)
{
	FILE	   *fp;
	char		buf[64];
	unsigned long long val;

	fp = fopen(path, "r");
	if (fp == NULL)
		return false;
	if (fgets(buf, sizeof(buf), fp) == NULL)
	{
		fclose(fp);
		return false;
	}
	fclose(fp);

	if (sscanf(buf, "%llu", &val) != 1 || val == 0)
		return false;

	*limit = (uint64) val;
	return true;
}

/*
 * Find this process's path within a cgroup hierarchy by parsing
 * /proc/self/cgroup, whose lines are "<id>:<controllers>:<path>".  Pass
 * controller = NULL for the cgroup v2 unified hierarchy (the line with an
 * empty controller field, "0::<path>"), or a controller name such as
 * "memory" for a v1 hierarchy.  Returns false if there is no such line.
 */
static bool
find_cgroup_path(const char *controller, char *relpath, size_t relpath_size)
{
	FILE	   *fp;
	char		line[MAXPGPATH];
	bool		found = false;

	fp = fopen("/proc/self/cgroup", "r");
	if (fp == NULL)
		return false;

	while (!found && fgets(line, sizeof(line), fp) != NULL)
	{
		char	   *controllers;
		char	   *path;

		/* Split "<id>:<controllers>:<path>" in place. */
		controllers = strchr(line, ':');
		if (controllers == NULL)
			continue;
		*controllers++ = '\0';
		path = strchr(controllers, ':');
		if (path == NULL)
			continue;
		*path++ = '\0';
		path[strcspn(path, "\n")] = '\0';

		if (controller == NULL)
		{
			/* v2 unified hierarchy: the controller field is empty. */
			if (controllers[0] != '\0')
				continue;
		}
		else
		{
			/*
			 * v1: the field is a comma-separated controller list, so match
			 * a whole element rather than a substring ("memory" must not
			 * match "hugetlb,memory_foo").
			 */
			char	   *tok = controllers;
			bool		match = false;

			while (*tok != '\0')
			{
				size_t		len = strcspn(tok, ",");

				if (strncmp(tok, controller, len) == 0 &&
					controller[len] == '\0')
				{
					match = true;
					break;
				}
				tok += len;
				if (*tok == ',')
					tok++;
			}
			if (!match)
				continue;
		}

		strlcpy(relpath, path, relpath_size);
		found = true;
	}

	fclose(fp);
	return found;
}

/*
 * Look for a memory limit in one cgroup hierarchy.  root is where the
 * hierarchy is mounted, filename the control file holding the limit, and
 * relpath this process's cgroup within it.
 *
 * A limit set on any ancestor cgroup binds this process just as tightly as
 * one set on its own, so walk up to the root and keep the smallest value
 * found.  Returns false if no cgroup on the path carries a limit.
 */
static bool
scan_cgroup_hierarchy(const char *root, const char *filename,
					  char *relpath, uint64 *limit)
{
	char		path[MAXPGPATH];
	bool		found = false;
	uint64		best = 0;

	for (;;)
	{
		char	   *slash;
		uint64		val;

		snprintf(path, MAXPGPATH, "%s%s/%s", root, relpath, filename);
		if (read_cgroup_limit(path, &val) && (!found || val < best))
		{
			best = val;
			found = true;
		}

		/* Stop once we have just examined the hierarchy root itself. */
		if (relpath[0] == '\0')
			break;
		slash = strrchr(relpath, '/');
		if (slash == NULL)
			break;
		*slash = '\0';
	}

	if (found)
		*limit = best;
	return found;
}

/*
 * Detect the cgroup memory limit applying to this process, if any.
 * Returns false when unconfined or when the limit cannot be determined.
 *
 * This matters because initdb is very often run inside a memory-limited
 * container.  sysconf(_SC_PHYS_PAGES) reports the *host's* RAM there, and
 * sizing shared_buffers off that would hand the postmaster far more memory
 * than the cgroup permits -- the shared_buffers probe would still pass
 * (Linux overcommits the mapping) and the cluster would then be OOM-killed
 * once the workload touched those pages.
 */
static bool
detect_cgroup_memory_limit(uint64 *limit)
{
	char		relpath[MAXPGPATH];

	/*
	 * cgroup v2.  Inside a cgroup namespace /proc/self/cgroup reports a
	 * path relative to the namespace root, which is exactly how the
	 * hierarchy is mounted, so the two compose directly.
	 */
	if (find_cgroup_path(NULL, relpath, sizeof(relpath)))
	{
		if (scan_cgroup_hierarchy("/sys/fs/cgroup", "memory.max",
								  relpath, limit))
			return true;
	}

	/* cgroup v1, whose memory controller mounts in its own subdirectory. */
	if (find_cgroup_path("memory", relpath, sizeof(relpath)))
	{
		if (scan_cgroup_hierarchy("/sys/fs/cgroup/memory",
								  "memory.limit_in_bytes", relpath, limit))
			return true;
	}

	/*
	 * Last resort: some container runtimes bind-mount the container's own
	 * cgroup directory at the hierarchy root without a matching
	 * /proc/self/cgroup entry.
	 */
	if (read_cgroup_limit("/sys/fs/cgroup/memory.max", limit))
		return true;
	if (read_cgroup_limit("/sys/fs/cgroup/memory/memory.limit_in_bytes", limit))
		return true;

	return false;
}

#endif							/* __linux__ */

/*
 * Detect the amount of RAM this cluster may actually use: physical RAM, or
 * the cgroup memory limit when one applies and is the tighter of the two.
 * Sets *from_cgroup so initdb can say which it used.  Returns 0 on failure.
 *
 * Note the cgroup limit is not compared against MemAvailable or any other
 * momentary free-memory figure: auto-tune is sizing a long-lived cluster,
 * so the ceiling is what matters, not what happens to be free during
 * initdb.
 */
static uint64
detect_total_ram(bool *from_cgroup)
{
	uint64		ram = detect_physical_ram();

	*from_cgroup = false;

#if defined(__linux__)
	{
		uint64		limit;

		/*
		 * An unconfined v1 cgroup reports a huge sentinel rather than
		 * absence, so take the limit only when it really is tighter.
		 */
		if (detect_cgroup_memory_limit(&limit) && (ram == 0 || limit < ram))
		{
			*from_cgroup = true;
			return limit;
		}
	}
#endif

	return ram;
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
 * Round a kB value to a unit a human reads at a glance: whole gigabytes
 * once the value reaches 1 GB, whole megabytes below that (never below
 * 1 MB).  Every tuned memory setting goes through this so initdb reports
 * and writes them as "<n>GB"/"<n>MB" rather than an unwieldy raw kB count.
 *
 * Sub-GB values round down, so a formula's result is never inflated past
 * what the host can back.  At or above 1 GB we round to the *nearest* GB
 * instead: flooring there would throw away up to 1023 MB, which on the
 * settings this tunes (shared_buffers, maintenance_work_mem) is a real
 * loss rather than a rounding artifact.
 *
 * Both a whole MB and a whole GB are multiples of BLCKSZ (8 kB), which
 * shared_buffers requires.
 */
static int
round_memory_kb(int kb)
{
	if (kb < KB_PER_MB)
		return KB_PER_MB;
	if (kb < KB_PER_GB)
		return (kb / KB_PER_MB) * KB_PER_MB;

	/* int64 so the rounding bias cannot overflow near INT_MAX kB. */
	return (int) ((((int64) kb + KB_PER_GB / 2) / KB_PER_GB) * KB_PER_GB);
}

AutoTuneSettings
auto_tune_compute(int max_connections)
{
	AutoTuneSettings s;
	uint64		ram_bytes;
	bool		ram_from_cgroup;
	int			cpus;
	bool		ssd;
	int			ram_kb;
	int			shared_kb;
	int			parallel_per_gather;

	memset(&s, 0, sizeof(s));

	ram_bytes = detect_total_ram(&ram_from_cgroup);
	cpus = detect_cpu_count();
	ssd = detect_ssd();

	if (ram_bytes == 0 || cpus <= 0 || max_connections <= 0)
	{
		s.valid = false;
		return s;
	}

	s.total_ram_bytes = ram_bytes;
	s.ram_from_cgroup = ram_from_cgroup;
	s.cpu_count = cpus;
	s.ssd_storage = ssd;

	/*
	 * Convert the memory budget to kB up front.  Cap at INT_MAX kB (~2 TB)
	 * so the int arithmetic below cannot overflow on enormous machines; the
	 * formulas still produce sensible values at the cap.
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
	s.shared_buffers_kb = round_memory_kb(shared_kb);

	/* Planner hint: combined OS + PG cache. */
	s.effective_cache_size_kb = round_memory_kb((int) ((int64) ram_kb * 3 / 4));

	/*
	 * maintenance_work_mem: governs VACUUM, CREATE INDEX, ALTER TABLE.
	 * Odoo's module updates create/rebuild many indexes, so be generous.
	 * RAM/16 capped at 2 GB matches PGTune.
	 */
	s.maintenance_work_mem_kb = round_memory_kb(clamp_int(ram_kb / 16, KB_PER_MB,
														2 * KB_PER_GB));

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
		s.work_mem_kb = round_memory_kb(work_mem_kb);
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
