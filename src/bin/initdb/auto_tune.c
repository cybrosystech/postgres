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
#if defined(__linux__)
#include <dirent.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/sysmacros.h>
#endif

#ifdef WIN32
#include <windows.h>
#endif

#include "access/xlog_internal.h"
#include "auto_tune.h"
#include "fe_utils/sysinfo.h"

#define KB_PER_MB			1024
#define KB_PER_GB			(1024 * 1024)

/*
 * Background-worker slots to hold back from the parallel-query budget.
 *
 * max_worker_processes is the whole pool that max_parallel_workers draws from,
 * and this fork keeps a fleet of always-on launchers running in it -- the
 * logical-replication launcher plus the dbblue BRIN, repack, index advisor,
 * audit pruner, backup, create-standby and wait-sampler launchers -- several
 * of which also spawn a per-database dynamic worker during a scan cycle.  Sized
 * from the CPU count alone, the pool would be eaten into by those workers and a
 * parallel query could never obtain its full max_parallel_workers.  So the pool
 * is widened by this many slots above the parallel budget.  It is a deliberate
 * over-estimate (the launchers rarely all run a dynamic worker at once); growing
 * the fleet past it just means an admin may want to raise max_worker_processes.
 */
#define RESERVED_BGWORKER_SLOTS		12

/*
 * Ceiling for the tuned shared_buffers, work_mem and maintenance_work_mem on
 * a 32-bit build.  There the server accepts at most MAX_KILOBYTES =
 * INT_MAX / 1024 kB for work_mem and maintenance_work_mem, 1 kB short of
 * 2 GB, so a 2 GB value would make the server reject postgresql.conf and
 * initdb fail.  The cap is 1 GB rather than MAX_KILOBYTES itself, because a
 * value just under 2 GB would be rounded back up to 2GB by round_memory_kb().
 * shared_buffers gets the same cap: a 32-bit process cannot map much more
 * than that, and a larger value would be refused by initdb's shared memory
 * probe and replaced with 128 MB.  A 64-bit build has no such ceiling.
 */
#if SIZEOF_SIZE_T > 4
#define MAX_TUNED_MEMORY_KB	INT_MAX
#else
#define MAX_TUNED_MEMORY_KB	KB_PER_GB
#endif

/*
 * A parallel hash join keeps its shared hash table in dynamic shared memory,
 * up to work_mem * hash_mem_multiplier (default 2) for each participant.  When
 * /dev/shm cannot hold that many at once, see auto_tune_compute().
 */
#define HASH_MEM_MULTIPLIER			2
#define PARALLEL_HASH_CONCURRENCY	4

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
	if (get_cgroup_path(NULL, relpath, sizeof(relpath)))
	{
		if (scan_cgroup_hierarchy("/sys/fs/cgroup", "memory.max",
								  relpath, limit))
			return true;
	}

	/* cgroup v1, whose memory controller mounts in its own subdirectory. */
	if (get_cgroup_path("memory", relpath, sizeof(relpath)))
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
 * Detect the number of CPUs this cluster may actually use: the online count,
 * or a cgroup CPU quota when one applies and is the tighter of the two.  Sets
 * *from_cgroup so initdb can say which it used.  Returns 0 on failure.
 *
 * As with memory, the quota is the ceiling that matters for sizing a long-lived
 * cluster, not whatever load average happens to hold during initdb.
 */
static int
detect_cpu_count(bool *from_cgroup)
{
	int			cpus = get_online_cpus();

	*from_cgroup = false;

#if defined(__linux__)
	{
		int			limit = get_cgroup_cpu_limit();

		if (limit > 0 && (cpus == 0 || limit < cpus))
		{
			*from_cgroup = true;
			return limit;
		}
	}
#endif

	return cpus;
}

#if defined(__linux__)
/*
 * Is the disk whose sysfs directory is dir a virtual one?  A virtual disk's
 * rotational flag describes the emulated controller, not the storage behind
 * it: virtio (vd*) and Xen (xvd*) disks, and the SCSI disks of QEMU, VMware,
 * Hyper-V/Azure and Google Cloud, report "rotational" even on SSD storage.
 */
static bool
is_virtual_disk(const char *dir)
{
	static const char *const vendors[] = {"QEMU", "VMware", "Msft", "Google", NULL};
	const char *name = last_dir_separator(dir);
	char		path[MAXPGPATH];
	char		vendor[64];
	FILE	   *fp;
	int			i;

	name = name ? name + 1 : dir;
	if (strncmp(name, "vd", 2) == 0 || strncmp(name, "xvd", 3) == 0)
		return true;

	snprintf(path, sizeof(path), "%s/device/vendor", dir);
	if ((fp = fopen(path, "r")) == NULL)
		return false;
	if (fgets(vendor, sizeof(vendor), fp) == NULL)
		vendor[0] = '\0';
	fclose(fp);
	for (i = 0; vendors[i] != NULL; i++)
		if (strncmp(vendor, vendors[i], strlen(vendors[i])) == 0)
			return true;
	return false;
}

/*
 * Is the block device whose sysfs directory is dir rotational?  Returns 1 if
 * so, 0 if not, -1 if unknown.  A partition is judged by its disk, and a
 * device-mapper (LVM) or md (RAID) device by the devices under it: rotational
 * if any of them is, since the slowest one sets the pace.
 */
static int
block_device_rotational(const char *dir, int depth)
{
	char		path[MAXPGPATH];
	char		real[PATH_MAX];	/* realpath() needs PATH_MAX */
	DIR		   *d;
	struct dirent *de;
	FILE	   *fp;
	int			result = -1;
	int			val;

	if (depth > 8)
		return -1;

	/* A partition: use the disk it belongs to, its parent directory. */
	snprintf(path, sizeof(path), "%s/partition", dir);
	if (access(path, F_OK) == 0)
	{
		snprintf(path, sizeof(path), "%s/..", dir);
		if (realpath(path, real) == NULL)
			return -1;
		return block_device_rotational(real, depth + 1);
	}

	/* LVM, RAID and the like: look at the devices underneath. */
	snprintf(path, sizeof(path), "%s/slaves", dir);
	if ((d = opendir(path)) != NULL)
	{
		while ((de = readdir(d)) != NULL)
		{
			int			r;

			if (de->d_name[0] == '.')
				continue;
			snprintf(path, sizeof(path), "%s/slaves/%s", dir, de->d_name);
			if (realpath(path, real) == NULL)
				continue;
			r = block_device_rotational(real, depth + 1);
			if (r == 1)
			{
				result = 1;
				break;
			}
			if (r == 0)
				result = 0;
		}
		closedir(d);
		if (result != -1)
			return result;
	}

	if (is_virtual_disk(dir))
		return 0;

	snprintf(path, sizeof(path), "%s/queue/rotational", dir);
	if ((fp = fopen(path, "r")) == NULL)
		return -1;
	if (fscanf(fp, "%d", &val) != 1)
		val = -1;
	fclose(fp);
	return val < 0 ? -1 : (val != 0);
}
#endif

/*
 * Best-effort SSD detection for the disk holding the data directory.  On
 * Linux, find the block device of data_dir and read its rotational flag,
 * through partitions, LVM and RAID (block_device_rotational()), and store the
 * device's name in device.  A rotating disk is reported only for a physical
 * disk that says so; a virtual disk, a filesystem without a single block
 * device (overlayfs, btrfs, NFS, ...), or anything else we cannot determine
 * is assumed to be an SSD, which is what most clusters now run on.
 */
static bool
detect_ssd(const char *data_dir, char *device, size_t devlen)
{
	device[0] = '\0';
#if defined(__linux__)
	{
		struct stat st;
		char		path[MAXPGPATH];
		char		real[PATH_MAX];	/* realpath() needs PATH_MAX */
		int			r;

		if (data_dir == NULL || stat(data_dir, &st) != 0)
			return true;
		snprintf(path, sizeof(path), "/sys/dev/block/%u:%u",
				 major(st.st_dev), minor(st.st_dev));
		if (realpath(path, real) == NULL)
			return true;
		strlcpy(device, last_dir_separator(real) + 1, devlen);
		r = block_device_rotational(real, 0);
		if (r >= 0)
			return r == 0;
	}
#endif
	return true;
}

/*
 * Size of /dev/shm in bytes, or 0 if unknown.  With the posix
 * dynamic_shared_memory_type, Linux's default, dynamic shared memory
 * segments are files there.  Docker gives a container a 64 MB /dev/shm
 * unless --shm-size says otherwise, whatever its --memory limit.
 */
static uint64
detect_dev_shm_size(void)
{
#if defined(__linux__)
	struct statvfs st;

	if (statvfs("/dev/shm", &st) == 0)
		return (uint64) st.f_frsize * st.f_blocks;
#endif
	return 0;
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
auto_tune_compute(int max_connections, const char *data_dir,
				  int wal_segment_size_mb)
{
	AutoTuneSettings s;
	uint64		ram_bytes;
	bool		ram_from_cgroup;
	int			cpus;
	bool		cpu_from_cgroup;
	bool		ssd;
	int			ram_kb;
	int			shared_kb;
	int			parallel_per_gather;

	memset(&s, 0, sizeof(s));

	ram_bytes = detect_total_ram(&ram_from_cgroup);
	cpus = detect_cpu_count(&cpu_from_cgroup);
	ssd = detect_ssd(data_dir, s.storage_device, sizeof(s.storage_device));

	if (ram_bytes == 0 || cpus <= 0 || max_connections <= 0)
	{
		s.valid = false;
		return s;
	}

	s.total_ram_bytes = ram_bytes;
	s.ram_from_cgroup = ram_from_cgroup;
	s.cpu_count = cpus;
	s.cpu_from_cgroup = cpu_from_cgroup;
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
	shared_kb = Min(shared_kb, MAX_TUNED_MEMORY_KB);
	s.shared_buffers_kb = round_memory_kb(shared_kb);

	/* Planner hint: combined OS + PG cache. */
	s.effective_cache_size_kb = round_memory_kb((int) ((int64) ram_kb * 3 / 4));

	/*
	 * maintenance_work_mem: governs VACUUM, CREATE INDEX, ALTER TABLE.
	 * Odoo's module updates create/rebuild many indexes, so be generous.
	 * RAM/16 capped at 2 GB matches PGTune; 1 GB on 32-bit builds, see
	 * MAX_TUNED_MEMORY_KB.
	 */
	s.maintenance_work_mem_kb = round_memory_kb(clamp_int(ram_kb / 16, KB_PER_MB,
														Min(2 * KB_PER_GB,
															MAX_TUNED_MEMORY_KB)));

	/*
	 * Parallelism.  Odoo rarely benefits from very wide parallel workers
	 * on a single query (most queries are short OLTP), but reports do —
	 * so we allow up to CPU/2 per gather, capped at 4 to keep one big
	 * report from starving everyone else.
	 */
	s.max_parallel_workers = clamp_int(cpus, 8, 1024);
	/*
	 * max_worker_processes is the whole pool max_parallel_workers draws from,
	 * so it must be wider than the parallel budget by the slots this fork's
	 * always-on launchers (and their dynamic workers) occupy; otherwise those
	 * workers starve parallel query of the slots it was just promised.  See
	 * RESERVED_BGWORKER_SLOTS.
	 */
	s.max_worker_processes = clamp_int(s.max_parallel_workers +
									   RESERVED_BGWORKER_SLOTS, 8, 1024);
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
		work_mem_kb = Min(work_mem_kb, MAX_TUNED_MEMORY_KB);
		s.work_mem_kb = round_memory_kb(work_mem_kb);
	}

	/*
	 * min_dynamic_shared_memory: a parallel hash join's shared hash table can
	 * take work_mem * HASH_MEM_MULTIPLIER per participant from dynamic shared
	 * memory, which by default is files in /dev/shm.  In a container /dev/shm
	 * is often 64 MB, too small even for one such join at the work_mem above,
	 * and the query fails with "No space left on device".  When /dev/shm
	 * cannot hold PARALLEL_HASH_CONCURRENCY of them, reserve that much at
	 * startup inside the main shared memory segment instead, which is
	 * anonymous memory bounded by the container's memory limit, not by
	 * /dev/shm; segments then come from there first.  Capped at RAM/16.
	 */
	s.dev_shm_bytes = detect_dev_shm_size();
	if (s.dev_shm_bytes > 0)
	{
		int64		need_kb = (int64) s.work_mem_kb * HASH_MEM_MULTIPLIER *
			(parallel_per_gather + 1) * PARALLEL_HASH_CONCURRENCY;

		if (s.dev_shm_bytes / 1024 < (uint64) need_kb)
		{
			need_kb = Min(need_kb, ram_kb / 16);
			need_kb = Min(need_kb, MAX_TUNED_MEMORY_KB / 4);
			s.min_dynamic_shared_memory_mb =
				round_memory_kb((int) need_kb) / KB_PER_MB;
		}
	}

	/*
	 * Storage cost model.  A rotating disk keeps the upstream default
	 * effective_io_concurrency (16, DEFAULT_EFFECTIVE_IO_CONCURRENCY) rather
	 * than going below it, so a wrong guess never reads ahead less than an
	 * untuned server.
	 */
	s.random_page_cost = ssd ? 1.1 : 4.0;
	s.effective_io_concurrency = ssd ? 200 : 16;

	/* Smooth checkpoints over the full interval. */
	s.checkpoint_completion_target = 0.9;

	/*
	 * WAL sizing: Odoo writes constantly (every business action is a
	 * transaction) so size for sustained throughput without forcing frequent
	 * checkpoints.  Scaled to RAM rather than fixed, so a small VM does not
	 * reserve up to 8 GB of WAL it has neither the write rate nor the disk for,
	 * while a well-provisioned host still reaches the 8 GB that busy Odoo wants.
	 * min_wal_size trails at a quarter of the ceiling, the usual ratio.
	 */
	s.max_wal_size_mb = clamp_int((int) ((int64) ram_kb / 4 / KB_PER_MB),
								  1024, 8192);
	s.min_wal_size_mb = s.max_wal_size_mb / 4;

	/*
	 * Never below what initdb writes untuned, DEFAULT_MIN_WAL_SEGS and
	 * DEFAULT_MAX_WAL_SEGS segments.  The server requires at least two
	 * segments of each, which with --wal-segsize of 256 MB or more the RAM
	 * based values above can miss, and initdb would fail.  With the default
	 * 16 MB segments those floors are 80 MB and 1 GB, below the values above.
	 */
	s.min_wal_size_mb = Max(s.min_wal_size_mb,
							DEFAULT_MIN_WAL_SEGS * wal_segment_size_mb);
	s.max_wal_size_mb = Max(s.max_wal_size_mb,
							DEFAULT_MAX_WAL_SEGS * wal_segment_size_mb);

	/*
	 * Cap the WAL a replication slot may hold back.  The default of -1 lets a
	 * slot whose standby never returns (dbblue_create_standby's, or any other)
	 * retain WAL until the disk fills and the primary stops.  Two
	 * max_wal_size cycles of headroom still lets a standby that was down
	 * across a couple of checkpoints catch up; beyond that the slot is
	 * invalidated and the standby must be rebuilt, but the primary survives.
	 */
	s.max_slot_wal_keep_size_mb = s.max_wal_size_mb * 2;

	/*
	 * Odoo joins many tables (res_partner, res_users, account_move_line, ...)
	 * with selective filters where better histograms pay off.  Bump from
	 * the upstream default of 100.
	 */
	s.default_statistics_target = 200;

	s.valid = true;
	return s;
}
