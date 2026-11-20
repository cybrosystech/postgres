/*-------------------------------------------------------------------------
 *
 * sysinfo.c
 *	  Detect the CPU resources available to this process.
 *
 * The host's online CPU count is often not what a program may actually use:
 * a container can be limited by a cgroup CPU quota ("docker run --cpus=N",
 * Kubernetes CPU limits) or pinned to some CPUs ("--cpuset-cpus", taskset),
 * and still see every host CPU as online.  These functions report each of
 * those separately so callers can combine them as they need.
 *
 * Shared by initdb's auto-tune and pg_restore's default --jobs.
 *
 * src/fe_utils/sysinfo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <limits.h>
#include <unistd.h>

#if defined(__linux__)
#include <sched.h>
#endif

#ifdef WIN32
#include <windows.h>
#endif

#include "fe_utils/sysinfo.h"

/*
 * Detect the number of online CPUs on the host.  Returns 0 on failure.
 * This ignores CPU affinity and cgroup quotas; see the functions below.
 */
int
get_online_cpus(void)
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
 * Count the CPUs this process may be scheduled on, as restricted by
 * sched_setaffinity(), taskset or "docker run --cpuset-cpus".  Returns 0 when
 * that cannot be determined (not Linux, or more CPUs than a cpu_set_t holds).
 */
int
get_affinity_cpus(void)
{
#if defined(__linux__) && defined(CPU_COUNT)
	cpu_set_t	set;

	CPU_ZERO(&set);
	if (sched_getaffinity(0, sizeof(set), &set) == 0)
	{
		int			n = CPU_COUNT(&set);

		if (n > 0)
			return n;
	}
#endif
	return 0;
}

#if defined(__linux__)

/*
 * Find this process's path within a cgroup hierarchy by parsing
 * /proc/self/cgroup, whose lines are "<id>:<controllers>:<path>".  Pass
 * controller = NULL for the cgroup v2 unified hierarchy (the line with an
 * empty controller field, "0::<path>"), or a controller name such as
 * "memory" for a v1 hierarchy.  Returns false if there is no such line.
 */
bool
get_cgroup_path(const char *controller, char *relpath, size_t relpath_size)
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
 * Turn a CFS quota/period pair (both in microseconds) into a whole CPU count.
 * Rounds up, so a fractional allowance such as 1.5 CPUs still tunes as 2, and
 * never returns less than 1.  Returns 0 for "no limit" (quota <= 0), which both
 * cgroup versions use to mean unconfined.
 */
static int
cpus_from_cfs(long long quota_us, long long period_us)
{
	long long	cpus;

	if (quota_us <= 0 || period_us <= 0)
		return 0;

	cpus = (quota_us + period_us - 1) / period_us;	/* ceil */
	if (cpus < 1)
		cpus = 1;
	if (cpus > INT_MAX)
		cpus = INT_MAX;
	return (int) cpus;
}

/*
 * Read a cgroup v2 cpu.max file, whose single line is "<quota> <period>" with
 * quota either a microsecond count or the literal "max" (no limit).  Sets
 * *cpus to the whole-CPU allowance and returns true only when a real limit is
 * present, so an unlimited level is skipped and the scan keeps looking upward.
 */
static bool
read_cgroup_cpu_v2(const char *path, int *cpus)
{
	FILE	   *fp;
	char		quota[64];
	long long	period;
	long long	q;
	int			n;

	fp = fopen(path, "r");
	if (fp == NULL)
		return false;
	n = fscanf(fp, "%63s %lld", quota, &period);
	fclose(fp);

	if (n != 2 || strcmp(quota, "max") == 0)
		return false;
	if (sscanf(quota, "%lld", &q) != 1)
		return false;

	*cpus = cpus_from_cfs(q, period);
	return (*cpus > 0);
}

/*
 * Read a cgroup v1 cpu controller directory, whose quota and period live in
 * separate files (cpu.cfs_quota_us is -1 when unconfined).  Same contract as
 * read_cgroup_cpu_v2.
 */
static bool
read_cgroup_cpu_v1(const char *dir, int *cpus)
{
	char		path[MAXPGPATH];
	FILE	   *fp;
	long long	quota;
	long long	period;

	snprintf(path, MAXPGPATH, "%s/cpu.cfs_quota_us", dir);
	fp = fopen(path, "r");
	if (fp == NULL)
		return false;
	if (fscanf(fp, "%lld", &quota) != 1)
	{
		fclose(fp);
		return false;
	}
	fclose(fp);

	snprintf(path, MAXPGPATH, "%s/cpu.cfs_period_us", dir);
	fp = fopen(path, "r");
	if (fp == NULL)
		return false;
	if (fscanf(fp, "%lld", &period) != 1)
	{
		fclose(fp);
		return false;
	}
	fclose(fp);

	*cpus = cpus_from_cfs(quota, period);
	return (*cpus > 0);
}

/*
 * Walk a cpu cgroup hierarchy from this process's cgroup up to the root,
 * keeping the smallest CPU allowance found: a quota on any ancestor binds just
 * as tightly as one on the leaf.  relpath is consumed in place.  Returns 0 if
 * no level carries a quota.
 */
static int
scan_cgroup_cpu(const char *root, bool v2, char *relpath)
{
	char		dir[MAXPGPATH];
	int			best = 0;

	for (;;)
	{
		int			cpus = 0;
		bool		found;

		snprintf(dir, MAXPGPATH, "%s%s", root, relpath);
		if (v2)
		{
			char		path[MAXPGPATH];

			snprintf(path, MAXPGPATH, "%s/cpu.max", dir);
			found = read_cgroup_cpu_v2(path, &cpus);
		}
		else
			found = read_cgroup_cpu_v1(dir, &cpus);

		if (found && cpus > 0 && (best == 0 || cpus < best))
			best = cpus;

		/* Stop once we have just examined the hierarchy root itself. */
		if (relpath[0] == '\0')
			break;
		{
			char	   *slash = strrchr(relpath, '/');

			if (slash == NULL)
				break;
			*slash = '\0';
		}
	}

	return best;
}

/*
 * Detect the CPU allowance a cgroup CPU quota imposes on this process, as a
 * whole CPU count, or 0 when unconfined or indeterminable.  This is what
 * "docker run --cpus=N" and a Kubernetes CPU limit set.  Such a container still
 * sees every host CPU as online, so get_online_cpus() alone over-counts there.
 */
int
get_cgroup_cpu_limit(void)
{
	char		relpath[MAXPGPATH];
	int			cpus;

	/* cgroup v2 unified hierarchy. */
	if (get_cgroup_path(NULL, relpath, sizeof(relpath)))
	{
		cpus = scan_cgroup_cpu("/sys/fs/cgroup", true, relpath);
		if (cpus > 0)
			return cpus;
	}

	/* cgroup v1, whose cpu controller mounts in its own subdirectory. */
	if (get_cgroup_path("cpu", relpath, sizeof(relpath)))
	{
		cpus = scan_cgroup_cpu("/sys/fs/cgroup/cpu", false, relpath);
		if (cpus > 0)
			return cpus;
	}

	/* Last resort: a container's own cpu.max bind-mounted at the root. */
	cpus = 0;
	if (read_cgroup_cpu_v2("/sys/fs/cgroup/cpu.max", &cpus) && cpus > 0)
		return cpus;

	return 0;
}

#else							/* !__linux__ */

bool
get_cgroup_path(const char *controller, char *relpath, size_t relpath_size)
{
	return false;
}

int
get_cgroup_cpu_limit(void)
{
	return 0;
}

#endif							/* __linux__ */
