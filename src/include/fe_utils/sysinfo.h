/*-------------------------------------------------------------------------
 *
 * sysinfo.h
 *	  Detect the CPU resources available to this process.
 *
 * Each function returns 0 when its answer is unknown or, for the limits,
 * when no such limit applies; callers combine them as they need.
 *
 * src/include/fe_utils/sysinfo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SYSINFO_H
#define SYSINFO_H

/* CPUs the host reports online; ignores affinity and cgroup quotas. */
extern int	get_online_cpus(void);

/* CPUs this process may run on (taskset, --cpuset-cpus); Linux only. */
extern int	get_affinity_cpus(void);

/* Whole-CPU allowance of a cgroup CPU quota (--cpus=N); Linux only. */
extern int	get_cgroup_cpu_limit(void);

/*
 * This process's path within a cgroup hierarchy, from /proc/self/cgroup:
 * controller NULL for the v2 unified hierarchy, or a v1 controller name.
 */
extern bool get_cgroup_path(const char *controller, char *relpath,
							size_t relpath_size);

#endif							/* SYSINFO_H */
