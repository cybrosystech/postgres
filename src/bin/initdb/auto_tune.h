/*-------------------------------------------------------------------------
 *
 * auto_tune.h
 *	  Auto-tune postgresql.conf based on detected hardware specs.
 *
 * The dbblue distribution of PostgreSQL ships with auto-tuned configuration
 * defaults instead of the upstream conservative ones.  At initdb time we
 * detect the host's RAM, CPU count and (best-effort) storage type, then
 * compute settings using PGTune-style formulas selected by workload profile.
 *
 * src/bin/initdb/auto_tune.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AUTO_TUNE_H
#define AUTO_TUNE_H

#include "c.h"

typedef enum
{
	WORKLOAD_MIXED = 0,
	WORKLOAD_OLTP,
	WORKLOAD_OLAP,
	WORKLOAD_WEB,
	WORKLOAD_DESKTOP,
} WorkloadProfile;

typedef struct AutoTuneSettings
{
	/* True if detection succeeded and the values below are usable. */
	bool		valid;

	/* Detected system specs */
	uint64		total_ram_bytes;
	int			cpu_count;
	bool		ssd_storage;

	WorkloadProfile workload;

	/* Memory settings, in kilobytes */
	int			shared_buffers_kb;
	int			effective_cache_size_kb;
	int			work_mem_kb;
	int			maintenance_work_mem_kb;
	int			wal_buffers_kb;		/* -1 means "auto" */

	/* Parallelism */
	int			max_worker_processes;
	int			max_parallel_workers;
	int			max_parallel_workers_per_gather;
	int			max_parallel_maintenance_workers;

	/* Storage / planner */
	double		random_page_cost;
	int			effective_io_concurrency;

	/* Checkpoints / WAL */
	double		checkpoint_completion_target;
	int			min_wal_size_mb;
	int			max_wal_size_mb;

	/* Statistics */
	int			default_statistics_target;
} AutoTuneSettings;

/*
 * Parse a workload string ("mixed", "oltp", "olap", "web", "desktop") into
 * a WorkloadProfile.  Returns true on success.
 */
extern bool auto_tune_parse_workload(const char *name, WorkloadProfile *out);

/*
 * Detect host specs and compute tuned settings for the chosen workload,
 * given the max_connections value already chosen by initdb.  On detection
 * failure, returns a struct with valid = false and the caller should fall
 * back to the upstream defaults.
 */
extern AutoTuneSettings auto_tune_compute(WorkloadProfile workload,
										  int max_connections);

#endif							/* AUTO_TUNE_H */
