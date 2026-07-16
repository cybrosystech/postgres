/*-------------------------------------------------------------------------
 *
 * auto_tune.h
 *	  Auto-tune postgresql.conf based on detected hardware specs.
 *
 * The dbblue distribution of PostgreSQL ships with auto-tuned configuration
 * defaults instead of the upstream conservative ones.  Formulas are tuned
 * for Odoo's workload shape (many short OLTP-ish transactions from the ORM,
 * occasional larger reports) so we expose no workload knob — the single
 * profile is the right answer for the product.
 *
 * src/bin/initdb/auto_tune.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AUTO_TUNE_H
#define AUTO_TUNE_H

#include "c.h"

typedef struct AutoTuneSettings
{
	/* True if detection succeeded and the values below are usable. */
	bool		valid;

	/* Detected system specs */
	uint64		total_ram_bytes;
	int			cpu_count;
	bool		ssd_storage;

	/* Memory settings, in kilobytes */
	int			shared_buffers_kb;
	int			effective_cache_size_kb;
	int			work_mem_kb;
	int			maintenance_work_mem_kb;

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
 * Detect host specs and compute tuned settings, given the max_connections
 * value already chosen by initdb.  On detection failure, returns a struct
 * with valid = false and the caller should fall back to upstream defaults.
 */
extern AutoTuneSettings auto_tune_compute(int max_connections);

#endif							/* AUTO_TUNE_H */
