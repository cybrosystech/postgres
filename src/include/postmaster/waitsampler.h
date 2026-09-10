/*-------------------------------------------------------------------------
 *
 * waitsampler.h
 *
 * Header file for the dbblue wait sampling collector process.  This
 * process samples backend wait events and persists them, as history and
 * profile rows, into a fixed database configured via
 * dbblue_wait_sampling_database.
 *
 * IDENTIFICATION
 *	  src/include/postmaster/waitsampler.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WAITSAMPLER_H
#define WAITSAMPLER_H

typedef enum
{
	DBBLUE_WS_PROFILE_QUERIES_NONE,	/* don't tag rows with queryid */
	DBBLUE_WS_PROFILE_QUERIES_TOP	/* tag rows with the top-level queryid */
} DbblueWSProfileQueries;

extern PGDLLIMPORT bool dbblue_wait_sampling_enabled;
extern PGDLLIMPORT int dbblue_wait_sampling_period;
extern PGDLLIMPORT int dbblue_wait_sampling_profile_period;
extern PGDLLIMPORT bool dbblue_wait_sampling_profile_pid;
extern PGDLLIMPORT int dbblue_wait_sampling_profile_queries;
extern PGDLLIMPORT bool dbblue_wait_sampling_sample_cpu;
extern PGDLLIMPORT int dbblue_wait_sampling_history_retention;
extern PGDLLIMPORT int dbblue_wait_sampling_flush_interval;
extern PGDLLIMPORT char *dbblue_wait_sampling_database;

extern void WaitSamplerRegister(void);
extern void WaitSamplerMain(Datum main_arg);

#endif							/* WAITSAMPLER_H */
