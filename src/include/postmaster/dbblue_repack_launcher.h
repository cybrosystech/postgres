/*-------------------------------------------------------------------------
 *
 * dbblue_repack_launcher.h
 *	  dbblue repack launcher background worker.
 *
 * Copyright (c) 2026, dbblue / Cybrosys Technologies
 *
 * src/include/postmaster/dbblue_repack_launcher.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBBLUE_REPACK_LAUNCHER_H
#define DBBLUE_REPACK_LAUNCHER_H

#include "utils/guc.h"

/* GUC variables (see guc_parameters.dat) */
extern PGDLLIMPORT bool dbblue_repack_enabled;
extern PGDLLIMPORT char *dbblue_repack_tables;
extern PGDLLIMPORT int dbblue_repack_naptime;
extern PGDLLIMPORT int dbblue_repack_min_interval;
extern PGDLLIMPORT double dbblue_repack_threshold;
extern PGDLLIMPORT char *dbblue_repack_database;

extern void RepackLauncherRegister(void);
extern void RepackLauncherMain(Datum main_arg);

/* GUC check hook for dbblue_repack_enabled */
extern bool dbblue_check_repack_enabled(bool *newval, void **extra,
										 GucSource source);

/* GUC check hook for dbblue_repack_tables */
extern bool dbblue_check_repack_tables(char **newval, void **extra,
										GucSource source);

#endif							/* DBBLUE_REPACK_LAUNCHER_H */
