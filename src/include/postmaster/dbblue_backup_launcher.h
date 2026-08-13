/*-------------------------------------------------------------------------
 *
 * dbblue_backup_launcher.h
 *	  dbblue backup launcher background worker.
 *
 * Copyright (c) 2026, dbblue / Cybrosys Technologies
 *
 * src/include/postmaster/dbblue_backup_launcher.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBBLUE_BACKUP_LAUNCHER_H
#define DBBLUE_BACKUP_LAUNCHER_H

#include "utils/guc.h"

/* GUC variables (see guc_parameters.dat) */
extern PGDLLIMPORT bool dbblue_backup_enabled;
extern PGDLLIMPORT char *dbblue_backup_database;
extern PGDLLIMPORT char *dbblue_backup_databases;
extern PGDLLIMPORT char *dbblue_backup_directory;
extern PGDLLIMPORT int dbblue_backup_interval;
extern PGDLLIMPORT int dbblue_backup_naptime;
extern PGDLLIMPORT int dbblue_backup_retention_count;
extern PGDLLIMPORT int dbblue_backup_timeout;

extern void BackupLauncherRegister(void);
extern void BackupLauncherMain(Datum main_arg);

/* GUC check hook for dbblue_backup_enabled */
extern bool dbblue_check_backup_enabled(bool *newval, void **extra,
										 GucSource source);

/* GUC check hook for dbblue_backup_databases */
extern bool dbblue_check_backup_databases(char **newval, void **extra,
										   GucSource source);

#endif							/* DBBLUE_BACKUP_LAUNCHER_H */
