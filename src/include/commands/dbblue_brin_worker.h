/*-------------------------------------------------------------------------
 *
 * dbblue_brin_worker.h
 *    Background workers for automatic BRIN index creation
 *
 * Copyright (c) 2026, Cybrosys Technologies
 *
 * IDENTIFICATION
 *    src/include/commands/dbblue_brin_worker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBBLUE_BRIN_WORKER_H
#define DBBLUE_BRIN_WORKER_H

#include "utils/guc.h"

/* Launcher: always running, holds no database connection. */
extern void DBBlueBrinLauncherMain(Datum main_arg);
extern void DBBlueBrinLauncherRegister(void);

/* Per-database scan worker, started dynamically by the launcher. */
extern void DBBlueBrinWorkerMain(Datum main_arg);

/* GUC check hook for the obsolete dbblue_brin_database */
extern bool dbblue_check_brin_database(char **newval, void **extra,
									   GucSource source);

#endif							/* DBBLUE_BRIN_WORKER_H */
