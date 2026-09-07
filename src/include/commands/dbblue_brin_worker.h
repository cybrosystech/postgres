/*-------------------------------------------------------------------------
 *
 * dbblue_brin_worker.h
 *    Background worker for automatic BRIN index creation
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

extern void DBBlueBrinWorkerMain(Datum main_arg);
extern void DBBlueBrinWorkerRegister(void);

/* GUC check hook for dbblue_create_brin */
extern bool dbblue_check_create_brin(bool *newval, void **extra,
									 GucSource source);

/* GUC check hook for dbblue_brin_database */
extern bool dbblue_check_brin_database(char **newval, void **extra,
									   GucSource source);

#endif							/* DBBLUE_BRIN_WORKER_H */
