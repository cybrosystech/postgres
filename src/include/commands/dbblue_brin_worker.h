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

extern void DBBlueBrinWorkerMain(Datum main_arg);
extern void DBBlueBrinWorkerRegister(void);

#endif							/* DBBLUE_BRIN_WORKER_H */
