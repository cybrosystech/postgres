/*-------------------------------------------------------------------------
 *
 * dbblue_auto_tuner.h
 *	  Header for the dbblue Auto Tuner background worker.
 *
 * Portions Copyright (c) 2026, dbblue / Cybrosys Technologies.
 *
 * IDENTIFICATION
 *	  src/include/postmaster/dbblue_auto_tuner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBBLUE_AUTO_TUNER_H
#define DBBLUE_AUTO_TUNER_H

#include "fmgr.h"
#include "utils/guc.h"

/* GUC-backed variables */
extern PGDLLIMPORT bool dbblue_auto_tune;
extern PGDLLIMPORT char *dbblue_auto_tune_database;

extern void DbblueAutoTunerRegister(void);
extern void DbblueAutoTunerMain(Datum main_arg);

#endif							/* DBBLUE_AUTO_TUNER_H */
