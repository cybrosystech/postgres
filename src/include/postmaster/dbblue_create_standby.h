/*-------------------------------------------------------------------------
 *
 * dbblue_create_standby.h
 *	  Header for the dbblue Create Standby background worker.
 *
 * Portions Copyright (c) 2026, dbblue / Cybrosys Technologies.
 *
 * IDENTIFICATION
 *	  src/include/postmaster/dbblue_create_standby.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBBLUE_CREATE_STANDBY_H
#define DBBLUE_CREATE_STANDBY_H

#include "fmgr.h"

/* GUC-backed variables (defined in dbblue_create_standby.c) */
extern PGDLLIMPORT bool dbblue_create_standby;
extern PGDLLIMPORT char *dbblue_standby_directory;
extern PGDLLIMPORT char *dbblue_standby_user;

extern void DbblueCreateStandbyRegister(void);
extern PGDLLEXPORT void DbblueCreateStandbyMain(Datum main_arg);

#endif							/* DBBLUE_CREATE_STANDBY_H */
