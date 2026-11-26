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

/* Values of dbblue_create_standby */
typedef enum DbblueCreateStandbyMode
{
	DBBLUE_STANDBY_OFF,			/* no standby: stopped, slot dropped */
	DBBLUE_STANDBY_PAUSED,		/* standby stopped, slot and files kept */
	DBBLUE_STANDBY_ON,			/* standby built, run and watched */
} DbblueCreateStandbyMode;

/*
 * The name the standby streams as, its cluster_name, which its walreceiver
 * sends as application_name: what pg_stat_replication shows for it, and
 * what synchronous_standby_names would have to name.  A promoted standby
 * keeps it, which is how the backup launcher recognizes one.
 */
#define DBBLUE_STANDBY_NAME			"dbblue_standby"

/* GUC-backed variables (defined in dbblue_create_standby.c) */
extern PGDLLIMPORT int dbblue_create_standby;
extern PGDLLIMPORT char *dbblue_standby_directory;
extern PGDLLIMPORT char *dbblue_standby_user;
extern PGDLLIMPORT int dbblue_standby_max_lag;

extern void DbblueCreateStandbyRegister(void);
extern PGDLLEXPORT void DbblueCreateStandbyMain(Datum main_arg);

#endif							/* DBBLUE_CREATE_STANDBY_H */
