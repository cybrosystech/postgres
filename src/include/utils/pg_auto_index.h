#ifndef PG_AUTO_INDEX_H
#define PG_AUTO_INDEX_H

#include "postgres.h"
#include "postmaster/bgworker.h"

/* GUC variables - defined in pg_auto_index.c */
extern bool  dbblue_auto_index_enabled;
extern int   dbblue_auto_index_retention_days;
extern int   dbblue_auto_index_check_interval;
extern bool  dbblue_auto_index_skip_on_standby;
extern char *dbblue_auto_index_database;

/* Bgworker registration - called once from PostmasterMain */
extern void DbblueAutoIndexRegister(void);

/* Bgworker entry point - must be extern for BGW lookup by name */
extern PGDLLEXPORT void DbblueAutoIndexMain(Datum main_arg);

#endif /* PG_AUTO_INDEX_H */
