/*-------------------------------------------------------------------------
 *
 * dbblue_audit_pruner.h
 *	  dbblue audit log retention background worker.
 *
 * Copyright (c) 2026, dbblue / Cybrosys Technologies
 *
 * src/include/postmaster/dbblue_audit_pruner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBBLUE_AUDIT_PRUNER_H
#define DBBLUE_AUDIT_PRUNER_H

#include "utils/guc.h"

/* GUC variables (see guc_parameters.dat) */
extern PGDLLIMPORT int dbblue_audit_prune_naptime;

extern void DbblueAuditPrunerRegister(void);

/* Launcher: always running, holds no database connection. */
extern void DbblueAuditPruneLauncherMain(Datum main_arg);

/* Per-database sweep worker, started dynamically by the launcher. */
extern void AuditPrunerMain(Datum main_arg);

#endif							/* DBBLUE_AUDIT_PRUNER_H */
