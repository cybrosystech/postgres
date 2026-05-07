/*-------------------------------------------------------------------------
 *
 * autopartition.h
 *    header for the DBblue auto-partition launcher background worker.
 *
 * Portions Copyright (c) 2026 Cybrosys / DBblue R&D
 *
 * src/include/postmaster/autopartition.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AUTOPARTITION_H
#define AUTOPARTITION_H

#include "postgres.h"

/* GUC variables (defined in autopartition.c) */
extern PGDLLIMPORT bool auto_partition_enabled;
extern PGDLLIMPORT int	auto_partition_naptime;

/*
 * Postmaster-time registration: called once during postmaster startup, just
 * like ApplyLauncherRegister().  Adds a static background worker to the
 * registration list so the launcher starts after recovery completes.
 */
extern void AutoPartitionLauncherRegister(void);

/*
 * Worker entry point.  Resolved by the bgworker framework via the
 * "postgres" library name + this function name.
 */
extern void AutoPartitionLauncherMain(Datum main_arg);

#endif							/* AUTOPARTITION_H */
