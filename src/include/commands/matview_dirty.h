/*-------------------------------------------------------------------------
 *
 * matview_dirty.h
 *	  DBblue: per-relation dirty tracking for REFRESH MATERIALIZED VIEW SKIP IF UNCHANGED
 *
 * src/include/commands/matview_dirty.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATVIEW_DIRTY_H
#define MATVIEW_DIRTY_H

#include "storage/shmem.h"
#include "utils/relcache.h"

/*
 * Maximum number of source-table OIDs tracked in shared memory.
 * Covers any realistic Odoo matview configuration.
 */
#define MATVIEW_DIRTY_MAX_RELS	256

/* Shared memory callbacks (registered in subsystemlist.h) */
extern const ShmemCallbacks MatviewDirtyShmemCallbacks;

/*
 * Record that the current transaction wrote to 'relid'.
 * Called from heap_insert / heap_update / heap_delete.
 * Must be very fast: just appends to a process-local list.
 */
extern void MatviewDirtyNote(Oid relid);

/*
 * Register 'relid' as a watched source table.
 * Called the first time a matview with auto_skip_unchanged=true is refreshed.
 * Inserts the entry with dirty=true so the first refresh always proceeds.
 */
extern void MatviewDirtyRegister(Oid relid);

/*
 * Return true if 'relid' is registered AND currently clean (no writes since
 * last refresh).  Returns false if not registered (unknown -> assume dirty).
 */
extern bool MatviewDirtyIsClean(Oid relid);

/*
 * Mark 'relid' as clean.  Called after a successful matview refresh.
 */
extern void MatviewDirtyMarkClean(Oid relid);

/*
 * Register the transaction callback that flushes the process-local dirty list
 * to shared memory on commit.  Called once per backend (lazy on first write).
 */
extern void MatviewDirtyRegisterCallback(void);

#endif							/* MATVIEW_DIRTY_H */
