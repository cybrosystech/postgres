/*-------------------------------------------------------------------------
 *
 * matview_dirty.h
 *	  DBblue: change tracking for REFRESH MATERIALIZED VIEW skip-if-unchanged
 *
 * See matview_dirty.c for the correctness argument.  The short version: a
 * REFRESH may be skipped only when the matview is provably a pure function of
 * a fully enumerated set of plain heap relations, none of which has changed
 * since the watermark recorded by that matview's own last successful refresh.
 * Anything that cannot be proven refuses to skip.
 *
 * src/include/commands/matview_dirty.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATVIEW_DIRTY_H
#define MATVIEW_DIRTY_H

#include "common/relpath.h"
#include "nodes/parsenodes.h"
#include "storage/shmem.h"
#include "utils/relcache.h"

/*
 * Tunables.  Every one of these bounds fails safe: exceeding it makes the
 * affected matview refuse to skip, never the other way round.
 */
#define MATVIEW_SKIP_MAX_RELS		1024	/* tracked sources, cluster-wide */
#define MATVIEW_SKIP_MAX_MATVIEWS	256 /* matviews holding a watermark */
#define MATVIEW_SKIP_MAX_SOURCES	32	/* source relations per matview */
#define MATVIEW_SKIP_MAX_XACT_RELS	256 /* relations written per transaction */

/*
 * What a refresh observed about its sources, captured before the data-fill
 * snapshot was taken.  RefreshMatViewByOid carries one of these on the stack
 * from the gate down to the mark-clean call.  'valid' false means the caller
 * must not record a watermark.
 */
typedef struct MatviewSkipCapture
{
	bool		valid;
	uint32		fingerprint;
	int			nsources;
	Oid			relid[MATVIEW_SKIP_MAX_SOURCES];
	uint64		gen[MATVIEW_SKIP_MAX_SOURCES];
	RelFileNumber rfn[MATVIEW_SKIP_MAX_SOURCES];
} MatviewSkipCapture;

/* GUC: cluster-wide off switch (the reloption is already opt-in per matview) */
extern PGDLLIMPORT bool dbblue_matview_skip_unchanged;

/* Shared memory callbacks (registered in subsystemlist.h) */
extern const ShmemCallbacks MatviewDirtyShmemCallbacks;

/*
 * Write path.  Called from heap_insert / heap_multi_insert / heap_delete /
 * heap_update.  Must be cheap: it only appends to a process-local array.
 */
extern void MatviewDirtyNote(Oid relid);

/* One-shot registration of the transaction callback, at backend startup. */
extern void MatviewDirtyRegisterCallback(void);

/*
 * Decide whether 'matviewRel' may skip its rebuild, and capture what a real
 * rebuild is about to observe.  Returns true only if the rebuild may be
 * skipped entirely.  *capture is always filled in; capture->valid says whether
 * MatviewSkipMarkClean() may be called after a successful rebuild.
 *
 * 'allow_skip' lets the caller ask for capture only (CREATE MATERIALIZED VIEW,
 * REFRESH ... WITH NO DATA, an unpopulated matview, CONCURRENTLY).
 */
extern bool MatviewSkipCheck(Relation matviewRel, Query *dataQuery,
							 bool allow_skip, MatviewSkipCapture *capture);

/*
 * Record the watermark after a successful rebuild.  Must be called after the
 * heap swap, so that the matview's post-swap relfilenumber can be stored as
 * the witness that this transaction actually committed.
 */
extern void MatviewSkipMarkClean(Oid matviewOid,
								 const MatviewSkipCapture *capture);

#endif							/* MATVIEW_DIRTY_H */
