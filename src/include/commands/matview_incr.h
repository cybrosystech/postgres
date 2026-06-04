/*-------------------------------------------------------------------------
 *
 * matview_incr.h
 *	  DBblue: incremental refresh for materialized views
 *
 * src/include/commands/matview_incr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATVIEW_INCR_H
#define MATVIEW_INCR_H

#include "fmgr.h"
#include "nodes/parsenodes.h"
#include "utils/relcache.h"

/*
 * Name of the hidden row-count column added to every incrementally-refreshed
 * matview.  It tracks how many source rows contribute to each group so the
 * group row can be deleted when the count drops to zero.
 */
#define MATVIEW_INCR_COUNT_COL	"__mv_count__"

/*
 * Transition-table aliases used in the internal triggers.
 * Must match the names embedded in the stored delta SQL.
 */
#define MATVIEW_INCR_NEWTABLE	"__mv_newtable"
#define MATVIEW_INCR_OLDTABLE	"__mv_oldtable"

/* OID of the matview_delta_apply trigger function (from pg_proc.dat) */
#define MATVIEW_DELTA_APPLY_OID		8335

/*
 * Set up incremental refresh for a newly created matview.
 * Called from ExecCreateTableAs after the matview is created and populated.
 * __mv_count__ must already be present (injected by MatviewIncrAddCountTarget).
 */
extern void MatviewIncrSetup(Oid mvrelid, Query *viewQuery);

/* Remove incremental-refresh infrastructure when a matview is dropped. */
extern void MatviewIncrTeardown(Oid mvrelid);

/*
 * Check whether the given Query is eligible for incremental refresh.
 * Returns true if eligible; sets *reason to a human-readable explanation
 * if not.
 */
extern bool MatviewIncrIsEligible(Query *viewQuery, const char **reason);

/*
 * Append COUNT(*) AS __mv_count__ to q->targetList.
 * Must be called for both the schema query and the view-definition query
 * before the matview is created.
 */
extern void MatviewIncrAddCountTarget(Query *q);

/* Trigger function — registered in pg_proc as matview_delta_apply */
extern Datum matview_delta_apply(PG_FUNCTION_ARGS);

#endif							/* MATVIEW_INCR_H */
