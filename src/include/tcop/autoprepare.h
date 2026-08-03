/*-------------------------------------------------------------------------
 *
 * autoprepare.h
 *	  Automatic server-side plan caching for repeated query shapes.
 *
 * dbblue Odoo-optimization feature.  Odoo (psycopg2) ships fully-literal SQL
 * over the simple-query protocol, so PostgreSQL re-plans every execution of
 * what are really a small, bounded set of query *shapes*.  Autoprepare
 * fingerprints each query (reusing the core query jumble / queryId), counts
 * how often a shape is seen, and once a shape crosses a threshold it caches a
 * parameterized CachedPlanSource and reuses it -- skipping the planner.
 *
 * src/include/tcop/autoprepare.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AUTOPREPARE_H
#define AUTOPREPARE_H

#include "nodes/parsenodes.h"
#include "utils/plancache.h"

/* GUCs (registered in autoprepare.c) */
extern PGDLLIMPORT bool autoprepare_enabled;
extern PGDLLIMPORT int	autoprepare_threshold;	/* cache after N sightings */
extern PGDLLIMPORT int	autoprepare_limit;		/* max cached shapes/backend */

/*
 * Result of consulting the autoprepare cache for an analyzed query.
 *
 *	APREP_MISS		  -- shape not (yet) cached; caller plans normally.  The
 *					     entry's sighting count was bumped as a side effect.
 *	APREP_HIT		  -- *plansource_out is a ready, validated CachedPlanSource
 *					     to execute via GetCachedPlan(); planning is skipped.
 *	APREP_UNCACHEABLE -- statement type is not eligible; do nothing special.
 */
typedef enum AutoprepareResult
{
	APREP_MISS,
	APREP_HIT,
	APREP_UNCACHEABLE,
}			AutoprepareResult;

/*
 * Main entry point, called from exec_simple_query() after parse-analysis and
 * before planning.  On APREP_HIT, *plansource_out and *boundParams_out are set
 * so the caller can GetCachedPlan() instead of planning.
 */
extern AutoprepareResult AutoprepareConsult(Query *analyzed_query,
											 const char *query_string,
											 CachedPlanSource **plansource_out,
											 ParamListInfo *boundParams_out);

/* Drop everything (called for DISCARD ALL / DEALLOCATE ALL). */
extern void AutoprepareReset(void);

/* GUC registration; call once from backend startup or _PG_init. */
extern void AutoprepareRegisterGUCs(void);

#endif							/* AUTOPREPARE_H */
