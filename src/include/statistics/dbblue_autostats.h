/*-------------------------------------------------------------------------
 *
 * dbblue_autostats.h
 *	  Detection of missing multi-column (extended) statistics.
 *
 * The planner estimates a list of restriction clauses on a single relation
 * by multiplying the per-clause selectivities together, which assumes the
 * columns are independent.  When they are not -- Odoo's
 * account_move_line(journal_id, account_id, parent_state) being the canonical
 * example -- the resulting row estimate can be orders of magnitude too low,
 * and the planner picks nested loops where it should have picked hash joins.
 *
 * CREATE STATISTICS fixes that, but only for column combinations somebody
 * thought to create an object for in advance.  This module records the
 * combinations the planner actually had to estimate independently, so that
 * the guesswork can be replaced with evidence.
 *
 * This is the detection half only ("phase 1"): it observes and reports via
 * the dbblue_stats_advisor view.  It never creates a statistics object and
 * never changes a plan.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/statistics/dbblue_autostats.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBBLUE_AUTOSTATS_H
#define DBBLUE_AUTOSTATS_H

#include "executor/execdesc.h"
#include "nodes/pathnodes.h"

/* GUCs (defined in dbblue_autostats.c) */
extern PGDLLIMPORT bool dbblue_autostats_enabled;
extern PGDLLIMPORT int dbblue_autostats_max;
extern PGDLLIMPORT int dbblue_autostats_local_max;
extern PGDLLIMPORT int dbblue_autostats_sample_rate;
extern PGDLLIMPORT double dbblue_autostats_min_error_factor;
extern PGDLLIMPORT int dbblue_autostats_min_rows;

/*
 * DBBlueAutoStatsShmemCallbacks is declared by storage/subsystems.h, which
 * generates externs from the list in storage/subsystemlist.h.
 */

/*
 * Record that 'clauses' (excluding those listed in 'estimatedclauses', which
 * extended statistics already handled) were estimated independently on 'rel',
 * producing overall selectivity 'sel'.  Called from
 * clauselist_selectivity_ext().
 */
extern void AutoStatsNoteClauses(PlannerInfo *root, RelOptInfo *rel,
								 List *clauses, Bitmapset *estimatedclauses,
								 double sel);

/*
 * Executor-side confirmation.  AutoStatsWantInstrumentation() decides whether
 * this execution should be sampled (and hence count rows); AutoStatsNoteExec()
 * compares the planner's estimate against the rows actually produced.  Called
 * from standard_ExecutorStart() / standard_ExecutorEnd().
 */
extern bool AutoStatsWantInstrumentation(void);
extern void AutoStatsNoteExec(QueryDesc *queryDesc);

#endif							/* DBBLUE_AUTOSTATS_H */
