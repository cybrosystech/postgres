/*-------------------------------------------------------------------------
 *
 * dbblue_index_advisor.h
 *	  Header for the dbblue Auto Index Suggestion background worker.
 *
 * Portions Copyright (c) 2026, dbblue / Cybrosys Technologies.
 *
 * IDENTIFICATION
 *	  src/include/postmaster/dbblue_index_advisor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBBLUE_INDEX_ADVISOR_H
#define DBBLUE_INDEX_ADVISOR_H

#include "fmgr.h"
#include "utils/guc.h"

/* GUC-backed variables */
extern PGDLLIMPORT bool dbblue_auto_index_suggestion_enabled;
extern PGDLLIMPORT int  dbblue_auto_index_suggestion_interval;
extern PGDLLIMPORT int  dbblue_auto_index_suggestion_min_calls;
extern PGDLLIMPORT double dbblue_auto_index_suggestion_min_cost_improvement;
extern PGDLLIMPORT int  dbblue_auto_index_suggestion_max_index_columns;
extern PGDLLIMPORT int  dbblue_auto_index_suggestion_top_n_queries;
extern PGDLLIMPORT char *dbblue_auto_index_suggestion_database;

extern void DbblueIndexAdvisorRegister(void);
extern void DbblueIndexAdvisorMain(Datum main_arg);

/* GUC check_hook: validates dbblue_auto_index_suggestion_enabled. */
extern bool dbblue_check_advisor_enabled(bool *newval, void **extra,
										 GucSource source);

#endif							/* DBBLUE_INDEX_ADVISOR_H */
