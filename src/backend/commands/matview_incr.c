/*-------------------------------------------------------------------------
 *
 * matview_incr.c
 *	  DBblue: incremental refresh for materialized views
 *
 * Phase 1 scope
 * -------------
 * Single source table, GROUP BY with SUM and/or COUNT(*) only.
 *
 * Phase 2 scope
 * -------------
 * Two source tables connected by an INNER JOIN, GROUP BY with SUM and/or
 * COUNT(*).  Delta SQL is stored per source table: when T1 changes the
 * trigger joins __mv_newtable/__mv_oldtable with the current state of T2,
 * and vice versa.
 *
 * AVG, HAVING, LEFT/OUTER JOINs, subqueries → Phase 3.
 *
 * Lifecycle
 * ---------
 * CREATE MATERIALIZED VIEW … WITH (incremental_refresh=true)
 *   → MatviewIncrSetup() called from ExecCreateTableAs after the matview
 *     table is created and initially populated:
 *     1. ALTER TABLE mv ADD COLUMN __mv_count__ bigint NOT NULL DEFAULT 0
 *     2. UPDATE mv SET __mv_count__ = <recomputed from source>
 *     3. Generate insert-delta and delete-delta SQL from the Query tree.
 *     4. Store in pg_dbblue_matview.
 *     5. CREATE UNIQUE INDEX on the GROUP BY columns (for ON CONFLICT).
 *     6. Install internal AFTER STATEMENT triggers on the source table.
 *
 * AFTER INSERT / DELETE / UPDATE on source table
 *   → matview_delta_apply() trigger:
 *     1. SPI_register_trigger_data() registers __mv_newtable/__mv_oldtable.
 *     2. Fetch stored delta SQL from pg_dbblue_matview.
 *     3. Prepare (once per backend) and cache the SPI plan.
 *     4. Execute insert-delta and/or delete-delta.
 *     5. Execute cleanup (DELETE WHERE __mv_count__ <= 0).
 *
 * Locking
 * -------
 * RowExclusiveLock on the matview — sufficient for single-table
 * non-conflicting group keys.  No BEFORE triggers needed.
 *
 * src/backend/commands/matview_incr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_dbblue_matview.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/matview.h"
#include "commands/matview_incr.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/* ----------
 * Process-local plan cache.
 *
 * Five plans per matview:
 *   INCR_PLAN_INS  — apply __mv_newtable delta to matview (INSERT ON CONFLICT)
 *   INCR_PLAN_DEL  — subtract __mv_oldtable delta from matview (UPDATE)
 *   INCR_PLAN_CLN  — remove zero-count groups (DELETE WHERE __mv_count__ <= 0)
 *   INCR_PLAN_HAV  — recompute __mv_having_ok__ for all active groups (HAVING)
 *   INCR_PLAN_LOCK — acquire per-group advisory locks before DELETE rescan (MIN/MAX)
 * ----------
 */
#define INCR_PLAN_INS	0
#define INCR_PLAN_DEL	1
#define INCR_PLAN_CLN	2
#define INCR_PLAN_HAV	3
#define INCR_PLAN_LOCK	4
#define INCR_NUM_PLANS	5

typedef struct IncrPlanKey
{
	Oid			mvrelid;
	Oid			srctable;	/* needed for Phase 2: same mv has plans per source table */
	int			plan_type;
} IncrPlanKey;

typedef struct IncrPlanEntry
{
	IncrPlanKey key;
	SPIPlanPtr	plan;
} IncrPlanEntry;

static HTAB *incr_plan_cache = NULL;

/*
 * Alias for the delta (transition) table in Phase 2+ SQL.
 * Non-delta join tables get per-varno aliases "_j<varno>_" built at runtime.
 */
#define INCR_DELTA_ALIAS	"_d_"

/*
 * One entry in the join list passed to incr_build_*_gen builders.
 * Phase 1: join_list = NIL.
 * Phase 2: one entry (the other table).
 * Phase 3+: one entry per additional table, in join order.
 */
typedef struct IncrJoinEntry
{
	int			varno;		/* varno of this table in viewQuery->rtable */
	Oid			oid;		/* table OID */
	Node	   *quals;		/* ON condition for this join step */
	JoinType	join_type;	/* JOIN_INNER (anchor), JOIN_LEFT, JOIN_RIGHT, JOIN_FULL */
} IncrJoinEntry;

/* ----------
 * Forward declarations
 * ----------
 */
static bool incr_is_hidden_col(const char *resname);
static Oid	incr_find_sum_agg(Oid avg_fnoid, Oid *rettype_out);
static Oid	incr_get_source_table(Query *viewQuery);
static List *incr_collect_tables(Query *viewQuery);
static List *incr_build_join_list_for_delta(List *all_tables, int delta_varno);
static bool incr_has_outer_join(List *all_tables);
static int	incr_outer_preserved_varno(List *all_tables);
static const char *incr_qual_get_colname_for_varno(Node *qual, List *rtable, int varno);
static int	incr_qual_get_other_varno(Node *qual, int own_varno);
static char *incr_build_outer_row_sync_sql(Oid mvrelid, Query *viewQuery,
										   int delta_varno, const char *delta_table,
										   List *all_tables);
static char *str_replace_all(const char *src, const char *from, const char *to);
static Node *find_connecting_qual(List *all_tables, int varno_a, int varno_b);
static char *qual_to_live_sql(Node *qual, List *rtable, List *all_tables,
							   int preserved_varno);
static char *incr_build_outer_sql(Oid mvrelid, Query *viewQuery,
								   int delta_varno, const char *delta_table,
								   List *all_tables, bool include_delete_step);
static void incr_collect_group_cols(Query *viewQuery, List **groupColNames);
static void incr_append_from_join(StringInfo buf, Query *viewQuery,
								  int delta_varno, const char *delta_table,
								  List *join_list);
static char *incr_build_row_ins_sql(Oid mvrelid, Query *viewQuery,
									int delta_varno, const char *delta_table,
									List *join_list);
static char *incr_build_row_del_sql(Oid mvrelid, Query *viewQuery,
									int delta_varno, const char *delta_table,
									List *join_list);
static char *incr_build_ins_sql_gen(Oid mvrelid, Query *viewQuery,
									int delta_varno, const char *delta_table,
									List *join_list);
static char *incr_build_backfill_sql_gen(Oid mvrelid, Query *viewQuery,
										 int delta_varno, const char *delta_table,
										 List *join_list);
static char *incr_build_del_sql_gen(Oid mvrelid, Query *viewQuery,
									int delta_varno, const char *delta_table,
									List *join_list);
static char *incr_build_cln_sql(Oid mvrelid);
static void incr_warn_row_level_missing_key(Query *viewQuery);
static void incr_store_catalog(Oid mvrelid, Oid srctable,
							   const char *ins_sql,
							   const char *del_sql,
							   const char *cln_sql,
							   const char *having_sql,
							   const char *lock_sql);
static void incr_create_unique_index(Oid mvrelid, List *groupColNames);
static bool incr_has_self_join(List *all_tables);
static int  incr_self_join_other_varno(List *all_tables, int own_varno, Oid shared_oid);
static char *incr_build_self_join_row_ins_sql(Oid mvrelid, Query *viewQuery,
											   int v1, int v2,
											   const char *delta_table,
											   List *all_tables);
static char *incr_build_self_join_row_del_sql(Oid mvrelid, Query *viewQuery,
											   int v1, int v2,
											   const char *delta_table,
											   List *all_tables);
static char *incr_build_self_join_agg_ins_sql(Oid mvrelid, Query *viewQuery,
											   int v1, int v2,
											   const char *delta_table,
											   List *all_tables);
static char *incr_build_self_join_agg_del_sql(Oid mvrelid, Query *viewQuery,
											   int v1, int v2,
											   const char *delta_table,
											   List *all_tables);
static bool incr_is_pure_union_all(Node *node);
static void incr_collect_union_branches(Query *viewQuery, List **branches);
static char *incr_build_union_ins_sql(Oid mvrelid, Query *viewQuery,
									   Query *branchQuery,
									   int delta_varno, const char *delta_table,
									   List *join_list);
static char *incr_build_union_del_sql(Oid mvrelid, Query *viewQuery,
									   Query *branchQuery,
									   int delta_varno, const char *delta_table,
									   List *join_list);
static void incr_union_dedup_backfill(Oid mvrelid, Query *viewQuery);
static void incr_setup_union_all(Oid mvrelid, Query *viewQuery);
static bool incr_has_minmax_agg(Query *viewQuery);
static char *incr_build_minmax_ins_sql_gen(Oid mvrelid, Query *viewQuery,
										   int delta_varno, const char *delta_table,
										   List *join_list);
static char *incr_build_minmax_del_sql_gen(Oid mvrelid, Query *viewQuery,
										   int delta_varno, const char *delta_table,
										   List *join_list, Oid delta_oid);
static char *incr_build_minmax_lock_sql_gen(Oid mvrelid, Query *viewQuery,
											int delta_varno, const char *delta_table,
											List *join_list);
/* Phase 16: CTE / FROM-subquery normalization */
static int   incr_single_base_varno(Query *q);
static int   incr_find_cte_varno(Query *q, const char *ctename);
static bool  incr_q_is_filter_proj(Query *q);
static bool  incr_q_is_single_agg(Query *q);
static bool  incr_q_is_distinct_only(Query *q);
static bool  incr_outer_sole_source_is(Query *outer, int src_varno);
static Node *incr_remap_var_mutator(Node *node, void *ctx_ptr);
static Node *incr_subst_col_mutator(Node *node, void *ctx_ptr);
static Node *incr_subst_merge_mutator(Node *node, void *ctx_ptr);
static bool  incr_try_inline_filter(Query *outer, Query *srcq, int src_varno);
static bool  incr_try_merge_agg(Query *outer, Query *srcq, int src_varno);
static bool  incr_try_merge_distinct(Query *outer, Query *srcq, int src_varno);
static bool  incr_subst_inner_cte_refs(Query *srcq, Query *outer_with_ctes);
static bool  incr_try_normalize_cte(Query *outer, CommonTableExpr *cte, int cte_varno);
static bool  incr_try_normalize_subq(Query *outer, int sq_varno);
static Query *incr_normalize_query_body(Query *q);

static bool incr_validate_expr(Node *expr, Query *viewQuery, bool allow_aggref);
static Node *incr_get_where_qual(Query *viewQuery);
static void incr_deparse_where_qual(Node *qual, List *rtable, int delta_varno,
									StringInfo buf);
static const char *incr_resolve_var_colname(Var *v, List *rtable,
											int *resolved_varno_out);
static void incr_deparse_having_cond(Node *expr, Query *viewQuery, StringInfo buf);
static char *incr_build_hav_sql(Oid mvrelid, Query *viewQuery);
static void incr_create_having_view(Oid mvrelid,
									const char *origschema,
									const char *origname,
									Query *viewQuery);
static void incr_create_trigger(Oid mvrelid, Oid srctable,
								int16 tgtype_event,
								const char *newtable,
								const char *oldtable);
static void incr_init_plan_cache(void);
static SPIPlanPtr incr_get_plan(Oid mvrelid, Oid srctable, int plan_type);
static void incr_cache_plan(Oid mvrelid, Oid srctable, int plan_type, SPIPlanPtr plan);
static char *incr_fetch_sql(Oid mvrelid, Oid srctable, int plan_type);

/* ----------
 * Qualified relation name helper
 * ----------
 */
static const char *
mv_qname(Oid relid)
{
	return quote_qualified_identifier(
		get_namespace_name(get_rel_namespace(relid)),
		get_rel_name(relid));
}

/* ============================================================
 * Public API
 * ============================================================
 */

/*
 * MatviewIncrIsEligible
 * Returns true if the query can be maintained incrementally (Phase 1 or 2).
 * Sets *reason on failure.
 */
bool
MatviewIncrIsEligible(Query *viewQuery, const char **reason)
{
	ListCell   *lc;
	int			nbasetables = 0;

	if (viewQuery->havingQual != NULL && viewQuery->groupClause == NIL)
	{
		*reason = "HAVING requires GROUP BY";
		return false;
	}
	if (viewQuery->havingQual != NULL)
	{
		if (!incr_validate_expr(viewQuery->havingQual, viewQuery, true))
		{
			*reason = "HAVING uses unsupported expressions; "
				"only maintained aggregates (COUNT/SUM/AVG), "
				"group columns, constants, and comparison/boolean operators allowed";
			return false;
		}
	}
	if (viewQuery->setOperations != NULL)
	{
		/*
		 * UNION ALL: validate the full tree and each branch, then return.
		 * All other eligibility checks below are for non-UNION-ALL queries.
		 */
		if (!incr_is_pure_union_all(viewQuery->setOperations))
		{
			*reason = "only UNION ALL is supported for set operations; "
					  "UNION DISTINCT, INTERSECT, and EXCEPT are not supported";
			return false;
		}
		if (viewQuery->groupClause != NIL)
		{
			*reason = "UNION ALL with GROUP BY is not supported; "
					  "place GROUP BY inside each branch query";
			return false;
		}
		if (viewQuery->distinctClause != NIL)
		{
			*reason = "UNION ALL with DISTINCT is not supported";
			return false;
		}
		if (viewQuery->havingQual != NULL)
		{
			*reason = "UNION ALL with HAVING is not supported";
			return false;
		}
		if (viewQuery->limitCount != NULL || viewQuery->limitOffset != NULL)
		{
			*reason = "UNION ALL with LIMIT/OFFSET is not supported";
			return false;
		}

		/* Validate each branch and check for cross-branch duplicate tables */
		{
			List	   *branches = NIL;
			ListCell   *blc;
			HTAB	   *seen_tables;
			HASHCTL		ctl;

			incr_collect_union_branches(viewQuery, &branches);

			memset(&ctl, 0, sizeof(ctl));
			ctl.keysize   = sizeof(Oid);
			ctl.entrysize = sizeof(Oid) * 2;
			ctl.hcxt      = CurrentMemoryContext;
			seen_tables = hash_create("union_seen", 16, &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

			foreach(blc, branches)
			{
				Query	   *branchQuery = (Query *) lfirst(blc);
				const char *branch_reason;
				List	   *btables;
				ListCell   *tlc;

				if (!MatviewIncrIsEligible(branchQuery, &branch_reason))
				{
					*reason = psprintf("UNION ALL branch is not eligible: %s",
									   branch_reason);
					hash_destroy(seen_tables);
					return false;
				}
				if (branchQuery->groupClause != NIL ||
					branchQuery->distinctClause != NIL)
				{
					*reason = "UNION ALL branches with GROUP BY or DISTINCT "
							  "are not supported; use a subquery if needed";
					hash_destroy(seen_tables);
					return false;
				}

				btables = incr_collect_tables(branchQuery);
				foreach(tlc, btables)
				{
					IncrJoinEntry *je = lfirst(tlc);
					bool		   found;

					hash_search(seen_tables, &je->oid, HASH_ENTER, &found);
					if (found)
					{
						*reason = psprintf(
							"table \"%s\" appears in multiple UNION ALL branches; "
							"each source table must appear in at most one branch",
							get_rel_name(je->oid));
						hash_destroy(seen_tables);
						return false;
					}
				}
			}
			hash_destroy(seen_tables);
		}
		return true; /* UNION ALL is eligible */
	}
	if (viewQuery->hasSubLinks)
	{
		*reason = "subqueries are not supported";
		return false;
	}
	if (viewQuery->distinctClause != NIL)
	{
		int			distinct_count = list_length(viewQuery->distinctClause);
		int			visible_count = 0;
		ListCell   *tc;

		if (viewQuery->groupClause != NIL)
		{
			*reason = "DISTINCT with GROUP BY is not supported";
			return false;
		}
		foreach(tc, viewQuery->targetList)
		{
			TargetEntry *te2 = lfirst_node(TargetEntry, tc);

			if (!te2->resjunk && !incr_is_hidden_col(te2->resname))
				visible_count++;
		}
		if (distinct_count < visible_count)
		{
			*reason = "DISTINCT ON is not supported; use full DISTINCT (DISTINCT on all output columns)";
			return false;
		}
		/* Full DISTINCT is allowed — MatviewIncrAddCountTarget converts it to
		 * GROUP BY on all output columns before the matview is created. */
	}
	if (viewQuery->limitCount != NULL || viewQuery->limitOffset != NULL)
	{
		*reason = "LIMIT/OFFSET cannot be maintained incrementally; "
				  "the result set would shift on every row change";
		return false;
	}
	if (viewQuery->hasWindowFuncs)
	{
		*reason = "window functions cannot be maintained incrementally";
		return false;
	}
	if (viewQuery->cteList != NIL)
	{
		*reason = "WITH clauses (CTEs) are not supported; inline the subquery instead";
		return false;
	}

	/* Count base relations; also check for LATERAL and duplicate OIDs */
	{
		/*
		 * Track how many times each table OID appears — self-join (2×) is
		 * allowed for row-level matviews; 3+ is not supported.
		 */
		HTAB	   *oid_counts;
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize   = sizeof(Oid);
		ctl.entrysize = sizeof(Oid) * 2; /* key + count */
		ctl.hcxt      = CurrentMemoryContext;
		oid_counts = hash_create("incr_oid_counts", 16, &ctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		foreach(lc, viewQuery->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
			Oid		*entry;
			bool	 found;

			/* PG19 RTE_GROUP, RTE_RESULT, and explicit-JOIN's RTE_JOIN are bookkeeping */
			if (rte->rtekind == RTE_GROUP || rte->rtekind == RTE_RESULT ||
				rte->rtekind == RTE_JOIN)
				continue;

			if (rte->lateral)
			{
				*reason = "LATERAL joins are not supported for incremental refresh";
				hash_destroy(oid_counts);
				return false;
			}

			if (rte->rtekind == RTE_RELATION)
			{
				nbasetables++;
				entry = (Oid *) hash_search(oid_counts, &rte->relid, HASH_ENTER, &found);
				if (!found)
					entry[1] = 1;
				else
				{
					entry[1]++;
					if (entry[1] > 2)
					{
						*reason = psprintf("table \"%s\" appears more than twice; "
										   "diamond join patterns are not supported",
										   get_rel_name(rte->relid));
						hash_destroy(oid_counts);
						return false;
					}
				}
			}
			else
			{
				*reason = "only plain table references are supported (no functions, VALUES, etc.)";
				hash_destroy(oid_counts);
				return false;
			}
		}

		hash_destroy(oid_counts);

		/* self-join + GROUP BY/DISTINCT is handled by incr_build_self_join_agg_* */
	}

	if (nbasetables == 1)
	{
		/* Phase 1: single source table — nothing extra to check */
	}
	else if (nbasetables >= 2)
	{
		/*
		 * Phase 2+: N-table JOIN.  Require an explicit JOIN ... ON tree
		 * rooted at the single FromExpr.fromlist entry.
		 */
		Node	   *jtree_root = NULL;

		if (IsA(viewQuery->jointree, FromExpr))
		{
			FromExpr   *fe = (FromExpr *) viewQuery->jointree;

			if (fe->fromlist != NIL)
				jtree_root = linitial(fe->fromlist);
		}

		if (jtree_root == NULL || !IsA(jtree_root, JoinExpr))
		{
			*reason = "multiple source tables require explicit INNER JOIN ... ON syntax";
			return false;
		}

		/*
		 * Walk every JoinExpr in the tree.
		 *
		 * Accepted:
		 *   INNER JOIN with ON (equi or non-equi) or without ON (CROSS JOIN)
		 *   LEFT / RIGHT / FULL OUTER JOIN with ON
		 *
		 * Rejected:
		 *   CROSS JOIN mixed with any outer join (the Phase 8 recompute strategy
		 *   needs an equi-join key to identify the preserved-side anchor)
		 *   FULL OUTER JOIN with GROUP BY (orphan rows produce NULL group keys
		 *   that break the ON CONFLICT UPSERT strategy)
		 */
		{
			List	   *stack = list_make1(jtree_root);
			ListCell   *slc;
			bool		has_full_join = false;
			bool		has_outer_join = false;
			bool		has_cross_join = false;

			foreach(slc, stack)
			{
				JoinExpr   *je = lfirst(slc);

				if (!IsA(je, JoinExpr))
					continue;
				if (je->jointype != JOIN_INNER &&
					je->jointype != JOIN_LEFT &&
					je->jointype != JOIN_RIGHT &&
					je->jointype != JOIN_FULL)
				{
					*reason = "only INNER/CROSS, LEFT, RIGHT, and FULL OUTER JOINs are supported";
					return false;
				}
				if (je->quals == NULL && je->jointype != JOIN_INNER)
				{
					*reason = "LEFT, RIGHT, and FULL OUTER JOINs require an ON condition";
					return false;
				}
				if (je->quals == NULL)
					has_cross_join = true;
				if (je->jointype == JOIN_LEFT || je->jointype == JOIN_RIGHT ||
					je->jointype == JOIN_FULL)
					has_outer_join = true;
				if (je->jointype == JOIN_FULL)
					has_full_join = true;
				if (IsA(je->larg, JoinExpr))
					stack = lappend(stack, je->larg);
				if (IsA(je->rarg, JoinExpr))
					stack = lappend(stack, je->rarg);
			}

			if (has_cross_join && has_outer_join)
			{
				*reason = "CROSS JOIN cannot be combined with LEFT/RIGHT/FULL OUTER JOIN "
						  "in an incremental matview";
				return false;
			}
			if (has_full_join && viewQuery->groupClause != NIL)
			{
				*reason = "FULL OUTER JOIN with GROUP BY is not supported; "
						  "omit GROUP BY or use INNER/LEFT/RIGHT JOIN instead";
				return false;
			}
		}
	}

	/* Validate SELECT list expressions */
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk)
			continue;
		if (incr_is_hidden_col(te->resname))
			continue;
		if (IsA(te->expr, Var))
			continue;

		if (viewQuery->groupClause == NIL)
		{
			/*
			 * Row-level view (no GROUP BY): Aggref is not meaningful here.
			 * Allow any expression that incr_validate_expr accepts.
			 */
			if (IsA(te->expr, Aggref))
			{
				*reason = "aggregate functions require GROUP BY";
				return false;
			}
			if (!incr_validate_expr((Node *) te->expr, NULL, false))
			{
				*reason = "SELECT expression uses unsupported constructs; "
					"only column references, constants, and simple "
					"operators/functions are allowed";
				return false;
			}
			continue;
		}

		/* GROUP BY view: only Var or supported Aggref */
		if (IsA(te->expr, Aggref))
		{
			Aggref	   *agg = (Aggref *) te->expr;
			char	   *fname = get_func_name(agg->aggfnoid);

			if (strcmp(fname, "sum") == 0 || strcmp(fname, "count") == 0 ||
				strcmp(fname, "avg") == 0 || strcmp(fname, "min") == 0 ||
				strcmp(fname, "max") == 0)
			{
				if (agg->args != NIL)
				{
					TargetEntry *arg_te = linitial_node(TargetEntry, agg->args);

					if (!incr_validate_expr((Node *) arg_te->expr, NULL, false))
					{
						*reason = psprintf("argument of aggregate \"%s\" uses "
										   "unsupported expressions; only column "
										   "references, constants, and arithmetic "
										   "operators are allowed", fname);
						return false;
					}
				}
				continue;
			}

			*reason = psprintf("aggregate \"%s\" not supported "
							   "(supported: SUM, COUNT, AVG, MIN, MAX)", fname);
			return false;
		}
		*reason = "only column references and SUM/COUNT/AVG/MIN/MAX aggregates are allowed "
			"in GROUP BY matviews";
		return false;
	}

	/* Validate WHERE clause if present (Phase 5) */
	{
		Node	   *where_qual = incr_get_where_qual(viewQuery);

		if (where_qual != NULL && !incr_validate_expr(where_qual, NULL, false))
		{
			*reason = "WHERE clause uses unsupported expressions; "
				"only column references, constants, comparisons, "
				"boolean operators, and IN lists are allowed";
			return false;
		}
	}

	return true;
}

/*
 * incr_install_triggers — install the three AFTER STATEMENT triggers (INSERT,
 * DELETE, UPDATE) on srctable that drive matview mvrelid.
 */
static void
incr_install_triggers(Oid mvrelid, Oid srctable)
{
	incr_create_trigger(mvrelid, srctable,
						TRIGGER_TYPE_INSERT,
						MATVIEW_INCR_NEWTABLE, NULL);
	incr_create_trigger(mvrelid, srctable,
						TRIGGER_TYPE_DELETE,
						NULL, MATVIEW_INCR_OLDTABLE);
	incr_create_trigger(mvrelid, srctable,
						TRIGGER_TYPE_UPDATE,
						MATVIEW_INCR_NEWTABLE, MATVIEW_INCR_OLDTABLE);
}

/*
 * MatviewIncrSetup
 * Called from ExecCreateTableAs after the matview is created and populated.
 * __mv_count__ is already present and populated — injected by
 * MatviewIncrAddCountTarget() before matview creation.
 *
 * Phase 1 (1 source table): 3 triggers on that table.
 * Phase 2 (2-table INNER JOIN): separate delta SQL per source table,
 *   6 triggers total (3 per table).
 */
void
MatviewIncrSetup(Oid mvrelid, Query *viewQuery)
{
	const char *reason;
	List	   *groupColNames = NIL;
	char	   *ins_sql,
			   *del_sql,
			   *cln_sql,
			   *hav_sql;
	int			nbasetables = 0;
	bool		hasHaving;
	char	   *origschema = NULL;
	char	   *origname = NULL;
	ListCell   *lc;

	if (!MatviewIncrIsEligible(viewQuery, &reason))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot use incremental_refresh: %s", reason)));

	/* UNION ALL: separate setup path */
	if (viewQuery->setOperations != NULL)
	{
		incr_setup_union_all(mvrelid, viewQuery);
		ereport(DEBUG1,
				(errmsg("DBblue: incremental refresh (UNION ALL) set up for matview %s",
						get_rel_name(mvrelid))));
		return;
	}

	/* Count base tables to determine phase */
	foreach(lc, viewQuery->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		if (rte->rtekind == RTE_RELATION)
			nbasetables++;
	}

	hasHaving = (viewQuery->havingQual != NULL);

	/*
	 * Row-level matviews (no GROUP BY) skip the unique index — there are no
	 * group-by columns to index on, and row identity uses full-row matching.
	 */
	if (viewQuery->groupClause != NIL)
	{
		incr_collect_group_cols(viewQuery, &groupColNames);
		incr_create_unique_index(mvrelid, groupColNames);
	}

	/*
	 * Step 2 (HAVING only): rename the internal matview to
	 * _dbblue_<mvrelid>_base so we can create a user-facing VIEW with the
	 * original name that filters on __mv_having_ok__.
	 *
	 * After this rename mv_qname(mvrelid) returns the base table name, so
	 * all subsequent SQL builders reference the base table automatically.
	 */
	if (hasHaving)
	{
		origschema = pstrdup(get_namespace_name(get_rel_namespace(mvrelid)));
		origname   = pstrdup(get_rel_name(mvrelid));
		RenameRelationInternal(mvrelid,
							   psprintf("_dbblue_%u_base", mvrelid),
							   false, false);
		/* Flush relcache so mv_qname() sees the new name immediately */
		CommandCounterIncrement();
	}

	cln_sql = incr_build_cln_sql(mvrelid);
	hav_sql = hasHaving ? incr_build_hav_sql(mvrelid, viewQuery) : NULL;

	if (nbasetables == 1)
	{
		/* ---- Phase 1 / Phase 9a: single source table ---- */
		Oid		srctable = incr_get_source_table(viewQuery);

		if (viewQuery->groupClause == NIL)
		{
			/* Phase 9a: row-level, no GROUP BY */
			incr_warn_row_level_missing_key(viewQuery);
			ins_sql = incr_build_row_ins_sql(mvrelid, viewQuery, -1,
											 MATVIEW_INCR_NEWTABLE, NIL);
			del_sql = incr_build_row_del_sql(mvrelid, viewQuery, -1,
											 MATVIEW_INCR_OLDTABLE, NIL);
			incr_store_catalog(mvrelid, srctable,
							   ins_sql, del_sql, "SELECT 1", NULL, NULL);
		}
		else
		{
			/* Phase 1: aggregate with GROUP BY */
			if (incr_has_minmax_agg(viewQuery))
			{
				char *lock_sql;

				ins_sql  = incr_build_minmax_ins_sql_gen(mvrelid, viewQuery, -1,
														 MATVIEW_INCR_NEWTABLE, NIL);
				del_sql  = incr_build_minmax_del_sql_gen(mvrelid, viewQuery, -1,
														 MATVIEW_INCR_OLDTABLE, NIL,
														 srctable);
				lock_sql = incr_build_minmax_lock_sql_gen(mvrelid, viewQuery, -1,
														  MATVIEW_INCR_OLDTABLE, NIL);
				incr_store_catalog(mvrelid, srctable, ins_sql, del_sql,
								   cln_sql, hav_sql, lock_sql);
			}
			else
			{
				ins_sql = incr_build_ins_sql_gen(mvrelid, viewQuery, -1,
												 MATVIEW_INCR_NEWTABLE, NIL);
				del_sql = incr_build_del_sql_gen(mvrelid, viewQuery, -1,
												 MATVIEW_INCR_OLDTABLE, NIL);
				incr_store_catalog(mvrelid, srctable, ins_sql, del_sql,
								   cln_sql, hav_sql, NULL);
			}

			if (hasHaving)
			{
				char *backfill_sql = incr_build_backfill_sql_gen(
					mvrelid, viewQuery, -1, mv_qname(srctable), NIL);
				int   spi_ret;

				OpenMatViewIncrementalMaintenance();
				SPI_connect();
				spi_ret = SPI_execute(backfill_sql, false, 0);
				SPI_finish();
				CloseMatViewIncrementalMaintenance();
				if (spi_ret < 0)
					elog(ERROR, "DBblue: HAVING backfill failed (code %d)",
						 spi_ret);
			}
		}

		incr_install_triggers(mvrelid, srctable);
	}
	else
	{
		/* ---- Phase 2+: N-table JOIN ---- */
		List	   *all_tables = incr_collect_tables(viewQuery);
		ListCell   *jlc;

		if (viewQuery->groupClause == NIL)
		{
			/* ---- Phase 9b: row-level JOIN matview ---- */
			incr_warn_row_level_missing_key(viewQuery);

			if (incr_has_outer_join(all_tables))
			{
				/* Outer join: sync-region approach preserves LEFT/RIGHT/FULL semantics */
				foreach(jlc, all_tables)
				{
					IncrJoinEntry *delta = lfirst(jlc);

					ins_sql = incr_build_outer_row_sync_sql(mvrelid, viewQuery,
															delta->varno,
															MATVIEW_INCR_NEWTABLE,
															all_tables);
					del_sql = incr_build_outer_row_sync_sql(mvrelid, viewQuery,
															delta->varno,
															MATVIEW_INCR_OLDTABLE,
															all_tables);
					incr_store_catalog(mvrelid, delta->oid,
									   ins_sql, del_sql, "SELECT 1", NULL, NULL);
					incr_install_triggers(mvrelid, delta->oid);
				}
			}
			else
			{
				/* Inner join (and CROSS JOIN): simple row-level delta.
			 * For self-joins, combine both roles into a UNION ALL. */
				if (incr_has_self_join(all_tables))
				{
					/*
					 * Self-join: iterate unique OIDs only.  For each
					 * self-joined OID, build a combined UNION ALL SQL that
					 * handles the delta in both roles.
					 */
					Bitmapset *done_oids = NULL;

					foreach(jlc, all_tables)
					{
						IncrJoinEntry *delta = lfirst(jlc);

						if (bms_is_member((int) delta->oid, done_oids))
							continue;

						/* Check if this OID appears twice (self-join) */
						{
						int v2 = incr_self_join_other_varno(all_tables,
														 delta->varno,
														 delta->oid);
						int v1;
						int vtmp;
						if (v2 != -1)
						{
							/* Ensure v1 < v2 so we process each pair once */
							v1 = delta->varno;
							if (v1 > v2) { vtmp = v1; v1 = v2; v2 = vtmp; }

							ins_sql = incr_build_self_join_row_ins_sql(
								mvrelid, viewQuery, v1, v2,
								MATVIEW_INCR_NEWTABLE, all_tables);
							del_sql = incr_build_self_join_row_del_sql(
								mvrelid, viewQuery, v1, v2,
								MATVIEW_INCR_OLDTABLE, all_tables);
						}
						else
						{
							/* Regular (non-self-joined) table */
							List *join_list = incr_build_join_list_for_delta(
								all_tables, delta->varno);
							ins_sql = incr_build_row_ins_sql(mvrelid, viewQuery,
															 delta->varno,
															 MATVIEW_INCR_NEWTABLE,
															 join_list);
							del_sql = incr_build_row_del_sql(mvrelid, viewQuery,
															 delta->varno,
															 MATVIEW_INCR_OLDTABLE,
															 join_list);
						}

						incr_store_catalog(mvrelid, delta->oid,
										   ins_sql, del_sql, "SELECT 1", NULL, NULL);
						incr_install_triggers(mvrelid, delta->oid);
						done_oids = bms_add_member(done_oids, (int) delta->oid);
						} /* end v2 block */
					}
				}
				else
				{
					foreach(jlc, all_tables)
					{
						IncrJoinEntry *delta     = lfirst(jlc);
						List		  *join_list = incr_build_join_list_for_delta(
							all_tables, delta->varno);

						ins_sql = incr_build_row_ins_sql(mvrelid, viewQuery,
														 delta->varno,
														 MATVIEW_INCR_NEWTABLE,
														 join_list);
						del_sql = incr_build_row_del_sql(mvrelid, viewQuery,
														 delta->varno,
														 MATVIEW_INCR_OLDTABLE,
														 join_list);
						incr_store_catalog(mvrelid, delta->oid,
										   ins_sql, del_sql, "SELECT 1", NULL, NULL);
						incr_install_triggers(mvrelid, delta->oid);
					}
				}
			}
		}
		else if (incr_has_outer_join(all_tables))
		{
			/* ---- Phase 8: outer join (LEFT/RIGHT/FULL) recompute strategy ---- */
			int		preserved_varno = incr_outer_preserved_varno(all_tables);

			foreach(jlc, all_tables)
			{
				IncrJoinEntry *delta        = lfirst(jlc);
				bool           is_preserved = (delta->varno == preserved_varno);

				ins_sql = incr_build_outer_sql(mvrelid, viewQuery,
											   delta->varno,
											   MATVIEW_INCR_NEWTABLE,
											   all_tables, false);
				del_sql = incr_build_outer_sql(mvrelid, viewQuery,
											   delta->varno,
											   MATVIEW_INCR_OLDTABLE,
											   all_tables, is_preserved);
				incr_store_catalog(mvrelid, delta->oid,
								   ins_sql, del_sql, "SELECT 1", hav_sql, NULL);
				incr_install_triggers(mvrelid, delta->oid);
			}
		}
		else
		{
			/* ---- Phase 2-7: N-table INNER JOIN ---- */
			if (incr_has_self_join(all_tables))
			{
				/*
				 * Self-join + GROUP BY: each self-joined OID needs both roles
				 * (e and m) merged into a single catalog entry.  We wrap two
				 * incr_build_ins/del_sql_gen calls in a CTE so they execute as
				 * one SPI statement.  Data-modifying CTEs always run to
				 * completion regardless of whether their output is referenced.
				 *
				 * MIN/MAX with self-join is not supported: the CTE-wrapping
				 * approach would require nested WITH which PostgreSQL disallows.
				 */
				Bitmapset *done_oids = NULL;

				if (incr_has_minmax_agg(viewQuery))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot use incremental_refresh: "
									"self-join with MIN or MAX is not yet supported")));

				foreach(jlc, all_tables)
				{
					IncrJoinEntry *delta = lfirst(jlc);

					if (bms_is_member((int) delta->oid, done_oids))
						continue;
					{
					int v2 = incr_self_join_other_varno(all_tables, delta->varno, delta->oid);
					int v1, vtmp;

					if (v2 != -1)
					{
						v1 = delta->varno;
						if (v1 > v2) { vtmp = v1; v1 = v2; v2 = vtmp; }

						ins_sql = incr_build_self_join_agg_ins_sql(
							mvrelid, viewQuery, v1, v2,
							MATVIEW_INCR_NEWTABLE, all_tables);
						del_sql = incr_build_self_join_agg_del_sql(
							mvrelid, viewQuery, v1, v2,
							MATVIEW_INCR_OLDTABLE, all_tables);
					}
					else
					{
						List *join_list = incr_build_join_list_for_delta(
							all_tables, delta->varno);

						if (incr_has_minmax_agg(viewQuery))
							ins_sql = incr_build_minmax_ins_sql_gen(
								mvrelid, viewQuery, delta->varno,
								MATVIEW_INCR_NEWTABLE, join_list);
						else
							ins_sql = incr_build_ins_sql_gen(mvrelid, viewQuery,
															 delta->varno,
															 MATVIEW_INCR_NEWTABLE,
															 join_list);
						del_sql = incr_build_del_sql_gen(mvrelid, viewQuery,
														 delta->varno,
														 MATVIEW_INCR_OLDTABLE,
														 join_list);
					}

					incr_store_catalog(mvrelid, delta->oid,
									   ins_sql, del_sql, cln_sql, hav_sql, NULL);
					incr_install_triggers(mvrelid, delta->oid);
					done_oids = bms_add_member(done_oids, (int) delta->oid);
					} /* end v2 block */
				}
			}
			else
			{
			foreach(jlc, all_tables)
			{
				IncrJoinEntry *delta     = lfirst(jlc);
				List		  *join_list = incr_build_join_list_for_delta(
					all_tables, delta->varno);

				if (incr_has_minmax_agg(viewQuery))
				{
					char *lock_sql;

					ins_sql  = incr_build_minmax_ins_sql_gen(
						mvrelid, viewQuery, delta->varno,
						MATVIEW_INCR_NEWTABLE, join_list);
					del_sql  = incr_build_minmax_del_sql_gen(mvrelid, viewQuery,
															 delta->varno,
															 MATVIEW_INCR_OLDTABLE,
															 join_list, delta->oid);
					lock_sql = incr_build_minmax_lock_sql_gen(mvrelid, viewQuery,
															  delta->varno,
															  MATVIEW_INCR_OLDTABLE,
															  join_list);
					incr_store_catalog(mvrelid, delta->oid,
									   ins_sql, del_sql, cln_sql, hav_sql, lock_sql);
				}
				else
				{
					ins_sql = incr_build_ins_sql_gen(mvrelid, viewQuery,
													 delta->varno,
													 MATVIEW_INCR_NEWTABLE,
													 join_list);
					del_sql = incr_build_del_sql_gen(mvrelid, viewQuery,
													 delta->varno,
													 MATVIEW_INCR_OLDTABLE,
													 join_list);
					incr_store_catalog(mvrelid, delta->oid,
									   ins_sql, del_sql, cln_sql, hav_sql, NULL);
				}
				incr_install_triggers(mvrelid, delta->oid);
			}
			} /* end !has_self_join */

			/*
			 * HAVING backfill: seed all groups from the real tables so groups
			 * that initially fail HAVING are tracked.
			 */
			if (hasHaving)
			{
				IncrJoinEntry *first_je  = linitial(all_tables);
				List		  *join_list = incr_build_join_list_for_delta(
					all_tables, first_je->varno);
				char		  *backfill_sql = incr_build_backfill_sql_gen(
					mvrelid, viewQuery,
					first_je->varno, mv_qname(first_je->oid), join_list);
				int			   spi_ret;

				OpenMatViewIncrementalMaintenance();
				SPI_connect();
				spi_ret = SPI_execute(backfill_sql, false, 0);
				SPI_finish();
				CloseMatViewIncrementalMaintenance();
				if (spi_ret < 0)
					elog(ERROR,
						 "DBblue: HAVING backfill (JOIN) failed (code %d)",
						 spi_ret);
			}
		}
	}

	/* Step 3 (HAVING only): create the user-facing VIEW over the base table */
	if (hasHaving)
		incr_create_having_view(mvrelid, origschema, origname, viewQuery);

	ereport(DEBUG1,
			(errmsg("DBblue: incremental refresh (Phase %d%s) set up for matview %s",
					nbasetables,
					hasHaving ? " + HAVING" : "",
					hasHaving ? origname : get_rel_name(mvrelid))));
}

/*
 * MatviewIncrTeardown
 * Remove pg_dbblue_matview rows on DROP MATERIALIZED VIEW.
 * Triggers are removed via DROP CASCADE automatically.
 */
void
MatviewIncrTeardown(Oid mvrelid)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tup;

	catalog = table_open(DbblueMatviewRelationId, RowExclusiveLock);
	ScanKeyInit(&key,
				Anum_pg_dbblue_matview_mvrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(mvrelid));
	scan = systable_beginscan(catalog,
							  DbblueMatviewMvrelidIndexId,
							  true, NULL, 1, &key);
	while ((tup = systable_getnext(scan)) != NULL)
		CatalogTupleDelete(catalog, &tup->t_self);
	systable_endscan(scan);
	table_close(catalog, RowExclusiveLock);
}

/*
 * MatviewIncrAddCountTarget
 * Append hidden maintenance columns to the query's target list.
 *
 * For every AVG(x) column in the query:
 *   SUM(x) AS __mv_avgsum_colname__   — running sum for that AVG
 *   COUNT(x) AS __mv_avgcnt_colname__ — running non-null count for that AVG
 *
 * Finally, always append:
 *   COUNT(*) AS __mv_count__           — source-row count per group
 *
 * Called before matview creation so these columns are part of the initial
 * schema and populated naturally by the first SELECT — no ALTER TABLE needed.
 */
void
MatviewIncrAddCountTarget(Query *q)
{
	List	   *orig_tl = list_copy(q->targetList);
	ListCell   *lc;
	int			next_resno = list_length(q->targetList) + 1;
	Aggref	   *aggref;
	TargetEntry *te;

	/* UNION ALL: __mv_count__ is added via ALTER TABLE in incr_setup_union_all */
	if (q->setOperations != NULL)
		return;

	/* Row-level views (no GROUP BY or DISTINCT) need no hidden maintenance columns */
	if (q->groupClause == NIL && q->distinctClause == NIL)
		return;

	/*
	 * DISTINCT is equivalent to GROUP BY on all output columns.  Convert it
	 * here so the rest of the aggregate machinery (COUNT(*) injection, SQL
	 * builders, unique index) works without modification.
	 *
	 * Also set hasAggs = true so the planner creates an Agg node and fills in
	 * aggtranstype for the COUNT(*) we are about to inject.  Without this the
	 * planner skips aggregate pre-processing and ExecInitAgg asserts on
	 * InvalidOid.
	 */
	if (q->distinctClause != NIL && q->groupClause == NIL)
	{
		q->groupClause    = q->distinctClause;
		q->distinctClause = NIL;
		q->hasAggs        = true;
	}

	/* Inject SUM(x) / COUNT(x) pairs for each AVG column */
	foreach(lc, orig_tl)
	{
		TargetEntry *orig_te = lfirst_node(TargetEntry, lc);
		Aggref	   *avg_agg;
		Oid			sum_fnoid,
					sum_rettype;
		Aggref	   *sum_agg,
				   *cnt_agg;

		if (orig_te->resjunk || !IsA(orig_te->expr, Aggref))
			continue;
		avg_agg = (Aggref *) orig_te->expr;
		if (strcmp(get_func_name(avg_agg->aggfnoid), "avg") != 0)
			continue;

		sum_fnoid = incr_find_sum_agg(avg_agg->aggfnoid, &sum_rettype);

		/* SUM(x) hidden column */
		sum_agg = copyObject(avg_agg);
		sum_agg->aggfnoid = sum_fnoid;
		sum_agg->aggtype = sum_rettype;
		sum_agg->aggtranstype = InvalidOid;
		sum_agg->aggno = -1;
		sum_agg->aggtransno = -1;
		te = makeTargetEntry((Expr *) sum_agg, next_resno++,
							 psprintf("%s%s", MATVIEW_INCR_AVGSUM_PREFIX,
									  orig_te->resname),
							 false);
		q->targetList = lappend(q->targetList, te);

		/* COUNT(x) hidden column — count("any") OID = 2147 */
		cnt_agg = makeNode(Aggref);
		cnt_agg->aggfnoid = 2147;
		cnt_agg->aggtype = INT8OID;
		cnt_agg->aggcollid = InvalidOid;
		cnt_agg->inputcollid = InvalidOid;
		cnt_agg->aggtranstype = InvalidOid;
		cnt_agg->aggargtypes = avg_agg->aggargtypes;
		cnt_agg->aggdirectargs = NIL;
		cnt_agg->args = copyObject(avg_agg->args);
		cnt_agg->aggorder = NIL;
		cnt_agg->aggdistinct = NIL;
		cnt_agg->aggfilter = NULL;
		cnt_agg->aggstar = false;
		cnt_agg->aggvariadic = false;
		cnt_agg->aggkind = AGGKIND_NORMAL;
		cnt_agg->aggpresorted = false;
		cnt_agg->agglevelsup = 0;
		cnt_agg->aggsplit = AGGSPLIT_SIMPLE;
		cnt_agg->aggno = -1;
		cnt_agg->aggtransno = -1;
		cnt_agg->location = -1;
		te = makeTargetEntry((Expr *) cnt_agg, next_resno++,
							 psprintf("%s%s", MATVIEW_INCR_AVGCNT_PREFIX,
									  orig_te->resname),
							 false);
		q->targetList = lappend(q->targetList, te);
	}

	/*
	 * If HAVING present, inject __mv_having_ok__ = true.  All rows in the
	 * initial population pass HAVING (PostgreSQL applies it during CREATE),
	 * so true is correct.  The hav_sql step recomputes it after every delta.
	 */
	if (q->havingQual != NULL)
	{
		Const	   *c = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool),
								  BoolGetDatum(true), false, true);
		te = makeTargetEntry((Expr *) c, next_resno++,
							 pstrdup(MATVIEW_INCR_HAVING_COL),
							 false);
		q->targetList = lappend(q->targetList, te);
	}

	/* Always append COUNT(*) AS __mv_count__ */
	aggref = makeNode(Aggref);
	aggref->aggfnoid = 2803;		/* count(*) — stable catalog OID */
	aggref->aggtype = INT8OID;
	aggref->aggcollid = InvalidOid;
	aggref->inputcollid = InvalidOid;
	aggref->aggtranstype = InvalidOid;
	aggref->aggargtypes = NIL;
	aggref->aggdirectargs = NIL;
	aggref->args = NIL;
	aggref->aggorder = NIL;
	aggref->aggdistinct = NIL;
	aggref->aggfilter = NULL;
	aggref->aggstar = true;
	aggref->aggvariadic = false;
	aggref->aggkind = AGGKIND_NORMAL;
	aggref->aggpresorted = false;
	aggref->agglevelsup = 0;
	aggref->aggsplit = AGGSPLIT_SIMPLE;
	aggref->aggno = -1;
	aggref->aggtransno = -1;
	aggref->location = -1;
	te = makeTargetEntry((Expr *) aggref, next_resno,
						 pstrdup(MATVIEW_INCR_COUNT_COL),
						 false);
	q->targetList = lappend(q->targetList, te);
}

/* ============================================================
 * Internal helpers — query introspection
 * ============================================================
 */

/* True for columns that are hidden maintenance state, not user-visible output */
static bool
incr_is_hidden_col(const char *resname)
{
	if (resname == NULL)
		return false;
	if (strcmp(resname, MATVIEW_INCR_COUNT_COL) == 0)
		return true;
	if (strcmp(resname, MATVIEW_INCR_HAVING_COL) == 0)
		return true;
	if (strncmp(resname, MATVIEW_INCR_AVGSUM_PREFIX,
				strlen(MATVIEW_INCR_AVGSUM_PREFIX)) == 0)
		return true;
	if (strncmp(resname, MATVIEW_INCR_AVGCNT_PREFIX,
				strlen(MATVIEW_INCR_AVGCNT_PREFIX)) == 0)
		return true;
	return false;
}

/*
 * incr_find_sum_agg
 * Return the SUM aggregate OID (and its return type) that corresponds to the
 * given AVG aggregate OID.  Covers all built-in numeric and interval types.
 */
static Oid
incr_find_sum_agg(Oid avg_fnoid, Oid *rettype_out)
{
	static const struct
	{
		Oid		avg;
		Oid		sum;
		Oid		ret;
	}			map[] = {
		{2100, 2107, NUMERICOID},	/* avg/sum(int8) */
		{2101, 2108, INT8OID},		/* avg/sum(int4) */
		{2102, 2109, INT8OID},		/* avg/sum(int2) */
		{2103, 2114, NUMERICOID},	/* avg/sum(numeric) */
		{2104, 2110, FLOAT4OID},	/* avg/sum(float4) */
		{2105, 2111, FLOAT8OID},	/* avg/sum(float8) */
		{2106, 2113, INTERVALOID},	/* avg/sum(interval) */
	};

	for (int i = 0; i < lengthof(map); i++)
	{
		if (map[i].avg == avg_fnoid)
		{
			*rettype_out = map[i].ret;
			return map[i].sum;
		}
	}
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("incremental_refresh: AVG aggregate OID %u not supported "
					"(numeric, integer, float, and interval types only)",
					avg_fnoid)));
	return InvalidOid;
}

/* ============================================================
 * HAVING helpers — Phase 4
 * ============================================================
 */

/*
 * incr_resolve_var_colname
 * incr_get_where_qual
 * Return the WHERE qual for single-table or explicit-JOIN queries.
 * For implicit joins (FROM t1, t2 WHERE ...) the quals already serve as the
 * ON clause in delta SQL, so we return NULL to avoid double-filtering.
 */
static Node *
incr_get_where_qual(Query *viewQuery)
{
	FromExpr   *fe;

	if (!IsA(viewQuery->jointree, FromExpr))
		return NULL;

	fe = (FromExpr *) viewQuery->jointree;

	/*
	 * Implicit join: fromlist has 2+ entries.  fe->quals is the join
	 * condition, already emitted as ON clause in delta SQL — not a WHERE.
	 */
	if (list_length(fe->fromlist) > 1)
		return NULL;

	return fe->quals;				/* single table or explicit JOIN */
}

/*
 * incr_validate_expr — general expression validator.
 *
 *   allow_aggref = true  (HAVING): Aggref allowed when matched in SELECT list;
 *                         FuncExpr allowed without volatility check.
 *   allow_aggref = false (WHERE / aggregate arg): no Aggref; FuncExpr must be
 *                         stable or immutable; ScalarArrayOpExpr and ArrayExpr
 *                         (IN lists) are also permitted.
 *
 * viewQuery is used only when allow_aggref=true; pass NULL otherwise.
 */
static bool
incr_validate_expr(Node *expr, Query *viewQuery, bool allow_aggref)
{
	ListCell   *lc;

	if (expr == NULL)
		return true;

	if (IsA(expr, Var) || IsA(expr, Const))
		return true;

	if (IsA(expr, Aggref))
	{
		Aggref	   *agg = (Aggref *) expr;
		char	   *fname;

		if (!allow_aggref)
			return false;

		fname = get_func_name(agg->aggfnoid);

		/* COUNT(*) is always maintained via __mv_count__ */
		if (strcmp(fname, "count") == 0 && agg->aggstar)
			return true;

		/* Other aggregates must match a SELECT target */
		foreach(lc, viewQuery->targetList)
		{
			TargetEntry *te = lfirst_node(TargetEntry, lc);

			if (!IsA(te->expr, Aggref))
				continue;
			if (((Aggref *) te->expr)->aggfnoid == agg->aggfnoid)
				return true;
		}
		return false;
	}

	if (IsA(expr, NullTest))
		return incr_validate_expr((Node *) ((NullTest *) expr)->arg,
								  viewQuery, allow_aggref);

	if (IsA(expr, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) expr;

		foreach(lc, op->args)
			if (!incr_validate_expr(lfirst(lc), viewQuery, allow_aggref))
				return false;
		return true;
	}

	if (IsA(expr, BoolExpr))
	{
		BoolExpr   *be = (BoolExpr *) expr;

		foreach(lc, be->args)
			if (!incr_validate_expr(lfirst(lc), viewQuery, allow_aggref))
				return false;
		return true;
	}

	if (IsA(expr, FuncExpr))
	{
		FuncExpr   *fe = (FuncExpr *) expr;

		if (fe->funcretset)
			return false;
		/* WHERE/agg-arg mode: volatile functions break incremental maintenance */
		if (!allow_aggref && func_volatile(fe->funcid) == PROVOLATILE_VOLATILE)
			return false;
		foreach(lc, fe->args)
			if (!incr_validate_expr(lfirst(lc), viewQuery, allow_aggref))
				return false;
		return true;
	}

	/* RelabelType: a no-op type coercion (e.g. varchar = 'x' -> text = 'x') */
	if (IsA(expr, RelabelType))
		return incr_validate_expr((Node *) ((RelabelType *) expr)->arg,
								  viewQuery, allow_aggref);

	if (!allow_aggref)
	{
		/* WHERE-only node types (not meaningful in HAVING) */
		if (IsA(expr, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *sao = (ScalarArrayOpExpr *) expr;

			foreach(lc, sao->args)
				if (!incr_validate_expr(lfirst(lc), viewQuery, allow_aggref))
					return false;
			return true;
		}

		if (IsA(expr, ArrayExpr))
		{
			ArrayExpr  *ae = (ArrayExpr *) expr;

			foreach(lc, ae->elements)
				if (!incr_validate_expr(lfirst(lc), viewQuery, allow_aggref))
					return false;
			return true;
		}
	}

	return false;
}

/*
 * incr_deparse_where_qual
 * Render a WHERE qual to SQL.
 *   delta_varno < 0  → Phase 1: Var emitted as bare column name
 *   delta_varno >= 1 → Phase 2: Var gets _d_ / _j_ table alias
 */
static void
incr_deparse_where_qual(Node *qual, List *rtable, int delta_varno, StringInfo buf)
{
	ListCell   *lc;

	if (qual == NULL)
		return;

	if (IsA(qual, Var))
	{
		Var		   *v = (Var *) qual;
		int			resolved_varno;
		const char *colname = incr_resolve_var_colname(v, rtable, &resolved_varno);

		if (delta_varno < 0)
			/* Phase 1: bare column name — transition table has no alias */
			appendStringInfoString(buf, quote_identifier(colname));
		else if (resolved_varno == delta_varno)
			/* delta table always gets _d_ */
			appendStringInfo(buf, "%s.%s", INCR_DELTA_ALIAS, quote_identifier(colname));
		else
			/* each join table gets its own _j<varno>_ alias */
			appendStringInfo(buf, "_j%d_.%s", resolved_varno, quote_identifier(colname));
		return;
	}

	if (IsA(qual, Const))
	{
		Const	   *c = (Const *) qual;

		if (c->constisnull)
			appendStringInfoString(buf, "NULL");
		else
		{
			Oid			outfunc;
			bool		typIsVarlena;
			char	   *val;

			getTypeOutputInfo(c->consttype, &outfunc, &typIsVarlena);
			val = OidOutputFunctionCall(outfunc, c->constvalue);
			appendStringInfo(buf, "'%s'::%s", val, format_type_be(c->consttype));
		}
		return;
	}

	if (IsA(qual, NullTest))
	{
		NullTest   *nt = (NullTest *) qual;
		StringInfoData abuf;

		initStringInfo(&abuf);
		incr_deparse_where_qual((Node *) nt->arg, rtable, delta_varno, &abuf);
		appendStringInfo(buf, "(%s IS %sNULL)", abuf.data,
						 nt->nulltesttype == IS_NOT_NULL ? "NOT " : "");
		return;
	}

	if (IsA(qual, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) qual;
		HeapTuple	tup;
		Form_pg_operator opform;
		char	   *opname;

		tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "incr_deparse_where_qual: operator %u not found", op->opno);
		opform = (Form_pg_operator) GETSTRUCT(tup);
		opname = pstrdup(NameStr(opform->oprname));
		ReleaseSysCache(tup);

		appendStringInfoChar(buf, '(');
		incr_deparse_where_qual(linitial(op->args), rtable, delta_varno, buf);
		appendStringInfo(buf, " %s ", opname);
		if (list_length(op->args) > 1)
			incr_deparse_where_qual(lsecond(op->args), rtable, delta_varno, buf);
		appendStringInfoChar(buf, ')');
		return;
	}

	if (IsA(qual, BoolExpr))
	{
		BoolExpr   *be = (BoolExpr *) qual;
		const char *opstr = (be->boolop == AND_EXPR) ? " AND " :
			(be->boolop == OR_EXPR) ? " OR " : "NOT ";
		bool		first = true;

		appendStringInfoChar(buf, '(');
		foreach(lc, be->args)
		{
			if (!first)
				appendStringInfoString(buf, opstr);
			if (be->boolop == NOT_EXPR)
				appendStringInfoString(buf, opstr);
			incr_deparse_where_qual(lfirst(lc), rtable, delta_varno, buf);
			first = false;
		}
		appendStringInfoChar(buf, ')');
		return;
	}

	if (IsA(qual, FuncExpr))
	{
		FuncExpr   *fe = (FuncExpr *) qual;

		/* Implicit cast: single-arg function — emit (arg)::returntype */
		if (list_length(fe->args) == 1)
		{
			appendStringInfoChar(buf, '(');
			incr_deparse_where_qual(linitial(fe->args), rtable, delta_varno, buf);
			appendStringInfo(buf, ")::%s", format_type_be(fe->funcresulttype));
		}
		else
		{
			char	   *fname = get_func_name(fe->funcid);
			bool		first = true;

			appendStringInfo(buf, "%s(", fname);
			foreach(lc, fe->args)
			{
				if (!first)
					appendStringInfoChar(buf, ',');
				incr_deparse_where_qual(lfirst(lc), rtable, delta_varno, buf);
				first = false;
			}
			appendStringInfoChar(buf, ')');
		}
		return;
	}

	if (IsA(qual, ArrayExpr))
	{
		ArrayExpr  *ae = (ArrayExpr *) qual;
		bool		first = true;

		appendStringInfoString(buf, "ARRAY[");
		foreach(lc, ae->elements)
		{
			if (!first)
				appendStringInfoChar(buf, ',');
			incr_deparse_where_qual(lfirst(lc), rtable, delta_varno, buf);
			first = false;
		}
		appendStringInfoChar(buf, ']');
		return;
	}

	if (IsA(qual, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *sao = (ScalarArrayOpExpr *) qual;
		HeapTuple	tup;
		Form_pg_operator opform;
		char	   *opname;

		tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(sao->opno));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "incr_deparse_where_qual: operator %u not found", sao->opno);
		opform = (Form_pg_operator) GETSTRUCT(tup);
		opname = pstrdup(NameStr(opform->oprname));
		ReleaseSysCache(tup);

		appendStringInfoChar(buf, '(');
		incr_deparse_where_qual(linitial(sao->args), rtable, delta_varno, buf);
		appendStringInfo(buf, " %s %s(", opname, sao->useOr ? "ANY" : "ALL");
		incr_deparse_where_qual(lsecond(sao->args), rtable, delta_varno, buf);
		appendStringInfoString(buf, "))");
		return;
	}

	if (IsA(qual, RelabelType))
	{
		RelabelType *rt = (RelabelType *) qual;

		appendStringInfoChar(buf, '(');
		incr_deparse_where_qual((Node *) rt->arg, rtable, delta_varno, buf);
		appendStringInfo(buf, ")::%s", format_type_be(rt->resulttype));
		return;
	}

	elog(ERROR,
		 "incr_deparse_where_qual: unsupported expression type %d",
		 (int) nodeTag(qual));
}

/*
 * Chase RTE_GROUP / RTE_JOIN indirection and return the base-table column
 * name for a Var node.  If resolved_varno_out is non-NULL, also returns the
 * varno of the resolved base RTE (used by incr_deparse_where_qual for aliasing).
 */
static const char *
incr_resolve_var_colname(Var *v, List *rtable, int *resolved_varno_out)
{
	RangeTblEntry *rte;

	for (;;)
	{
		rte = rt_fetch(v->varno, rtable);
		if (rte->rtekind == RTE_RELATION)
			break;
		if (rte->rtekind == RTE_JOIN)
		{
			Node	   *av = list_nth(rte->joinaliasvars, v->varattno - 1);

			if (!IsA(av, Var))
				elog(ERROR, "incr_resolve_var_colname: non-Var in joinaliasvars");
			v = (Var *) av;
		}
		else if (rte->rtekind == RTE_GROUP)
		{
			Node	   *ge = list_nth(rte->groupexprs, v->varattno - 1);

			if (!IsA(ge, Var))
				elog(ERROR, "incr_resolve_var_colname: non-Var in groupexprs");
			v = (Var *) ge;
		}
		else
			elog(ERROR, "incr_resolve_var_colname: unexpected RTE kind %d",
				 (int) rte->rtekind);
	}
	if (resolved_varno_out)
		*resolved_varno_out = v->varno;
	return get_attname(rte->relid, v->varattno, false);
}

/*
 * incr_deparse_having_cond
 * Render the HAVING expression as SQL using matview column names.
 * Aggregates are mapped to the corresponding output column name or
 * the hidden __mv_count__ column.
 */
static void
incr_deparse_having_cond(Node *expr, Query *viewQuery, StringInfo buf)
{
	if (expr == NULL)
		return;

	if (IsA(expr, Aggref))
	{
		Aggref	   *hagg = (Aggref *) expr;
		char	   *fname = get_func_name(hagg->aggfnoid);
		ListCell   *lc;

		if (strcmp(fname, "count") == 0 && hagg->aggstar)
		{
			appendStringInfoString(buf, quote_identifier(MATVIEW_INCR_COUNT_COL));
			return;
		}

		/* Find matching SELECT aggregate by function OID */
		foreach(lc, viewQuery->targetList)
		{
			TargetEntry *te = lfirst_node(TargetEntry, lc);

			if (!IsA(te->expr, Aggref))
				continue;
			if (((Aggref *) te->expr)->aggfnoid == hagg->aggfnoid)
			{
				appendStringInfoString(buf, quote_identifier(te->resname));
				return;
			}
		}
		elog(ERROR, "incr_deparse_having_cond: aggregate %s not found in SELECT list",
			 fname);
	}
	else if (IsA(expr, Var))
	{
		/* Group column — resolve to base column name, then find the output resname */
		Var		   *v = (Var *) expr;
		const char *colname = incr_resolve_var_colname(v, viewQuery->rtable, NULL);
		ListCell   *lc;

		foreach(lc, viewQuery->targetList)
		{
			TargetEntry *te = lfirst_node(TargetEntry, lc);

			if (!IsA(te->expr, Var))
				continue;
			if (strcmp(incr_resolve_var_colname((Var *) te->expr,
												viewQuery->rtable, NULL),
					   colname) == 0)
			{
				appendStringInfoString(buf, quote_identifier(te->resname));
				return;
			}
		}
		appendStringInfoString(buf, quote_identifier(colname));
	}
	else if (IsA(expr, Const))
	{
		Const	   *c = (Const *) expr;

		if (c->constisnull)
		{
			appendStringInfoString(buf, "NULL");
		}
		else
		{
			Oid			outfunc;
			bool		typIsVarlena;
			char	   *val;

			getTypeOutputInfo(c->consttype, &outfunc, &typIsVarlena);
			val = OidOutputFunctionCall(outfunc, c->constvalue);
			appendStringInfo(buf, "'%s'::%s", val, format_type_be(c->consttype));
		}
	}
	else if (IsA(expr, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) expr;
		HeapTuple	tup;
		Form_pg_operator opform;
		char	   *opname;

		tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "incr_deparse_having_cond: operator %u not found", op->opno);
		opform = (Form_pg_operator) GETSTRUCT(tup);
		opname = pstrdup(NameStr(opform->oprname));
		ReleaseSysCache(tup);

		appendStringInfoChar(buf, '(');
		incr_deparse_having_cond(linitial(op->args), viewQuery, buf);
		appendStringInfo(buf, " %s ", opname);
		if (list_length(op->args) > 1)
			incr_deparse_having_cond(lsecond(op->args), viewQuery, buf);
		appendStringInfoChar(buf, ')');
	}
	else if (IsA(expr, BoolExpr))
	{
		BoolExpr   *be = (BoolExpr *) expr;
		const char *opstr = (be->boolop == AND_EXPR) ? " AND " :
			(be->boolop == OR_EXPR) ? " OR " : "NOT ";
		ListCell   *lc;
		bool		first = true;

		appendStringInfoChar(buf, '(');
		foreach(lc, be->args)
		{
			if (!first)
				appendStringInfoString(buf, opstr);
			if (be->boolop == NOT_EXPR)
				appendStringInfoString(buf, opstr);
			incr_deparse_having_cond(lfirst(lc), viewQuery, buf);
			first = false;
		}
		appendStringInfoChar(buf, ')');
	}
	else if (IsA(expr, FuncExpr))
	{
		/* Implicit type-coercion function — emit as arg::returntype */
		FuncExpr   *fe = (FuncExpr *) expr;

		if (list_length(fe->args) == 1)
		{
			appendStringInfoChar(buf, '(');
			incr_deparse_having_cond(linitial(fe->args), viewQuery, buf);
			appendStringInfo(buf, ")::%s", format_type_be(fe->funcresulttype));
		}
		else
			elog(ERROR,
				 "incr_deparse_having_cond: unsupported FuncExpr with %d args",
				 list_length(fe->args));
	}
	else
		elog(ERROR,
			 "incr_deparse_having_cond: unsupported expression type %d",
			 (int) nodeTag(expr));
}

/*
 * incr_build_hav_sql
 * Builds the HAVING maintenance step SQL:
 *   UPDATE <base_table> SET __mv_having_ok__ = (<having_cond>)
 *   WHERE __mv_count__ > 0
 *
 * Runs after every delta to recompute visibility for all live groups.
 * The base table name is derived from mv_qname(mvrelid), which at this
 * point already reflects the renamed _dbblue_<mvrelid>_base table.
 */
static char *
incr_build_hav_sql(Oid mvrelid, Query *viewQuery)
{
	StringInfoData buf;
	StringInfoData cond;

	initStringInfo(&buf);
	initStringInfo(&cond);

	incr_deparse_having_cond(viewQuery->havingQual, viewQuery, &cond);

	appendStringInfo(&buf,
					 "UPDATE %s SET %s=(%s) WHERE %s>0",
					 mv_qname(mvrelid),
					 quote_identifier(MATVIEW_INCR_HAVING_COL),
					 cond.data,
					 quote_identifier(MATVIEW_INCR_COUNT_COL));

	return buf.data;
}

/*
 * incr_create_having_view
 * Create a non-materialized VIEW in <origschema>.<origname> that selects
 * only the user-visible (non-hidden) columns from the renamed base table
 * filtered by __mv_having_ok__.
 */
static void
incr_create_having_view(Oid mvrelid,
						const char *origschema,
						const char *origname,
						Query *viewQuery)
{
	StringInfoData buf;
	ListCell   *lc;
	bool		first = true;
	int			ret;

	initStringInfo(&buf);
	appendStringInfo(&buf, "CREATE VIEW %s AS SELECT ",
					 quote_qualified_identifier(origschema, origname));

	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk || incr_is_hidden_col(te->resname))
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(te->resname));
		first = false;
	}

	appendStringInfo(&buf, " FROM %s WHERE %s",
					 mv_qname(mvrelid),
					 quote_identifier(MATVIEW_INCR_HAVING_COL));

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "incr_create_having_view: SPI_connect failed");

	ret = SPI_exec(buf.data, 0);
	SPI_finish();

	if (ret < 0)
		elog(ERROR, "incr_create_having_view: CREATE VIEW failed: %s",
			 SPI_result_code_string(ret));
}

static Oid
incr_get_source_table(Query *viewQuery)
{
	ListCell   *lc;

	foreach(lc, viewQuery->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		if (rte->rtekind == RTE_RELATION)
			return rte->relid;
	}
	elog(ERROR, "MatviewIncrSetup: no source table found");
	return InvalidOid;
}

/* Collect GROUP BY output column names (as they appear in the matview) */
static void
incr_collect_group_cols(Query *viewQuery, List **groupColNames)
{
	ListCell   *lc;

	*groupColNames = NIL;
	foreach(lc, viewQuery->groupClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
		TargetEntry *te = get_sortgroupclause_tle(sgc, viewQuery->targetList);

		*groupColNames = lappend(*groupColNames,
								 makeString(pstrdup(te->resname)));
	}
}

/* ============================================================
 * SQL builders — unified for Phase 1 (single table) and Phase 2+ (N-way joins)
 *
 * delta_varno: -1  = Phase 1 (bare column names, no table alias)
 *             >=1  = Phase 2+ (_d_ for the delta table; _j<varno>_ for each
 *                              additional join table)
 * delta_table: FROM source name — "__mv_newtable", "__mv_oldtable", or the
 *              qualified real table name (used by the HAVING backfill).
 * join_list:  NIL          = Phase 1 (no join)
 *             List of IncrJoinEntry* = one entry per additional join table,
 *             in join order.  Phase 3+ just adds more entries here.
 * ============================================================
 */

/*
 * incr_append_from_join
 * Append the FROM clause (including optional joins) to buf.
 */
static void
incr_append_from_join(StringInfo buf, Query *viewQuery,
					  int delta_varno,
					  const char *delta_table,
					  List *join_list)
{
	ListCell   *lc;

	if (join_list == NIL)
	{
		appendStringInfo(buf, " FROM %s", delta_table);
		return;
	}

	appendStringInfo(buf, " FROM %s %s", delta_table, INCR_DELTA_ALIAS);
	foreach(lc, join_list)
	{
		IncrJoinEntry  *je = lfirst(lc);

		if (je->quals == NULL)
		{
			/* CROSS JOIN — no ON condition */
			appendStringInfo(buf, " CROSS JOIN %s _j%d_",
							 mv_qname(je->oid), je->varno);
		}
		else
		{
			StringInfoData	jbuf;

			initStringInfo(&jbuf);
			incr_deparse_where_qual(je->quals, viewQuery->rtable, delta_varno, &jbuf);
			appendStringInfo(buf, " JOIN %s _j%d_ ON (%s)",
							 mv_qname(je->oid), je->varno, jbuf.data);
		}
	}
}

/*
 * incr_warn_row_level_missing_key
 *
 * For row-level incremental matviews (no GROUP BY), the del_sql identifies
 * matview rows by matching ALL selected columns.  If two rows in the matview
 * are identical, a single-row DELETE on a source table will remove BOTH.
 *
 * The safest way to avoid this is to include the primary key of every source
 * table in the SELECT list so that every matview row is distinct.
 *
 * This function checks each base relation in the query.  If the SELECT list
 * does NOT contain any primary-key column for that table, it emits a WARNING
 * so the user is informed at CREATE MATERIALIZED VIEW time.
 */
static void
incr_warn_row_level_missing_key(Query *viewQuery)
{
	ListCell *lc;

	foreach(lc, viewQuery->rtable)
	{
		RangeTblEntry  *rte     = lfirst_node(RangeTblEntry, lc);
		int             varno   = foreach_current_index(lc) + 1;
		Relation        rel;
		List		   *idxlist;
		ListCell	   *ilc;
		Bitmapset	   *pk_attrs = NULL;
		bool            covered = false;

		if (rte->rtekind != RTE_RELATION)
			continue;

		/* Collect primary key attribute numbers for this table */
		rel     = table_open(rte->relid, AccessShareLock);
		idxlist = RelationGetIndexList(rel);

		foreach(ilc, idxlist)
		{
			Oid			 indexoid  = lfirst_oid(ilc);
			HeapTuple	 indextup  = SearchSysCache1(INDEXRELID,
												ObjectIdGetDatum(indexoid));
			Form_pg_index idxform;
			int			 k;

			if (!HeapTupleIsValid(indextup))
				continue;
			idxform = (Form_pg_index) GETSTRUCT(indextup);

			if (!idxform->indisprimary)
			{
				ReleaseSysCache(indextup);
				continue;
			}
			for (k = 0; k < idxform->indnkeyatts; k++)
				pk_attrs = bms_add_member(pk_attrs,
										  (int) idxform->indkey.values[k]);
			ReleaseSysCache(indextup);
			break;				/* only one primary key per table */
		}

		table_close(rel, AccessShareLock);

		if (pk_attrs == NULL)
			continue;			/* table has no PK — nothing to check */

		/* Check whether any PK column of this table appears in the SELECT */
		{
			ListCell *tlc;

			foreach(tlc, viewQuery->targetList)
			{
				TargetEntry *te = lfirst_node(TargetEntry, tlc);
				Var         *v;

				if (te->resjunk || !IsA(te->expr, Var))
					continue;
				v = (Var *) te->expr;
				if (v->varno != varno)
					continue;
				if (bms_is_member((int) v->varattno, pk_attrs))
				{
					covered = true;
					break;
				}
			}
		}

		bms_free(pk_attrs);

		if (!covered)
			ereport(WARNING,
					(errmsg("DBblue incremental matview: table \"%s\" has no "
							"primary-key column in the SELECT list",
							get_rel_name(rte->relid)),
					 errdetail("If two matview rows are identical, a single-row "
							   "DELETE on \"%s\" will remove all matching rows. "
							   "Include the primary key column(s) of each source "
							   "table to avoid this.",
							   get_rel_name(rte->relid))));
	}
}

/*
 * incr_build_row_ins_sql — INSERT delta for row-level (no GROUP BY) matviews.
 *
 *   INSERT INTO mv (col1, col2, ...)
 *   SELECT expr1, expr2, ...
 *   FROM __mv_newtable _d_
 *   [JOIN T2 _j2_ ON ...] [WHERE ...]
 */
static char *
incr_build_row_ins_sql(Oid mvrelid, Query *viewQuery,
					   int delta_varno, const char *delta_table,
					   List *join_list)
{
	StringInfoData	buf;
	ListCell	   *lc;
	const char	   *mvname = mv_qname(mvrelid);
	bool			first;

	initStringInfo(&buf);

	appendStringInfo(&buf, "INSERT INTO %s (", mvname);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk) continue;
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(&buf, ") SELECT ");

	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te = lfirst_node(TargetEntry, lc);
		StringInfoData	ebuf;

		if (te->resjunk) continue;
		if (!first) appendStringInfoChar(&buf, ',');
		first = false;

		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
								delta_varno, &ebuf);
		appendStringInfoString(&buf, ebuf.data);
	}

	incr_append_from_join(&buf, viewQuery, delta_varno, delta_table, join_list);

	{
		Node *wq = incr_get_where_qual(viewQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, viewQuery->rtable, delta_varno, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
	}

	return buf.data;
}

/*
 * incr_build_row_del_sql — DELETE delta for row-level (no GROUP BY) matviews.
 *
 *   DELETE FROM mv
 *   USING (
 *     SELECT expr1 AS col1, expr2 AS col2, ...
 *     FROM __mv_oldtable _d_ [JOIN T2 _j2_ ON ...] [WHERE ...]
 *   ) _old_
 *   WHERE mv.col1 IS NOT DISTINCT FROM _old_.col1 AND ...
 *
 * IS NOT DISTINCT FROM handles NULLs in joined columns correctly.
 */
static char *
incr_build_row_del_sql(Oid mvrelid, Query *viewQuery,
					   int delta_varno, const char *delta_table,
					   List *join_list)
{
	StringInfoData	buf;
	ListCell	   *lc;
	const char	   *mvname = mv_qname(mvrelid);
	bool			first;

	initStringInfo(&buf);

	appendStringInfo(&buf, "DELETE FROM %s WHERE (", mvname);

	/* Build the column list for the tuple identity */
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk) continue;
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(&buf, ") IN (SELECT ");

	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te = lfirst_node(TargetEntry, lc);
		StringInfoData	ebuf;

		if (te->resjunk) continue;
		if (!first) appendStringInfoChar(&buf, ',');
		first = false;

		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
								delta_varno, &ebuf);
		appendStringInfoString(&buf, ebuf.data);
	}

	incr_append_from_join(&buf, viewQuery, delta_varno, delta_table, join_list);

	{
		Node *wq = incr_get_where_qual(viewQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, viewQuery->rtable, delta_varno, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
	}

	appendStringInfoString(&buf, ")");

	return buf.data;
}

/* ============================================================
 * Self-join helpers (Phase 11)
 * ============================================================ */

/*
 * incr_has_self_join — true if any two all_tables entries share an OID.
 */
static bool
incr_has_self_join(List *all_tables)
{
	ListCell *lc1, *lc2;

	foreach(lc1, all_tables)
	{
		IncrJoinEntry *je1 = lfirst(lc1);
		foreach(lc2, all_tables)
		{
			IncrJoinEntry *je2 = lfirst(lc2);
			if (je2 != je1 && je2->oid == je1->oid)
				return true;
		}
	}
	return false;
}

/*
 * incr_self_join_other_varno — given one varno of a self-joined OID, return
 * the other varno.
 */
static int
incr_self_join_other_varno(List *all_tables, int own_varno, Oid shared_oid)
{
	ListCell *lc;

	foreach(lc, all_tables)
	{
		IncrJoinEntry *je = lfirst(lc);
		if (je->oid == shared_oid && je->varno != own_varno)
			return je->varno;
	}
	return -1;
}

/*
 * incr_build_self_join_select — emit one SELECT arm of the self-join UNION ALL.
 * Appends directly to *buf; does NOT emit "INSERT INTO mv".
 */
static void
incr_build_self_join_select(StringInfo buf, Query *viewQuery,
							int delta_varno, const char *delta_table,
							List *all_tables)
{
	List	   *join_list = incr_build_join_list_for_delta(all_tables, delta_varno);
	ListCell   *lc;
	bool		first;
	Node	   *wq;

	appendStringInfoString(buf, "SELECT ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te = lfirst_node(TargetEntry, lc);
		StringInfoData	ebuf;

		if (te->resjunk) continue;
		if (!first) appendStringInfoChar(buf, ',');
		first = false;
		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
								delta_varno, &ebuf);
		appendStringInfoString(buf, ebuf.data);
	}

	incr_append_from_join(buf, viewQuery, delta_varno, delta_table, join_list);

	wq = incr_get_where_qual(viewQuery);
	if (wq != NULL)
	{
		StringInfoData wbuf;
		initStringInfo(&wbuf);
		incr_deparse_where_qual(wq, viewQuery->rtable, delta_varno, &wbuf);
		appendStringInfo(buf, " WHERE %s", wbuf.data);
	}
}

/*
 * incr_build_self_join_row_ins_sql
 * INSERT for a self-join: handles both roles (v1 = anchor, v2 = join partner)
 * by unioning two SELECT arms.
 *
 *   INSERT INTO mv (cols)
 *   SELECT ... FROM delta _d_ JOIN t _j<v2>_ ON ...   -- delta as v1
 *   UNION ALL
 *   SELECT ... FROM t _j<v1>_ JOIN delta _d_ ON ...   -- delta as v2
 */
static char *
incr_build_self_join_row_ins_sql(Oid mvrelid, Query *viewQuery,
								  int v1, int v2,
								  const char *delta_table,
								  List *all_tables)
{
	StringInfoData	buf;
	ListCell	   *lc;
	bool			first;

	initStringInfo(&buf);

	/* INSERT INTO mv (...) */
	appendStringInfo(&buf, "INSERT INTO %s (", mv_qname(mvrelid));
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		if (te->resjunk) continue;
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(&buf, ") ");

	incr_build_self_join_select(&buf, viewQuery, v1, delta_table, all_tables);
	appendStringInfoString(&buf, "\nUNION ALL\n");
	incr_build_self_join_select(&buf, viewQuery, v2, delta_table, all_tables);

	return buf.data;
}

/*
 * incr_build_self_join_row_del_sql
 * DELETE for a self-join: same UNION ALL strategy as INSERT.
 *
 *   DELETE FROM mv WHERE (cols) IN (
 *     SELECT ... FROM delta _d_ JOIN t _j<v2>_ ON ...
 *     UNION ALL
 *     SELECT ... FROM t _j<v1>_ JOIN delta _d_ ON ...
 *   )
 */
static char *
incr_build_self_join_row_del_sql(Oid mvrelid, Query *viewQuery,
								  int v1, int v2,
								  const char *delta_table,
								  List *all_tables)
{
	StringInfoData	buf;
	ListCell	   *lc;
	bool			first;

	initStringInfo(&buf);

	appendStringInfo(&buf, "DELETE FROM %s WHERE (", mv_qname(mvrelid));
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		if (te->resjunk) continue;
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(&buf, ") IN (\n");

	incr_build_self_join_select(&buf, viewQuery, v1, delta_table, all_tables);
	appendStringInfoString(&buf, "\nUNION ALL\n");
	incr_build_self_join_select(&buf, viewQuery, v2, delta_table, all_tables);

	appendStringInfoString(&buf, "\n)");

	return buf.data;
}

/*
 * incr_build_ins_sql_gen — INSERT delta (all phases)
 *
 *   INSERT INTO mv (cols)
 *   SELECT ... FROM delta_table [_d_ JOIN t _j<v>_ ON (...)] [WHERE ...]
 *   GROUP BY ...
 *   ON CONFLICT (group_cols) DO UPDATE SET +deltas
 */
static char *
incr_build_ins_sql_gen(Oid mvrelid, Query *viewQuery,
					   int delta_varno,
					   const char *delta_table,
					   List *join_list)
{
	StringInfoData buf;
	List	   *groupColNames = NIL;
	ListCell   *lc,
			   *gcl;
	const char *mvname = mv_qname(mvrelid);
	bool		first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	initStringInfo(&buf);

	/* INSERT INTO mv (...) */
	appendStringInfo(&buf, "INSERT INTO %s (", mvname);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk)
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(&buf, ") SELECT ");

	/* SELECT expressions */
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk)
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;

		if (strcmp(te->resname, MATVIEW_INCR_COUNT_COL) == 0)
			appendStringInfoString(&buf, "COUNT(*)");
		else if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			appendStringInfoString(&buf, "true");
		else if (IsA(te->expr, Var))
		{
			StringInfoData ebuf;

			initStringInfo(&ebuf);
			incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
									delta_varno, &ebuf);
			appendStringInfoString(&buf, ebuf.data);
		}
		else if (IsA(te->expr, Aggref))
		{
			Aggref		   *agg = (Aggref *) te->expr;
			char		   *fname = get_func_name(agg->aggfnoid);
			StringInfoData	ebuf;

			if (strcmp(fname, "count") == 0 && agg->aggstar)
				appendStringInfoString(&buf, "COUNT(*)");
			else if (agg->args != NIL)
			{
				TargetEntry *arg_te = linitial_node(TargetEntry, agg->args);

				initStringInfo(&ebuf);
				incr_deparse_where_qual((Node *) arg_te->expr, viewQuery->rtable,
										delta_varno, &ebuf);
				appendStringInfo(&buf, "%s(%s)", fname, ebuf.data);
			}
			else
				appendStringInfo(&buf, "%s(*)", fname);
		}
		else
			elog(ERROR,
				 "incr_build_ins_sql_gen: unexpected expression type %d",
				 (int) nodeTag(te->expr));
	}

	/* FROM ... [JOIN ...] [WHERE ...] */
	incr_append_from_join(&buf, viewQuery, delta_varno, delta_table, join_list);
	{
		Node	   *wq = incr_get_where_qual(viewQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, viewQuery->rtable, delta_varno, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
	}

	/* GROUP BY */
	appendStringInfoString(&buf, " GROUP BY ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te = lfirst_node(TargetEntry, lc);
		StringInfoData	ebuf;

		if (te->resjunk || !IsA(te->expr, Var))
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
								delta_varno, &ebuf);
		appendStringInfoString(&buf, ebuf.data);
	}

	/* ON CONFLICT (group_cols) DO UPDATE SET */
	appendStringInfoString(&buf, " ON CONFLICT (");
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first)
			appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}
	appendStringInfoString(&buf, ") DO UPDATE SET ");

	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		const char  *colq;

		if (te->resjunk || IsA(te->expr, Var))
			continue;
		if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			continue;
		/* hidden avgsum/avgcnt emitted as part of their parent AVG column */
		if (strncmp(te->resname, MATVIEW_INCR_AVGSUM_PREFIX,
					strlen(MATVIEW_INCR_AVGSUM_PREFIX)) == 0 ||
			strncmp(te->resname, MATVIEW_INCR_AVGCNT_PREFIX,
					strlen(MATVIEW_INCR_AVGCNT_PREFIX)) == 0)
			continue;

		colq = quote_identifier(te->resname);

		if (IsA(te->expr, Aggref) &&
			strcmp(get_func_name(((Aggref *) te->expr)->aggfnoid), "avg") == 0)
		{
			/* AVG: update hidden sum/cnt then recompute visible avg */
			Aggref	   *agg = (Aggref *) te->expr;
			char	   *sum_col = psprintf("%s%s", MATVIEW_INCR_AVGSUM_PREFIX, te->resname);
			char	   *cnt_col = psprintf("%s%s", MATVIEW_INCR_AVGCNT_PREFIX, te->resname);
			const char *sum_q = quote_identifier(sum_col);
			const char *cnt_q = quote_identifier(cnt_col);
			const char *type_name = format_type_be(agg->aggtype);

			if (!first)
				appendStringInfoChar(&buf, ',');
			appendStringInfo(&buf,
							 "%s=%s.%s+EXCLUDED.%s"
							 ",%s=%s.%s+EXCLUDED.%s"
							 ",%s=((%s.%s+EXCLUDED.%s)::%s/NULLIF(%s.%s+EXCLUDED.%s,0))",
							 sum_q, mvname, sum_q, sum_q,
							 cnt_q, mvname, cnt_q, cnt_q,
							 colq,
							 mvname, sum_q, sum_q, type_name,
							 mvname, cnt_q, cnt_q);
			first = false;
		}
		else if (IsA(te->expr, Aggref))
		{
			/* MIN/MAX: replace if better; everything else: accumulate */
			char *fn = get_func_name(((Aggref *) te->expr)->aggfnoid);

			if (!first)
				appendStringInfoChar(&buf, ',');
			if (strcmp(fn, "min") == 0)
				appendStringInfo(&buf, "%s=LEAST(%s.%s,EXCLUDED.%s)",
								 colq, mvname, colq, colq);
			else if (strcmp(fn, "max") == 0)
				appendStringInfo(&buf, "%s=GREATEST(%s.%s,EXCLUDED.%s)",
								 colq, mvname, colq, colq);
			else
				appendStringInfo(&buf, "%s=%s.%s+EXCLUDED.%s", colq, mvname, colq, colq);
			first = false;
		}
		else
		{
			if (!first)
				appendStringInfoChar(&buf, ',');
			appendStringInfo(&buf, "%s=%s.%s+EXCLUDED.%s", colq, mvname, colq, colq);
			first = false;
		}
	}

	return buf.data;
}

/*
 * incr_build_backfill_sql_gen — one-time HAVING backfill (all phases)
 *
 * Like incr_build_ins_sql_gen but:
 *   __mv_having_ok__ = false   (DO NOTHING leaves passing rows intact)
 *   ON CONFLICT DO NOTHING     (no delta accumulation)
 *
 * delta_table must be the actual source table name, not a transition table.
 */
static char *
incr_build_backfill_sql_gen(Oid mvrelid, Query *viewQuery,
							int delta_varno,
							const char *delta_table,
							List *join_list)
{
	StringInfoData buf;
	List	   *groupColNames = NIL;
	ListCell   *lc,
			   *gcl;
	const char *mvname = mv_qname(mvrelid);
	bool		first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	initStringInfo(&buf);

	/* INSERT INTO mv (...) */
	appendStringInfo(&buf, "INSERT INTO %s (", mvname);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk)
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(&buf, ") SELECT ");

	/* SELECT expressions — same as ins_sql_gen except __mv_having_ok__ = false */
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk)
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;

		if (strcmp(te->resname, MATVIEW_INCR_COUNT_COL) == 0)
			appendStringInfoString(&buf, "COUNT(*)");
		else if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			/* false so DO NOTHING leaves already-passing rows (having_ok=true) alone */
			appendStringInfoString(&buf, "false");
		else if (IsA(te->expr, Var))
		{
			StringInfoData ebuf;

			initStringInfo(&ebuf);
			incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
									delta_varno, &ebuf);
			appendStringInfoString(&buf, ebuf.data);
		}
		else if (IsA(te->expr, Aggref))
		{
			Aggref		   *agg = (Aggref *) te->expr;
			char		   *fname = get_func_name(agg->aggfnoid);
			StringInfoData	ebuf;

			if (strcmp(fname, "count") == 0 && agg->aggstar)
				appendStringInfoString(&buf, "COUNT(*)");
			else if (agg->args != NIL)
			{
				TargetEntry *arg_te = linitial_node(TargetEntry, agg->args);

				initStringInfo(&ebuf);
				incr_deparse_where_qual((Node *) arg_te->expr, viewQuery->rtable,
										delta_varno, &ebuf);
				appendStringInfo(&buf, "%s(%s)", fname, ebuf.data);
			}
			else
				appendStringInfo(&buf, "%s(*)", fname);
		}
		else
			elog(ERROR,
				 "incr_build_backfill_sql_gen: unexpected expression type %d",
				 (int) nodeTag(te->expr));
	}

	/* FROM ... [JOIN ...] [WHERE ...] */
	incr_append_from_join(&buf, viewQuery, delta_varno, delta_table, join_list);
	{
		Node	   *wq = incr_get_where_qual(viewQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, viewQuery->rtable, delta_varno, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
	}

	/* GROUP BY */
	appendStringInfoString(&buf, " GROUP BY ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te = lfirst_node(TargetEntry, lc);
		StringInfoData	ebuf;

		if (te->resjunk || !IsA(te->expr, Var))
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
								delta_varno, &ebuf);
		appendStringInfoString(&buf, ebuf.data);
	}

	/* ON CONFLICT DO NOTHING — passing groups already present */
	appendStringInfoString(&buf, " ON CONFLICT (");
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first)
			appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}
	appendStringInfoString(&buf, ") DO NOTHING");

	return buf.data;
}

/*
 * incr_build_del_sql_gen — DELETE delta (all phases)
 *
 *   WITH d AS (
 *     SELECT group_col AS colname, ..., agg_col AS colname, ...,
 *            COUNT(*) AS __mv_count__
 *     FROM delta_table [_d_ JOIN t _j<v>_ ON (...)] [WHERE ...]
 *     GROUP BY ...
 *   )
 *   UPDATE mv SET agg = mv.agg - d.agg, ..., __mv_count__ = mv.__mv_count__ - d.__mv_count__
 *   FROM d
 *   WHERE mv.g1 = d.g1 AND ...
 *
 * The visible AVG column is excluded from the CTE and recomputed from the
 * hidden sum/cnt columns in the UPDATE SET.
 */
static char *
incr_build_del_sql_gen(Oid mvrelid, Query *viewQuery,
					   int delta_varno,
					   const char *delta_table,
					   List *join_list)
{
	StringInfoData buf;
	List	   *groupColNames = NIL;
	ListCell   *lc,
			   *gcl;
	const char *mvname = mv_qname(mvrelid);
	const char *cntcol = quote_identifier(MATVIEW_INCR_COUNT_COL);
	bool		first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	initStringInfo(&buf);

	/* WITH d AS (SELECT ... */
	appendStringInfoString(&buf, "WITH d AS (SELECT ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk)
			continue;
		/* visible AVG is recomputed from hidden sum/cnt — exclude from CTE */
		if (IsA(te->expr, Aggref) &&
			strcmp(get_func_name(((Aggref *) te->expr)->aggfnoid), "avg") == 0)
			continue;
		/* HAVING flag is not a delta quantity */
		if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			continue;

		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;

		if (strcmp(te->resname, MATVIEW_INCR_COUNT_COL) == 0)
			appendStringInfo(&buf, "COUNT(*) AS %s", cntcol);
		else if (IsA(te->expr, Var))
		{
			StringInfoData ebuf;

			initStringInfo(&ebuf);
			incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
									delta_varno, &ebuf);
			appendStringInfo(&buf, "%s AS %s", ebuf.data,
							 quote_identifier(te->resname));
		}
		else if (IsA(te->expr, Aggref))
		{
			Aggref		   *agg = (Aggref *) te->expr;
			char		   *fname = get_func_name(agg->aggfnoid);
			const char	   *colq = quote_identifier(te->resname);
			StringInfoData	ebuf;

			if (strcmp(fname, "count") == 0 && agg->aggstar)
				appendStringInfo(&buf, "COUNT(*) AS %s", colq);
			else if (agg->args != NIL)
			{
				TargetEntry *arg_te = linitial_node(TargetEntry, agg->args);

				initStringInfo(&ebuf);
				incr_deparse_where_qual((Node *) arg_te->expr, viewQuery->rtable,
										delta_varno, &ebuf);
				appendStringInfo(&buf, "%s(%s) AS %s", fname, ebuf.data, colq);
			}
			else
				appendStringInfo(&buf, "%s(*) AS %s", fname, colq);
		}
		else
			elog(ERROR,
				 "incr_build_del_sql_gen: unexpected expression type %d",
				 (int) nodeTag(te->expr));
	}

	/* FROM ... [JOIN ...] [WHERE ...] GROUP BY ... ) */
	incr_append_from_join(&buf, viewQuery, delta_varno, delta_table, join_list);
	{
		Node	   *wq = incr_get_where_qual(viewQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, viewQuery->rtable, delta_varno, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
	}
	appendStringInfoString(&buf, " GROUP BY ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te = lfirst_node(TargetEntry, lc);
		StringInfoData	ebuf;

		if (te->resjunk || !IsA(te->expr, Var))
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
								delta_varno, &ebuf);
		appendStringInfoString(&buf, ebuf.data);
	}
	appendStringInfoString(&buf, ") ");

	/* UPDATE mv SET agg = mv.agg - d.agg, ... */
	appendStringInfo(&buf, "UPDATE %s SET ", mvname);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		const char  *colq;

		if (te->resjunk || IsA(te->expr, Var))
			continue;
		if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			continue;
		/* hidden avgsum/avgcnt emitted as part of their parent AVG column */
		if (strncmp(te->resname, MATVIEW_INCR_AVGSUM_PREFIX,
					strlen(MATVIEW_INCR_AVGSUM_PREFIX)) == 0 ||
			strncmp(te->resname, MATVIEW_INCR_AVGCNT_PREFIX,
					strlen(MATVIEW_INCR_AVGCNT_PREFIX)) == 0)
			continue;

		colq = quote_identifier(te->resname);

		if (IsA(te->expr, Aggref) &&
			strcmp(get_func_name(((Aggref *) te->expr)->aggfnoid), "avg") == 0)
		{
			/* AVG: subtract from hidden sum/cnt then recompute visible avg */
			Aggref	   *agg = (Aggref *) te->expr;
			char	   *sum_col = psprintf("%s%s", MATVIEW_INCR_AVGSUM_PREFIX, te->resname);
			char	   *cnt_col = psprintf("%s%s", MATVIEW_INCR_AVGCNT_PREFIX, te->resname);
			const char *sum_q = quote_identifier(sum_col);
			const char *cnt_q = quote_identifier(cnt_col);
			const char *type_name = format_type_be(agg->aggtype);

			if (!first)
				appendStringInfoChar(&buf, ',');
			appendStringInfo(&buf,
							 "%s=%s.%s-d.%s"
							 ",%s=%s.%s-d.%s"
							 ",%s=((%s.%s-d.%s)::%s/NULLIF(%s.%s-d.%s,0))",
							 sum_q, mvname, sum_q, sum_q,
							 cnt_q, mvname, cnt_q, cnt_q,
							 colq,
							 mvname, sum_q, sum_q, type_name,
							 mvname, cnt_q, cnt_q);
			first = false;
		}
		else
		{
			if (!first)
				appendStringInfoChar(&buf, ',');
			appendStringInfo(&buf, "%s=%s.%s-d.%s", colq, mvname, colq, colq);
			first = false;
		}
	}

	/* FROM d WHERE mv.g = d.g AND ... */
	appendStringInfo(&buf, " FROM d WHERE ");
	first = true;
	foreach(gcl, groupColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(gcl)));

		if (!first)
			appendStringInfoString(&buf, " AND ");
		appendStringInfo(&buf, "%s.%s=d.%s", mvname, colq, colq);
		first = false;
	}

	return buf.data;
}

static char *
incr_build_cln_sql(Oid mvrelid)
{
	return psprintf("DELETE FROM %s WHERE %s<=0",
					mv_qname(mvrelid),
					quote_identifier(MATVIEW_INCR_COUNT_COL));
}



/* ============================================================
 * Join table helpers — Phase 2+
 * ============================================================
 */

/*
 * incr_collect_tables
 *
 * Walk the explicit JoinExpr tree and return a flat List of IncrJoinEntry*
 * in left-to-right join order.  The first entry always has quals=NULL (it
 * is the leftmost RangeTblRef).  Every subsequent entry carries the ON
 * condition of the JoinExpr that introduces it.
 *
 * Example: T1 JOIN T2 ON c12 JOIN T3 ON c23 produces:
 *   [{varno=1, oid=T1, quals=NULL}, {varno=2, oid=T2, quals=c12},
 *    {varno=3, oid=T3, quals=c23}]
 */
static void
incr_collect_tables_recurse(Node *node, List *rtable, List **entries)
{
	if (node == NULL)
		return;

	if (IsA(node, JoinExpr))
	{
		JoinExpr   *je = (JoinExpr *) node;

		incr_collect_tables_recurse(je->larg, rtable, entries);

		/* rarg must be a leaf RangeTblRef in a left-deep tree */
		if (IsA(je->rarg, RangeTblRef))
		{
			RangeTblRef    *rtr = (RangeTblRef *) je->rarg;
			RangeTblEntry  *rte = rt_fetch(rtr->rtindex, rtable);
			IncrJoinEntry  *entry = palloc0(sizeof(IncrJoinEntry));

			entry->varno = rtr->rtindex;
			entry->oid = rte->relid;
			entry->quals = je->quals;
			entry->join_type = je->jointype;	/* JOIN_INNER, JOIN_LEFT, JOIN_RIGHT */
			*entries = lappend(*entries, entry);
		}
		else
			elog(ERROR, "DBblue: incr_collect_tables: unexpected rarg node type %d",
				 (int) nodeTag(je->rarg));
	}
	else if (IsA(node, RangeTblRef))
	{
		RangeTblRef    *rtr = (RangeTblRef *) node;
		RangeTblEntry  *rte = rt_fetch(rtr->rtindex, rtable);
		IncrJoinEntry  *entry = palloc0(sizeof(IncrJoinEntry));

		entry->varno = rtr->rtindex;
		entry->oid = rte->relid;
		entry->quals = NULL;
		entry->join_type = JOIN_INNER;			/* anchor — always included */
		*entries = lappend(*entries, entry);
	}
	else
		elog(ERROR, "DBblue: incr_collect_tables: unexpected node type %d",
			 (int) nodeTag(node));
}

static List *
incr_collect_tables(Query *viewQuery)
{
	List	   *entries = NIL;
	FromExpr   *fe;

	if (!IsA(viewQuery->jointree, FromExpr))
		elog(ERROR, "DBblue: incr_collect_tables: jointree is not a FromExpr");

	fe = (FromExpr *) viewQuery->jointree;
	incr_collect_tables_recurse(linitial(fe->fromlist),
								viewQuery->rtable, &entries);
	return entries;
}

/*
 * incr_qual_varnos_walker / incr_qual_varnos
 * Return the set of base-level varno values referenced by an expression.
 */
static bool
incr_qual_varnos_walker(Node *node, Bitmapset **varnos)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var *v = (Var *) node;

		if (v->varlevelsup == 0)
			*varnos = bms_add_member(*varnos, v->varno);
		return false;
	}
	return expression_tree_walker(node, incr_qual_varnos_walker, varnos);
}

/*
 * incr_build_join_list_for_delta
 *
 * Given the full table list (from incr_collect_tables) and the varno of the
 * delta table, return the remaining tables in an order where each entry's
 * ON condition only references varnos already in the "known" set (delta +
 * previously added entries).
 *
 * This greedy expansion works for chains, stars, and any acyclic join graph.
 */
static List *
incr_build_join_list_for_delta(List *all_tables, int delta_varno)
{
	Bitmapset  *known = bms_make_singleton(delta_varno);
	List	   *all_quals = NIL;
	List	   *remaining = NIL;
	List	   *result = NIL;
	ListCell   *lc;

	/*
	 * Collect every non-NULL ON condition from the original join tree.
	 * The leftmost table has quals=NULL in all_tables (it is the bare
	 * starting leaf of the left-deep tree), but its join condition is
	 * stored in another entry's quals field.  By searching all_quals we
	 * can always find a connecting condition regardless of which table is
	 * the delta.
	 */
	foreach(lc, all_tables)
	{
		IncrJoinEntry *je = lfirst(lc);

		if (je->quals != NULL)
			all_quals = lappend(all_quals, je->quals);
	}

	/* Build the candidate list (all tables except the delta) */
	foreach(lc, all_tables)
	{
		IncrJoinEntry *je = lfirst(lc);

		if (je->varno != delta_varno)
			remaining = lappend(remaining, je);
	}

	/*
	 * Greedy: repeatedly scan remaining; each pass adds all entries whose
	 * connecting condition only references already-known varnos.
	 *
	 * We search all_quals (not just je->quals) so that the leftmost table
	 * (quals=NULL in all_tables) still gets the right ON clause when it
	 * appears as a non-delta join table.
	 *
	 * After each deletion we restart the scan from list_head because
	 * PostgreSQL 13+ uses a flat array for List — list_delete_cell shifts
	 * elements down, making any pre-saved "next" pointer stale and
	 * tripping the Assert in lnext().
	 */
	while (remaining != NIL)
	{
		bool		progress = false;

		lc = list_head(remaining);
		while (lc != NULL)
		{
			IncrJoinEntry *je = lfirst(lc);
			Node		  *connecting_qual = NULL;
			ListCell	  *qlc;

			/* Find an ON condition that connects je->varno to the known set */
			foreach(qlc, all_quals)
			{
				Node	   *q = lfirst(qlc);
				Bitmapset  *refs = NULL;
				bool		this_table_in_q;
				bool		others_all_known;

				incr_qual_varnos_walker(q, &refs);
				this_table_in_q = bms_is_member(je->varno, refs);
				others_all_known = bms_is_subset(
					bms_del_member(bms_copy(refs), je->varno), known);
				bms_free(refs);

				if (this_table_in_q && others_all_known)
				{
					connecting_qual = q;
					break;
				}
			}

			/*
		 * If a connecting condition was found, use it (covers regular tables
		 * and the leftmost anchor whose ON condition lives in another entry).
		 * If no connecting condition exists AND this entry has no original ON
		 * condition (je->quals == NULL), it is a true CROSS JOIN — include it
		 * unconditionally with a NULL quals so incr_append_from_join emits
		 * "CROSS JOIN".
		 * If no connecting condition exists AND je->quals != NULL, we cannot
		 * place this table yet; continue scanning.
		 */
		if (connecting_qual != NULL || je->quals == NULL)
			{
				IncrJoinEntry *new_je = palloc(sizeof(IncrJoinEntry));

				new_je->varno = je->varno;
				new_je->oid = je->oid;
				/* Use found connecting_qual; NULL only for true CROSS JOIN */
				new_je->quals = connecting_qual;
				result = lappend(result, new_je);
				remaining = list_delete_cell(remaining, lc);
				known = bms_add_member(known, je->varno);
				progress = true;
				break;			/* restart scan — stale pointer after delete */
			}

			lc = lnext(remaining, lc);
		}

		if (!progress)
			elog(ERROR,
				 "DBblue: cannot determine a valid join order for incremental refresh; "
				 "check that join conditions form an acyclic graph");
	}

	return result;
}

/*
 * incr_has_outer_join
 * Returns true if any entry in all_tables was introduced by a LEFT or RIGHT
 * JOIN (i.e., the matview has at least one outer join).
 */
static bool
incr_has_outer_join(List *all_tables)
{
	ListCell *lc;

	foreach(lc, all_tables)
	{
		IncrJoinEntry *je = lfirst(lc);

		if (je->join_type == JOIN_LEFT || je->join_type == JOIN_RIGHT ||
			je->join_type == JOIN_FULL)
			return true;
	}
	return false;
}

/*
 * incr_outer_preserved_varno
 * Return the varno of the "preserved" (anchor) side for an outer-join matview:
 *   LEFT JOIN  → anchor/first entry (join_type = JOIN_INNER in all_tables)
 *   FULL JOIN  → same as LEFT JOIN: treat anchor as preserved
 *   RIGHT JOIN → last entry that has join_type = JOIN_RIGHT
 */
static int
incr_outer_preserved_varno(List *all_tables)
{
	IncrJoinEntry *first = linitial(all_tables);
	ListCell *lc;

	/* If all non-anchor entries are LEFT JOIN, the anchor is preserved */
	foreach(lc, all_tables)
	{
		IncrJoinEntry *je = lfirst(lc);

		if (je->join_type == JOIN_RIGHT)
		{
			/*
			 * Pure RIGHT JOIN: the last entry with JOIN_RIGHT is the
			 * preserved side (equivalent to it being the anchor in a
			 * mirrored LEFT JOIN).
			 */
			IncrJoinEntry *last = llast(all_tables);

			return last->varno;
		}
	}
	/* Pure LEFT JOIN: anchor is preserved */
	return first->varno;
}

/*
 * str_replace_all — replace every occurrence of 'from' in 'src' with 'to'.
 * Returns a palloc'd string.
 */
static char *
str_replace_all(const char *src, const char *from, const char *to)
{
	StringInfoData	buf;
	const char	   *pos;
	size_t			fromlen = strlen(from);

	initStringInfo(&buf);
	while ((pos = strstr(src, from)) != NULL)
	{
		appendBinaryStringInfo(&buf, src, (int) (pos - src));
		appendStringInfoString(&buf, to);
		src = pos + fromlen;
	}
	appendStringInfoString(&buf, src);
	return buf.data;
}

/*
 * find_connecting_qual — return the first qual in all_tables that references
 * both varno_a and varno_b.  Returns NULL if none found.
 */
static Node *
find_connecting_qual(List *all_tables, int varno_a, int varno_b)
{
	ListCell *lc;

	foreach(lc, all_tables)
	{
		IncrJoinEntry *je    = lfirst(lc);
		Node          *q     = je->quals;
		Bitmapset     *refs  = NULL;

		if (q == NULL)
			continue;
		incr_qual_varnos_walker(q, &refs);
		if (bms_is_member(varno_a, refs) && bms_is_member(varno_b, refs))
		{
			bms_free(refs);
			return q;
		}
		bms_free(refs);
	}
	return NULL;
}

/*
 * qual_to_live_sql — deparse a qual/expression using "live table" aliases:
 *   preserved table  →  _ltp_
 *   other tables     →  _lt<varno>_
 */
static char *
qual_to_live_sql(Node *qual, List *rtable, List *all_tables, int preserved_varno)
{
	StringInfoData	buf;
	ListCell	   *lc;
	char		   *result;

	initStringInfo(&buf);
	/* delta_varno=0: no real table has varno 0, so all Vars get _j<n>_ */
	incr_deparse_where_qual(qual, rtable, 0, &buf);
	result = buf.data;

	/* Replace the preserved table alias first */
	result = str_replace_all(result, psprintf("_j%d_", preserved_varno), "_ltp_");

	/* Replace all other table aliases */
	foreach(lc, all_tables)
	{
		IncrJoinEntry *je = lfirst(lc);

		if (je->varno == preserved_varno)
			continue;
		result = str_replace_all(result,
								 psprintf("_j%d_", je->varno),
								 psprintf("_lt%d_", je->varno));
	}
	return result;
}

/*
 * incr_build_outer_sql
 *
 * Build the SQL for one delta source in an outer-join incremental matview.
 * Instead of incrementally adding/subtracting deltas (which requires NULL
 * sentinel tracking), we use a "recompute affected groups" strategy:
 *
 *   1. Find the GROUP BY keys touched by this delta (the "affected" CTE).
 *   2. Re-run the full join query on live tables for those groups only
 *      (the "new_agg" CTE).
 *   3. UPSERT (REPLACE semantics) those groups into the matview.
 *   4. (del_sql only, preserved-side delta) DELETE groups whose preserved
 *      side no longer has rows.
 *
 * delta_varno:       varno of the delta table in viewQuery->rtable.
 * delta_table:       transition table name ("__mv_newtable" / "__mv_oldtable").
 * all_tables:        flat list of IncrJoinEntry*, left-to-right join order.
 * include_delete_step: true for del_sql when delta is the preserved side;
 *                     the function then emits a trailing CTE DELETE.
 */
static char *
incr_build_outer_sql(Oid mvrelid, Query *viewQuery,
					  int delta_varno, const char *delta_table,
					  List *all_tables, bool include_delete_step)
{
	StringInfoData	buf;
	ListCell	   *lc;
	int				preserved_varno = incr_outer_preserved_varno(all_tables);
	IncrJoinEntry  *preserved_entry = NULL;
	const char	   *mvname = mv_qname(mvrelid);
	bool			first;

	/* Find the preserved-side entry */
	foreach(lc, all_tables)
	{
		IncrJoinEntry *je = lfirst(lc);

		if (je->varno == preserved_varno)
		{
			preserved_entry = je;
			break;
		}
	}
	Assert(preserved_entry != NULL);

	initStringInfo(&buf);

	/* ----------------------------------------------------------------
	 * _affected_ CTE: GROUP BY keys touched by this delta.
	 *   delta = preserved side → group cols come directly from delta rows.
	 *   delta = optional side  → join delta with preserved to reach keys.
	 * ---------------------------------------------------------------- */
	appendStringInfoString(&buf, "WITH _affected_ AS (\n  SELECT DISTINCT ");
	first = true;
	foreach(lc, viewQuery->groupClause)
	{
		SortGroupClause *sgc     = lfirst_node(SortGroupClause, lc);
		TargetEntry     *te      = get_sortgroupclause_tle(sgc, viewQuery->targetList);
		int              rv;
		const char      *src_col = incr_resolve_var_colname(
			(Var *) te->expr, viewQuery->rtable, &rv);

		if (!first) appendStringInfoString(&buf, ", ");
		first = false;
		appendStringInfo(&buf, "%s.%s AS %s",
						 (delta_varno == preserved_varno) ? "_d_" : "_ltp_",
						 quote_identifier(src_col),
						 quote_identifier(te->resname));
	}

	if (delta_varno == preserved_varno)
	{
		appendStringInfo(&buf, "\n  FROM %s _d_\n),\n", delta_table);
	}
	else
	{
		Node          *conn_qual = find_connecting_qual(all_tables,
														delta_varno,
														preserved_varno);
		StringInfoData jbuf;
		char          *cond_sql;

		if (conn_qual == NULL)
			elog(ERROR,
				 "DBblue: no join condition between delta (varno=%d) "
				 "and preserved table (varno=%d)",
				 delta_varno, preserved_varno);

		initStringInfo(&jbuf);
		incr_deparse_where_qual(conn_qual, viewQuery->rtable, delta_varno, &jbuf);
		cond_sql = str_replace_all(jbuf.data,
								   psprintf("_j%d_", preserved_varno),
								   "_ltp_");

		appendStringInfo(&buf,
						 "\n  FROM %s _d_\n  JOIN %s _ltp_ ON (%s)\n),\n",
						 delta_table,
						 mv_qname(preserved_entry->oid),
						 cond_sql);
	}

	/* ----------------------------------------------------------------
	 * _new_agg_ CTE: recompute the full join for affected groups.
	 * Preserved table is the anchor (_ltp_); others use _lt<varno>_.
	 * ---------------------------------------------------------------- */
	appendStringInfoString(&buf, "_new_agg_ AS (\n  SELECT ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk) continue;
		if (!first) appendStringInfoString(&buf, ", ");
		first = false;

		if (strcmp(te->resname, MATVIEW_INCR_COUNT_COL) == 0)
		{
			appendStringInfo(&buf, "COUNT(*) AS %s",
							 quote_identifier(MATVIEW_INCR_COUNT_COL));
		}
		else if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
		{
			appendStringInfo(&buf, "true AS %s",
							 quote_identifier(MATVIEW_INCR_HAVING_COL));
		}
		else if (IsA(te->expr, Var))
		{
			int         rv;
			const char *src_col = incr_resolve_var_colname(
				(Var *) te->expr, viewQuery->rtable, &rv);
			const char *tbl_alias = (rv == preserved_varno)
									 ? "_ltp_"
									 : psprintf("_lt%d_", rv);

			appendStringInfo(&buf, "%s.%s AS %s",
							 tbl_alias,
							 quote_identifier(src_col),
							 quote_identifier(te->resname));
		}
		else if (IsA(te->expr, Aggref))
		{
			Aggref     *agg   = (Aggref *) te->expr;
			char       *fname = get_func_name(agg->aggfnoid);

			if (agg->aggstar || agg->args == NIL)
			{
				appendStringInfo(&buf, "%s(*) AS %s",
								 fname, quote_identifier(te->resname));
			}
			else
			{
				TargetEntry *arg_te  = linitial_node(TargetEntry, agg->args);
				char        *arg_sql = qual_to_live_sql(
					(Node *) arg_te->expr,
					viewQuery->rtable, all_tables, preserved_varno);

				appendStringInfo(&buf, "%s(%s%s) AS %s",
								 fname,
								 (agg->aggdistinct != NIL) ? "DISTINCT " : "",
								 arg_sql,
								 quote_identifier(te->resname));
			}
		}
		else
		{
			char *expr_sql = qual_to_live_sql((Node *) te->expr,
											  viewQuery->rtable,
											  all_tables, preserved_varno);

			appendStringInfo(&buf, "%s AS %s",
							 expr_sql, quote_identifier(te->resname));
		}
	}

	/* FROM preserved anchored on _affected_ via ON clause */
	appendStringInfo(&buf, "\n  FROM %s _ltp_\n  JOIN _affected_ ON (",
					 mv_qname(preserved_entry->oid));
	first = true;
	foreach(lc, viewQuery->groupClause)
	{
		SortGroupClause *sgc     = lfirst_node(SortGroupClause, lc);
		TargetEntry     *te      = get_sortgroupclause_tle(sgc, viewQuery->targetList);
		int              rv;
		const char      *src_col = incr_resolve_var_colname(
			(Var *) te->expr, viewQuery->rtable, &rv);

		if (!first) appendStringInfoString(&buf, " AND ");
		first = false;
		appendStringInfo(&buf, "_ltp_.%s = _affected_.%s",
						 quote_identifier(src_col),
						 quote_identifier(te->resname));
	}
	appendStringInfoString(&buf, ")");

	/* JOIN non-preserved tables using their original join type */
	foreach(lc, all_tables)
	{
		IncrJoinEntry *je       = lfirst(lc);
		const char    *join_kw;
		Node          *qual;
		char          *cond_sql;

		if (je->varno == preserved_varno) continue;

		/*
		 * Use the all-quals pool to find the connecting condition.  The anchor
		 * entry (larg of the first JoinExpr) always has quals=NULL; its
		 * condition lives in the rarg entry.  find_connecting_qual() finds it
		 * regardless of which side owns the node.
		 */
		qual = find_connecting_qual(all_tables, je->varno, preserved_varno);
		if (qual == NULL)
			qual = je->quals;		/* fallback for non-anchor tables */
		if (qual == NULL)
			elog(ERROR,
				 "DBblue: no join condition found for table varno=%d in outer-join matview",
				 je->varno);

		join_kw  = (je->join_type == JOIN_FULL) ? "FULL JOIN" : "LEFT JOIN";
		cond_sql = qual_to_live_sql(qual, viewQuery->rtable,
									all_tables, preserved_varno);

		appendStringInfo(&buf, "\n  %s %s _lt%d_ ON (%s)",
						 join_kw, mv_qname(je->oid), je->varno, cond_sql);
	}

	/* GROUP BY using live-table aliases for source column names */
	appendStringInfoString(&buf, "\n  GROUP BY ");
	first = true;
	foreach(lc, viewQuery->groupClause)
	{
		SortGroupClause *sgc     = lfirst_node(SortGroupClause, lc);
		TargetEntry     *te      = get_sortgroupclause_tle(sgc, viewQuery->targetList);
		int              rv;
		const char      *src_col = incr_resolve_var_colname(
			(Var *) te->expr, viewQuery->rtable, &rv);
		const char      *tbl_alias = (rv == preserved_varno)
									  ? "_ltp_"
									  : psprintf("_lt%d_", rv);

		if (!first) appendStringInfoString(&buf, ", ");
		first = false;
		appendStringInfo(&buf, "%s.%s", tbl_alias, quote_identifier(src_col));
	}
	appendStringInfoString(&buf, "\n),\n");

	/* ----------------------------------------------------------------
	 * _upd_ CTE: UPSERT the recomputed rows into the matview.
	 * ---------------------------------------------------------------- */
	appendStringInfo(&buf, "_upd_ AS (\n  INSERT INTO %s (", mvname);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk) continue;
		if (!first) appendStringInfoString(&buf, ", ");
		appendStringInfoString(&buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(&buf, ")\n  SELECT ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk) continue;
		if (!first) appendStringInfoString(&buf, ", ");
		appendStringInfoString(&buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(&buf, " FROM _new_agg_\n  ON CONFLICT (");
	first = true;
	foreach(lc, viewQuery->groupClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
		TargetEntry     *te  = get_sortgroupclause_tle(sgc, viewQuery->targetList);

		if (!first) appendStringInfoString(&buf, ", ");
		appendStringInfoString(&buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(&buf, ") DO UPDATE SET ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te          = lfirst_node(TargetEntry, lc);
		bool            is_group_col = false;
		ListCell       *gcl;

		if (te->resjunk) continue;
		foreach(gcl, viewQuery->groupClause)
		{
			SortGroupClause *sgc2 = lfirst_node(SortGroupClause, gcl);
			TargetEntry     *gte  = get_sortgroupclause_tle(sgc2,
															 viewQuery->targetList);

			if (strcmp(te->resname, gte->resname) == 0)
			{
				is_group_col = true;
				break;
			}
		}
		if (is_group_col) continue;

		if (!first) appendStringInfoString(&buf, ", ");
		appendStringInfo(&buf, "%s=EXCLUDED.%s",
						 quote_identifier(te->resname),
						 quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(&buf, "\n)\n");

	/* ----------------------------------------------------------------
	 * Final statement.
	 * del_sql on preserved side: also DELETE groups that vanished.
	 * Otherwise: benign SELECT that forces _upd_ to execute.
	 * ---------------------------------------------------------------- */
	if (include_delete_step)
	{
		bool		has_full_join = false;
		ListCell   *jlc;

		foreach(jlc, all_tables)
		{
			IncrJoinEntry *je2 = lfirst(jlc);
			if (je2->join_type == JOIN_FULL) { has_full_join = true; break; }
		}

		appendStringInfo(&buf,
						 "DELETE FROM %s _mv_\nUSING _affected_\nWHERE ",
						 mvname);
		first = true;
		foreach(lc, viewQuery->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
			TargetEntry     *te  = get_sortgroupclause_tle(sgc, viewQuery->targetList);
			const char      *col = quote_identifier(te->resname);

			if (!first) appendStringInfoString(&buf, " AND ");
			appendStringInfo(&buf, "_mv_.%s = _affected_.%s", col, col);
			first = false;
		}

		/* NOT EXISTS on the preserved table */
		appendStringInfo(&buf,
						 "\n  AND NOT EXISTS (\n"
						 "    SELECT 1 FROM %s _chk_\n    WHERE ",
						 mv_qname(preserved_entry->oid));
		first = true;
		foreach(lc, viewQuery->groupClause)
		{
			SortGroupClause *sgc     = lfirst_node(SortGroupClause, lc);
			TargetEntry     *te      = get_sortgroupclause_tle(sgc, viewQuery->targetList);
			int              rv;
			const char      *src_col = incr_resolve_var_colname(
				(Var *) te->expr, viewQuery->rtable, &rv);

			if (!first) appendStringInfoString(&buf, " AND ");
			appendStringInfo(&buf, "_chk_.%s = _mv_.%s",
							 quote_identifier(src_col),
							 quote_identifier(te->resname));
			first = false;
		}
		appendStringInfoString(&buf, "\n  )");

		/*
		 * FULL JOIN: a group can survive on EITHER side.  Add NOT EXISTS
		 * checks for every non-preserved table using their FK column.
		 */
		if (has_full_join)
		{
			int chk_alias = 2;

			foreach(jlc, all_tables)
			{
				IncrJoinEntry *je2 = lfirst(jlc);
				Node		  *q;
				const char	  *fk_col;

				if (je2->varno == preserved_varno)
					continue;

				q = find_connecting_qual(all_tables, je2->varno, preserved_varno);
				if (q == NULL)
					q = je2->quals;
				if (q == NULL)
					continue;

				fk_col = incr_qual_get_colname_for_varno(q, viewQuery->rtable,
														 je2->varno);
				if (fk_col == NULL)
					continue;

				appendStringInfo(&buf,
								 "\n  AND NOT EXISTS (\n"
								 "    SELECT 1 FROM %s _chk%d_\n    WHERE ",
								 mv_qname(je2->oid), chk_alias);
				first = true;
				foreach(lc, viewQuery->groupClause)
				{
					SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
					TargetEntry     *te  = get_sortgroupclause_tle(sgc,
																   viewQuery->targetList);

					if (!first) appendStringInfoString(&buf, " AND ");
					appendStringInfo(&buf, "_chk%d_.%s = _mv_.%s",
									 chk_alias,
									 quote_identifier(fk_col),
									 quote_identifier(te->resname));
					first = false;
					break;		/* use first group key for the FK match */
				}
				appendStringInfoString(&buf, "\n  )");
				chk_alias++;
			}
		}
	}
	else
	{
		/* DML CTEs (_upd_) always execute; this SELECT just terminates the WITH. */
		appendStringInfoString(&buf, "SELECT 1");
	}

	return buf.data;
}

/*
 * incr_qual_get_colname_for_varno
 * Given a join qual (typically an equality OpExpr), return the source
 * column name for the Var belonging to the requested varno.
 * Handles simple A=B OpExpr and AND BoolExpr of such conditions.
 * Returns NULL if the varno is not found in the qual.
 */
static const char *
incr_qual_get_colname_for_varno(Node *qual, List *rtable, int varno)
{
	if (qual == NULL)
		return NULL;

	if (IsA(qual, OpExpr))
	{
		OpExpr *op = (OpExpr *) qual;

		if (list_length(op->args) == 2)
		{
			Node *lhs = linitial(op->args);
			Node *rhs = lsecond(op->args);
			int   rv;

			if (IsA(lhs, Var) && ((Var *) lhs)->varno == varno)
				return incr_resolve_var_colname((Var *) lhs, rtable, &rv);
			if (IsA(rhs, Var) && ((Var *) rhs)->varno == varno)
				return incr_resolve_var_colname((Var *) rhs, rtable, &rv);
		}
	}
	else if (IsA(qual, BoolExpr))
	{
		BoolExpr *bexpr = (BoolExpr *) qual;
		ListCell *alc;

		foreach(alc, bexpr->args)
		{
			const char *result =
				incr_qual_get_colname_for_varno(lfirst(alc), rtable, varno);

			if (result != NULL)
				return result;
		}
	}
	return NULL;
}

/*
 * incr_qual_get_other_varno
 * Given an equality join qual and one side's varno, return the varno of
 * the other side.  Returns -1 if not found.
 */
static int
incr_qual_get_other_varno(Node *qual, int own_varno)
{
	if (qual == NULL)
		return -1;

	if (IsA(qual, OpExpr))
	{
		OpExpr *op = (OpExpr *) qual;

		if (list_length(op->args) == 2)
		{
			Node *lhs = linitial(op->args);
			Node *rhs = lsecond(op->args);

			if (IsA(lhs, Var) && ((Var *) lhs)->varno == own_varno && IsA(rhs, Var))
				return ((Var *) rhs)->varno;
			if (IsA(rhs, Var) && ((Var *) rhs)->varno == own_varno && IsA(lhs, Var))
				return ((Var *) lhs)->varno;
		}
	}
	else if (IsA(qual, BoolExpr))
	{
		BoolExpr *bexpr = (BoolExpr *) qual;
		ListCell *alc;

		foreach(alc, bexpr->args)
		{
			int result = incr_qual_get_other_varno(lfirst(alc), own_varno);

			if (result != -1)
				return result;
		}
	}
	return -1;
}

/*
 * incr_build_outer_row_sync_sql
 * "Sync-region" SQL for row-level (no GROUP BY) outer-join matviews.
 *
 * Strategy: identify the "affected region" from the delta via the delta's
 * DIRECT join-neighbor key, delete all current matview rows in that region,
 * then re-insert fresh rows from the live query for that region.
 *
 * Using the direct neighbor (not always the ultimate preserved anchor) handles
 * N-table chains like (c JOIN o LEFT JOIN i): when i fires, the region key is
 * o.id (direct neighbor), not c.id (ultimate anchor).
 *
 * Generated SQL:
 *   WITH
 *     _aff_ AS (SELECT DISTINCT <delta_jkey> AS jkey FROM <delta_table>),
 *     _del_ AS (
 *       DELETE FROM mv WHERE <mv_peer_key> IN (SELECT jkey FROM _aff_)
 *       [OR <mv_delta_pk> IN (SELECT <delta_pk> FROM delta_table)]  -- FULL JOIN
 *     )
 *   INSERT INTO mv (cols)
 *   SELECT cols FROM <preserved_table> _ltp_
 *   [JOIN_TYPE] <other_table> _lt<n>_ ON (cond) ...
 *   WHERE <peer_alias>.<peer_key> IN (SELECT jkey FROM _aff_)
 *   [OR _lt<n>_.<fk> IN (SELECT jkey FROM _aff_)]  -- FULL JOIN
 *   [AND <view_where>]
 */
static char *
incr_build_outer_row_sync_sql(Oid mvrelid, Query *viewQuery,
							   int delta_varno, const char *delta_table,
							   List *all_tables)
{
	StringInfoData	buf;
	ListCell	   *lc;
	int				preserved_varno  = incr_outer_preserved_varno(all_tables);
	IncrJoinEntry  *preserved_entry  = NULL;
	IncrJoinEntry  *delta_entry      = NULL;
	const char	   *mvname           = mv_qname(mvrelid);
	bool			delta_is_preserved;
	bool			has_full_join     = false;
	Node		   *conn_qual;
	const char	   *delta_jkey_col;	/* column in delta for _aff_ */
	int				peer_varno;		/* direct neighbor of delta */
	const char	   *peer_jkey_col;	/* peer's key column for INSERT WHERE */
	const char	   *mv_peer_col      = NULL;	/* matview col for peer key */
	const char	   *mv_delta_pk_col  = NULL;
	const char	   *delta_pk_src_col = NULL;
	bool			first;

	foreach(lc, all_tables)
	{
		IncrJoinEntry *je = lfirst(lc);

		if (je->varno == preserved_varno)
			preserved_entry = je;
		if (je->varno == delta_varno)
			delta_entry = je;
		if (je->join_type == JOIN_FULL)
			has_full_join = true;
	}
	Assert(preserved_entry != NULL && delta_entry != NULL);

	delta_is_preserved = (delta_varno == preserved_varno);

	/*
	 * Determine the region key.
	 *
	 * For the preserved (anchor) delta: use the ON condition to the first
	 * non-preserved neighbor; the preserved table's own key is the region.
	 *
	 * For a non-preserved delta: use delta_entry->quals (the ON condition of
	 * the JoinExpr step that introduced this table) to find the DIRECT
	 * neighbor.  This handles N-table chains correctly — for
	 * c JOIN o LEFT JOIN i with delta=i, the direct neighbor is o (not c).
	 */
	if (delta_is_preserved)
	{
		IncrJoinEntry *other = NULL;

		foreach(lc, all_tables)
		{
			IncrJoinEntry *je = lfirst(lc);

			if (je->varno != preserved_varno) { other = je; break; }
		}
		conn_qual = find_connecting_qual(all_tables, preserved_varno,
										 other ? other->varno : -1);
		delta_jkey_col = incr_qual_get_colname_for_varno(conn_qual,
														  viewQuery->rtable,
														  preserved_varno);
		peer_varno    = preserved_varno;
		peer_jkey_col = delta_jkey_col;
	}
	else
	{
		/*
		 * Use the delta's stored ON condition (direct neighbor).
		 * Fall back to find_connecting_qual only if quals is NULL.
		 */
		conn_qual = delta_entry->quals;
		if (conn_qual == NULL)
			conn_qual = find_connecting_qual(all_tables, delta_varno,
											 preserved_varno);
		if (conn_qual == NULL)
			elog(ERROR,
				 "DBblue: no join condition found for delta table (varno=%d) "
				 "in outer-join row-level matview",
				 delta_varno);

		delta_jkey_col = incr_qual_get_colname_for_varno(conn_qual,
														  viewQuery->rtable,
														  delta_varno);
		peer_varno     = incr_qual_get_other_varno(conn_qual, delta_varno);
		if (peer_varno == -1)
			elog(ERROR,
				 "DBblue: cannot identify peer varno in join condition for "
				 "delta table (varno=%d)",
				 delta_varno);
		peer_jkey_col = incr_qual_get_colname_for_varno(conn_qual,
														 viewQuery->rtable,
														 peer_varno);
	}

	if (delta_jkey_col == NULL || peer_jkey_col == NULL)
		elog(ERROR,
			 "DBblue: cannot determine join-key columns for delta (varno=%d) "
			 "in outer-join row-level matview",
			 delta_varno);

	/*
	 * Find the matview column corresponding to peer_varno.peer_jkey_col.
	 * This is used in the DELETE WHERE clause to identify affected rows.
	 */
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		Var         *v;
		int          rv;
		const char  *src_col;

		if (te->resjunk || !IsA(te->expr, Var))
			continue;
		v = (Var *) te->expr;
		if (v->varno != peer_varno)
			continue;
		src_col = incr_resolve_var_colname(v, viewQuery->rtable, &rv);
		if (strcmp(src_col, peer_jkey_col) == 0)
		{
			mv_peer_col = te->resname;
			break;
		}
	}

	if (mv_peer_col == NULL)
	{
		/*
		 * The peer's join-key column is not in the SELECT list.  We cannot
		 * identify the affected region.  Emit a clear error — the PK warning
		 * at CREATE time should have flagged this already.
		 */
		RangeTblEntry *peer_rte = rt_fetch(peer_varno, viewQuery->rtable);

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("DBblue: outer-join incremental matview requires the "
						"join-key column \"%s\" of table \"%s\" in the SELECT list",
						peer_jkey_col,
						get_rel_name(peer_rte->relid))));
	}

	/*
	 * For FULL JOIN + non-preserved delta: standalone non-preserved rows have
	 * a NULL preserved-side key and will not be found by the standard DELETE.
	 * Use the delta table's own PK to identify and delete them.
	 */
	if (has_full_join && !delta_is_preserved)
	{
		Relation   delta_rel = table_open(delta_entry->oid, AccessShareLock);
		List      *idxlist   = RelationGetIndexList(delta_rel);
		AttrNumber pk_attnum = InvalidAttrNumber;

		foreach(lc, idxlist)
		{
			Oid           indexoid = lfirst_oid(lc);
			HeapTuple     indextup = SearchSysCache1(INDEXRELID,
													 ObjectIdGetDatum(indexoid));
			Form_pg_index idxform;

			if (!HeapTupleIsValid(indextup))
				continue;
			idxform = (Form_pg_index) GETSTRUCT(indextup);
			if (idxform->indisprimary)
			{
				pk_attnum = idxform->indkey.values[0];
				ReleaseSysCache(indextup);
				break;
			}
			ReleaseSysCache(indextup);
		}
		list_free(idxlist);
		table_close(delta_rel, AccessShareLock);

		if (pk_attnum != InvalidAttrNumber)
		{
			delta_pk_src_col = get_attname(delta_entry->oid, pk_attnum, true);

			foreach(lc, viewQuery->targetList)
			{
				TargetEntry *te = lfirst_node(TargetEntry, lc);
				Var         *v;

				if (te->resjunk || !IsA(te->expr, Var))
					continue;
				v = (Var *) te->expr;
				if (v->varno == delta_varno && v->varattno == pk_attnum)
				{
					mv_delta_pk_col = te->resname;
					break;
				}
			}
		}
	}

	initStringInfo(&buf);

	/* ---- _aff_: affected region (join-key values from delta) ---- */
	appendStringInfo(&buf,
					 "WITH _aff_ AS (\n"
					 "  SELECT DISTINCT %s AS jkey FROM %s\n),\n",
					 quote_identifier(delta_jkey_col), delta_table);

	/* ---- _del_: remove current matview rows in the region ---- */
	appendStringInfo(&buf,
					 "_del_ AS (\n"
					 "  DELETE FROM %s\n"
					 "  WHERE %s IN (SELECT jkey FROM _aff_)",
					 mvname,
					 quote_identifier(mv_peer_col));

	/*
	 * FULL JOIN + non-preserved delta: also delete standalone non-preserved
	 * rows using the delta table's own PK (those rows have NULL peer key).
	 */
	if (has_full_join && !delta_is_preserved &&
		mv_delta_pk_col != NULL && delta_pk_src_col != NULL)
	{
		appendStringInfo(&buf,
						 "\n     OR %s IN (SELECT %s FROM %s)",
						 quote_identifier(mv_delta_pk_col),
						 quote_identifier(delta_pk_src_col),
						 delta_table);
	}
	appendStringInfoString(&buf, "\n)\n");

	/* ---- INSERT: fresh rows for the affected region ---- */
	appendStringInfo(&buf, "INSERT INTO %s (", mvname);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk) continue;
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(&buf, ")\nSELECT ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		char        *expr_sql;

		if (te->resjunk) continue;
		if (!first) appendStringInfoChar(&buf, ',');
		first = false;
		expr_sql = qual_to_live_sql((Node *) te->expr,
									viewQuery->rtable, all_tables, preserved_varno);
		appendStringInfoString(&buf, expr_sql);
	}

	/* FROM preserved anchor + other tables with their original join types */
	appendStringInfo(&buf, "\nFROM %s _ltp_", mv_qname(preserved_entry->oid));
	foreach(lc, all_tables)
	{
		IncrJoinEntry *je      = lfirst(lc);
		const char    *join_kw;
		Node          *q;
		char          *cond_sql;

		if (je->varno == preserved_varno)
			continue;

		join_kw = (je->join_type == JOIN_FULL)  ? "FULL JOIN"  :
				  (je->join_type == JOIN_LEFT)  ? "LEFT JOIN"  :
				  (je->join_type == JOIN_RIGHT) ? "RIGHT JOIN" : "JOIN";

		q = find_connecting_qual(all_tables, je->varno, preserved_varno);
		if (q == NULL)
			q = je->quals;
		cond_sql = qual_to_live_sql(q, viewQuery->rtable, all_tables, preserved_varno);
		appendStringInfo(&buf, "\n  %s %s _lt%d_ ON (%s)",
						 join_kw, mv_qname(je->oid), je->varno, cond_sql);
	}

	/*
	 * WHERE: restrict to the affected region.
	 * Use the peer table alias — _ltp_ for preserved, _lt<n>_ for others.
	 */
	{
		const char *peer_alias = (peer_varno == preserved_varno)
								  ? "_ltp_"
								  : psprintf("_lt%d_", peer_varno);

		appendStringInfo(&buf,
						 "\nWHERE %s.%s IN (SELECT jkey FROM _aff_)",
						 peer_alias, quote_identifier(peer_jkey_col));

		if (has_full_join)
		{
			/* Also include standalone non-preserved rows in the region */
			foreach(lc, all_tables)
			{
				IncrJoinEntry *je = lfirst(lc);
				Node          *q;
				const char    *other_fk_col;

				if (je->varno == preserved_varno)
					continue;

				q = find_connecting_qual(all_tables, je->varno, preserved_varno);
				if (q == NULL)
					q = je->quals;
				other_fk_col = incr_qual_get_colname_for_varno(q,
															   viewQuery->rtable,
															   je->varno);
				if (other_fk_col)
					appendStringInfo(&buf,
									 "\n   OR _lt%d_.%s IN (SELECT jkey FROM _aff_)",
									 je->varno, quote_identifier(other_fk_col));
			}
		}
	}

	/* View's own WHERE clause, ANDed with the region filter */
	{
		Node *wq = incr_get_where_qual(viewQuery);

		if (wq != NULL)
		{
			char *wq_sql = qual_to_live_sql(wq, viewQuery->rtable,
											all_tables, preserved_varno);

			appendStringInfo(&buf, "\n  AND (%s)", wq_sql);
		}
	}

	return buf.data;
}

/* ============================================================
 * Catalog helpers
 * ============================================================
 */

static void
incr_store_catalog(Oid mvrelid, Oid srctable,
				   const char *ins_sql,
				   const char *del_sql,
				   const char *cln_sql,
				   const char *having_sql,
				   const char *lock_sql)
{
	Relation	catalog;
	HeapTuple	tup;
	Datum		values[Natts_pg_dbblue_matview];
	bool		nulls[Natts_pg_dbblue_matview];

	MemSet(nulls, false, sizeof(nulls));
	values[Anum_pg_dbblue_matview_mvrelid - 1] = ObjectIdGetDatum(mvrelid);
	values[Anum_pg_dbblue_matview_srctable - 1] = ObjectIdGetDatum(srctable);
	values[Anum_pg_dbblue_matview_ins_sql - 1] = CStringGetTextDatum(ins_sql);
	values[Anum_pg_dbblue_matview_del_sql - 1] = CStringGetTextDatum(del_sql);
	values[Anum_pg_dbblue_matview_cln_sql - 1] = CStringGetTextDatum(cln_sql);
	if (having_sql)
		values[Anum_pg_dbblue_matview_having_sql - 1] = CStringGetTextDatum(having_sql);
	else
	{
		values[Anum_pg_dbblue_matview_having_sql - 1] = (Datum) 0;
		nulls[Anum_pg_dbblue_matview_having_sql - 1] = true;
	}
	if (lock_sql)
		values[Anum_pg_dbblue_matview_lock_sql - 1] = CStringGetTextDatum(lock_sql);
	else
	{
		values[Anum_pg_dbblue_matview_lock_sql - 1] = (Datum) 0;
		nulls[Anum_pg_dbblue_matview_lock_sql - 1] = true;
	}

	catalog = table_open(DbblueMatviewRelationId, RowExclusiveLock);
	tup = heap_form_tuple(RelationGetDescr(catalog), values, nulls);
	CatalogTupleInsert(catalog, tup);
	heap_freetuple(tup);
	table_close(catalog, RowExclusiveLock);
}

/* ============================================================
 * Index + trigger creation
 * ============================================================
 */

static void
incr_create_unique_index(Oid mvrelid, List *groupColNames)
{
	StringInfoData sql;
	ListCell   *lc;
	bool		first = true;
	int			ret;

	initStringInfo(&sql);
	appendStringInfo(&sql, "CREATE UNIQUE INDEX ON %s (", mv_qname(mvrelid));
	foreach(lc, groupColNames)
	{
		if (!first) appendStringInfoChar(&sql, ',');
		appendStringInfoString(&sql, quote_identifier(strVal(lfirst(lc))));
		first = false;
	}
	appendStringInfoChar(&sql, ')');

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "incr_create_unique_index: SPI_connect failed");
	ret = SPI_execute(sql.data, false, 0);
	SPI_finish();

	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "incr_create_unique_index: failed (%d)", ret);
}

static void
incr_create_trigger(Oid mvrelid, Oid srctable,
					int16 tgtype_event,
					const char *newtable,
					const char *oldtable)
{
	CreateTrigStmt stmt;
	char		argbuf[32];
	List	   *transRels = NIL;

	MemSet(&stmt, 0, sizeof(stmt));
	stmt.replace = false;
	stmt.isconstraint = false;
	stmt.trigname = psprintf("__mv_delta_%u_%d", mvrelid, (int) tgtype_event);
	stmt.relation = makeRangeVar(
		get_namespace_name(get_rel_namespace(srctable)),
		get_rel_name(srctable), -1);
	/* pg_catalog.matview_delta_apply — registered in pg_proc.dat */
	stmt.funcname = list_make2(makeString("pg_catalog"),
							   makeString("matview_delta_apply"));
	stmt.row = false;			/* STATEMENT level */
	stmt.timing = TRIGGER_TYPE_AFTER;
	stmt.events = tgtype_event;
	stmt.columns = NIL;
	stmt.whenClause = NULL;

	/* Transition table specs */
	if (newtable)
	{
		TriggerTransition *tt = makeNode(TriggerTransition);

		tt->name = pstrdup(newtable);
		tt->isNew = true;
		tt->isTable = true;
		transRels = lappend(transRels, tt);
	}
	if (oldtable)
	{
		TriggerTransition *tt = makeNode(TriggerTransition);

		tt->name = pstrdup(oldtable);
		tt->isNew = false;
		tt->isTable = true;
		transRels = lappend(transRels, tt);
	}
	stmt.transitionRels = transRels;

	/* Matview OID as trigger argument */
	snprintf(argbuf, sizeof(argbuf), "%u", mvrelid);
	stmt.args = list_make1(makeString(pstrdup(argbuf)));

	{
		ObjectAddress trigaddr,
					  mvaddr;

		trigaddr = CreateTriggerFiringOn(&stmt, NULL,
										 InvalidOid,	/* relOid — derived from stmt.relation */
										 InvalidOid,	/* refRelOid */
										 InvalidOid,	/* constraintOid */
										 InvalidOid,	/* indexOid */
										 InvalidOid,	/* funcoid — looked up from funcname */
										 InvalidOid,	/* parentTriggerOid */
										 NULL,			/* whenClause */
										 true,			/* isInternal */
										 false,			/* in_partition */
										 TRIGGER_FIRES_ON_ORIGIN);

		/* DROP MATERIALIZED VIEW will cascade-drop this trigger automatically */
		ObjectAddressSet(mvaddr, RelationRelationId, mvrelid);
		recordDependencyOn(&trigaddr, &mvaddr, DEPENDENCY_INTERNAL);
	}
}

/* ============================================================
 * Trigger function: matview_delta_apply
 * ============================================================
 */

static void
incr_init_plan_cache(void)
{
	HASHCTL		ctl;

	if (incr_plan_cache != NULL)
		return;
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(IncrPlanKey);
	ctl.entrysize = sizeof(IncrPlanEntry);
	incr_plan_cache = hash_create("DBblue matview incr plan cache",
								  64, &ctl,
								  HASH_ELEM | HASH_BLOBS);
}

static SPIPlanPtr
incr_get_plan(Oid mvrelid, Oid srctable, int plan_type)
{
	IncrPlanKey key = {mvrelid, srctable, plan_type};
	IncrPlanEntry *entry;

	if (incr_plan_cache == NULL)
		return NULL;
	entry = hash_search(incr_plan_cache, &key, HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;
	/* Invalidate stale plans (schema change) */
	if (entry->plan && !SPI_plan_is_valid(entry->plan))
	{
		SPI_freeplan(entry->plan);
		entry->plan = NULL;
	}
	return entry->plan;
}

static void
incr_cache_plan(Oid mvrelid, Oid srctable, int plan_type, SPIPlanPtr plan)
{
	IncrPlanKey key = {mvrelid, srctable, plan_type};
	IncrPlanEntry *entry;
	bool		found;

	incr_init_plan_cache();
	entry = hash_search(incr_plan_cache, &key, HASH_ENTER, &found);
	entry->key = key;
	if (found && entry->plan)
		SPI_freeplan(entry->plan);
	entry->plan = plan;
}

static char *
incr_fetch_sql(Oid mvrelid, Oid srctable, int plan_type)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData keys[2];
	HeapTuple	tup;
	char	   *sql = NULL;
	int			attnum;

	attnum = (plan_type == INCR_PLAN_INS)  ? Anum_pg_dbblue_matview_ins_sql  :
			 (plan_type == INCR_PLAN_DEL)  ? Anum_pg_dbblue_matview_del_sql  :
			 (plan_type == INCR_PLAN_CLN)  ? Anum_pg_dbblue_matview_cln_sql  :
			 (plan_type == INCR_PLAN_HAV)  ? Anum_pg_dbblue_matview_having_sql :
											 Anum_pg_dbblue_matview_lock_sql;

	catalog = table_open(DbblueMatviewRelationId, AccessShareLock);
	ScanKeyInit(&keys[0], Anum_pg_dbblue_matview_mvrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(mvrelid));
	ScanKeyInit(&keys[1], Anum_pg_dbblue_matview_srctable,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(srctable));
	scan = systable_beginscan(catalog, DbblueMatviewIndexId,
							  true, NULL, 2, keys);
	if ((tup = systable_getnext(scan)) != NULL)
	{
		Datum	d;
		bool	isnull;

		d = heap_getattr(tup, attnum, RelationGetDescr(catalog), &isnull);
		if (!isnull)
			sql = TextDatumGetCString(d);
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
	return sql;
}

PG_FUNCTION_INFO_V1(matview_delta_apply);

/*
 * matview_delta_apply — AFTER STATEMENT trigger function
 *
 * tgargs[0] = matview OID (as cstring)
 */
Datum
matview_delta_apply(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Oid			mvrelid;
	Oid			srctable;
	bool		is_insert,
				is_delete,
				is_update;
	int			ret;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "matview_delta_apply: not called as trigger");
	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event))
		elog(ERROR, "matview_delta_apply: must be an AFTER trigger");
	if (TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		elog(ERROR, "matview_delta_apply: must be a STATEMENT trigger");

	mvrelid = DatumGetObjectId(
		DirectFunctionCall1(oidin,
							CStringGetDatum(trigdata->tg_trigger->tgargs[0])));
	srctable = RelationGetRelid(trigdata->tg_relation);

	is_insert = TRIGGER_FIRED_BY_INSERT(trigdata->tg_event);
	is_delete = TRIGGER_FIRED_BY_DELETE(trigdata->tg_event);
	is_update = TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "matview_delta_apply: SPI_connect failed");

	/* Register __mv_newtable / __mv_oldtable as ENRs visible to SPI queries */
	SPI_register_trigger_data(trigdata);

	/* Allow DML on the matview during delta application */
	OpenMatViewIncrementalMaintenance();

	/* RowExclusiveLock — sufficient for non-conflicting group keys */
	LockRelationOid(mvrelid, RowExclusiveLock);

	/* ----- insert delta (INSERT or UPDATE new-side) ----- */
	if (is_insert || is_update)
	{
		SPIPlanPtr	plan = incr_get_plan(mvrelid, srctable, INCR_PLAN_INS);

		if (plan == NULL)
		{
			char *sql = incr_fetch_sql(mvrelid, srctable, INCR_PLAN_INS);

			if (!sql)
				elog(ERROR, "matview_delta_apply: missing insert-delta SQL for mv %u",
					 mvrelid);
			plan = SPI_prepare(sql, 0, NULL);
			if (!plan)
				elog(ERROR, "matview_delta_apply: SPI_prepare (insert) failed: %s",
					 SPI_result_code_string(SPI_result));
			SPI_keepplan(plan);
			incr_cache_plan(mvrelid, srctable, INCR_PLAN_INS, plan);
		}

		ret = SPI_execute_plan(plan, NULL, NULL, false, 0);
		if (ret < 0)
			elog(ERROR, "matview_delta_apply: insert delta failed: %s",
				 SPI_result_code_string(ret));
	}

	/* ----- delete delta (DELETE or UPDATE old-side) ----- */
	if (is_delete || is_update)
	{
		/*
		 * For MIN/MAX views a lock_sql row is stored.  Execute it first as a
		 * separate SPI call so it gets a fresh READ COMMITTED snapshot — this
		 * forces all concurrent INSERT transactions to commit before we
		 * proceed.  The subsequent del_sql (rescan) then also gets a fresh
		 * snapshot that includes those INSERTs, eliminating the stale-snapshot
		 * MIN/MAX race.  For non-MIN/MAX views lock_sql is NULL and we skip
		 * straight to del_sql.
		 */
		{
			char *lock_sql_str = incr_fetch_sql(mvrelid, srctable, INCR_PLAN_LOCK);

			if (lock_sql_str)
			{
				SPIPlanPtr lplan = incr_get_plan(mvrelid, srctable, INCR_PLAN_LOCK);

				if (lplan == NULL)
				{
					lplan = SPI_prepare(lock_sql_str, 0, NULL);
					if (!lplan)
						elog(ERROR,
							 "matview_delta_apply: SPI_prepare (lock) failed: %s",
							 SPI_result_code_string(SPI_result));
					SPI_keepplan(lplan);
					incr_cache_plan(mvrelid, srctable, INCR_PLAN_LOCK, lplan);
				}

				ret = SPI_execute_plan(lplan, NULL, NULL, false, 0);
				if (ret < 0)
					elog(ERROR, "matview_delta_apply: lock step failed: %s",
						 SPI_result_code_string(ret));
			}
		}

		{
			SPIPlanPtr	plan = incr_get_plan(mvrelid, srctable, INCR_PLAN_DEL);

			if (plan == NULL)
			{
				char *sql = incr_fetch_sql(mvrelid, srctable, INCR_PLAN_DEL);

				if (!sql)
					elog(ERROR, "matview_delta_apply: missing delete-delta SQL for mv %u",
						 mvrelid);
				plan = SPI_prepare(sql, 0, NULL);
				if (!plan)
					elog(ERROR, "matview_delta_apply: SPI_prepare (delete) failed: %s",
						 SPI_result_code_string(SPI_result));
				SPI_keepplan(plan);
				incr_cache_plan(mvrelid, srctable, INCR_PLAN_DEL, plan);
			}

			ret = SPI_execute_plan(plan, NULL, NULL, false, 0);
			if (ret < 0)
				elog(ERROR, "matview_delta_apply: delete delta failed: %s",
					 SPI_result_code_string(ret));
		}

		/* Cleanup: remove group rows whose count dropped to zero */
		{
			SPIPlanPtr	cplan = incr_get_plan(mvrelid, srctable, INCR_PLAN_CLN);

			if (cplan == NULL)
			{
				char *sql = incr_fetch_sql(mvrelid, srctable, INCR_PLAN_CLN);

				if (!sql)
					elog(ERROR, "matview_delta_apply: missing cleanup SQL for mv %u",
						 mvrelid);
				cplan = SPI_prepare(sql, 0, NULL);
				if (!cplan)
					elog(ERROR, "matview_delta_apply: SPI_prepare (cleanup) failed: %s",
						 SPI_result_code_string(SPI_result));
				SPI_keepplan(cplan);
				incr_cache_plan(mvrelid, srctable, INCR_PLAN_CLN, cplan);
			}

			ret = SPI_execute_plan(cplan, NULL, NULL, false, 0);
			if (ret < 0)
				elog(ERROR, "matview_delta_apply: cleanup failed: %s",
					 SPI_result_code_string(ret));
		}
	}

	/* ----- HAVING step: recompute __mv_having_ok__ for all active groups ----- */
	{
		char	   *sql = incr_fetch_sql(mvrelid, srctable, INCR_PLAN_HAV);

		if (sql)					/* NULL = no HAVING clause on this matview */
		{
			SPIPlanPtr	hplan = incr_get_plan(mvrelid, srctable, INCR_PLAN_HAV);

			if (hplan == NULL)
			{
				hplan = SPI_prepare(sql, 0, NULL);
				if (!hplan)
					elog(ERROR,
						 "matview_delta_apply: SPI_prepare (having) failed: %s",
						 SPI_result_code_string(SPI_result));
				SPI_keepplan(hplan);
				incr_cache_plan(mvrelid, srctable, INCR_PLAN_HAV, hplan);
			}

			ret = SPI_execute_plan(hplan, NULL, NULL, false, 0);
			if (ret < 0)
				elog(ERROR, "matview_delta_apply: having step failed: %s",
					 SPI_result_code_string(ret));
		}
	}

	CloseMatViewIncrementalMaintenance();
	SPI_finish();
	return PointerGetDatum(NULL);
}

/* ============================================================
 * Self-join + GROUP BY/DISTINCT helpers — Phase 14
 * ============================================================ */

/*
 * incr_build_self_join_agg_ins_sql
 *
 * INSERT delta for a self-join with GROUP BY.  Both roles (v1 and v2) must be
 * merged into a single catalog entry because the self-joined table has one OID.
 *
 * Strategy: wrap arm1 in a data-modifying CTE so both arms execute in a
 * single SPI call.  PostgreSQL guarantees data-modifying CTEs run exactly
 * once regardless of whether their output is referenced by the outer query.
 *
 *   WITH _sj_ins_ AS ( <arm1: INSERT ... ON CONFLICT DO UPDATE> )
 *   <arm2: INSERT ... ON CONFLICT DO UPDATE>
 */
static char *
incr_build_self_join_agg_ins_sql(Oid mvrelid, Query *viewQuery,
								  int v1, int v2,
								  const char *delta_table,
								  List *all_tables)
{
	List *jl1  = incr_build_join_list_for_delta(all_tables, v1);
	List *jl2  = incr_build_join_list_for_delta(all_tables, v2);
	char *arm1 = incr_build_ins_sql_gen(mvrelid, viewQuery, v1, delta_table, jl1);
	char *arm2 = incr_build_ins_sql_gen(mvrelid, viewQuery, v2, delta_table, jl2);

	return psprintf("WITH _sj_ins_ AS (%s) %s", arm1, arm2);
}

/*
 * incr_build_self_join_agg_del_sql
 *
 * DELETE delta for a self-join with GROUP BY.  Same CTE wrapping strategy.
 * Each arm is "WITH d AS (...) UPDATE mv SET ... FROM d WHERE ...".
 * Both are wrapped as outer CTEs with a terminal SELECT 1.
 *
 *   WITH _sj_d1_ AS ( <arm1: WITH d AS (...) UPDATE ...> ),
 *        _sj_d2_ AS ( <arm2: WITH d AS (...) UPDATE ...> )
 *   SELECT 1
 */
static char *
incr_build_self_join_agg_del_sql(Oid mvrelid, Query *viewQuery,
								  int v1, int v2,
								  const char *delta_table,
								  List *all_tables)
{
	List *jl1  = incr_build_join_list_for_delta(all_tables, v1);
	List *jl2  = incr_build_join_list_for_delta(all_tables, v2);
	char *arm1 = incr_build_del_sql_gen(mvrelid, viewQuery, v1, delta_table, jl1);
	char *arm2 = incr_build_del_sql_gen(mvrelid, viewQuery, v2, delta_table, jl2);

	return psprintf("WITH _sj_d1_ AS (%s),_sj_d2_ AS (%s) SELECT 1",
					arm1, arm2);
}

/* ============================================================
 * UNION ALL helpers — Phase 13
 * ============================================================
 */

/*
 * incr_is_pure_union_all — returns true iff every node in the setOperations
 * tree is a UNION ALL.  Leaves (RangeTblRef) are always acceptable.
 */
static bool
incr_is_pure_union_all(Node *node)
{
	if (IsA(node, SetOperationStmt))
	{
		SetOperationStmt *so = (SetOperationStmt *) node;

		if (so->op != SETOP_UNION || !so->all)
			return false;
		return incr_is_pure_union_all(so->larg) &&
			   incr_is_pure_union_all(so->rarg);
	}
	/* Leaf: RangeTblRef */
	return true;
}

static void
incr_collect_union_branches_recurse(Node *node, List *rtable, List **branches)
{
	if (IsA(node, SetOperationStmt))
	{
		SetOperationStmt *so = (SetOperationStmt *) node;

		incr_collect_union_branches_recurse(so->larg, rtable, branches);
		incr_collect_union_branches_recurse(so->rarg, rtable, branches);
	}
	else if (IsA(node, RangeTblRef))
	{
		RangeTblRef   *rtr = (RangeTblRef *) node;
		RangeTblEntry *rte = rt_fetch(rtr->rtindex, rtable);

		Assert(rte->rtekind == RTE_SUBQUERY);
		*branches = lappend(*branches, rte->subquery);
	}
}

static void
incr_collect_union_branches(Query *viewQuery, List **branches)
{
	*branches = NIL;
	incr_collect_union_branches_recurse(viewQuery->setOperations,
										viewQuery->rtable, branches);
}

/*
 * incr_build_union_ins_sql
 *
 * INSERT INTO mv (col1, ..., __mv_count__)
 * SELECT expr1, ..., COUNT(*)
 * FROM delta_table [_d_ JOIN ...] [WHERE ...]
 * GROUP BY expr1, ...
 * ON CONFLICT (col1, ...) DO UPDATE SET __mv_count__ = mv.__mv_count__ + EXCLUDED.__mv_count__
 *
 * viewQuery targetList  → matview column names (resname)
 * branchQuery targetList → column expressions
 * delta_varno = -1 for single-table branch (bare names), ≥1 for JOIN branch
 */
static char *
incr_build_union_ins_sql(Oid mvrelid, Query *viewQuery, Query *branchQuery,
						 int delta_varno, const char *delta_table,
						 List *join_list)
{
	StringInfoData	buf;
	ListCell	   *vlc,	/* view targetList cursor */
				   *blc;	/* branch targetList cursor */
	const char	   *mvname    = mv_qname(mvrelid);
	const char	   *cntcol    = quote_identifier(MATVIEW_INCR_COUNT_COL);
	List		   *view_cols  = NIL;	/* non-junk, non-hidden view TEs */
	List		   *branch_cols = NIL;	/* matching branch TEs */
	bool			first;

	/* Collect visible (non-junk, non-hidden) column pairs */
	forboth(vlc, viewQuery->targetList, blc, branchQuery->targetList)
	{
		TargetEntry *vte = lfirst_node(TargetEntry, vlc);
		TargetEntry *bte = lfirst_node(TargetEntry, blc);

		if (vte->resjunk || incr_is_hidden_col(vte->resname))
			continue;
		view_cols   = lappend(view_cols,   vte);
		branch_cols = lappend(branch_cols, bte);
	}

	initStringInfo(&buf);

	/* INSERT INTO mv (col1, ..., __mv_count__) */
	appendStringInfo(&buf, "INSERT INTO %s (", mvname);
	first = true;
	foreach(vlc, view_cols)
	{
		TargetEntry *vte = lfirst_node(TargetEntry, vlc);

		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(vte->resname));
		first = false;
	}
	appendStringInfo(&buf, ",%s) SELECT ", cntcol);

	/* SELECT expr1, ..., COUNT(*) */
	first = true;
	foreach(blc, branch_cols)
	{
		TargetEntry    *bte = lfirst_node(TargetEntry, blc);
		StringInfoData	ebuf;

		if (!first) appendStringInfoChar(&buf, ',');
		first = false;

		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) bte->expr, branchQuery->rtable,
								delta_varno, &ebuf);
		appendStringInfoString(&buf, ebuf.data);
	}
	appendStringInfo(&buf, ",COUNT(*)");

	/* FROM delta [_d_ JOIN ...] [WHERE ...] */
	incr_append_from_join(&buf, branchQuery, delta_varno, delta_table, join_list);
	{
		Node *wq = incr_get_where_qual(branchQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, branchQuery->rtable, delta_varno, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
	}

	/* GROUP BY expr1, ... */
	appendStringInfoString(&buf, " GROUP BY ");
	first = true;
	foreach(blc, branch_cols)
	{
		TargetEntry    *bte = lfirst_node(TargetEntry, blc);
		StringInfoData	ebuf;

		if (!first) appendStringInfoChar(&buf, ',');
		first = false;

		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) bte->expr, branchQuery->rtable,
								delta_varno, &ebuf);
		appendStringInfoString(&buf, ebuf.data);
	}

	/* ON CONFLICT (col1, ...) DO UPDATE SET __mv_count__ = mv + EXCLUDED */
	appendStringInfoString(&buf, " ON CONFLICT (");
	first = true;
	foreach(vlc, view_cols)
	{
		TargetEntry *vte = lfirst_node(TargetEntry, vlc);

		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(vte->resname));
		first = false;
	}
	appendStringInfo(&buf,
					 ") DO UPDATE SET %s=%s.%s+EXCLUDED.%s",
					 cntcol, mvname, cntcol, cntcol);

	return buf.data;
}

/*
 * incr_build_union_del_sql
 *
 * WITH d AS (
 *   SELECT expr1 AS col1, ..., COUNT(*) AS __mv_count__
 *   FROM delta_table [_d_ JOIN ...] [WHERE ...]
 *   GROUP BY expr1, ...
 * )
 * UPDATE mv SET __mv_count__ = mv.__mv_count__ - d.__mv_count__
 * FROM d
 * WHERE mv.col1 = d.col1 AND ...
 */
static char *
incr_build_union_del_sql(Oid mvrelid, Query *viewQuery, Query *branchQuery,
						 int delta_varno, const char *delta_table,
						 List *join_list)
{
	StringInfoData	buf;
	ListCell	   *vlc,
				   *blc;
	const char	   *mvname   = mv_qname(mvrelid);
	const char	   *cntcol   = quote_identifier(MATVIEW_INCR_COUNT_COL);
	List		   *view_cols  = NIL;
	List		   *branch_cols = NIL;
	bool			first;

	forboth(vlc, viewQuery->targetList, blc, branchQuery->targetList)
	{
		TargetEntry *vte = lfirst_node(TargetEntry, vlc);
		TargetEntry *bte = lfirst_node(TargetEntry, blc);

		if (vte->resjunk || incr_is_hidden_col(vte->resname))
			continue;
		view_cols   = lappend(view_cols,   vte);
		branch_cols = lappend(branch_cols, bte);
	}

	initStringInfo(&buf);

	/* WITH d AS (SELECT expr1 AS col1, ..., COUNT(*) AS __mv_count__ */
	appendStringInfoString(&buf, "WITH d AS (SELECT ");
	first = true;
	forboth(vlc, view_cols, blc, branch_cols)
	{
		TargetEntry    *vte = lfirst_node(TargetEntry, vlc);
		TargetEntry    *bte = lfirst_node(TargetEntry, blc);
		StringInfoData	ebuf;

		if (!first) appendStringInfoChar(&buf, ',');
		first = false;

		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) bte->expr, branchQuery->rtable,
								delta_varno, &ebuf);
		appendStringInfo(&buf, "%s AS %s", ebuf.data,
						 quote_identifier(vte->resname));
	}
	appendStringInfo(&buf, ",COUNT(*) AS %s", cntcol);

	/* FROM ... [WHERE ...] GROUP BY ... ) */
	incr_append_from_join(&buf, branchQuery, delta_varno, delta_table, join_list);
	{
		Node *wq = incr_get_where_qual(branchQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, branchQuery->rtable, delta_varno, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
	}
	appendStringInfoString(&buf, " GROUP BY ");
	first = true;
	foreach(blc, branch_cols)
	{
		TargetEntry    *bte = lfirst_node(TargetEntry, blc);
		StringInfoData	ebuf;

		if (!first) appendStringInfoChar(&buf, ',');
		first = false;

		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) bte->expr, branchQuery->rtable,
								delta_varno, &ebuf);
		appendStringInfoString(&buf, ebuf.data);
	}
	appendStringInfoString(&buf, ") ");

	/* UPDATE mv SET __mv_count__ = mv.__mv_count__ - d.__mv_count__ FROM d WHERE ... */
	appendStringInfo(&buf,
					 "UPDATE %s SET %s=%s.%s-d.%s FROM d WHERE ",
					 mvname, cntcol, mvname, cntcol, cntcol);
	first = true;
	foreach(vlc, view_cols)
	{
		TargetEntry *vte = lfirst_node(TargetEntry, vlc);
		const char  *colq = quote_identifier(vte->resname);

		if (!first) appendStringInfoString(&buf, " AND ");
		appendStringInfo(&buf, "%s.%s IS NOT DISTINCT FROM d.%s",
						 mvname, colq, colq);
		first = false;
	}

	return buf.data;
}

/*
 * incr_union_dedup_backfill
 *
 * The matview was created from a plain UNION ALL query (no __mv_count__ yet).
 * We need to:
 *   1. Aggregate the existing rows into (col1,...,count) via a temp table.
 *   2. Truncate the matview.
 *   3. Re-insert the deduped rows with their counts.
 */
static void
incr_union_dedup_backfill(Oid mvrelid, Query *viewQuery)
{
	StringInfoData	col_list;
	StringInfoData	sql;
	ListCell	   *lc;
	const char	   *mvname = mv_qname(mvrelid);
	const char	   *cntcol = quote_identifier(MATVIEW_INCR_COUNT_COL);
	bool			first;
	int				ret;

	/* Build comma-separated list of visible column names */
	initStringInfo(&col_list);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk || incr_is_hidden_col(te->resname))
			continue;
		if (!first) appendStringInfoChar(&col_list, ',');
		appendStringInfoString(&col_list, quote_identifier(te->resname));
		first = false;
	}

	OpenMatViewIncrementalMaintenance();
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "incr_union_dedup_backfill: SPI_connect failed");

	/* Step 1: create temp table with aggregated counts */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "CREATE TEMP TABLE __dbblue_union_tmp__ AS "
					 "SELECT %s,COUNT(*) AS __cnt__ FROM %s GROUP BY %s",
					 col_list.data, mvname, col_list.data);
	ret = SPI_execute(sql.data, false, 0);
	if (ret < 0)
		elog(ERROR, "incr_union_dedup_backfill: CREATE TEMP TABLE failed (%d)", ret);

	/* Step 2: delete all rows from the matview */
	initStringInfo(&sql);
	appendStringInfo(&sql, "DELETE FROM %s", mvname);
	ret = SPI_execute(sql.data, false, 0);
	if (ret < 0)
		elog(ERROR, "incr_union_dedup_backfill: DELETE failed (%d)", ret);

	/* Step 3: re-insert deduped rows with counts */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO %s (%s,%s) "
					 "SELECT %s,__cnt__ FROM __dbblue_union_tmp__",
					 mvname, col_list.data, cntcol, col_list.data);
	ret = SPI_execute(sql.data, false, 0);
	if (ret < 0)
		elog(ERROR, "incr_union_dedup_backfill: INSERT failed (%d)", ret);

	/* Step 4: drop temp table */
	ret = SPI_execute("DROP TABLE __dbblue_union_tmp__", false, 0);
	if (ret < 0)
		elog(ERROR, "incr_union_dedup_backfill: DROP TABLE failed (%d)", ret);

	SPI_finish();
	CloseMatViewIncrementalMaintenance();
}

/*
 * incr_setup_union_all
 *
 * Orchestrate incremental setup for a UNION ALL matview:
 *   1. ALTER TABLE mv ADD COLUMN __mv_count__ bigint NOT NULL DEFAULT 0
 *   2. Dedup-backfill: collapse duplicate rows into a single row with count
 *   3. Create unique index on all visible columns
 *   4. For each UNION ALL branch, install per-table triggers
 */
static void
incr_setup_union_all(Oid mvrelid, Query *viewQuery)
{
	List	   *branches = NIL;
	List	   *allColNames = NIL;
	ListCell   *lc;
	char	   *alter_sql;
	int			ret;
	const char *cntcol = quote_identifier(MATVIEW_INCR_COUNT_COL);

	/* Step 1: add __mv_count__ column */
	alter_sql = psprintf(
		"ALTER TABLE %s ADD COLUMN %s bigint NOT NULL DEFAULT 0",
		mv_qname(mvrelid), cntcol);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "incr_setup_union_all: SPI_connect failed");
	ret = SPI_execute(alter_sql, false, 0);
	SPI_finish();
	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "incr_setup_union_all: ALTER TABLE failed (%d)", ret);

	CommandCounterIncrement();

	/* Step 2: dedup-backfill */
	incr_union_dedup_backfill(mvrelid, viewQuery);
	CommandCounterIncrement();

	/* Step 3: unique index on all visible columns */
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk || incr_is_hidden_col(te->resname))
			continue;
		allColNames = lappend(allColNames, makeString(pstrdup(te->resname)));
	}
	incr_create_unique_index(mvrelid, allColNames);

	/* Step 4: per-branch triggers */
	incr_collect_union_branches(viewQuery, &branches);

	foreach(lc, branches)
	{
		Query	   *branchQuery = (Query *) lfirst(lc);
		List	   *all_tables  = incr_collect_tables(branchQuery);
		ListCell   *jlc;
		char	   *ins_sql,
				   *del_sql;
		char	   *cln_sql = psprintf("DELETE FROM %s WHERE %s<=0",
									  mv_qname(mvrelid), cntcol);

		foreach(jlc, all_tables)
		{
			IncrJoinEntry *delta     = lfirst(jlc);
			List		  *join_list;
			int			   dv;

			if (list_length(all_tables) == 1)
			{
				/* Single-table branch: no alias */
				join_list = NIL;
				dv        = -1;
			}
			else
			{
				join_list = incr_build_join_list_for_delta(all_tables, delta->varno);
				dv        = delta->varno;
			}

			ins_sql = incr_build_union_ins_sql(mvrelid, viewQuery, branchQuery,
											   dv, MATVIEW_INCR_NEWTABLE,
											   join_list);
			del_sql = incr_build_union_del_sql(mvrelid, viewQuery, branchQuery,
											   dv, MATVIEW_INCR_OLDTABLE,
											   join_list);

			incr_store_catalog(mvrelid, delta->oid,
							   ins_sql, del_sql, cln_sql, NULL, NULL);
			incr_install_triggers(mvrelid, delta->oid);
		}
	}
}

/* ============================================================
 * MIN/MAX aggregate helpers — Phase 15
 * ============================================================
 */

/*
 * incr_has_minmax_agg
 * Returns true if viewQuery's target list contains any MIN or MAX aggregate.
 */
static bool
incr_has_minmax_agg(Query *viewQuery)
{
	ListCell *lc;

	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk || !IsA(te->expr, Aggref))
			continue;
		{
			char *fname = get_func_name(((Aggref *) te->expr)->aggfnoid);

			if (strcmp(fname, "min") == 0 || strcmp(fname, "max") == 0)
				return true;
		}
	}
	return false;
}

/*
 * incr_build_minmax_ins_sql_gen — INSERT delta for MIN/MAX views with advisory lock
 *
 * Unlike incr_build_ins_sql_gen (which uses INSERT ... ON CONFLICT), this
 * generates a CTE-based INSERT+UPDATE that acquires the same advisory lock
 * as incr_build_minmax_del_sql_gen before touching a matview row.  This
 * serialises concurrent INSERT and DELETE operations on the same group key,
 * preventing a concurrent DELETE's stale new_agg scan from overwriting an
 * INSERT that committed between the advisory-lock acquisition and the UPDATE.
 *
 *   WITH ins AS (SELECT <aliased group cols + aggregates>
 *                FROM __mv_newtable__ [JOIN ...] [WHERE ...] GROUP BY ...),
 *        lock_mv AS (SELECT pg_advisory_xact_lock(<oid>, hashtext(<key>))
 *                    FROM ins),
 *        upd AS (UPDATE mv SET min=LEAST(mv.min,ins.min),
 *                              max=GREATEST(mv.max,ins.max), cnt=mv.cnt+ins.cnt ...
 *                FROM ins WHERE mv.g=ins.g AND lock_ref >= 0
 *                RETURNING <group cols>)
 *   INSERT INTO mv (<cols>)
 *   SELECT ins.<col>, ... FROM ins
 *   WHERE NOT EXISTS (SELECT 1 FROM upd WHERE upd.g=ins.g ...)
 */
static char *
incr_build_minmax_ins_sql_gen(Oid mvrelid, Query *viewQuery,
							   int delta_varno, const char *delta_table,
							   List *join_list)
{
	StringInfoData  buf;
	List		   *groupColNames = NIL;
	ListCell	   *lc,
				   *gcl;
	const char	   *mvname = mv_qname(mvrelid);
	bool			first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	initStringInfo(&buf);

	/* ----------------------------------------------------------------
	 * ins CTE: aggregate the delta rows — same expressions as the INSERT
	 * SELECT in incr_build_ins_sql_gen, but with explicit column aliases
	 * so the CTE columns can be referenced by name in upd and the final
	 * INSERT SELECT.
	 * ---------------------------------------------------------------- */
	appendStringInfoString(&buf, "WITH ins AS (SELECT ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te = lfirst_node(TargetEntry, lc);
		const char	   *colq;

		if (te->resjunk)
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;

		colq = quote_identifier(te->resname);

		if (strcmp(te->resname, MATVIEW_INCR_COUNT_COL) == 0)
			appendStringInfo(&buf, "COUNT(*) AS %s", colq);
		else if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			appendStringInfo(&buf, "true AS %s", colq);
		else if (IsA(te->expr, Var))
		{
			StringInfoData ebuf;

			initStringInfo(&ebuf);
			incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
									delta_varno, &ebuf);
			appendStringInfo(&buf, "%s AS %s", ebuf.data, colq);
		}
		else if (IsA(te->expr, Aggref))
		{
			Aggref		   *agg = (Aggref *) te->expr;
			char		   *fname = get_func_name(agg->aggfnoid);
			StringInfoData	ebuf;

			if (strcmp(fname, "count") == 0 && agg->aggstar)
				appendStringInfo(&buf, "COUNT(*) AS %s", colq);
			else if (agg->args != NIL)
			{
				TargetEntry *arg_te = linitial_node(TargetEntry, agg->args);

				initStringInfo(&ebuf);
				incr_deparse_where_qual((Node *) arg_te->expr, viewQuery->rtable,
										delta_varno, &ebuf);
				appendStringInfo(&buf, "%s(%s) AS %s", fname, ebuf.data, colq);
			}
			else
				appendStringInfo(&buf, "%s(*) AS %s", fname, colq);
		}
		else
			elog(ERROR,
				 "incr_build_minmax_ins_sql_gen: unexpected expression type %d",
				 (int) nodeTag(te->expr));
	}

	/* FROM ... [JOIN ...] [WHERE ...] GROUP BY ... ) */
	incr_append_from_join(&buf, viewQuery, delta_varno, delta_table, join_list);
	{
		Node	   *wq = incr_get_where_qual(viewQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, viewQuery->rtable, delta_varno, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
	}
	appendStringInfoString(&buf, " GROUP BY ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te = lfirst_node(TargetEntry, lc);
		StringInfoData	ebuf;

		if (te->resjunk || !IsA(te->expr, Var))
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
								delta_varno, &ebuf);
		appendStringInfoString(&buf, ebuf.data);
	}
	appendStringInfoString(&buf, "),");

	/* ----------------------------------------------------------------
	 * lock_mv CTE: acquire per-group advisory lock before touching the
	 * matview row.  Uses the same key space as incr_build_minmax_del_sql_gen
	 * so INSERT and DELETE on the same group key serialise globally.
	 * ---------------------------------------------------------------- */
	appendStringInfo(&buf,
					 " lock_mv AS (SELECT pg_advisory_xact_lock(%u, hashtext(",
					 (unsigned) mvrelid);
	if (list_length(groupColNames) == 1)
	{
		const char *colq = quote_identifier(strVal(linitial(groupColNames)));

		appendStringInfo(&buf, "ins.%s::text", colq);
	}
	else
	{
		first = true;
		foreach(gcl, groupColNames)
		{
			const char *colq = quote_identifier(strVal(lfirst(gcl)));

			if (!first)
				appendStringInfoString(&buf, " || '|' || ");
			first = false;
			appendStringInfo(&buf, "ins.%s::text", colq);
		}
	}
	appendStringInfoString(&buf, ")) FROM ins),");

	/* ----------------------------------------------------------------
	 * upd CTE: UPDATE existing matview rows.
	 * Lock reference forces sequencing after lock_mv.
	 * ---------------------------------------------------------------- */
	appendStringInfo(&buf, " upd AS (UPDATE %s SET ", mvname);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		const char  *colq;

		if (te->resjunk || IsA(te->expr, Var))
			continue;
		if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			continue;
		if (strncmp(te->resname, MATVIEW_INCR_AVGSUM_PREFIX,
					strlen(MATVIEW_INCR_AVGSUM_PREFIX)) == 0 ||
			strncmp(te->resname, MATVIEW_INCR_AVGCNT_PREFIX,
					strlen(MATVIEW_INCR_AVGCNT_PREFIX)) == 0)
			continue;

		colq = quote_identifier(te->resname);

		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;

		if (IsA(te->expr, Aggref) &&
			strcmp(get_func_name(((Aggref *) te->expr)->aggfnoid), "avg") == 0)
		{
			Aggref	   *agg = (Aggref *) te->expr;
			char	   *sum_col = psprintf("%s%s", MATVIEW_INCR_AVGSUM_PREFIX, te->resname);
			char	   *cnt_col = psprintf("%s%s", MATVIEW_INCR_AVGCNT_PREFIX, te->resname);
			const char *sum_q = quote_identifier(sum_col);
			const char *cnt_q = quote_identifier(cnt_col);
			const char *type_name = format_type_be(agg->aggtype);

			appendStringInfo(&buf,
							 "%s=%s.%s+ins.%s"
							 ",%s=%s.%s+ins.%s"
							 ",%s=((%s.%s+ins.%s)::%s/NULLIF(%s.%s+ins.%s,0))",
							 sum_q, mvname, sum_q, sum_q,
							 cnt_q, mvname, cnt_q, cnt_q,
							 colq,
							 mvname, sum_q, sum_q, type_name,
							 mvname, cnt_q, cnt_q);
		}
		else if (IsA(te->expr, Aggref))
		{
			char *fn = get_func_name(((Aggref *) te->expr)->aggfnoid);

			if (strcmp(fn, "min") == 0)
				appendStringInfo(&buf, "%s=LEAST(%s.%s,ins.%s)",
								 colq, mvname, colq, colq);
			else if (strcmp(fn, "max") == 0)
				appendStringInfo(&buf, "%s=GREATEST(%s.%s,ins.%s)",
								 colq, mvname, colq, colq);
			else
				appendStringInfo(&buf, "%s=%s.%s+ins.%s",
								 colq, mvname, colq, colq);
		}
		else
			appendStringInfo(&buf, "%s=%s.%s+ins.%s",
							 colq, mvname, colq, colq);
	}

	appendStringInfoString(&buf, " FROM ins WHERE ");
	first = true;
	foreach(gcl, groupColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(gcl)));

		if (!first)
			appendStringInfoString(&buf, " AND ");
		first = false;
		appendStringInfo(&buf, "%s.%s=ins.%s", mvname, colq, colq);
	}
	appendStringInfoString(&buf, " AND (SELECT COUNT(*) FROM lock_mv) >= 0 RETURNING ");
	first = true;
	foreach(gcl, groupColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(gcl)));

		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		/* qualify to resolve ambiguity when mv and ins share column names */
		appendStringInfo(&buf, "%s.%s", mvname, colq);
	}
	appendStringInfoString(&buf, ")");

	/* ----------------------------------------------------------------
	 * Final INSERT: rows not matched by upd are new groups.
	 * ---------------------------------------------------------------- */
	appendStringInfo(&buf, " INSERT INTO %s (", mvname);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk)
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		appendStringInfoString(&buf, quote_identifier(te->resname));
	}
	appendStringInfoString(&buf, ") SELECT ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk)
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		appendStringInfo(&buf, "ins.%s", quote_identifier(te->resname));
	}
	appendStringInfoString(&buf, " FROM ins WHERE NOT EXISTS (SELECT 1 FROM upd WHERE ");
	first = true;
	foreach(gcl, groupColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(gcl)));

		if (!first)
			appendStringInfoString(&buf, " AND ");
		first = false;
		appendStringInfo(&buf, "upd.%s=ins.%s", colq, colq);
	}
	appendStringInfoChar(&buf, ')');

	/*
	 * ON CONFLICT: two concurrent INSERTs can both reach the final INSERT
	 * when the group is brand-new.  The advisory lock serialises them for
	 * existing groups (upd returns the key so the INSERT is skipped), but
	 * for new groups one transaction's INSERT commits between the other's
	 * advisory-lock acquisition and its final INSERT.  ON CONFLICT resolves
	 * this with the same LEAST/GREATEST/+ logic as the original upsert path.
	 */
	appendStringInfoString(&buf, " ON CONFLICT (");
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
	}
	appendStringInfoString(&buf, ") DO UPDATE SET ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		const char  *colq;

		if (te->resjunk || IsA(te->expr, Var))
			continue;
		if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			continue;
		if (strncmp(te->resname, MATVIEW_INCR_AVGSUM_PREFIX,
					strlen(MATVIEW_INCR_AVGSUM_PREFIX)) == 0 ||
			strncmp(te->resname, MATVIEW_INCR_AVGCNT_PREFIX,
					strlen(MATVIEW_INCR_AVGCNT_PREFIX)) == 0)
			continue;

		colq = quote_identifier(te->resname);

		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;

		if (IsA(te->expr, Aggref) &&
			strcmp(get_func_name(((Aggref *) te->expr)->aggfnoid), "avg") == 0)
		{
			Aggref	   *agg = (Aggref *) te->expr;
			char	   *sum_col = psprintf("%s%s", MATVIEW_INCR_AVGSUM_PREFIX, te->resname);
			char	   *cnt_col = psprintf("%s%s", MATVIEW_INCR_AVGCNT_PREFIX, te->resname);
			const char *sum_q = quote_identifier(sum_col);
			const char *cnt_q = quote_identifier(cnt_col);
			const char *type_name = format_type_be(agg->aggtype);

			appendStringInfo(&buf,
							 "%s=%s.%s+EXCLUDED.%s"
							 ",%s=%s.%s+EXCLUDED.%s"
							 ",%s=((%s.%s+EXCLUDED.%s)::%s/NULLIF(%s.%s+EXCLUDED.%s,0))",
							 sum_q, mvname, sum_q, sum_q,
							 cnt_q, mvname, cnt_q, cnt_q,
							 colq,
							 mvname, sum_q, sum_q, type_name,
							 mvname, cnt_q, cnt_q);
		}
		else if (IsA(te->expr, Aggref))
		{
			char *fn = get_func_name(((Aggref *) te->expr)->aggfnoid);

			if (strcmp(fn, "min") == 0)
				appendStringInfo(&buf, "%s=LEAST(%s.%s,EXCLUDED.%s)",
								 colq, mvname, colq, colq);
			else if (strcmp(fn, "max") == 0)
				appendStringInfo(&buf, "%s=GREATEST(%s.%s,EXCLUDED.%s)",
								 colq, mvname, colq, colq);
			else
				appendStringInfo(&buf, "%s=%s.%s+EXCLUDED.%s",
								 colq, mvname, colq, colq);
		}
		else
			appendStringInfo(&buf, "%s=%s.%s+EXCLUDED.%s",
							 colq, mvname, colq, colq);
	}

	return buf.data;
}

/*
 * incr_build_minmax_del_sql_gen — rescan-based DELETE delta for MIN/MAX views
 *
 * MIN and MAX cannot be decremented like SUM; when the min/max row is deleted
 * we must re-scan the source table(s) to find the new extremum.  Strategy:
 *
 *   1. affected CTE   — collect the GROUP BY keys touched by the delta.
 *   2. new_agg CTE    — recompute all aggregates from live tables for those keys.
 *   3. upd CTE        — UPDATE matview rows that still have live rows.
 *   4. Final DELETE   — remove matview rows whose group vanished entirely.
 *
 * Single-table (delta_varno < 0):
 *   affected uses bare column names (no alias).
 *   new_agg restricts via "(g1[,g2...]) IN (SELECT g1[,g2...] FROM affected)".
 *
 * JOIN (delta_varno >= 1):
 *   Standard _d_ / _j<v>_ alias scheme throughout.
 *   new_agg joins live tables to affected ON group-key equality.
 */
static char *
incr_build_minmax_del_sql_gen(Oid mvrelid, Query *viewQuery,
							   int delta_varno, const char *delta_table,
							   List *join_list, Oid delta_oid)
{
	StringInfoData	buf;
	List		   *groupColNames = NIL;
	ListCell	   *lc,
				   *gcl;
	const char	   *mvname   = mv_qname(mvrelid);
	const char	   *livename = mv_qname(delta_oid);
	bool			has_join = (join_list != NIL || delta_varno >= 1);
	bool			first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	initStringInfo(&buf);

	/* ----------------------------------------------------------------
	 * affected CTE: distinct group keys touched by the old-table delta.
	 * ---------------------------------------------------------------- */
	appendStringInfoString(&buf, "WITH affected AS (SELECT DISTINCT ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te = lfirst_node(TargetEntry, lc);
		StringInfoData	ebuf;

		if (te->resjunk || !IsA(te->expr, Var))
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
								delta_varno, &ebuf);
		if (has_join)
			/* In JOIN mode emit  _d_.col AS resname  so affected.resname is stable */
			appendStringInfo(&buf, "%s AS %s", ebuf.data,
							 quote_identifier(te->resname));
		else
			appendStringInfoString(&buf, ebuf.data);
	}
	/* FROM __mv_oldtable__ [_d_ JOIN ...] */
	incr_append_from_join(&buf, viewQuery, delta_varno, delta_table, join_list);
	{
		Node *wq = incr_get_where_qual(viewQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, viewQuery->rtable, delta_varno, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
	}
	appendStringInfoString(&buf, "),");

	/* ----------------------------------------------------------------
	 * old_delta CTE: count rows and sum SUM-aggregate arguments from the
	 * delta per affected group.  Used in upd to maintain COUNT and SUM via
	 * delta arithmetic (col = mv.col - del_col) rather than the rescan
	 * value, which avoids a race where a concurrent INSERT commits between
	 * new_agg's scan and the row-lock acquisition in upd, causing its delta
	 * to be silently overwritten by a stale absolute value.
	 * ---------------------------------------------------------------- */
	appendStringInfoString(&buf, " old_delta AS (SELECT ");
	first = true;
	{
		ListCell *lc2;

		foreach(lc2, viewQuery->targetList)
		{
			TargetEntry    *te2 = lfirst_node(TargetEntry, lc2);
			StringInfoData  ebuf2;

			if (te2->resjunk || !IsA(te2->expr, Var))
				continue;
			if (!first)
				appendStringInfoChar(&buf, ',');
			first = false;
			initStringInfo(&ebuf2);
			incr_deparse_where_qual((Node *) te2->expr, viewQuery->rtable,
									delta_varno, &ebuf2);
			if (has_join)
				appendStringInfo(&buf, "%s AS %s", ebuf2.data,
								 quote_identifier(te2->resname));
			else
				appendStringInfoString(&buf, ebuf2.data);
		}
	}
	appendStringInfoString(&buf, ",COUNT(*) AS del_cnt");
	/* SUM delta columns: SUM(arg) AS del_<resname> for each SUM aggregate */
	{
		ListCell *lc2;

		foreach(lc2, viewQuery->targetList)
		{
			TargetEntry    *te2 = lfirst_node(TargetEntry, lc2);
			Aggref		   *agg2;
			TargetEntry    *arg_te;
			StringInfoData  ebuf2;

			if (te2->resjunk || !IsA(te2->expr, Aggref))
				continue;
			if (strcmp(te2->resname, MATVIEW_INCR_HAVING_COL) == 0 ||
				strcmp(te2->resname, MATVIEW_INCR_COUNT_COL) == 0)
				continue;

			agg2 = (Aggref *) te2->expr;
			if (strcmp(get_func_name(agg2->aggfnoid), "sum") != 0)
				continue;
			if (agg2->args == NIL)
				continue;

			arg_te = linitial_node(TargetEntry, agg2->args);
			initStringInfo(&ebuf2);
			incr_deparse_where_qual((Node *) arg_te->expr, viewQuery->rtable,
									delta_varno, &ebuf2);
			appendStringInfo(&buf, ",SUM(%s) AS %s",
							 ebuf2.data,
							 quote_identifier(psprintf("del_%s", te2->resname)));
		}
	}
	incr_append_from_join(&buf, viewQuery, delta_varno, delta_table, join_list);
	{
		Node *wq2 = incr_get_where_qual(viewQuery);

		if (wq2 != NULL)
		{
			StringInfoData wbuf2;

			initStringInfo(&wbuf2);
			incr_deparse_where_qual(wq2, viewQuery->rtable, delta_varno, &wbuf2);
			appendStringInfo(&buf, " WHERE %s", wbuf2.data);
		}
	}
	appendStringInfoString(&buf, " GROUP BY ");
	first = true;
	{
		ListCell *lc2;

		foreach(lc2, viewQuery->targetList)
		{
			TargetEntry    *te2 = lfirst_node(TargetEntry, lc2);
			StringInfoData  ebuf2;

			if (te2->resjunk || !IsA(te2->expr, Var))
				continue;
			if (!first)
				appendStringInfoChar(&buf, ',');
			first = false;
			if (has_join)
			{
				appendStringInfoString(&buf, quote_identifier(te2->resname));
			}
			else
			{
				initStringInfo(&ebuf2);
				incr_deparse_where_qual((Node *) te2->expr, viewQuery->rtable,
										delta_varno, &ebuf2);
				appendStringInfoString(&buf, ebuf2.data);
			}
		}
	}
	appendStringInfoString(&buf, "),");

	/* ----------------------------------------------------------------
	 * new_agg CTE: recompute aggregates from live tables for affected groups.
	 * ---------------------------------------------------------------- */
	appendStringInfoString(&buf, " new_agg AS (SELECT ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te = lfirst_node(TargetEntry, lc);
		StringInfoData	ebuf;

		if (te->resjunk)
			continue;
		if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;

		if (strcmp(te->resname, MATVIEW_INCR_COUNT_COL) == 0)
		{
			appendStringInfo(&buf, "COUNT(*) AS %s",
							 quote_identifier(MATVIEW_INCR_COUNT_COL));
		}
		else if (IsA(te->expr, Var))
		{
			initStringInfo(&ebuf);
			incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
									delta_varno, &ebuf);
			appendStringInfo(&buf, "%s AS %s", ebuf.data,
							 quote_identifier(te->resname));
		}
		else if (IsA(te->expr, Aggref))
		{
			Aggref		   *agg   = (Aggref *) te->expr;
			char		   *fname = get_func_name(agg->aggfnoid);
			const char	   *colq  = quote_identifier(te->resname);

			if (strcmp(fname, "count") == 0 && agg->aggstar)
				appendStringInfo(&buf, "COUNT(*) AS %s", colq);
			else if (agg->args != NIL)
			{
				TargetEntry *arg_te = linitial_node(TargetEntry, agg->args);

				initStringInfo(&ebuf);
				incr_deparse_where_qual((Node *) arg_te->expr, viewQuery->rtable,
										delta_varno, &ebuf);
				appendStringInfo(&buf, "%s(%s) AS %s", fname, ebuf.data, colq);
			}
			else
				appendStringInfo(&buf, "%s(*) AS %s", fname, colq);
		}
		else
			elog(ERROR,
				 "incr_build_minmax_del_sql_gen: unexpected expression type %d",
				 (int) nodeTag(te->expr));
	}

	/* FROM live_tables [JOIN affected ON grp-key conditions] */
	if (has_join)
	{
		/* Rebuild same alias scheme but using live tables */
		appendStringInfo(&buf, " FROM %s %s", livename, INCR_DELTA_ALIAS);
		foreach(lc, join_list)
		{
			IncrJoinEntry  *je = lfirst(lc);

			if (je->quals == NULL)
			{
				appendStringInfo(&buf, " CROSS JOIN %s _j%d_",
								 mv_qname(je->oid), je->varno);
			}
			else
			{
				StringInfoData jbuf;

				initStringInfo(&jbuf);
				incr_deparse_where_qual(je->quals, viewQuery->rtable,
										delta_varno, &jbuf);
				appendStringInfo(&buf, " JOIN %s _j%d_ ON (%s)",
								 mv_qname(je->oid), je->varno, jbuf.data);
			}
		}

		/* JOIN affected ON (alias.col = affected.resname AND ...) */
		appendStringInfoString(&buf, " JOIN affected ON (");
		first = true;
		foreach(lc, viewQuery->targetList)
		{
			TargetEntry    *te = lfirst_node(TargetEntry, lc);
			StringInfoData	ebuf;

			if (te->resjunk || !IsA(te->expr, Var))
				continue;
			if (!first)
				appendStringInfoString(&buf, " AND ");
			first = false;
			initStringInfo(&ebuf);
			incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
									delta_varno, &ebuf);
			appendStringInfo(&buf, "%s=affected.%s",
							 ebuf.data, quote_identifier(te->resname));
		}
		appendStringInfoChar(&buf, ')');
	}
	else
	{
		/* Single-table: FROM live_table WHERE (g1[,g2]) IN (SELECT g1[,g2] FROM affected) */
		appendStringInfo(&buf, " FROM %s", livename);

		/* Collect group-col TEs for the IN list */
		{
			List	   *grp_tes = NIL;
			ListCell   *glc;

			foreach(glc, viewQuery->targetList)
			{
				TargetEntry *te = lfirst_node(TargetEntry, glc);

				if (!te->resjunk && IsA(te->expr, Var))
					grp_tes = lappend(grp_tes, te);
			}

			if (list_length(grp_tes) == 1)
			{
				TargetEntry *te = linitial(grp_tes);
				const char  *colq = quote_identifier(te->resname);

				appendStringInfo(&buf, " WHERE %s IN (SELECT %s FROM affected)",
								 colq, colq);
			}
			else
			{
				/* Row expression: (g1, g2, ...) IN (SELECT g1, g2, ... FROM affected) */
				appendStringInfoString(&buf, " WHERE (");
				first = true;
				foreach(glc, grp_tes)
				{
					TargetEntry *te = lfirst(glc);

					if (!first)
						appendStringInfoChar(&buf, ',');
					first = false;
					appendStringInfoString(&buf, quote_identifier(te->resname));
				}
				appendStringInfoString(&buf, ") IN (SELECT ");
				first = true;
				foreach(glc, grp_tes)
				{
					TargetEntry *te = lfirst(glc);

					if (!first)
						appendStringInfoChar(&buf, ',');
					first = false;
					appendStringInfoString(&buf, quote_identifier(te->resname));
				}
				appendStringInfoString(&buf, " FROM affected)");
			}
		}
	}

	/* AND view WHERE clause (additional source-table filter) */
	{
		Node *wq = incr_get_where_qual(viewQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, viewQuery->rtable, delta_varno, &wbuf);
			if (has_join)
				appendStringInfo(&buf, " AND %s", wbuf.data);
			else
				appendStringInfo(&buf, " AND %s", wbuf.data);
		}
	}

	/* GROUP BY */
	appendStringInfoString(&buf, " GROUP BY ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te = lfirst_node(TargetEntry, lc);
		StringInfoData	ebuf;

		if (te->resjunk || !IsA(te->expr, Var))
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
								delta_varno, &ebuf);
		appendStringInfoString(&buf, ebuf.data);
	}
	appendStringInfoString(&buf, "),");

	/* ----------------------------------------------------------------
	 * upd CTE: UPDATE matview with recomputed values for surviving groups.
	 * ---------------------------------------------------------------- */
	appendStringInfo(&buf, " upd AS (UPDATE %s SET ", mvname);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		const char  *colq;

		if (te->resjunk || IsA(te->expr, Var))
			continue;
		if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			continue;

		colq = quote_identifier(te->resname);
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;

		/*
		 * COUNT and SUM columns use delta arithmetic (mv.col - del_col)
		 * rather than the rescan value from new_agg.  This prevents a
		 * concurrent INSERT that commits between new_agg's READ COMMITTED
		 * scan and the row-lock acquisition in this UPDATE from being
		 * silently lost.
		 */
		{
			bool		is_delta = false;
			const char *delta_colname = NULL;

			if (strcmp(te->resname, MATVIEW_INCR_COUNT_COL) == 0)
			{
				is_delta = true;
				delta_colname = "del_cnt";
			}
			else if (IsA(te->expr, Aggref))
			{
				Aggref *agg2   = (Aggref *) te->expr;
				char   *fname2 = get_func_name(agg2->aggfnoid);

				if (strcmp(fname2, "count") == 0)
				{
					is_delta = true;
					delta_colname = "del_cnt";
				}
				else if (strcmp(fname2, "sum") == 0)
				{
					is_delta = true;
					delta_colname = psprintf("del_%s", te->resname);
				}
			}

			if (is_delta)
				appendStringInfo(&buf, "%s=%s.%s-old_delta.%s",
								 colq, mvname, colq,
								 quote_identifier(delta_colname));
			else
				appendStringInfo(&buf, "%s=new_agg.%s", colq, colq);
		}
	}
	appendStringInfo(&buf, " FROM new_agg JOIN old_delta USING (");
	first = true;
	foreach(gcl, groupColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(gcl)));

		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		appendStringInfoString(&buf, colq);
	}
	appendStringInfoString(&buf, ") WHERE ");
	first = true;
	foreach(gcl, groupColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(gcl)));

		if (!first)
			appendStringInfoString(&buf, " AND ");
		first = false;
		appendStringInfo(&buf, "%s.%s=new_agg.%s", mvname, colq, colq);
	}
	appendStringInfoString(&buf, ")");

	/* ----------------------------------------------------------------
	 * Final DELETE: remove groups that vanished entirely (not in new_agg).
	 * ---------------------------------------------------------------- */
	appendStringInfo(&buf, " DELETE FROM %s USING affected WHERE ", mvname);
	first = true;
	foreach(gcl, groupColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(gcl)));

		if (!first)
			appendStringInfoString(&buf, " AND ");
		first = false;
		appendStringInfo(&buf, "%s.%s=affected.%s", mvname, colq, colq);
	}
	appendStringInfoString(&buf,
						   " AND NOT EXISTS (SELECT 1 FROM new_agg WHERE ");
	first = true;
	foreach(gcl, groupColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(gcl)));

		if (!first)
			appendStringInfoString(&buf, " AND ");
		first = false;
		appendStringInfo(&buf, "new_agg.%s=%s.%s", colq, mvname, colq);
	}
	appendStringInfoChar(&buf, ')');

	return buf.data;
}

/*
 * incr_build_minmax_lock_sql_gen — lock SQL for two-phase MIN/MAX DELETE
 *
 * Generates a SELECT that acquires one advisory transaction lock per affected
 * group key from __mv_oldtable__.  Executed as a separate SPI call BEFORE
 * del_sql so that READ COMMITTED takes a new snapshot after the locks are
 * held, making all concurrent committed INSERTs visible to new_agg.
 *
 * Lock key space: (matview OID as int4, hashtext of concatenated group cols).
 */
static char *
incr_build_minmax_lock_sql_gen(Oid mvrelid, Query *viewQuery,
							   int delta_varno, const char *delta_table,
							   List *join_list)
{
	StringInfoData	buf;
	List		   *groupColNames = NIL;
	ListCell	   *lc,
				   *gcl;
	bool			has_join = (join_list != NIL || delta_varno >= 1);
	bool			first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	initStringInfo(&buf);

	/* SELECT pg_advisory_xact_lock(oid, hashtext(g1::text [|| '|' || g2::text ...])) */
	appendStringInfo(&buf,
					 "SELECT pg_advisory_xact_lock(%u, hashtext(",
					 (unsigned) mvrelid);
	if (list_length(groupColNames) == 1)
	{
		const char *colq = quote_identifier(strVal(linitial(groupColNames)));

		appendStringInfo(&buf, "%s::text", colq);
	}
	else
	{
		first = true;
		foreach(gcl, groupColNames)
		{
			const char *colq = quote_identifier(strVal(lfirst(gcl)));

			if (!first)
				appendStringInfoString(&buf, " || '|' || ");
			first = false;
			appendStringInfo(&buf, "%s::text", colq);
		}
	}
	appendStringInfoString(&buf, ")) FROM (SELECT DISTINCT ");

	/* emit group-col expressions */
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te = lfirst_node(TargetEntry, lc);
		StringInfoData	ebuf;

		if (te->resjunk || !IsA(te->expr, Var))
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
								delta_varno, &ebuf);
		if (has_join)
			appendStringInfo(&buf, "%s AS %s", ebuf.data,
							 quote_identifier(te->resname));
		else
			appendStringInfoString(&buf, ebuf.data);
	}

	/* FROM delta_table [alias JOIN ...] */
	incr_append_from_join(&buf, viewQuery, delta_varno, delta_table, join_list);

	/* Optional WHERE from view definition */
	{
		Node *wq = incr_get_where_qual(viewQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, viewQuery->rtable, delta_varno, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
	}
	appendStringInfoString(&buf, ") _aff_");

	return buf.data;
}


/* ================================================================
 * Phase 16: CTE / FROM-subquery normalization
 *
 * Pre-processing pass that rewrites CTEs and non-LATERAL FROM-subqueries
 * into equivalent forms that the existing IVM SQL generators support.
 *
 * Three transformations:
 *
 *  T1 (filter/projection inline): CTE or subquery has a single base
 *     table, no GROUP BY, no aggregates, no DISTINCT, no set ops.
 *     Replace the RTE_CTE/RTE_SUBQUERY slot with the base-table RTE,
 *     substitute all column references, merge WHERE conditions.
 *
 *  T2 (aggregate merge): CTE/subquery has GROUP BY + aggregates; the
 *     outer query uses it as its sole source with no GROUP BY of its
 *     own.  Outer WHERE becomes HAVING in the merged query.
 *
 *  T3 (DISTINCT merge): CTE/subquery has DISTINCT (no GROUP BY); outer
 *     uses it as sole source with no aggregates.  Outer WHERE merges
 *     into the inner WHERE.
 *
 * Nested CTE chains are unravelled iteratively.
 * ================================================================ */

/* ----------------------------------------------------------------
 * Mutator context types
 * ---------------------------------------------------------------- */

typedef struct IncrVarRemap
{
	int		src_varno;
	int		dst_varno;
} IncrVarRemap;

typedef struct IncrSubstColCtx
{
	int		src_varno;
	List   *src_tlist;
	int		src_base_varno;		/* inner base-table varno in source query */
} IncrSubstColCtx;

typedef struct IncrSubstMergeCtx
{
	int		src_varno;
	List   *src_tlist;
} IncrSubstMergeCtx;


/* ----------------------------------------------------------------
 * incr_remap_var_mutator: copy-on-write remap Var.varno
 * ---------------------------------------------------------------- */
static Node *
incr_remap_var_mutator(Node *node, void *ctx_ptr)
{
	IncrVarRemap *ctx = (IncrVarRemap *) ctx_ptr;

	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var *v = (Var *) node;

		if (v->varno == ctx->src_varno && v->varlevelsup == 0)
		{
			Var *nv = (Var *) copyObject(v);

			nv->varno = ctx->dst_varno;
			return (Node *) nv;
		}
	}
	return expression_tree_mutator(node, incr_remap_var_mutator, ctx_ptr);
}

/*
 * incr_subst_col_mutator (T1): substitute Var(src_varno, K) with the
 * K-th target expression from src_tlist, remapping the inner base-table
 * varno to src_varno so the expression fits in the outer query's rtable.
 */
static Node *
incr_subst_col_mutator(Node *node, void *ctx_ptr)
{
	IncrSubstColCtx *ctx = (IncrSubstColCtx *) ctx_ptr;

	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var *v = (Var *) node;

		if (v->varno == ctx->src_varno && v->varlevelsup == 0 &&
			v->varattno >= 1)
		{
			TargetEntry *te;
			Node		*expr;
			IncrVarRemap remap;

			te   = list_nth_node(TargetEntry, ctx->src_tlist,
								 v->varattno - 1);
			expr = (Node *) copyObject(te->expr);

			remap.src_varno = ctx->src_base_varno;
			remap.dst_varno = ctx->src_varno;
			return incr_remap_var_mutator(expr, &remap);
		}
	}
	return expression_tree_mutator(node, incr_subst_col_mutator, ctx_ptr);
}

/*
 * incr_subst_merge_mutator (T2/T3): substitute Var(src_varno, K) with
 * the K-th target expression AS-IS (inner varnos preserved because we
 * are building into the inner query's structure).
 */
static Node *
incr_subst_merge_mutator(Node *node, void *ctx_ptr)
{
	IncrSubstMergeCtx *ctx = (IncrSubstMergeCtx *) ctx_ptr;

	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var *v = (Var *) node;

		if (v->varno == ctx->src_varno && v->varlevelsup == 0 &&
			v->varattno >= 1)
		{
			TargetEntry *te = list_nth_node(TargetEntry, ctx->src_tlist,
											v->varattno - 1);

			return (Node *) copyObject(te->expr);
		}
	}
	return expression_tree_mutator(node, incr_subst_merge_mutator, ctx_ptr);
}

/* Apply IncrSubstColCtx to target list, jointree, HAVING, and GROUP RTE
 * groupexprs.  In PostgreSQL 16+, grouped TL entries use Var(group_rte, K)
 * rather than Var(src, K), so we must also substitute the GROUP RTE's
 * groupexprs to keep the group key expressions consistent. */
static void
incr_apply_subst_col(Query *q, IncrSubstColCtx *ctx)
{
	ListCell   *lc;

	q->targetList = (List *)
		incr_subst_col_mutator((Node *) q->targetList, ctx);
	if (q->jointree)
		q->jointree = (FromExpr *)
			incr_subst_col_mutator((Node *) q->jointree, ctx);
	if (q->havingQual)
		q->havingQual = incr_subst_col_mutator(q->havingQual, ctx);

	foreach(lc, q->rtable)
	{
		RangeTblEntry *r = lfirst_node(RangeTblEntry, lc);

		if (r->rtekind == RTE_GROUP && r->groupexprs != NIL)
			r->groupexprs = (List *)
				incr_subst_col_mutator((Node *) r->groupexprs, ctx);
	}
}


/* ----------------------------------------------------------------
 * Predicate helpers
 * ---------------------------------------------------------------- */

/*
 * Return the varno of the single RTE_RELATION in q, or -1.
 * Skips system RTEs (JOIN, RESULT, GROUP).
 */
static int
incr_single_base_varno(Query *q)
{
	ListCell   *lc;
	int			vno		 = 1;
	int			base_vno = -1;

	foreach(lc, q->rtable)
	{
		RangeTblEntry *r = lfirst_node(RangeTblEntry, lc);

		if (r->rtekind == RTE_JOIN || r->rtekind == RTE_RESULT ||
			r->rtekind == RTE_GROUP)
		{
			vno++;
			continue;
		}
		if (r->rtekind == RTE_RELATION)
		{
			if (base_vno != -1)
				return -1;		/* more than one */
			base_vno = vno;
		}
		else
			return -1;			/* subquery, CTE, function, … */
		vno++;
	}
	return base_vno;
}

/* Find the outer-query varno for RTE_CTE named ctename, or -1. */
static int
incr_find_cte_varno(Query *q, const char *ctename)
{
	ListCell   *lc;
	int			vno = 1;

	foreach(lc, q->rtable)
	{
		RangeTblEntry *r = lfirst_node(RangeTblEntry, lc);

		if (r->rtekind == RTE_CTE &&
			strcmp(r->ctename, ctename) == 0 &&
			r->ctelevelsup == 0)
			return vno;
		vno++;
	}
	return -1;
}

/* Single base table, no GROUP BY, no aggregates, no DISTINCT,
 * no set ops, no window funcs, no CTEs, no sublinks. */
static bool
incr_q_is_filter_proj(Query *q)
{
	if (q->groupClause != NIL || q->hasAggs)
		return false;
	if (q->distinctClause != NIL || q->setOperations)
		return false;
	if (q->limitCount || q->limitOffset)
		return false;
	if (q->hasWindowFuncs || q->cteList != NIL || q->hasSubLinks)
		return false;
	return (incr_single_base_varno(q) > 0);
}

/* Single base table, GROUP BY + aggregates, no set ops, no window funcs,
 * no CTEs, no sublinks. */
static bool
incr_q_is_single_agg(Query *q)
{
	if (q->groupClause == NIL || !q->hasAggs)
		return false;
	if (q->setOperations || q->hasWindowFuncs)
		return false;
	if (q->cteList != NIL || q->hasSubLinks)
		return false;
	return (incr_single_base_varno(q) > 0);
}

/* Single base table, DISTINCT (no GROUP BY, no aggregates), no set ops,
 * no window funcs, no CTEs, no sublinks. */
static bool
incr_q_is_distinct_only(Query *q)
{
	if (q->distinctClause == NIL || q->groupClause != NIL || q->hasAggs)
		return false;
	if (q->setOperations || q->hasWindowFuncs)
		return false;
	if (q->cteList != NIL || q->hasSubLinks)
		return false;
	return (incr_single_base_varno(q) > 0);
}

/*
 * True if outer's only non-system RTE is at src_varno (the CTE/subquery
 * we want to inline/merge).
 */
static bool
incr_outer_sole_source_is(Query *outer, int src_varno)
{
	ListCell   *lc;
	int			vno = 1;

	foreach(lc, outer->rtable)
	{
		RangeTblEntry *r = lfirst_node(RangeTblEntry, lc);

		if (r->rtekind == RTE_JOIN || r->rtekind == RTE_RESULT ||
			r->rtekind == RTE_GROUP)
		{
			vno++;
			continue;
		}
		if (vno != src_varno)
			return false;
		vno++;
	}
	return true;
}


/* ----------------------------------------------------------------
 * T1: Inline a filter/projection CTE or FROM-subquery.
 *
 * Replaces the slot at src_varno in outer->rtable with the source's
 * base-table RTE.  Substitutes all column references in outer.
 * Merges source WHERE into outer WHERE.
 *
 * Does NOT remove the CTE from outer->cteList; caller does that.
 * ---------------------------------------------------------------- */
static bool
incr_try_inline_filter(Query *outer, Query *srcq, int src_varno)
{
	int				base_vno;
	RangeTblEntry  *base_rte;
	RangeTblEntry  *new_rte;
	IncrSubstColCtx subst;

	if (!incr_q_is_filter_proj(srcq))
		return false;

	base_vno = incr_single_base_varno(srcq);
	if (base_vno < 0)
		return false;

	base_rte = list_nth_node(RangeTblEntry, srcq->rtable, base_vno - 1);
	new_rte  = copyObject(base_rte);

	/* Migrate the base table's permission entry to the outer query */
	if (base_rte->perminfoindex != 0 && srcq->rteperminfos != NIL)
	{
		RTEPermissionInfo *src_perm;
		RTEPermissionInfo *new_perm;

		src_perm = list_nth_node(RTEPermissionInfo, srcq->rteperminfos,
								 base_rte->perminfoindex - 1);
		new_perm = copyObject(src_perm);
		outer->rteperminfos = lappend(outer->rteperminfos, new_perm);
		new_rte->perminfoindex = list_length(outer->rteperminfos);
	}
	else
		new_rte->perminfoindex = 0;

	/* Replace the CTE/subquery slot with the base table */
	lfirst(list_nth_cell(outer->rtable, src_varno - 1)) = new_rte;

	/* Substitute all column references in outer */
	subst.src_varno      = src_varno;
	subst.src_tlist      = srcq->targetList;
	subst.src_base_varno = base_vno;
	incr_apply_subst_col(outer, &subst);

	/* Merge source WHERE into outer WHERE */
	if (srcq->jointree != NULL && srcq->jointree->quals != NULL)
	{
		IncrVarRemap remap;
		Node		*extra;

		extra = copyObject(srcq->jointree->quals);

		remap.src_varno = base_vno;
		remap.dst_varno = src_varno;
		extra = incr_remap_var_mutator(extra, &remap);

		if (outer->jointree->quals == NULL)
			outer->jointree->quals = extra;
		else
			outer->jointree->quals =
				(Node *) makeBoolExpr(AND_EXPR,
									  list_make2(outer->jointree->quals,
												 extra),
									  -1);
	}

	return true;
}


/* ----------------------------------------------------------------
 * T2: Merge a single-table aggregate CTE/subquery into the outer query.
 *
 * Outer must use the CTE/subquery as its sole source, have no GROUP BY /
 * aggregates of its own.  Outer WHERE becomes HAVING in the merged query.
 *
 * Replaces outer query fields in-place.
 * ---------------------------------------------------------------- */
static bool
incr_try_merge_agg(Query *outer, Query *srcq, int src_varno)
{
	Query			  *new_q;
	IncrSubstMergeCtx  ctx;
	ListCell		  *olc;
	ListCell		  *nlc;

	if (!incr_q_is_single_agg(srcq))
		return false;
	if (!incr_outer_sole_source_is(outer, src_varno))
		return false;
	if (outer->hasAggs || outer->groupClause != NIL)
		return false;
	if (outer->hasSubLinks)
		return false;

	new_q = copyObject(srcq);

	/* Merge outer WHERE → HAVING */
	if (outer->jointree != NULL && outer->jointree->quals != NULL)
	{
		Node *having;

		ctx.src_varno = src_varno;
		ctx.src_tlist = srcq->targetList;
		having = incr_subst_merge_mutator(
			copyObject(outer->jointree->quals), &ctx);

		if (new_q->havingQual == NULL)
			new_q->havingQual = having;
		else
			new_q->havingQual =
				(Node *) makeBoolExpr(AND_EXPR,
									  list_make2(new_q->havingQual, having),
									  -1);
	}

	/* Preserve outer column aliases */
	olc = list_head(outer->targetList);
	nlc = list_head(new_q->targetList);
	while (olc && nlc)
	{
		TargetEntry *ote = lfirst_node(TargetEntry, olc);
		TargetEntry *nte = lfirst_node(TargetEntry, nlc);

		if (!ote->resjunk && !nte->resjunk && ote->resname)
			nte->resname = pstrdup(ote->resname);
		olc = lnext(outer->targetList, olc);
		nlc = lnext(new_q->targetList, nlc);
	}

	/* Replace outer query with merged form */
	outer->cteList		  = NIL;
	outer->rtable		  = new_q->rtable;
	outer->rteperminfos	  = new_q->rteperminfos;
	outer->jointree		  = new_q->jointree;
	outer->targetList	  = new_q->targetList;
	outer->groupClause	  = new_q->groupClause;
	outer->havingQual	  = new_q->havingQual;
	outer->hasAggs		  = new_q->hasAggs;
	outer->hasSubLinks	  = new_q->hasSubLinks;
	outer->hasWindowFuncs = new_q->hasWindowFuncs;
	outer->distinctClause = new_q->distinctClause;
	outer->hasDistinctOn  = new_q->hasDistinctOn;
	outer->setOperations  = new_q->setOperations;
	outer->sortClause	  = new_q->sortClause;
	outer->limitCount	  = new_q->limitCount;
	outer->limitOffset	  = new_q->limitOffset;
	outer->hasGroupRTE	  = new_q->hasGroupRTE;

	return true;
}


/* ----------------------------------------------------------------
 * T3: Merge a DISTINCT-only CTE/subquery into the outer query.
 *
 * Outer must be sole-source with no GROUP BY / aggregates / DISTINCT.
 * Outer WHERE merges into the inner WHERE.
 *
 * Replaces outer query fields in-place.
 * ---------------------------------------------------------------- */
static bool
incr_try_merge_distinct(Query *outer, Query *srcq, int src_varno)
{
	Query			  *new_q;
	IncrSubstMergeCtx  ctx;
	ListCell		  *olc;
	ListCell		  *nlc;

	if (!incr_q_is_distinct_only(srcq))
		return false;
	if (!incr_outer_sole_source_is(outer, src_varno))
		return false;
	if (outer->hasAggs || outer->groupClause != NIL)
		return false;
	if (outer->distinctClause != NIL || outer->hasSubLinks)
		return false;

	new_q = copyObject(srcq);

	/* Merge outer WHERE into new_q WHERE */
	if (outer->jointree != NULL && outer->jointree->quals != NULL)
	{
		Node *extra;

		ctx.src_varno = src_varno;
		ctx.src_tlist = srcq->targetList;
		extra = incr_subst_merge_mutator(
			copyObject(outer->jointree->quals), &ctx);

		if (new_q->jointree->quals == NULL)
			new_q->jointree->quals = extra;
		else
			new_q->jointree->quals =
				(Node *) makeBoolExpr(AND_EXPR,
									  list_make2(new_q->jointree->quals,
												 extra),
									  -1);
	}

	/* Preserve outer column aliases */
	olc = list_head(outer->targetList);
	nlc = list_head(new_q->targetList);
	while (olc && nlc)
	{
		TargetEntry *ote = lfirst_node(TargetEntry, olc);
		TargetEntry *nte = lfirst_node(TargetEntry, nlc);

		if (!ote->resjunk && !nte->resjunk && ote->resname)
			nte->resname = pstrdup(ote->resname);
		olc = lnext(outer->targetList, olc);
		nlc = lnext(new_q->targetList, nlc);
	}

	/* Replace outer query with merged form */
	outer->cteList		  = NIL;
	outer->rtable		  = new_q->rtable;
	outer->rteperminfos	  = new_q->rteperminfos;
	outer->jointree		  = new_q->jointree;
	outer->targetList	  = new_q->targetList;
	outer->groupClause	  = new_q->groupClause;
	outer->havingQual	  = new_q->havingQual;
	outer->hasAggs		  = new_q->hasAggs;
	outer->hasSubLinks	  = new_q->hasSubLinks;
	outer->hasWindowFuncs = new_q->hasWindowFuncs;
	outer->distinctClause = new_q->distinctClause;
	outer->hasDistinctOn  = new_q->hasDistinctOn;
	outer->setOperations  = new_q->setOperations;
	outer->sortClause	  = new_q->sortClause;
	outer->limitCount	  = new_q->limitCount;
	outer->limitOffset	  = new_q->limitOffset;
	outer->hasGroupRTE	  = new_q->hasGroupRTE;

	return true;
}


/* ----------------------------------------------------------------
 * incr_subst_inner_cte_refs — inline CTEs from outer_with_ctes into
 * srcq's body (handles nested CTE chains before inlining srcq itself).
 * Returns true if any substitution occurred.
 * ---------------------------------------------------------------- */
static bool
incr_subst_inner_cte_refs(Query *srcq, Query *outer_with_ctes)
{
	bool	any_changed = false;
	bool	changed;

	do
	{
		ListCell   *lc;
		int			vno;

		changed = false;
		vno		= 1;
		foreach(lc, srcq->rtable)
		{
			RangeTblEntry *r = lfirst_node(RangeTblEntry, lc);

			if (r->rtekind == RTE_CTE && r->ctelevelsup == 1)
			{
				ListCell *clc;

				foreach(clc, outer_with_ctes->cteList)
				{
					CommonTableExpr *ref =
						lfirst_node(CommonTableExpr, clc);

					if (strcmp(ref->ctename, r->ctename) == 0 &&
						!ref->cterecursive)
					{
						Query *ref_body =
							castNode(Query, ref->ctequery);

						if (incr_try_inline_filter(srcq, ref_body, vno) ||
							incr_try_merge_agg(srcq, ref_body, vno) ||
							incr_try_merge_distinct(srcq, ref_body, vno))
						{
							changed		= true;
							any_changed = true;
						}
						break;
					}
				}
				if (changed)
					break;
			}
			vno++;
		}
	} while (changed);

	return any_changed;
}


/* ----------------------------------------------------------------
 * Dispatch: try to normalize one CTE in the outer query.
 * ---------------------------------------------------------------- */
static bool
incr_try_normalize_cte(Query *outer, CommonTableExpr *cte, int cte_varno)
{
	Query *cteq;

	if (cte->cterecursive)
		return false;

	cteq = castNode(Query, cte->ctequery);

	/* Pre-process: inline any CTEs referenced within cteq */
	incr_subst_inner_cte_refs(cteq, outer);

	/* T1: filter/projection inline */
	if (incr_try_inline_filter(outer, cteq, cte_varno))
	{
		outer->cteList = list_delete_ptr(outer->cteList, cte);
		return true;
	}
	/* T2: aggregate merge */
	if (incr_try_merge_agg(outer, cteq, cte_varno))
		return true;	/* cteList set to NIL inside T2 */
	/* T3: DISTINCT merge */
	if (incr_try_merge_distinct(outer, cteq, cte_varno))
		return true;	/* cteList set to NIL inside T3 */

	return false;
}


/* ----------------------------------------------------------------
 * Dispatch: try to normalize one FROM-subquery in the outer query.
 * ---------------------------------------------------------------- */
static bool
incr_try_normalize_subq(Query *outer, int sq_varno)
{
	RangeTblEntry *rte;
	Query		  *sq;

	rte = list_nth_node(RangeTblEntry, outer->rtable, sq_varno - 1);
	if (rte->rtekind != RTE_SUBQUERY || rte->lateral)
		return false;

	sq = rte->subquery;

	if (incr_try_inline_filter(outer, sq, sq_varno))
		return true;
	if (incr_try_merge_agg(outer, sq, sq_varno))
		return true;
	if (incr_try_merge_distinct(outer, sq, sq_varno))
		return true;

	return false;
}


/* ----------------------------------------------------------------
 * incr_normalize_query_body — iteratively normalize q in place.
 * ---------------------------------------------------------------- */
static Query *
incr_normalize_query_body(Query *q)
{
	bool	changed;

	do
	{
		ListCell   *lc;
		int			vno;

		changed = false;

		/* Process CTEs — iterate over a snapshot so mutations are safe */
		{
			List	   *snap = list_copy(q->cteList);

			foreach(lc, snap)
			{
				CommonTableExpr *cte = lfirst_node(CommonTableExpr, lc);
				int				 cv  = incr_find_cte_varno(q, cte->ctename);

				if (cv < 0)
					continue;	/* not directly referenced in outer FROM */

				if (incr_try_normalize_cte(q, cte, cv))
				{
					changed = true;
					break;	/* restart outer loop */
				}
			}
			list_free(snap);
		}

		/* Process FROM-subqueries */
		if (!changed)
		{
			vno = 1;
			foreach(lc, q->rtable)
			{
				RangeTblEntry *r = lfirst_node(RangeTblEntry, lc);

				if (r->rtekind == RTE_SUBQUERY && !r->lateral)
				{
					if (incr_try_normalize_subq(q, vno))
					{
						changed = true;
						break;
					}
				}
				vno++;
			}
		}
	} while (changed);

	/* Remove CTEs no longer directly referenced in outer FROM */
	{
		List	   *snap2 = list_copy(q->cteList);
		ListCell   *lc2;

		foreach(lc2, snap2)
		{
			CommonTableExpr *cte = lfirst_node(CommonTableExpr, lc2);

			if (incr_find_cte_varno(q, cte->ctename) < 0)
				q->cteList = list_delete_ptr(q->cteList, cte);
		}
		list_free(snap2);
	}

	return q;
}


/*
 * MatviewIncrNormalize — public entry point.
 *
 * Returns a normalized copy of viewQuery if any CTE or FROM-subquery
 * was inlined/merged, otherwise returns viewQuery unchanged.
 */
Query *
MatviewIncrNormalize(Query *viewQuery)
{
	ListCell   *lc;
	bool		has_cte;
	bool		has_subq;
	Query	   *q;

	has_cte  = (viewQuery->cteList != NIL);
	has_subq = false;

	if (!has_cte)
	{
		foreach(lc, viewQuery->rtable)
		{
			RangeTblEntry *r = lfirst_node(RangeTblEntry, lc);

			if (r->rtekind == RTE_SUBQUERY && !r->lateral)
			{
				has_subq = true;
				break;
			}
		}
	}

	if (!has_cte && !has_subq)
		return viewQuery;	/* nothing to normalize */

	q = copyObject(viewQuery);
	incr_normalize_query_body(q);
	return q;
}
