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
#include "access/xact.h"
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
#include "miscadmin.h"
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
#include "utils/ruleutils.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/* ----------
 * Process-local plan cache.
 *
 * Five plans per matview:
 *   INCR_PLAN_INS  — apply __mv_newtable delta to matview (INSERT ON CONFLICT)
 *   INCR_PLAN_DEL  — subtract __mv_oldtable delta from matview (UPDATE)
 *   INCR_PLAN_CLN  — remove zero-count groups (DELETE WHERE __mv_count__ <= 0)
 *   INCR_PLAN_HAV  — recompute __mv_having_ok__ for all active groups (HAVING)
 *   INCR_PLAN_LOCK — matview-level advisory lock, run first, that serializes
 *                    maintenance of the recompute/multiset shapes (row-level,
 *                    UNION ALL, outer join, self-join, MIN/MAX) so they stay
 *                    correct under concurrent writers at READ COMMITTED; NULL
 *                    (skipped) for the additive ON CONFLICT shapes
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
 * GUC (default off): route plain single-table aggregate delta-SQL generation
 * through the Query-tree deparse core instead of the hand-written builders.
 * See matview_incr.h and INCREMENTAL_MATVIEW_PHASE2_DESIGN.md.
 */
bool		dbblue_ivm_deparse_delta = true;

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
static bool incr_build_affected_sql(StringInfo buf, Query *viewQuery,
									int delta_varno, const char *delta_table,
									List *all_tables);
static char *incr_build_recompute_sql(Oid mvrelid, Query *viewQuery,
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
/* Phase 2 deparse core — plain single-table aggregate (gated by GUC) */
static void incr_emit_ins_head(StringInfo buf, Oid mvrelid, Query *viewQuery);
static void incr_emit_ins_conflict_tail(StringInfo buf, Oid mvrelid,
										 Query *viewQuery);
static void incr_emit_del_update_tail(StringInfo buf, Oid mvrelid,
									  Query *viewQuery);
static Query *incr_build_delta_select_query(Query *viewQuery, Oid srctable,
											 const char *enrName);
static char *incr_build_ins_sql_deparse(Oid mvrelid, Query *viewQuery,
										Oid srctable, const char *enrName);
static char *incr_build_del_sql_deparse(Oid mvrelid, Query *viewQuery,
										Oid srctable, const char *enrName);
static void incr_emit_conflict_do_nothing(StringInfo buf, Query *viewQuery);
static char *incr_build_backfill_sql_deparse(Oid mvrelid, Query *viewQuery);
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
static void incr_setup_union_all(Oid mvrelid, Query *viewQuery, bool mv_populated);
static bool incr_has_minmax_agg(Query *viewQuery);
static bool incr_has_distinct_agg(Query *viewQuery);
static bool incr_is_recompute_only_func(const char *fname);
static bool incr_needs_recompute(Query *viewQuery);
static char *incr_build_minmax_ins_sql_gen(Oid mvrelid, Query *viewQuery,
										   int delta_varno, const char *delta_table,
										   List *join_list);
static char *incr_build_minmax_del_sql_gen(Oid mvrelid, Query *viewQuery,
										   int delta_varno, const char *delta_table,
										   List *join_list, Oid delta_oid);
static char *incr_build_mv_lock_sql(Oid mvrelid);
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
static const char *incr_having_agg_column(Aggref *hagg, Query *viewQuery);
static bool incr_agg_arg_deparse_safe(Node *expr);
static bool incr_aggs_need_deparse(Query *viewQuery);
static bool incr_inner_join_deparse_shape(Query *viewQuery, int nbasetables);
static bool incr_recompute_outer_shape(Query *viewQuery, int nbasetables);
static bool incr_full_join_single_side_keys(Query *viewQuery);
static bool incr_full_join_coalesce_keys(Query *viewQuery);
static bool incr_is_coalesce_of_join_keys(Node *gexpr, Query *viewQuery);
static bool incr_try_resolve_var_to_rel(Var *v, List *rtable, int *varno_out);
static void incr_append_recompute_tail(StringInfo buf, const char *mvname,
											 Query *viewQuery, bool actual_delete_step);
static Query *incr_build_delta_select_query_at_varno(Query *viewQuery,
													 int target_varno,
													 const char *enrName);
static char *incr_build_self_recompute_sql(Oid mvrelid, Query *viewQuery,
									   int v1, int v2, const char *delta_table,
									   List *all_tables, bool include_delete_step);
static bool incr_self_outer_supported_shape(Query *viewQuery);
static bool incr_self_recompute_shape(Query *viewQuery);
static Node *incr_group_key_expr(Query *q, TargetEntry *te);
static bool incr_group_needs_deparse(Query *viewQuery);
static Node *incr_get_where_qual(Query *viewQuery);
static void incr_deparse_where_qual(Node *qual, List *rtable, int delta_varno,
									StringInfo buf);
static const char *incr_resolve_var_colname(Var *v, List *rtable,
											int *resolved_varno_out);
static void incr_deparse_having_cond(Node *expr, Query *viewQuery, StringInfo buf);
static char *incr_build_hav_sql(Oid mvrelid, Query *viewQuery);
static void incr_link_having_base_to_view(Oid base);
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
	bool		deparse_agg_shape;
	bool		self_join_seen = false;

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
					self_join_seen = true;
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

		/* self-join + GROUP BY is handled by incr_build_self_recompute_sql */
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
			if (has_outer_join && self_join_seen &&
				!incr_self_outer_supported_shape(viewQuery))
			{
				/*
				 * The outer-join maintenance path registers one catalog row per
				 * join alias keyed by (mvrelid, srctable); a self-join would
				 * collide on that key.  The self-outer builder handles the
				 * supported shape (two-way self LEFT/RIGHT join, GROUP BY keys
				 * on the preserved anchor) with a single combined catalog row;
				 * everything else (optional-side keys, FULL self join, 3+-table,
				 * or row-level self outer join) is rejected cleanly rather than
				 * leaking an internal unique-constraint violation at CREATE.
				 */
				*reason = "a self-join combined with LEFT/RIGHT/FULL OUTER JOIN "
						  "is supported only as a two-table self LEFT/RIGHT join "
						  "with GROUP BY keys on the preserved side";
				return false;
			}
			if (has_full_join && viewQuery->groupClause != NIL)
			{
				/*
				 * FULL OUTER JOIN + GROUP BY is supported for the provably
				 * correct subset only: exactly two joined tables where every
				 * GROUP BY key is a plain column (Var) from a SINGLE one of
				 * them.  In that shape the sole group arm 1 cannot see is the
				 * all-NULL group (the other table's orphans); incr_build_recompute_sql
				 * adds a dedicated NULL arm for deltas on the key side.
				 *
				 * Rejected (an orphan flip can move a row BETWEEN non-NULL
				 * groups, which the recompute arms do not track):
				 *   • expression keys such as GROUP BY COALESCE(a.k, b.k);
				 *   • mixed-side keys such as GROUP BY a.k, b.j;
				 *   • FULL joins of three or more tables.
				 */
				if (nbasetables != 2 ||
					!(incr_full_join_single_side_keys(viewQuery) ||
					  incr_full_join_coalesce_keys(viewQuery)))
				{
					*reason = "FULL OUTER JOIN with GROUP BY is supported only "
							  "when exactly two tables are joined and every GROUP BY "
							  "key is either a plain column from a single one of them "
							  "or COALESCE(<left key>, <right key>) of the join keys";
					return false;
				}
			}
		}

	}

	/*
	 * Shapes the deparse delta core is wired for: a GROUP BY aggregate with no
	 * MIN/MAX, over either a single table or a pure INNER JOIN (no outer join,
	 * no self-join).  HAVING is supported on both (delta strips it; the
	 * failing-group backfill uses deparse too).  For those an aggregate argument
	 * may use any deterministic scalar expression (CASE, COALESCE, function
	 * calls) because ruleutils renders it; other shapes keep the restricted hand
	 * grammar until deparse is widened to them.  This must mirror the routing
	 * decision in MatviewIncrSetup so a shape accepted here is actually one the
	 * deparse path will build (and re-build identically on restore).
	 */
	deparse_agg_shape = (viewQuery->groupClause != NIL &&
						!incr_has_minmax_agg(viewQuery) &&
						!incr_needs_recompute(viewQuery) &&
						(nbasetables == 1 ||
						 incr_inner_join_deparse_shape(viewQuery, nbasetables)));

	/*
	 * GROUP BY on an expression (e.g. GROUP BY date_trunc('month', d)) is
	 * maintained by the deparse core: it copies the view Query — grouping
	 * expressions and all — and only swaps the source RTE for the transition
	 * table, so ruleutils renders the same GROUP BY over the changed rows, and
	 * every consumer keys on the matview OUTPUT column that holds the value.
	 * Three conditions make that safe and well-defined:
	 *   1. the shape is one the deparse path actually builds (deparse_agg_shape:
	 *      single-table or INNER JOIN, no MIN/MAX, no self-join);
	 *   2. the expression is IMMUTABLE and free of subqueries/aggregates/window
	 *      functions (incr_agg_arg_deparse_safe) — a stable/volatile key could
	 *      map the same row to different groups on its insert- vs delete-delta
	 *      and corrupt the running totals;
	 *   3. the expression is in the SELECT list (a non-junk output column), so
	 *      it has a place to live and to key the unique index / ON CONFLICT on.
	 * A plain-column key (Var) is always fine and skips these checks.
	 */
	{
		ListCell   *glc;

		foreach(glc, viewQuery->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, glc);
			TargetEntry	   *te = get_sortgroupclause_tle(sgc,
														 viewQuery->targetList);
			Node		   *gexpr = incr_group_key_expr(viewQuery, te);

			if (gexpr == NULL || IsA(gexpr, Var))
				continue;			/* plain column — always supported */

			/*
			 * COALESCE(<left key>, <right key>) of a FULL join's equi-key is an
			 * expression key over an outer join, but it is orphan-flip invariant
			 * (incr_is_coalesce_of_join_keys) so the recompute path maintains it
			 * correctly — allow it past the single-table/INNER-only restriction.
			 */
			if (!deparse_agg_shape &&
				!incr_is_coalesce_of_join_keys(gexpr, viewQuery))
			{
				*reason = "GROUP BY on an expression is supported only for "
					"single-table and INNER JOIN aggregates without MIN/MAX or "
					"self-join (or COALESCE of a FULL join's keys)";
				return false;
			}
			if (!incr_agg_arg_deparse_safe(gexpr))
			{
				*reason = "GROUP BY expression must be immutable and free of "
					"subqueries, aggregates, and window functions to be "
					"maintained incrementally";
				return false;
			}
			if (te->resjunk || te->resname == NULL)
			{
				*reason = "a GROUP BY expression must appear in the SELECT list "
					"to be maintained incrementally";
				return false;
			}
		}
	}

	/*
	 * Reserved-name guard: the engine adds hidden maintenance columns named with
	 * the "__mv_" prefix (e.g. __mv_count__, __mv_avgsum_<col>, __mv_having_ok__).
	 * A user output column using that prefix collides — a user "__mv_count__" is
	 * taken for the per-group row count and corrupts the zero-count cleanup
	 * (groups whose key value <= 0 vanish on DELETE).  Reject it.
	 *
	 * Exception: a restored incremental matview's stored query already carries
	 * the hidden columns verbatim — __mv_count__ present as COUNT(*).  Detect that
	 * exact shape and skip the guard so dump/restore is never refused.  A user
	 * collision instead names __mv_count__ as something other than COUNT(*) (or
	 * uses another reserved prefix with no __mv_count__ present at all).
	 */
	{
		bool	already_prepared = false;

		foreach(lc, viewQuery->targetList)
		{
			TargetEntry *te = lfirst_node(TargetEntry, lc);

			if (!te->resjunk && te->resname != NULL &&
				strcmp(te->resname, MATVIEW_INCR_COUNT_COL) == 0 &&
				IsA(te->expr, Aggref) &&
				((Aggref *) te->expr)->args == NIL &&
				strcmp(get_func_name(((Aggref *) te->expr)->aggfnoid), "count") == 0)
			{
				already_prepared = true;
				break;
			}
		}

		if (!already_prepared)
		{
			foreach(lc, viewQuery->targetList)
			{
				TargetEntry *te = lfirst_node(TargetEntry, lc);

				if (!te->resjunk && te->resname != NULL &&
					incr_is_hidden_col(te->resname))
				{
					*reason = "output column name uses the reserved \"__mv_\" "
						"prefix used by the incremental engine's hidden "
						"maintenance columns; rename the column";
					return false;
				}
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
			bool		is_additive =
				(strcmp(fname, "sum") == 0 || strcmp(fname, "count") == 0 ||
				 strcmp(fname, "avg") == 0 || strcmp(fname, "min") == 0 ||
				 strcmp(fname, "max") == 0);
			bool		is_float_sumavg =
				(strcmp(fname, "sum") == 0 || strcmp(fname, "avg") == 0) &&
				(agg->aggtype == FLOAT4OID || agg->aggtype == FLOAT8OID);
			bool		needs_rc =
				agg->aggdistinct != NIL || agg->aggfilter != NULL ||
				incr_is_recompute_only_func(fname) || is_float_sumavg;

			if (!is_additive && !incr_is_recompute_only_func(fname))
			{
				*reason = psprintf("aggregate \"%s\" not supported (supported: SUM, "
								   "COUNT, AVG, MIN, MAX, STDDEV, VARIANCE, BOOL_AND, "
								   "BOOL_OR, STRING_AGG, ARRAY_AGG, JSON(B)_AGG; "
								   "COUNT(DISTINCT); any with FILTER)", fname);
				return false;
			}

			if (needs_rc)
			{
				/*
				 * Recompute path: DISTINCT, stddev/variance/bool, collect
				 * aggregates (string_agg/array_agg/json(b)_agg), FILTERed
				 * aggregates, and float SUM/AVG (recompute avoids running-total
				 * rounding drift).  All are maintained by recomputing each
				 * affected group from the live table(s) via the deparse engine,
				 * which renders the aggregate — DISTINCT, FILTER, and all — verbatim.
				 * Supported over a single table, INNER JOIN, or a supported outer
				 * join; ordered-set aggregates (aggorder) and self-joins are not
				 * yet supported.
				 */
				if (!(nbasetables == 1 ||
					  incr_inner_join_deparse_shape(viewQuery, nbasetables) ||
					  incr_recompute_outer_shape(viewQuery, nbasetables) ||
					  incr_self_recompute_shape(viewQuery)) ||
					agg->aggorder != NIL)
				{
					*reason = psprintf("incremental %s(...) with DISTINCT / FILTER / "
									   "stddev / collect / float is supported only over "
									   "a single table, INNER JOIN, a supported outer "
									   "join, or a two-way self join, without "
									   "ordered-set aggregates", fname);
					return false;
				}
				if (agg->args != NIL)
				{
					Node *arg = (Node *) linitial_node(TargetEntry, agg->args)->expr;

					if (contain_mutable_functions(arg) ||
						!(incr_validate_expr(arg, NULL, false) ||
						  incr_agg_arg_deparse_safe(arg)))
					{
						*reason = psprintf("argument of aggregate \"%s\" uses "
									   "unsupported or non-immutable expressions", fname);
						return false;
					}
				}
				if (agg->aggfilter != NULL &&
					!incr_agg_arg_deparse_safe((Node *) agg->aggfilter))
				{
					*reason = psprintf("FILTER condition of aggregate \"%s\" must be "
								   "immutable and free of subqueries, aggregates, and "
								   "window functions", fname);
					return false;
				}
				continue;
			}

			/*
			 * Additive path: exact SUM/COUNT/AVG/MIN/MAX maintained by per-row
			 * delta.  Accept the hand grammar always; for the plain single-table
			 * shape also accept any deterministic deparse-able expression.
			 */
			if (agg->args != NIL)
			{
				TargetEntry *arg_te = linitial_node(TargetEntry, agg->args);
				Node	   *arg = (Node *) arg_te->expr;
				bool	arg_ok = !contain_mutable_functions(arg) &&
					(incr_validate_expr(arg, NULL, false) ||
					 (deparse_agg_shape && incr_agg_arg_deparse_safe(arg)));

				if (!arg_ok)
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
 * incr_install_triggers — install the AFTER STATEMENT triggers on srctable
 * that drive matview mvrelid: INSERT, DELETE, UPDATE (delta-based) plus
 * TRUNCATE (full-recompute fallback).
 *
 * TRUNCATE removes every row at once and exposes no per-row transition data,
 * so the delta machinery cannot represent it.  Its trigger therefore carries
 * no transition tables; matview_delta_apply() recognises the TRUNCATE event
 * and falls back to a full REFRESH.  Without this trigger a TRUNCATE on a
 * source table would silently leave the matview stale.
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
	incr_create_trigger(mvrelid, srctable,
						TRIGGER_TYPE_TRUNCATE,
						NULL, NULL);
}

/*
 * incr_notice_serialized_shape
 * Emit a one-time NOTICE at CREATE for the recompute/multiset shapes (UNION
 * ALL, self-join, outer join) whose incremental maintenance recomputes or
 * overwrites a region (rather than accumulating additively under the matview
 * row lock).  These shapes serialize their maintenance on a matview-level
 * advisory lock, which makes them correct at every isolation level — including
 * READ COMMITTED — but means concurrent writers to the source tables apply
 * their deltas one at a time.  The additive shapes (single-table / INNER JOIN
 * SUM/COUNT/AVG) are lock-free and unaffected.  Verified by
 * concurrency_exotic.sh.  Purely informational; never blocks a write.
 */
static void
incr_notice_serialized_shape(Query *viewQuery)
{
	const char *shape = NULL;

	if (viewQuery->setOperations != NULL)
		shape = "UNION ALL";
	else
	{
		List *tabs = incr_collect_tables(viewQuery);

		if (incr_has_self_join(tabs))
			shape = "a self-join";
		else if (incr_has_outer_join(tabs))
			shape = "an outer join";
	}

	if (shape != NULL)
		ereport(NOTICE,
				(errmsg("incremental materialized view uses %s; its maintenance is serialized under concurrent writes",
						shape),
				 errdetail("This shape recomputes a region per delta, so it takes a matview-level lock to stay consistent with a full REFRESH at any isolation level (including READ COMMITTED). Concurrent writers to the source tables therefore apply their deltas one at a time.")));
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
	bool		mv_populated;
	char	   *origschema = NULL;
	char	   *origname = NULL;
	ListCell   *lc;

	if (!MatviewIncrIsEligible(viewQuery, &reason))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot use incremental_refresh: %s", reason)));

	/* Inform operators that recompute/multiset shapes serialize maintenance. */
	incr_notice_serialized_shape(viewQuery);

	/*
	 * Is the matview already populated?  On the normal CREATE ... WITH DATA
	 * path it is (createas refreshed it before calling us), so the one-time
	 * backfills that seed hidden state (HAVING failing-group seeding, UNION
	 * dedup) run here.  On the pg_dump/restore path the matview is created
	 * WITH NO DATA and populated later by a standalone REFRESH, so those
	 * backfills — and the HAVING rename/view scaffolding — are deferred to
	 * MatviewIncrPostRefresh(), which runs after that REFRESH populates it.
	 */
	{
		HeapTuple	cltup = SearchSysCache1(RELOID, ObjectIdGetDatum(mvrelid));

		mv_populated = HeapTupleIsValid(cltup)
			? ((Form_pg_class) GETSTRUCT(cltup))->relispopulated : true;
		if (HeapTupleIsValid(cltup))
			ReleaseSysCache(cltup);
	}

	/* UNION ALL: separate setup path */
	if (viewQuery->setOperations != NULL)
	{
		incr_setup_union_all(mvrelid, viewQuery, mv_populated);
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
	if (hasHaving && mv_populated)
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
							   ins_sql, del_sql, "SELECT 1", NULL,
								   incr_build_mv_lock_sql(mvrelid));
		}
		else
		{
			/* Phase 1: aggregate with GROUP BY */
			bool	used_deparse = !incr_has_minmax_agg(viewQuery) &&
				!incr_needs_recompute(viewQuery) &&
				(dbblue_ivm_deparse_delta || incr_aggs_need_deparse(viewQuery) ||
				 incr_group_needs_deparse(viewQuery));

			if (incr_needs_recompute(viewQuery))
			{
				/* DISTINCT / stddev / bool aggregate — maintained by recomputing
				 * each affected group from the live table via the shared
				 * deparse-based recompute engine (incr_build_recompute_sql, which
				 * degenerates to plain arm-1 + tail here: no orphan/NULL arms
				 * fire for a single table).  The deparse renders every aggregate
				 * verbatim, so this is correct for any aggregate alongside;
				 * serialized on the matview-level lock like the other recompute
				 * shapes. */
				List *rc_tables = incr_collect_tables(viewQuery);
				int   rc_varno  = ((IncrJoinEntry *) linitial(rc_tables))->varno;

				ins_sql = incr_build_recompute_sql(mvrelid, viewQuery, rc_varno,
											   MATVIEW_INCR_NEWTABLE,
											   rc_tables, false);
				del_sql = incr_build_recompute_sql(mvrelid, viewQuery, rc_varno,
											   MATVIEW_INCR_OLDTABLE,
											   rc_tables, true);
				incr_store_catalog(mvrelid, srctable, ins_sql, del_sql,
								   cln_sql, hav_sql, incr_build_mv_lock_sql(mvrelid));
			}
			else if (incr_has_minmax_agg(viewQuery))
			{
				char *lock_sql;

				ins_sql  = incr_build_minmax_ins_sql_gen(mvrelid, viewQuery, -1,
														 MATVIEW_INCR_NEWTABLE, NIL);
				del_sql  = incr_build_minmax_del_sql_gen(mvrelid, viewQuery, -1,
														 MATVIEW_INCR_OLDTABLE, NIL,
														 srctable);
				lock_sql = incr_build_mv_lock_sql(mvrelid);
				incr_store_catalog(mvrelid, srctable, ins_sql, del_sql,
								   cln_sql, hav_sql, lock_sql);
			}
			else if (used_deparse)
			{
				/* Phase 2: produce the delta SELECT via the ruleutils deparse
				 * core (single-table aggregate; MIN/MAX above keeps its hand
				 * builders).  HAVING is supported: incr_build_delta_select_query
				 * strips havingQual so the delta covers every group, and the
				 * separate __mv_having_ok__ flag + hav_sql recompute (stored
				 * here) maintain the filter — identical to the hand path.
				 * Auto-routed when the shape needs it (e.g. SUM(CASE ...)) so
				 * such matviews are restorable regardless of the GUC; the GUC
				 * additionally forces deparse for shapes both paths can express. */
				ins_sql = incr_build_ins_sql_deparse(mvrelid, viewQuery,
													 srctable, MATVIEW_INCR_NEWTABLE);
				del_sql = incr_build_del_sql_deparse(mvrelid, viewQuery,
													 srctable, MATVIEW_INCR_OLDTABLE);
				incr_store_catalog(mvrelid, srctable, ins_sql, del_sql,
								   cln_sql, hav_sql, NULL);
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

			if (hasHaving && mv_populated)
			{
				char *backfill_sql = used_deparse
					? incr_build_backfill_sql_deparse(mvrelid, viewQuery)
					: incr_build_backfill_sql_gen(mvrelid, viewQuery, -1,
												  mv_qname(srctable), NIL);
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
									   ins_sql, del_sql, "SELECT 1", NULL,
								   incr_build_mv_lock_sql(mvrelid));
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
										   ins_sql, del_sql, "SELECT 1", NULL,
								   incr_build_mv_lock_sql(mvrelid));
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
										   ins_sql, del_sql, "SELECT 1", NULL,
								   incr_build_mv_lock_sql(mvrelid));
						incr_install_triggers(mvrelid, delta->oid);
					}
				}
			}
		}
		else if (incr_has_outer_join(all_tables))
		{
			/* ---- Phase 8: outer join (LEFT/RIGHT/FULL) recompute strategy ---- */
			Bitmapset  *done_oids = NULL;

			foreach(jlc, all_tables)
			{
				IncrJoinEntry *delta = lfirst(jlc);
				int			   v2;

				if (bms_is_member((int) delta->oid, done_oids))
					continue;		/* self-joined OID already handled below */

				v2 = incr_self_join_other_varno(all_tables, delta->varno,
												delta->oid);
				if (v2 != -1)
				{
					/*
					 * Self OUTER join: the same table appears in two roles, so
					 * a single combined statement handles both (like the INNER
					 * self-join path) and one catalog row is stored for the OID
					 * — registering per-varno would collide on (mvrelid,oid).
					 * The supported shape is gated by incr_self_outer_supported_shape.
					 */
					int v1 = delta->varno;

					if (v1 > v2) { int t = v1; v1 = v2; v2 = t; }
					ins_sql = incr_build_self_recompute_sql(mvrelid, viewQuery,
														v1, v2,
														MATVIEW_INCR_NEWTABLE,
														all_tables, false);
					del_sql = incr_build_self_recompute_sql(mvrelid, viewQuery,
														v1, v2,
														MATVIEW_INCR_OLDTABLE,
														all_tables, true);
					done_oids = bms_add_member(done_oids, (int) delta->oid);
				}
				else
				{
					ins_sql = incr_build_recompute_sql(mvrelid, viewQuery,
												   delta->varno,
												   MATVIEW_INCR_NEWTABLE,
												   all_tables, false);
					/*
					 * Always include the DELETE step for del_sql.  The step
					 * uses NOT EXISTS in _new_agg_ (the live recompute result).
					 * Correct for all shapes:
					 *   - Preserved/inner-dim delete: vanished groups absent from
					 *     _new_agg_ → NOT EXISTS fires.
					 *   - Optional-side delete (preserved group key): preserved
					 *     rows remain → _new_agg_ has all groups → no-op.
					 *   - Optional-side delete (optional group key, arm 2 active):
					 *     arm 2 adds NULL to _affected_; DELETE removes it from
					 *     the MV when all orphaned rows are gone.
					 */
					del_sql = incr_build_recompute_sql(mvrelid, viewQuery,
												   delta->varno,
												   MATVIEW_INCR_OLDTABLE,
												   all_tables, true);
				}
				incr_store_catalog(mvrelid, delta->oid,
								   ins_sql, del_sql, "SELECT 1", hav_sql,
								   incr_build_mv_lock_sql(mvrelid));
				incr_install_triggers(mvrelid, delta->oid);
			}
		}
		else
		{
			/* ---- Phase 2-7: N-table INNER JOIN ---- */
			/* Pure INNER JOIN aggregates ALWAYS use the deparse core (not gated
			 * by the GUC): it computes per-table join deltas correctly for any
			 * number of tables, whereas the hand join-delta builder
			 * mis-attributes a single-row delta to other groups once there are
			 * 3+ tables.  Drives both the delta SQL and the failing-group
			 * backfill.  MIN/MAX and self-joins keep their hand builders. */
			bool	used_deparse = !incr_has_self_join(all_tables) &&
				!incr_has_minmax_agg(viewQuery) &&
				!incr_needs_recompute(viewQuery);

			if (incr_needs_recompute(viewQuery) &&
				!incr_has_self_join(all_tables))
			{
				/* DISTINCT / stddev / bool aggregate over an INNER JOIN —
				 * recompute each affected group from the live join, per source
				 * table, via the shared deparse-based recompute engine
				 * (incr_build_recompute_sql degenerates to arm-1 + tail here: the
				 * orphan/NULL arms never fire for INNER joins).  Self INNER joins
				 * are handled by the self-join branch below (one combined catalog
				 * row, dual-role recompute).  Correct for
				 * every aggregate in the matview; serialized on the
				 * matview-level lock.  (Eligibility allows these only for
				 * single-table or INNER JOIN, so this branch never sees a
				 * self-join or outer join.) */
				foreach(jlc, all_tables)
				{
					IncrJoinEntry *delta = lfirst(jlc);

					ins_sql = incr_build_recompute_sql(mvrelid, viewQuery,
												   delta->varno,
												   MATVIEW_INCR_NEWTABLE,
												   all_tables, false);
					del_sql = incr_build_recompute_sql(mvrelid, viewQuery,
												   delta->varno,
												   MATVIEW_INCR_OLDTABLE,
												   all_tables, true);
					incr_store_catalog(mvrelid, delta->oid, ins_sql, del_sql,
									   cln_sql, hav_sql,
									   incr_build_mv_lock_sql(mvrelid));
					incr_install_triggers(mvrelid, delta->oid);
				}
			}
			else if (incr_has_self_join(all_tables))
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

						/* Self INNER join + GROUP BY: dual-role recompute
						 * via the shared engine (role arms + delta⋈delta arm
						 * on deletes + generic tail). */
						ins_sql = incr_build_self_recompute_sql(
							mvrelid, viewQuery, v1, v2,
							MATVIEW_INCR_NEWTABLE, all_tables, false);
						del_sql = incr_build_self_recompute_sql(
							mvrelid, viewQuery, v1, v2,
							MATVIEW_INCR_OLDTABLE, all_tables, true);
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
									   ins_sql, del_sql, cln_sql, hav_sql,
									   incr_build_mv_lock_sql(mvrelid));
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
					lock_sql = incr_build_mv_lock_sql(mvrelid);
					incr_store_catalog(mvrelid, delta->oid,
									   ins_sql, del_sql, cln_sql, hav_sql, lock_sql);
				}
					else
					{
						/* Phase 2: INNER JOIN delta via the deparse core.  This is the
						 * only remaining case here: a non-self, non-MIN/MAX inner join
						 * (used_deparse).  The delta for table delta->oid swaps only that
						 * table's RTE for the transition-table ENR; the others stay
						 * relations, so deparse renders the join.  The hand join-delta
						 * builder is intentionally not used (it mis-attributes a
						 * single-row delta to other groups for 3+ tables). */
						Assert(used_deparse);
						ins_sql = incr_build_ins_sql_deparse(mvrelid, viewQuery,
									 delta->oid, MATVIEW_INCR_NEWTABLE);
						del_sql = incr_build_del_sql_deparse(mvrelid, viewQuery,
									 delta->oid, MATVIEW_INCR_OLDTABLE);
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
			if (hasHaving && mv_populated)
			{
				IncrJoinEntry *first_je  = linitial(all_tables);
				List		  *join_list = incr_build_join_list_for_delta(
					all_tables, first_je->varno);
				char		  *backfill_sql = used_deparse
					? incr_build_backfill_sql_deparse(mvrelid, viewQuery)
					: incr_build_backfill_sql_gen(
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

	/*
	 * Step 3 (HAVING only): rename the matview to its base name and create the
	 * user-facing filtering VIEW.  Only on the WITH DATA path (mv_populated).
	 * On restore the base is already named "_dbblue_<oid>_base" and the view
	 * is dumped/restored as its own object, so this is skipped.
	 */
	if (hasHaving && mv_populated)
		incr_create_having_view(mvrelid, origschema, origname, viewQuery);

	ereport(DEBUG1,
			(errmsg("DBblue: incremental refresh (Phase %d%s) set up for matview %s",
					nbasetables,
					hasHaving ? " + HAVING" : "",
					(hasHaving && origname) ? origname : get_rel_name(mvrelid))));
}

/*
 * MatviewIncrPostRefresh
 *
 * Run the one-time hidden-state backfills that must happen *after* an
 * incremental matview is populated by a full REFRESH:
 *
 *   - HAVING: seed groups that currently fail the HAVING condition (with
 *     __mv_having_ok__ = false) so they are tracked and can become visible
 *     later via an incremental delta.  The visible (passing) groups were
 *     populated by the REFRESH itself; this adds the rest from the source.
 *   - UNION ALL: collapse the raw per-branch rows the REFRESH produced into
 *     one row per distinct value with a correct __mv_count__.
 *
 * Called from RefreshMatViewByOid for any non-create REFRESH of a matview that
 * has incremental infrastructure.  This makes a plain REFRESH of a HAVING or
 * UNION ALL incremental matview correct, and — because pg_dump restores such a
 * matview as CREATE ... WITH NO DATA followed by a standalone REFRESH — it is
 * what re-arms incremental maintenance across a dump/restore cycle.
 *
 * A no-op for matviews without incremental setup, and for single-table /
 * JOIN aggregate matviews (whose deltas are self-contained, needing no seed).
 */
void
MatviewIncrPostRefresh(Oid mvrelid, Query *viewQuery)
{
	int			nbasetables = 0;
	ListCell   *lc;

	/* Only matviews that actually have incremental infrastructure. */
	if (!MatviewIncrIsSetUp(mvrelid))
		return;

	/*
	 * UNION ALL is maintained as a plain multiset (duplicates kept, no
	 * __mv_count__, no unique index); a REFRESH/restore reloads the raw rows,
	 * which is already correct, so there is nothing to re-seed here.
	 */
	if (viewQuery->setOperations != NULL)
		return;

	/* Everything below is HAVING-only; plain aggregates need no backfill. */
	if (viewQuery->havingQual == NULL)
		return;

	foreach(lc, viewQuery->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		if (rte->rtekind == RTE_RELATION)
			nbasetables++;
	}

	{
		char	   *backfill_sql;
		int			spi_ret;

		if (nbasetables == 1)
		{
			Oid		srctable = incr_get_source_table(viewQuery);
			/* Mirror the CREATE-path routing so the backfill SQL is rebuilt the
			 * same way on restore (expression-arg HAVING must use deparse — the
			 * hand backfill can't render CASE/COALESCE/etc.). */
			bool	used_deparse = !incr_has_minmax_agg(viewQuery) &&
				(dbblue_ivm_deparse_delta || incr_aggs_need_deparse(viewQuery) ||
				 incr_group_needs_deparse(viewQuery));

			backfill_sql = used_deparse
				? incr_build_backfill_sql_deparse(mvrelid, viewQuery)
				: incr_build_backfill_sql_gen(mvrelid, viewQuery, -1,
											  mv_qname(srctable), NIL);
		}
		else
		{
			List		  *all_tables = incr_collect_tables(viewQuery);
			IncrJoinEntry *first_je   = linitial(all_tables);
			List		  *join_list  = incr_build_join_list_for_delta(
				all_tables, first_je->varno);
			/* Mirror the CREATE-path routing: pure INNER JOINs always deparse. */
			bool		   used_deparse = !incr_has_self_join(all_tables) &&
				!incr_has_outer_join(all_tables) &&
				!incr_has_minmax_agg(viewQuery);

			backfill_sql = used_deparse
				? incr_build_backfill_sql_deparse(mvrelid, viewQuery)
				: incr_build_backfill_sql_gen(
					mvrelid, viewQuery, first_je->varno,
					mv_qname(first_je->oid), join_list);
		}

		OpenMatViewIncrementalMaintenance();
		SPI_connect();
		spi_ret = SPI_execute(backfill_sql, false, 0);
		SPI_finish();
		CloseMatViewIncrementalMaintenance();
		if (spi_ret < 0)
			elog(ERROR, "DBblue: HAVING post-refresh backfill failed (code %d)",
				 spi_ret);
		CommandCounterIncrement();
	}

	/*
	 * Re-establish the base->view INTERNAL dependency (the create path records
	 * it in incr_create_having_view, but on restore the view is its own dumped
	 * object and that path is skipped).  Idempotent, so harmless on a re-run.
	 */
	incr_link_having_base_to_view(mvrelid);
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
 * MatviewIncrCheckColumnRename
 *
 * Block renaming a source-table column that an incremental matview depends on.
 *
 * PostgreSQL allows renaming a column that ordinary views/rules reference,
 * because rules track columns by attribute number and follow the rename
 * automatically.  But the incremental engine stores its delta SQL as text with
 * literal column names; a rename would silently invalidate it and every write
 * to the source table would then fail with a confusing "column ... does not
 * exist".  DROP COLUMN and ALTER COLUMN TYPE are already refused by the
 * matview's column-level dependency — this gives renames the same early, clear
 * error instead of a deferred failure.
 *
 * The matview's rewrite rule already records a dependency on (relid, attnum);
 * we error if any such dependent matview is incremental.  Columns no
 * incremental matview uses (or non-incremental matviews) are unaffected.
 */
void
MatviewIncrCheckColumnRename(Oid relid, int attnum)
{
	char	   *sql;
	char	   *mvname = NULL;
	MemoryContext outer = CurrentMemoryContext;
	int			ret;

	if (attnum <= 0)
		return;

	/* No user matviews during bootstrap; the catalog may not exist yet. */
	if (IsBootstrapProcessingMode())
		return;

	sql = psprintf(
		"SELECT c.relname FROM pg_catalog.pg_depend d "
		"JOIN pg_catalog.pg_rewrite rw "
		"  ON d.classid='pg_catalog.pg_rewrite'::pg_catalog.regclass AND d.objid=rw.oid "
		"JOIN pg_catalog.pg_class c ON c.oid=rw.ev_class AND c.relkind='m' "
		"JOIN pg_catalog.pg_dbblue_matview m ON m.mvrelid=c.oid "
		"WHERE d.refclassid='pg_catalog.pg_class'::pg_catalog.regclass "
		"  AND d.refobjid=%u AND d.refobjsubid=%d LIMIT 1",
		relid, attnum);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "MatviewIncrCheckColumnRename: SPI_connect failed");
	ret = SPI_execute(sql, true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		char *v = SPI_getvalue(SPI_tuptable->vals[0],
							   SPI_tuptable->tupdesc, 1);

		/* Copy out of the SPI context, which SPI_finish will free. */
		if (v != NULL)
			mvname = MemoryContextStrdup(outer, v);
	}
	SPI_finish();

	if (mvname != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot rename column: it is used by incremental materialized view \"%s\"",
						mvname),
				 errdetail("Incremental refresh stores delta SQL by column name, which a rename would invalidate."),
				 errhint("Drop and recreate the materialized view, then rename the column.")));
}

/*
 * MatviewIncrIsSetUp
 * Return true if a pg_dbblue_matview catalog row already exists for this
 * matview (i.e. incremental setup has run).  Used by ExecCreateTableAs to make
 * setup idempotent on the WITH NO DATA path taken by pg_dump/restore — the
 * incremental_refresh reloption is present but the triggers/catalog are not,
 * and must be re-established.
 */
bool
MatviewIncrIsSetUp(Oid mvrelid)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData key;
	bool		found;

	catalog = table_open(DbblueMatviewRelationId, AccessShareLock);
	ScanKeyInit(&key,
				Anum_pg_dbblue_matview_mvrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(mvrelid));
	scan = systable_beginscan(catalog,
							  DbblueMatviewMvrelidIndexId,
							  true, NULL, 1, &key);
	found = (systable_getnext(scan) != NULL);
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);

	return found;
}

/*
 * incr_key_var_nullable — true if a grouping/distinct key Var can be NULL
 * (base column nullable, or an outer join can NULL-extend it).  Conservative:
 * returns true if the key can't be resolved to a plain base column.
 */
static bool
incr_key_var_nullable(Var *v, List *rtable)
{
	for (;;)
	{
		RangeTblEntry *rte;

		if (!bms_is_empty(v->varnullingrels))
			return true;
		rte = rt_fetch(v->varno, rtable);
		if (rte->rtekind == RTE_RELATION)
		{
			HeapTuple	tp;
			bool		nullable = true;

			tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(rte->relid),
								 Int16GetDatum(v->varattno));
			if (HeapTupleIsValid(tp))
			{
				nullable = !((Form_pg_attribute) GETSTRUCT(tp))->attnotnull;
				ReleaseSysCache(tp);
			}
			return nullable;
		}
		else if (rte->rtekind == RTE_JOIN)
		{
			Node *av = list_nth(rte->joinaliasvars, v->varattno - 1);

			if (!IsA(av, Var))
				return true;
			v = (Var *) av;
		}
		else if (rte->rtekind == RTE_GROUP)
		{
			Node *ge = list_nth(rte->groupexprs, v->varattno - 1);

			if (!IsA(ge, Var))
				return true;
			v = (Var *) ge;
		}
		else
			return true;
	}
}

/*
 * incr_rewrite_aggfilter_mutator
 * Rewrite  agg(arg) FILTER (WHERE cond)  ->  agg(CASE WHEN cond THEN arg END),
 * which is the exact SQL equivalence, so the existing delta machinery (the
 * deparse core, which already maintains CASE aggregate arguments) handles
 * FILTER with no delta-builder changes.
 *
 * Only SUM / COUNT / AVG are rewritten (no DISTINCT, no ordered-set): those are
 * the aggregates the deparse path maintains.  MIN/MAX FILTER is left untouched so
 * eligibility rejects it cleanly (the hand MIN/MAX builder cannot render CASE).
 *   - COUNT(*) FILTER (WHERE c)  -> COUNT(CASE WHEN c THEN 1 END)  [count(any)]
 *   - agg(x)   FILTER (WHERE c)  -> agg(CASE WHEN c THEN x END)
 */
static Node *
incr_rewrite_aggfilter_mutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Aggref))
	{
		/* expression_tree_mutator returns a fresh Aggref with children mutated */
		Aggref	   *agg = (Aggref *) expression_tree_mutator(node,
											incr_rewrite_aggfilter_mutator, context);
		char	   *fname;

		if (agg->aggfilter == NULL ||
			agg->aggdistinct != NIL || agg->aggorder != NIL)
			return (Node *) agg;

		fname = get_func_name(agg->aggfnoid);
		if (fname == NULL ||
			(strcmp(fname, "sum") != 0 && strcmp(fname, "count") != 0 &&
			 strcmp(fname, "avg") != 0))
			return (Node *) agg;		/* MIN/MAX/other: leave for rejection */

		if (agg->aggstar)
		{
			/* COUNT(*) FILTER -> COUNT(CASE WHEN cond THEN 1 END) */
			CaseWhen   *w = makeNode(CaseWhen);
			CaseExpr   *c = makeNode(CaseExpr);

			w->expr = agg->aggfilter;
			w->result = (Expr *) makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
										   Int32GetDatum(1), false, true);
			w->location = -1;

			c->casetype = INT4OID;
			c->casecollid = InvalidOid;
			c->arg = NULL;
			c->args = list_make1(w);
			c->defresult = (Expr *) makeNullConst(INT4OID, -1, InvalidOid);
			c->location = -1;

			agg->aggfnoid = 2147;		/* count("any") */
			agg->aggstar = false;
			agg->aggargtypes = list_make1_oid(INT4OID);
			agg->args = list_make1(makeTargetEntry((Expr *) c, 1, NULL, false));
		}
		else
		{
			/* agg(x) FILTER -> agg(CASE WHEN cond THEN x END) */
			TargetEntry *arg_te = linitial_node(TargetEntry, agg->args);
			Node	   *arg = (Node *) arg_te->expr;
			CaseWhen   *w = makeNode(CaseWhen);
			CaseExpr   *c = makeNode(CaseExpr);

			w->expr = agg->aggfilter;
			w->result = (Expr *) arg;
			w->location = -1;

			c->casetype = exprType(arg);
			c->casecollid = exprCollation(arg);
			c->arg = NULL;
			c->args = list_make1(w);
			c->defresult = (Expr *) makeNullConst(exprType(arg), exprTypmod(arg),
												  exprCollation(arg));
			c->location = -1;

			arg_te->expr = (Expr *) c;
		}
		agg->aggfilter = NULL;
		return (Node *) agg;
	}

	return expression_tree_mutator(node, incr_rewrite_aggfilter_mutator, context);
}

/*
 * MatviewIncrRewriteAggFilters
 * Apply the FILTER -> CASE rewrite across the query's SELECT list and HAVING
 * clause, in place.  Call on BOTH the schema (execution) query and the stored
 * view query, before eligibility and MatviewIncrAddCountTarget, so the matview
 * schema, initial population, REFRESH, and incremental deltas all agree.
 */
void
MatviewIncrRewriteAggFilters(Query *q)
{
	ListCell   *lc;

	foreach(lc, q->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		te->expr = (Expr *) incr_rewrite_aggfilter_mutator((Node *) te->expr, NULL);
	}
	q->havingQual = incr_rewrite_aggfilter_mutator(q->havingQual, NULL);
}

/*
 * MatviewIncrAddNotNullKeyFilters
 *
 * A NULL value in a GROUP BY / DISTINCT key cannot be maintained incrementally.
 * Rather than block the source write (the guard's old behavior) or corrupt the
 * matview, we keep NULL-key rows *outside the matview's scope* by injecting
 * "<key> IS NOT NULL" into the view's WHERE clause for every nullable key.
 *
 * Because the filter lives in the stored view query, the initial population, a
 * full REFRESH, and the incremental deltas all agree (they all exclude NULL
 * keys) — so the matview stays consistent, and writes to the source are never
 * blocked.  The matview simply does not represent NULL-key rows (the sensible
 * default for a grouped report; a NULL "unknown" group is rarely wanted).
 *
 * Returns the list of key column names that were filtered (for a NOTICE), or
 * NIL if every key is already NOT NULL.  Idempotent-safe to call on both the
 * schema query and the stored view query.
 */
List *
MatviewIncrAddNotNullKeyFilters(Query *q)
{
	List	   *keyvars = NIL;	/* base Var nodes of the grouping/distinct keys */
	List	   *injected = NIL;	/* column-name strings, for the NOTICE */
	List	   *newquals = NIL;
	ListCell   *lc;

	/*
	 * Idempotency: if __mv_count__ is already present the query was already
	 * prepared (e.g. reparsed from a pg_dump/restore, which carries the
	 * IS NOT NULL filters verbatim).  Re-injecting would duplicate the quals.
	 */
	foreach(lc, q->targetList)
	{
		TargetEntry *t = lfirst_node(TargetEntry, lc);

		if (!t->resjunk && t->resname != NULL &&
			strcmp(t->resname, MATVIEW_INCR_COUNT_COL) == 0)
			return NIL;
	}

	/*
	 * NULL group keys are now maintained with full fidelity for every supported
	 * shape: the unique index is NULLS NOT DISTINCT and all delta/rescan/recompute
	 * builders match group keys with IS NOT DISTINCT FROM (the shared-shell
	 * additive shapes, MIN/MAX, the DISTINCT / stddev / bool recompute path, and
	 * self-joins).  So a NULL or partial-NULL key is one arbiter row maintained
	 * exactly like a full REFRESH — no shape needs NULL keys excluded today.  The
	 * key-filter injection below is retained as a hook should a future shape be
	 * unable to match NULL-safely; set needs_exclusion for it there.
	 */
	{
		bool		needs_exclusion = false;

		if (!needs_exclusion)
			return NIL;
	}

	/* Collect key Vars: from the grouping RTE, or (DISTINCT) the output Vars. */
	{
		bool		found_group_rte = false;

		foreach(lc, q->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
			ListCell   *gc;

			if (rte->rtekind != RTE_GROUP)
				continue;
			found_group_rte = true;
			foreach(gc, rte->groupexprs)
			{
				Node *ge = (Node *) lfirst(gc);

				if (IsA(ge, Var))
					keyvars = lappend(keyvars, ge);
			}
			break;
		}
		if (!found_group_rte && q->distinctClause != NIL)
		{
			foreach(lc, q->targetList)
			{
				TargetEntry *te = lfirst_node(TargetEntry, lc);

				if (te->resjunk || incr_is_hidden_col(te->resname))
					continue;
				if (IsA(te->expr, Var))
					keyvars = lappend(keyvars, te->expr);
			}
		}
	}

	foreach(lc, keyvars)
	{
		Var		   *v = (Var *) lfirst(lc);
		NullTest   *nt;
		int			rv;

		if (!incr_key_var_nullable(v, q->rtable))
			continue;

		nt = makeNode(NullTest);
		nt->arg = (Expr *) copyObject(v);
		nt->nulltesttype = IS_NOT_NULL;
		nt->argisrow = false;
		nt->location = -1;
		newquals = lappend(newquals, nt);
		injected = lappend(injected,
						   makeString(pstrdup(incr_resolve_var_colname(v, q->rtable, &rv))));
	}

	if (newquals == NIL)
		return NIL;

	/* AND the IS NOT NULL tests into the existing WHERE quals. */
	if (q->jointree->quals != NULL)
		newquals = lcons(q->jointree->quals, newquals);
	if (list_length(newquals) == 1)
		q->jointree->quals = (Node *) linitial(newquals);
	else
		q->jointree->quals = (Node *) makeBoolExpr(AND_EXPR, newquals, -1);

	return injected;
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

	/*
	 * Idempotency guard: if __mv_count__ is already present the query has
	 * already been prepared (e.g. it is the stored view query reparsed from a
	 * pg_dump/restore, which carries the hidden columns verbatim).  Re-adding
	 * would create a duplicate column, so do nothing.
	 */
	foreach(lc, q->targetList)
	{
		TargetEntry *t = lfirst_node(TargetEntry, lc);

		if (!t->resjunk && t->resname != NULL &&
			strcmp(t->resname, MATVIEW_INCR_COUNT_COL) == 0)
			return;
	}

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
	 * Inject COUNT(x) AS __mv_sumcnt_<col> for each visible SUM(x) so SUM can
	 * show SQL-exact NULL once a group's last non-NULL input is removed
	 * (visible_sum = sumcnt=0 ? NULL : running_sum).  Both the shared shells and
	 * the MIN/MAX builders render the counter; only self-joins skip it (their
	 * recompute path derives SUM directly, so an emptied non-NULL set already
	 * yields NULL without a counter).
	 */
	{
		bool		want_sumcnt = true;
		ListCell   *l2;
		List	   *seen = NIL;

		foreach(l2, q->rtable)		/* detect self-join (duplicate relation OID) */
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, l2);

			if (!want_sumcnt)
				break;
			if (rte->rtekind != RTE_RELATION)
				continue;
			if (list_member_oid(seen, rte->relid))
				want_sumcnt = false;
			else
				seen = lappend_oid(seen, rte->relid);
		}

		if (want_sumcnt)
		{
			foreach(lc, orig_tl)
			{
				TargetEntry *orig_te = lfirst_node(TargetEntry, lc);
				Aggref	   *sum_agg;
				Aggref	   *cnt_agg;

				if (orig_te->resjunk || !IsA(orig_te->expr, Aggref))
					continue;
				sum_agg = (Aggref *) orig_te->expr;
				if (strcmp(get_func_name(sum_agg->aggfnoid), "sum") != 0)
					continue;
				if (sum_agg->aggstar || sum_agg->args == NIL)
					continue;

				/* COUNT(x) over the SUM's argument — count("any") OID 2147 */
				cnt_agg = makeNode(Aggref);
				cnt_agg->aggfnoid = 2147;
				cnt_agg->aggtype = INT8OID;
				cnt_agg->aggcollid = InvalidOid;
				cnt_agg->inputcollid = InvalidOid;
				cnt_agg->aggtranstype = InvalidOid;
				cnt_agg->aggargtypes = sum_agg->aggargtypes;
				cnt_agg->aggdirectargs = NIL;
				cnt_agg->args = copyObject(sum_agg->args);
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
									 psprintf("%s%s", MATVIEW_INCR_SUMCNT_PREFIX,
											  orig_te->resname),
									 false);
				q->targetList = lappend(q->targetList, te);
			}
		}
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
	if (strncmp(resname, MATVIEW_INCR_SUMCNT_PREFIX,
				strlen(MATVIEW_INCR_SUMCNT_PREFIX)) == 0)
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
 * incr_agg_arg_unsafe_walker — reject the constructs that make an aggregate
 * argument unmaintainable by a per-row delta.  Used (with the immutability
 * check below) when the deparse delta core renders the SELECT: ruleutils can
 * render arbitrary scalar expressions, so the only hard limits are semantic.
 */
static bool
incr_agg_arg_unsafe_walker(Node *node, void *ctx)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref) || IsA(node, GroupingFunc) ||
		IsA(node, WindowFunc) || IsA(node, SubLink) || IsA(node, Query))
		return true;			/* unmaintainable — stop and report */
	return expression_tree_walker(node, incr_agg_arg_unsafe_walker, ctx);
}

/*
 * incr_agg_arg_deparse_safe — true if expr is a deterministic, deparse-able
 * aggregate argument: no nested aggregate / window / subquery, and IMMUTABLE
 * (a stable/volatile function would return different values for the same row
 * across transactions, so its insert-delta and a later delete-delta would not
 * cancel — the running total would drift).  This is the bar for the expression
 * shapes that only the deparse core can express (CASE, COALESCE, function
 * calls); the hand grammar (incr_validate_expr) remains accepted as-is.
 */
static bool
incr_agg_arg_deparse_safe(Node *expr)
{
	if (expr == NULL)
		return true;
	if (incr_agg_arg_unsafe_walker(expr, NULL))
		return false;
	if (contain_mutable_functions(expr))
		return false;
	return true;
}

/*
 * incr_aggs_need_deparse — true if any aggregate argument in this aggregate
 * query (single-table or INNER JOIN) is outside the hand builders' grammar (so
 * the delta SQL must be produced by the deparse core).  Used to AUTO-ROUTE such
 * shapes to deparse regardless of the GUC, which keeps them restorable: the
 * restore path re-runs setup and routes the same way.
 */
/*
 * incr_inner_join_deparse_shape — true for a multi-table query whose per-table
 * deltas the deparse core can build: a pure INNER JOIN (no outer join, no
 * self-join).  Outer joins use the recompute strategy and self-joins the
 * combined-role builders, neither of which the deparse path expresses yet.
 * Mirrors the routing in MatviewIncrSetup's N-table INNER JOIN branch.
 */
static bool
incr_inner_join_deparse_shape(Query *viewQuery, int nbasetables)
{
	List	   *tabs;

	if (nbasetables < 2)
		return false;
	tabs = incr_collect_tables(viewQuery);
	return !incr_has_outer_join(tabs) && !incr_has_self_join(tabs);
}

/*
 * incr_recompute_outer_shape — true for a query the Phase 8 outer-join
 * recompute builder (incr_build_recompute_sql) can maintain a DISTINCT / stddev /
 * variance / bool aggregate over.
 *
 * The builder uses dbblue_deparse_query to render both _affected_ (delta group
 * keys via the transition-table ENR) and _new_agg_ (live recompute from the
 * real join), so it is correct for any number of tables and any mix of INNER
 * and LEFT/RIGHT outer joins.
 *
 * Group keys from optional (LEFT/RIGHT-joined) tables are now supported when
 * the optional table is DIRECTLY connected to the preserved anchor (its ON
 * condition references preserved_varno on one side).  For those shapes
 * incr_build_recompute_sql adds a second UNION arm to _affected_ that captures
 * preserved rows whose join status changed (newly-orphaned or newly-matched),
 * covering the NULL group that arm 1 cannot see.
 *
 * FULL OUTER JOIN + GROUP BY is gated separately (incr_full_join_single_side_keys
 * at CREATE time): the two-table, single-side plain-column shape is supported and
 * incr_build_recompute_sql adds a dedicated all-NULL arm for key-side deltas; every
 * other FULL-join shape is rejected before this function is reached.
 *
 * Still rejected:
 *   • Optional-side group key with an INDIRECT join to preserved (multi-hop
 *     chain): arm 2 cannot be built without joining the intermediate table.
 *   • Outer-join + self-join (dedicated path).
 */
static bool
incr_recompute_outer_shape(Query *viewQuery, int nbasetables)
{
	List	   *tabs;
	int			preserved_varno;
	ListCell   *lc;

	tabs = incr_collect_tables(viewQuery);
	if (!incr_has_outer_join(tabs) || incr_has_self_join(tabs))
		return false;

	/*
	 * Check each GROUP BY key from a truly optional (non-preserved
	 * LEFT/RIGHT-joined) table.  Such keys are allowed only when the optional
	 * table is DIRECTLY connected to the preserved anchor — i.e. the ON
	 * condition of the optional table's join step references preserved_varno
	 * on the other side.  Multi-hop connections (e.g. p INNER d LEFT c with
	 * GROUP BY c.k) are rejected because arm 2 would need to traverse the
	 * intermediate table chain.
	 *
	 * Note: the preserved table in a RIGHT JOIN has join_type == JOIN_RIGHT,
	 * so we must exclude it from the "optional" test.
	 */
	preserved_varno = incr_outer_preserved_varno(tabs);
	foreach(lc, viewQuery->groupClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
		TargetEntry	   *te  = get_sortgroupclause_tle(sgc, viewQuery->targetList);
		int				rv;
		ListCell	   *jlc;

		if (!IsA(te->expr, Var))
			continue;			/* expression key — deparse handles it */

		incr_resolve_var_colname((Var *) te->expr, viewQuery->rtable, &rv);

		/* Check if this key is from a truly optional-side table */
		foreach(jlc, tabs)
		{
			IncrJoinEntry *je = lfirst(jlc);

			if (je->varno != rv)
				continue;
			if (je->varno == preserved_varno)
				break;			/* preserved anchor: always safe */
			if (je->join_type != JOIN_LEFT && je->join_type != JOIN_RIGHT)
				break;			/* INNER-joined: always safe */

			/*
			 * Optional-side group key.  Allowed only when the optional table
			 * is directly connected to the preserved anchor.  Detected by
			 * checking that one side of the ON condition is preserved_varno.
			 */
			if (incr_qual_get_other_varno(je->quals, je->varno) != preserved_varno)
				return false;	/* indirect connection — cannot build arm 2 */
			break;				/* direct connection — arm 2 handles it */
		}
	}
	return true;
}

/*
 * incr_try_resolve_var_to_rel — like incr_resolve_var_colname, but returns
 * false instead of elog'ing when the Var resolves through a JOIN or GROUP alias
 * that is not itself a plain Var.  A FULL JOIN USING/NATURAL column, for
 * example, is a COALESCE(a.k, b.k) in joinaliasvars — COALESCE-like and not
 * supported by the single-side FULL-join strategy — so callers gating shapes at
 * CREATE time can reject cleanly rather than raise an internal error.
 */
static bool
incr_try_resolve_var_to_rel(Var *v, List *rtable, int *varno_out)
{
	for (;;)
	{
		RangeTblEntry *rte = rt_fetch(v->varno, rtable);
		Node		  *sub;

		if (rte->rtekind == RTE_RELATION)
		{
			*varno_out = v->varno;
			return true;
		}
		if (rte->rtekind == RTE_JOIN)
		{
			if (v->varattno < 1 ||
				v->varattno > list_length(rte->joinaliasvars))
				return false;
			sub = list_nth(rte->joinaliasvars, v->varattno - 1);
		}
		else if (rte->rtekind == RTE_GROUP)
		{
			if (v->varattno < 1 ||
				v->varattno > list_length(rte->groupexprs))
				return false;
			sub = list_nth(rte->groupexprs, v->varattno - 1);
		}
		else
			return false;
		if (!IsA(sub, Var))
			return false;
		v = (Var *) sub;
	}
}

/*
 * incr_eqjoin_matches_vars — true if qual is (or AND-contains) an equality
 * OpExpr whose two Var operands are exactly v1 and v2 (in either order, matched
 * by varno + varattno).  Used to confirm a COALESCE's two arguments are the two
 * sides of the join's equi-key.
 */
static bool
incr_eqjoin_matches_vars(Node *qual, Var *v1, Var *v2)
{
	if (qual == NULL)
		return false;
	if (IsA(qual, BoolExpr))
	{
		BoolExpr   *b = (BoolExpr *) qual;
		ListCell   *l;

		if (b->boolop != AND_EXPR)
			return false;
		foreach(l, b->args)
			if (incr_eqjoin_matches_vars((Node *) lfirst(l), v1, v2))
				return true;
		return false;
	}
	if (IsA(qual, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) qual;
		Node	   *l,
				   *r;
		Var		   *lv,
				   *rv;
		char	   *opname;

		if (list_length(op->args) != 2)
			return false;
		l = linitial(op->args);
		r = lsecond(op->args);
		if (!IsA(l, Var) || !IsA(r, Var))
			return false;
		opname = get_opname(op->opno);
		if (opname == NULL || strcmp(opname, "=") != 0)
			return false;			/* only equi-joins give v1 = v2 on a match */
		lv = (Var *) l;
		rv = (Var *) r;
		return ((lv->varno == v1->varno && lv->varattno == v1->varattno &&
				 rv->varno == v2->varno && rv->varattno == v2->varattno) ||
				(lv->varno == v2->varno && lv->varattno == v2->varattno &&
				 rv->varno == v1->varno && rv->varattno == v1->varattno));
	}
	return false;
}

/*
 * incr_is_coalesce_of_join_keys — true if gexpr is COALESCE(x, y) where {x, y}
 * are exactly the two sides of an equi-join condition in the query (the FULL-
 * join key-merge idiom, e.g. GROUP BY COALESCE(a.k, b.k) over a FULL JOIN ON
 * a.k = b.k).  Such a key is INVARIANT under an orphan flip: on a matched row
 * x = y so COALESCE is the shared value, and on either orphan the surviving
 * side still holds that value.  So the row never moves between groups and never
 * enters an all-NULL group — the plain recompute arms (no NULL arm) are correct.
 */
static bool
incr_is_coalesce_of_join_keys(Node *gexpr, Query *viewQuery)
{
	CoalesceExpr *c;
	Var		   *v1,
			   *v2;
	List	   *tabs;
	ListCell   *lc;

	if (gexpr == NULL || !IsA(gexpr, CoalesceExpr))
		return false;
	c = (CoalesceExpr *) gexpr;
	if (list_length(c->args) != 2 ||
		!IsA(linitial(c->args), Var) || !IsA(lsecond(c->args), Var))
		return false;
	v1 = (Var *) linitial(c->args);
	v2 = (Var *) lsecond(c->args);

	tabs = incr_collect_tables(viewQuery);
	foreach(lc, tabs)
	{
		IncrJoinEntry *je = lfirst(lc);

		if (incr_eqjoin_matches_vars(je->quals, v1, v2))
			return true;
	}
	return false;
}

/*
 * incr_full_join_coalesce_keys — true iff every GROUP BY key is a
 * COALESCE-of-join-keys expression (see incr_is_coalesce_of_join_keys).  The
 * secondary supported FULL-join shape: no NULL group, no relocation, so arm 1
 * of the recompute is sufficient.
 */
static bool
incr_full_join_coalesce_keys(Query *viewQuery)
{
	ListCell   *lc;

	if (viewQuery->groupClause == NIL)
		return false;
	foreach(lc, viewQuery->groupClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
		TargetEntry	   *te  = get_sortgroupclause_tle(sgc, viewQuery->targetList);

		if (!incr_is_coalesce_of_join_keys(incr_group_key_expr(viewQuery, te),
										   viewQuery))
			return false;
	}
	return true;
}

/*
 * incr_full_join_single_side_keys — true iff every GROUP BY key of viewQuery
 * is a plain column (Var) resolving to the SAME base relation.
 *
 * Gate for FULL OUTER JOIN + GROUP BY.  With a single-side plain-column key
 * the recompute strategy is provably correct: an orphan flip on the delta side
 * keeps a row's key value within its group (for keys on the surviving side) or
 * moves it to/from the all-NULL group (for keys on the flipping side), and the
 * dedicated NULL arm in incr_build_recompute_sql covers exactly that all-NULL
 * group.  Expression keys (e.g. COALESCE(a.k, b.k)) and mixed-side keys can
 * relocate a row BETWEEN non-NULL groups on a flip, which the recompute arms do
 * not track, so they are rejected.
 */
static bool
incr_full_join_single_side_keys(Query *viewQuery)
{
	ListCell   *lc;
	int			key_varno = -1;

	if (viewQuery->groupClause == NIL)
		return false;

	foreach(lc, viewQuery->groupClause)
	{
		SortGroupClause *sgc   = lfirst_node(SortGroupClause, lc);
		TargetEntry	    *te    = get_sortgroupclause_tle(sgc,
														 viewQuery->targetList);
		Node			*gexpr = incr_group_key_expr(viewQuery, te);
		int				 rv;

		if (gexpr == NULL || !IsA(gexpr, Var))
			return false;			/* expression key — not supported */
		/*
		 * Resolve without elog: a FULL JOIN USING/NATURAL merged column is a
		 * COALESCE in joinaliasvars, which is COALESCE-like and unsupported —
		 * reject it cleanly here rather than crash in incr_resolve_var_colname.
		 */
		if (!incr_try_resolve_var_to_rel((Var *) gexpr, viewQuery->rtable, &rv))
			return false;
		if (key_varno == -1)
			key_varno = rv;
		else if (rv != key_varno)
			return false;			/* mixed-side keys — not supported */
	}
	return true;
}

static bool
incr_aggs_need_deparse(Query *viewQuery)
{
	ListCell   *lc;

	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		Aggref	   *agg;
		TargetEntry *arg_te;

		if (te->resjunk || !IsA(te->expr, Aggref))
			continue;
		agg = (Aggref *) te->expr;
		if (agg->args == NIL)			/* COUNT(*) */
			continue;
		arg_te = linitial_node(TargetEntry, agg->args);
		if (!incr_validate_expr((Node *) arg_te->expr, NULL, false))
			return true;				/* hand grammar can't render it */
	}
	return false;
}

/*
 * incr_group_key_expr — the underlying grouping expression for a group target
 * entry, resolving the PG17+ RTE_GROUP indirection.  Since the grouping-set
 * refactor, a reference to a grouping key in the target list is a Var pointing
 * at the query's RTE_GROUP, whose groupexprs hold the real expressions (a plain
 * column is a Var there too; an expression like date_trunc(...) is the FuncExpr).
 * Returns the resolved expression, or te->expr unchanged if it is not such a
 * grouping Var.
 */
static Node *
incr_group_key_expr(Query *q, TargetEntry *te)
{
	if (te != NULL && IsA(te->expr, Var))
	{
		Var			   *v = (Var *) te->expr;
		RangeTblEntry  *grte;

		if (v->varno >= 1 && v->varno <= list_length(q->rtable))
		{
			grte = rt_fetch(v->varno, q->rtable);
			if (grte->rtekind == RTE_GROUP &&
				v->varattno >= 1 &&
				v->varattno <= list_length(grte->groupexprs))
				return (Node *) list_nth(grte->groupexprs, v->varattno - 1);
		}
	}
	return te ? (Node *) te->expr : NULL;
}

/*
 * incr_group_needs_deparse — true if any GROUP BY key is a non-Var expression
 * (e.g. GROUP BY date_trunc('month', d)).  Such shapes can only be produced by
 * the deparse core (the hand builders resolve grouping keys to bare column
 * names), so they must AUTO-ROUTE to deparse regardless of the GUC — which also
 * keeps them restorable, since the restore path re-runs setup and routes the
 * same way.  Eligibility has already confirmed the expression is safe.
 */
static bool
incr_group_needs_deparse(Query *viewQuery)
{
	ListCell   *lc;

	foreach(lc, viewQuery->groupClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
		TargetEntry	   *te = get_sortgroupclause_tle(sgc, viewQuery->targetList);
		Node		   *gexpr = incr_group_key_expr(viewQuery, te);

		if (gexpr != NULL && !IsA(gexpr, Var))
			return true;
	}
	return false;
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
/*
 * incr_having_agg_column — the stored matview column that holds the value of a
 * HAVING aggregate.
 *
 * count(*) is always maintained as __mv_count__.  Any other aggregate must be
 * present in the SELECT list and is bound to that output column.  The match is
 * ARGUMENT-AWARE (full structural equality via equal()), not function-OID only:
 * SUM(a) and SUM(b) — or SUM(CASE ...) with different branches — are distinct
 * aggregates and must bind to their own columns, never to the first sum found.
 *
 * Returns the column name, or NULL if the HAVING aggregate is not a stored
 * column (HAVING references an aggregate absent from the SELECT list).  Used by
 * both eligibility (incr_validate_expr) and the recompute SQL builder
 * (incr_deparse_having_cond) so the two never disagree.
 */
static const char *
incr_having_agg_column(Aggref *hagg, Query *viewQuery)
{
	ListCell   *lc;

	if (hagg->aggstar && strcmp(get_func_name(hagg->aggfnoid), "count") == 0)
		return MATVIEW_INCR_COUNT_COL;

	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (IsA(te->expr, Aggref) && equal(te->expr, hagg))
			return te->resname;
	}
	return NULL;
}

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

		if (!allow_aggref)
			return false;

		/* HAVING aggregate must resolve to a stored matview column */
		return incr_having_agg_column(agg, viewQuery) != NULL;
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

	/* Searched CASE: every WHEN condition / result and the ELSE must validate.
	 * (Simple "CASE x WHEN v" — arg != NULL — uses CaseTestExpr internally and is
	 * not handled; rewrite as a searched CASE.) */
	if (IsA(expr, CaseExpr))
	{
		CaseExpr   *c = (CaseExpr *) expr;
		ListCell   *cl;

		if (c->arg != NULL)
			return false;
		foreach(cl, c->args)
		{
			CaseWhen *w = lfirst_node(CaseWhen, cl);

			if (!incr_validate_expr((Node *) w->expr, viewQuery, allow_aggref) ||
				!incr_validate_expr((Node *) w->result, viewQuery, allow_aggref))
				return false;
		}
		return incr_validate_expr((Node *) c->defresult, viewQuery, allow_aggref);
	}

	/* COALESCE(a, b, …): every argument must validate. */
	if (IsA(expr, CoalesceExpr))
	{
		ListCell *cl;

		foreach(cl, ((CoalesceExpr *) expr)->args)
			if (!incr_validate_expr(lfirst(cl), viewQuery, allow_aggref))
				return false;
		return true;
	}

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

		/*
		 * A single-arg FuncExpr is a cast ONLY when funcformat says so; a
		 * genuine single-argument function (floor(amt), abs(amt), ...) must be
		 * rendered as a call.  Treating every single-arg FuncExpr as a cast
		 * silently dropped the function and corrupted the running total.
		 */
		if (list_length(fe->args) == 1 &&
			(fe->funcformat == COERCE_IMPLICIT_CAST ||
			 fe->funcformat == COERCE_EXPLICIT_CAST))
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

	/* Searched CASE: CASE WHEN <cond> THEN <result> [ … ] [ELSE <def>] END */
	if (IsA(qual, CaseExpr))
	{
		CaseExpr   *c = (CaseExpr *) qual;
		ListCell   *cl;

		appendStringInfoString(buf, "CASE");
		foreach(cl, c->args)
		{
			CaseWhen *w = lfirst_node(CaseWhen, cl);

			appendStringInfoString(buf, " WHEN ");
			incr_deparse_where_qual((Node *) w->expr, rtable, delta_varno, buf);
			appendStringInfoString(buf, " THEN ");
			incr_deparse_where_qual((Node *) w->result, rtable, delta_varno, buf);
		}
		/* defresult is a NULL Const for an implicit ELSE; render it either way */
		appendStringInfoString(buf, " ELSE ");
		incr_deparse_where_qual((Node *) c->defresult, rtable, delta_varno, buf);
		appendStringInfoString(buf, " END");
		return;
	}

	/* COALESCE(a, b, …) */
	if (IsA(qual, CoalesceExpr))
	{
		CoalesceExpr *ce = (CoalesceExpr *) qual;
		ListCell	 *cl;
		bool		  first = true;

		appendStringInfoString(buf, "COALESCE(");
		foreach(cl, ce->args)
		{
			if (!first)
				appendStringInfoChar(buf, ',');
			first = false;
			incr_deparse_where_qual(lfirst(cl), rtable, delta_varno, buf);
		}
		appendStringInfoChar(buf, ')');
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
		const char *col = incr_having_agg_column(hagg, viewQuery);

		/* argument-aware bind (eligibility guarantees a match exists) */
		if (col == NULL)
			elog(ERROR, "incr_deparse_having_cond: aggregate %s not found in SELECT list",
				 get_func_name(hagg->aggfnoid));
		appendStringInfoString(buf, quote_identifier(col));
		return;
	}
	else if (IsA(expr, Var))
	{
		/*
		 * A grouping key referenced in HAVING.  Since PG17 it is a Var pointing
		 * at the query's RTE_GROUP, and the matching SELECT target entry is the
		 * same Var (same varno/varattno); its output column is where the key's
		 * value lives in the matview.  Bind to that output column by matching the
		 * RTE_GROUP slot — this works whether the key is a plain column or an
		 * expression (e.g. date_trunc('month', d), (x % n)), whose RTE_GROUP
		 * groupexprs entry is a non-Var that has no base-column name to resolve
		 * to.  Matching by slot also handles aliasing (GROUP BY cat → SELECT cat
		 * AS k binds to "k").
		 */
		Var		   *v = (Var *) expr;
		ListCell   *lc;
		RangeTblEntry *vrte = (v->varno >= 1 &&
							   v->varno <= list_length(viewQuery->rtable))
			? rt_fetch(v->varno, viewQuery->rtable) : NULL;

		if (vrte != NULL && vrte->rtekind == RTE_GROUP)
		{
			foreach(lc, viewQuery->targetList)
			{
				TargetEntry *te = lfirst_node(TargetEntry, lc);
				Var		   *tv;

				if (te->resjunk || !IsA(te->expr, Var))
					continue;
				tv = (Var *) te->expr;
				if (tv->varno == v->varno && tv->varattno == v->varattno)
				{
					appendStringInfoString(buf, quote_identifier(te->resname));
					return;
				}
			}
		}

		/* Fallback: a non-grouping Var resolves to its base column name. */
		appendStringInfoString(buf,
			quote_identifier(incr_resolve_var_colname(v, viewQuery->rtable,
													  NULL)));
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
		FuncExpr   *fe = (FuncExpr *) expr;

		/*
		 * A single-arg FuncExpr is a cast ONLY when funcformat says so; a genuine
		 * single-argument function (abs(x), floor(x), ...) and any multi-argument
		 * function (power(x,2), ...) must render as a call.  (Matches
		 * incr_deparse_where_qual; treating every 1-arg FuncExpr as a cast both
		 * dropped real functions and errored on multi-arg ones.)
		 */
		if (list_length(fe->args) == 1 &&
			(fe->funcformat == COERCE_IMPLICIT_CAST ||
			 fe->funcformat == COERCE_EXPLICIT_CAST))
		{
			appendStringInfoChar(buf, '(');
			incr_deparse_having_cond(linitial(fe->args), viewQuery, buf);
			appendStringInfo(buf, ")::%s", format_type_be(fe->funcresulttype));
		}
		else
		{
			char	   *fname = get_func_name(fe->funcid);
			ListCell   *lc;
			bool		first = true;

			appendStringInfo(buf, "%s(", fname);
			foreach(lc, fe->args)
			{
				if (!first)
					appendStringInfoChar(buf, ',');
				incr_deparse_having_cond(lfirst(lc), viewQuery, buf);
				first = false;
			}
			appendStringInfoChar(buf, ')');
		}
	}
	else if (IsA(expr, NullTest))
	{
		/* HAVING <expr> IS [NOT] NULL — the inner expr renders to a matview
		 * column (group key / aggregate output) or constant. */
		NullTest   *nt = (NullTest *) expr;

		appendStringInfoChar(buf, '(');
		incr_deparse_having_cond((Node *) nt->arg, viewQuery, buf);
		appendStringInfoString(buf,
			nt->nulltesttype == IS_NULL ? " IS NULL)" : " IS NOT NULL)");
	}
	else if (IsA(expr, RelabelType))
	{
		/* No-op type coercion (e.g. varchar -> text) — render arg::resulttype. */
		RelabelType *rt = (RelabelType *) expr;

		appendStringInfoChar(buf, '(');
		incr_deparse_having_cond((Node *) rt->arg, viewQuery, buf);
		appendStringInfo(buf, ")::%s", format_type_be(rt->resulttype));
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

	/*
	 * Tie the hidden base matview's lifetime to the user-facing view by
	 * recording an INTERNAL dependency from the base to the view.  This makes
	 * the two objects live and die together:
	 *   - DROP VIEW <name>  drops the base too (and, via doDeletion, its
	 *     incremental triggers and catalog rows) — no orphaned _dbblue_*_base.
	 *   - DROP MATERIALIZED VIEW _dbblue_*_base is redirected with a hint to
	 *     drop the user-facing view instead.
	 * The view already carries a normal dependency on the base (its query
	 * reads from it); the two are walked in different drop scenarios, so the
	 * pair is resolved cleanly in both directions.
	 */
	CommandCounterIncrement();
	{
		Oid			viewoid = get_relname_relid(origname,
												get_rel_namespace(mvrelid));

		if (OidIsValid(viewoid))
		{
			ObjectAddress baseaddr,
						  viewaddr;

			ObjectAddressSet(baseaddr, RelationRelationId, mvrelid);
			ObjectAddressSet(viewaddr, RelationRelationId, viewoid);
			recordDependencyOn(&baseaddr, &viewaddr, DEPENDENCY_INTERNAL);
		}
	}
}

/*
 * incr_link_having_base_to_view
 *
 * Record the INTERNAL dependency that ties a HAVING base matview's lifetime to
 * its user-facing filtering view (see incr_create_having_view).  Used on the
 * pg_dump/restore path, where the view is restored as its own object and
 * incr_create_having_view is not run, so the dependency would otherwise be
 * missing and dropping the view would orphan the base.
 *
 * Finds the view by its rewrite rule's dependency on the base, and is a no-op
 * if the link already exists (so it is safe to call on every REFRESH).
 */
static void
incr_link_having_base_to_view(Oid base)
{
	Oid			viewoid = InvalidOid;
	char	   *sql;
	int			ret;

	sql = psprintf(
		"SELECT r.ev_class FROM pg_catalog.pg_rewrite r "
		"JOIN pg_catalog.pg_depend d ON d.classid='pg_catalog.pg_rewrite'::pg_catalog.regclass "
		"  AND d.objid = r.oid "
		"WHERE d.refclassid='pg_catalog.pg_class'::pg_catalog.regclass "
		"  AND d.refobjid=%u AND r.ev_class<>%u "
		"  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend e "
		"     WHERE e.classid='pg_catalog.pg_class'::pg_catalog.regclass AND e.objid=%u "
		"       AND e.refclassid='pg_catalog.pg_class'::pg_catalog.regclass "
		"       AND e.refobjid=r.ev_class AND e.deptype='i') "
		"LIMIT 1",
		base, base, base);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "incr_link_having_base_to_view: SPI_connect failed");
	ret = SPI_execute(sql, true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool	isnull;
		Datum	d = SPI_getbinval(SPI_tuptable->vals[0],
								  SPI_tuptable->tupdesc, 1, &isnull);

		if (!isnull)
			viewoid = DatumGetObjectId(d);
	}
	SPI_finish();

	if (OidIsValid(viewoid))
	{
		ObjectAddress baseaddr,
					  viewaddr;

		ObjectAddressSet(baseaddr, RelationRelationId, base);
		ObjectAddressSet(viewaddr, RelationRelationId, viewoid);
		recordDependencyOn(&baseaddr, &viewaddr, DEPENDENCY_INTERNAL);
		CommandCounterIncrement();
	}
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
	StringInfoData	buf,
					part,
					sel,
					grp,
					joincond;
	ListCell	   *lc;
	const char	   *mvname = mv_qname(mvrelid);
	Node		   *wq = incr_get_where_qual(viewQuery);
	bool			first;

	initStringInfo(&buf);
	initStringInfo(&part);		/* PARTITION BY _m.col, ...           */
	initStringInfo(&sel);		/* delta SELECT: <expr> AS col, ...   */
	initStringInfo(&grp);		/* delta GROUP BY: <expr>, ...        */
	initStringInfo(&joincond);	/* _m.col IS NOT DISTINCT FROM _rd.col */

	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry    *te = lfirst_node(TargetEntry, lc);
		const char	   *cq;
		StringInfoData	ebuf;

		if (te->resjunk)
			continue;
		cq = quote_identifier(te->resname);
		initStringInfo(&ebuf);
		incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
								delta_varno, &ebuf);
		if (!first)
		{
			appendStringInfoChar(&part, ',');
			appendStringInfoChar(&sel, ',');
			appendStringInfoChar(&grp, ',');
			appendStringInfoString(&joincond, " AND ");
		}
		appendStringInfo(&part, "_m.%s", cq);
		appendStringInfo(&sel, "%s AS %s", ebuf.data, cq);
		appendStringInfoString(&grp, ebuf.data);
		appendStringInfo(&joincond, "_m.%s IS NOT DISTINCT FROM _rd.%s", cq, cq);
		first = false;
	}

	/*
	 * A row-level matview keeps duplicate output rows, so a DELETE must remove
	 * exactly the deleted MULTIPLICITY of each tuple, not every value-identical
	 * copy.  Aggregate the delta into one row per distinct output tuple with its
	 * count _k, number the matview's copies of that tuple, and drop the first _k.
	 */
	appendStringInfo(&buf,
					 "DELETE FROM %s WHERE ctid IN ("
					 "SELECT s.ctid FROM ("
					 "SELECT _m.ctid, row_number() OVER (PARTITION BY %s) AS _rn, _rd._k "
					 "FROM %s _m JOIN (SELECT %s, count(*) AS _k",
					 mvname, part.data, mvname, sel.data);

	incr_append_from_join(&buf, viewQuery, delta_varno, delta_table, join_list);
	if (wq != NULL)
	{
		StringInfoData wbuf;

		initStringInfo(&wbuf);
		incr_deparse_where_qual(wq, viewQuery->rtable, delta_varno, &wbuf);
		appendStringInfo(&buf, " WHERE %s", wbuf.data);
	}
	appendStringInfo(&buf,
					 " GROUP BY %s) _rd ON (%s)) s WHERE s._rn <= s._k)",
					 grp.data, joincond.data);

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
 * incr_nullsafe_accum — build a NULL-safe running-total expression.
 *
 * SUM over an all-NULL delta group yields NULL, and "running + NULL" (or
 * "running - NULL") would corrupt the stored total to NULL.  Emit instead:
 *   add:  (CASE WHEN <delta> IS NULL THEN <run>
 *               WHEN <run> IS NULL THEN <delta> ELSE <run>+<delta> END)
 *   sub:  (CASE WHEN <delta> IS NULL THEN <run> ELSE <run>-<delta> END)
 * This is type-agnostic (no literal zero, so SUM(interval) etc. are safe).
 * COUNT deltas are never NULL, so wrapping them is a harmless no-op.
 *
 * (A group whose every contributing value is NULL will show 0 rather than the
 * SQL-exact NULL after the last non-NULL value is removed — an accepted
 * residual that would require a per-column non-NULL counter to close.)
 */
static char *
incr_nullsafe_accum(const char *run, const char *delta, bool subtract)
{
	if (subtract)
		return psprintf("(CASE WHEN %s IS NULL THEN %s ELSE %s-%s END)",
						delta, run, run, delta);
	return psprintf("(CASE WHEN %s IS NULL THEN %s WHEN %s IS NULL THEN %s "
					"ELSE %s+%s END)",
					delta, run, run, delta, run, delta);
}

/* ============================================================
 * Phase 2 deparse core — plain single-table aggregate
 *
 * The INS/DEL "shells" (the INSERT head + ON CONFLICT accumulation tail, and
 * the DEL UPDATE...FROM d tail) are factored into the helpers below so that
 * BOTH the hand-written builders and the deparse-based builders share the exact
 * same merge logic.  Only the delta SELECT body differs: the hand builders
 * deparse each target expression themselves, while the deparse builders let
 * ruleutils render the whole SELECT from the view Query with the source table
 * swapped for its transition-table ENR.  Gated by dbblue_ivm_deparse_delta.
 * ============================================================
 */

/*
 * incr_sumcnt_sibling — quoted name of the __mv_sumcnt_<resname> hidden column
 * if the target list carries one (i.e. <resname> is a SUM column with the
 * all-NULL→NULL non-null counter), else NULL.
 */
static const char *
incr_sumcnt_sibling(Query *viewQuery, const char *resname)
{
	char	   *want = psprintf("%s%s", MATVIEW_INCR_SUMCNT_PREFIX, resname);
	ListCell   *lc;

	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (!te->resjunk && te->resname != NULL &&
			strcmp(te->resname, want) == 0)
			return quote_identifier(want);
	}
	return NULL;
}

/*
 * incr_emit_ins_head — "INSERT INTO mv (col1,col2,...) "
 * Column list is the view's non-junk target list, in order.
 */
static void
incr_emit_ins_head(StringInfo buf, Oid mvrelid, Query *viewQuery)
{
	ListCell   *lc;
	bool		first = true;

	appendStringInfo(buf, "INSERT INTO %s (", mv_qname(mvrelid));
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk)
			continue;
		if (!first)
			appendStringInfoChar(buf, ',');
		appendStringInfoString(buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(buf, ") ");
}

/*
 * incr_emit_ins_conflict_tail — " ON CONFLICT (group_cols) DO UPDATE SET ..."
 * Accumulates each delta into the stored running totals.  AVG is recomputed
 * from its hidden sum/cnt pair; MIN/MAX use LEAST/GREATEST; everything else
 * uses the NULL-safe additive accumulator.
 */
static void
incr_emit_ins_conflict_tail(StringInfo buf, Oid mvrelid, Query *viewQuery)
{
	const char *mvname = mv_qname(mvrelid);
	List	   *groupColNames = NIL;
	ListCell   *lc,
			   *gcl;
	bool		first;

	incr_collect_group_cols(viewQuery, &groupColNames);

	appendStringInfoString(buf, " ON CONFLICT (");
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first)
			appendStringInfoChar(buf, ',');
		appendStringInfoString(buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}
	appendStringInfoString(buf, ") DO UPDATE SET ");

	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		const char  *colq;

		if (te->resjunk || IsA(te->expr, Var))
			continue;
		if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			continue;
		/* hidden avgsum/avgcnt/sumcnt emitted as part of their parent column */
		if (strncmp(te->resname, MATVIEW_INCR_AVGSUM_PREFIX,
					strlen(MATVIEW_INCR_AVGSUM_PREFIX)) == 0 ||
			strncmp(te->resname, MATVIEW_INCR_AVGCNT_PREFIX,
					strlen(MATVIEW_INCR_AVGCNT_PREFIX)) == 0 ||
			strncmp(te->resname, MATVIEW_INCR_SUMCNT_PREFIX,
					strlen(MATVIEW_INCR_SUMCNT_PREFIX)) == 0)
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

			char	   *sum_expr = incr_nullsafe_accum(
				psprintf("%s.%s", mvname, sum_q),
				psprintf("EXCLUDED.%s", sum_q), false);

			if (!first)
				appendStringInfoChar(buf, ',');
			appendStringInfo(buf,
							 "%s=%s"
							 ",%s=%s.%s+EXCLUDED.%s"
							 ",%s=(%s::%s/NULLIF(%s.%s+EXCLUDED.%s,0))",
							 sum_q, sum_expr,
							 cnt_q, mvname, cnt_q, cnt_q,
							 colq, sum_expr, type_name,
							 mvname, cnt_q, cnt_q);
			first = false;
		}
		else if (IsA(te->expr, Aggref))
		{
			/* MIN/MAX: replace if better; everything else: accumulate */
			char	   *fn = get_func_name(((Aggref *) te->expr)->aggfnoid);
			const char *scq = (strcmp(fn, "sum") == 0)
				? incr_sumcnt_sibling(viewQuery, te->resname) : NULL;

			if (!first)
				appendStringInfoChar(buf, ',');
			if (strcmp(fn, "min") == 0)
				appendStringInfo(buf, "%s=LEAST(%s.%s,EXCLUDED.%s)",
								 colq, mvname, colq, colq);
			else if (strcmp(fn, "max") == 0)
				appendStringInfo(buf, "%s=GREATEST(%s.%s,EXCLUDED.%s)",
								 colq, mvname, colq, colq);
			else if (scq != NULL)
				/* SUM with non-null counter: maintain the counter and show
				 * SQL-exact NULL when no non-NULL inputs remain. */
				appendStringInfo(buf,
								 "%s=%s.%s+EXCLUDED.%s"
								 ",%s=CASE WHEN %s.%s+EXCLUDED.%s=0 THEN NULL ELSE %s END",
								 scq, mvname, scq, scq,
								 colq, mvname, scq, scq,
								 incr_nullsafe_accum(psprintf("%s.%s", mvname, colq),
													 psprintf("EXCLUDED.%s", colq), false));
			else
				appendStringInfo(buf, "%s=%s", colq,
								 incr_nullsafe_accum(psprintf("%s.%s", mvname, colq),
													 psprintf("EXCLUDED.%s", colq), false));
			first = false;
		}
		else
		{
			if (!first)
				appendStringInfoChar(buf, ',');
			appendStringInfo(buf, "%s=%s", colq,
							 incr_nullsafe_accum(psprintf("%s.%s", mvname, colq),
												 psprintf("EXCLUDED.%s", colq), false));
			first = false;
		}
	}
}

/*
 * incr_emit_del_update_tail — "UPDATE mv SET ... FROM d WHERE mv.g=d.g AND ..."
 * Subtracts the delta CTE "d" from the stored running totals (mirror of the
 * INSERT conflict tail).  The CTE "d" must expose every column referenced here
 * under its output name (it does: both the hand and deparse SELECTs alias to
 * the view's output column names).
 */
static void
incr_emit_del_update_tail(StringInfo buf, Oid mvrelid, Query *viewQuery)
{
	const char *mvname = mv_qname(mvrelid);
	List	   *groupColNames = NIL;
	ListCell   *lc,
			   *gcl;
	bool		first;

	incr_collect_group_cols(viewQuery, &groupColNames);

	appendStringInfo(buf, "UPDATE %s SET ", mvname);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		const char  *colq;

		if (te->resjunk || IsA(te->expr, Var))
			continue;
		if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			continue;
		/* hidden avgsum/avgcnt/sumcnt emitted as part of their parent column */
		if (strncmp(te->resname, MATVIEW_INCR_AVGSUM_PREFIX,
					strlen(MATVIEW_INCR_AVGSUM_PREFIX)) == 0 ||
			strncmp(te->resname, MATVIEW_INCR_AVGCNT_PREFIX,
					strlen(MATVIEW_INCR_AVGCNT_PREFIX)) == 0 ||
			strncmp(te->resname, MATVIEW_INCR_SUMCNT_PREFIX,
					strlen(MATVIEW_INCR_SUMCNT_PREFIX)) == 0)
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

			char	   *sum_expr = incr_nullsafe_accum(
				psprintf("%s.%s", mvname, sum_q),
				psprintf("d.%s", sum_q), true);

			if (!first)
				appendStringInfoChar(buf, ',');
			appendStringInfo(buf,
							 "%s=%s"
							 ",%s=%s.%s-d.%s"
							 ",%s=(%s::%s/NULLIF(%s.%s-d.%s,0))",
							 sum_q, sum_expr,
							 cnt_q, mvname, cnt_q, cnt_q,
							 colq, sum_expr, type_name,
							 mvname, cnt_q, cnt_q);
			first = false;
		}
		else
		{
			const char *scq = (IsA(te->expr, Aggref) &&
							   strcmp(get_func_name(((Aggref *) te->expr)->aggfnoid), "sum") == 0)
				? incr_sumcnt_sibling(viewQuery, te->resname) : NULL;

			if (!first)
				appendStringInfoChar(buf, ',');
			if (scq != NULL)
				/* SUM with non-null counter: subtract from the counter and show
				 * SQL-exact NULL when no non-NULL inputs remain. */
				appendStringInfo(buf,
								 "%s=%s.%s-d.%s"
								 ",%s=CASE WHEN %s.%s-d.%s=0 THEN NULL ELSE %s END",
								 scq, mvname, scq, scq,
								 colq, mvname, scq, scq,
								 incr_nullsafe_accum(psprintf("%s.%s", mvname, colq),
													 psprintf("d.%s", colq), true));
			else
				appendStringInfo(buf, "%s=%s", colq,
								 incr_nullsafe_accum(psprintf("%s.%s", mvname, colq),
													 psprintf("d.%s", colq), true));
			first = false;
		}
	}

	appendStringInfo(buf, " FROM d WHERE ");
	first = true;
	foreach(gcl, groupColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(gcl)));

		if (!first)
			appendStringInfoString(buf, " AND ");
		/* NULL-safe so a NULL/partial-NULL group key matches its delta row */
		appendStringInfo(buf, "%s.%s IS NOT DISTINCT FROM d.%s", mvname, colq, colq);
		first = false;
	}
}

/*
 * incr_build_delta_select_query — copy the stored view Query and swap the
 * single source relation RTE for a named-tuplestore (ENR) RTE that points at
 * the transition table enrName ("__mv_newtable" / "__mv_oldtable").  The
 * resulting Query deparses (via dbblue_deparse_query) to the per-group delta
 * SELECT over only the changed rows.  Mirrors addRangeTableEntryForENR().
 *
 * The copied Query already carries the hidden maintenance targets
 * (__mv_count__, AVG sum/cnt pairs, __mv_having_ok__) added by
 * MatviewIncrAddCountTarget, so the deparsed SELECT yields the matview's full
 * column set.  The visible AVG column is kept in the SELECT — harmless for the
 * DELETE CTE because the UPDATE recomputes AVG from the sum/cnt pair and never
 * reads it.
 */
static Query *
incr_build_delta_select_query(Query *viewQuery, Oid srctable, const char *enrName)
{
	Query		   *q = copyObject(viewQuery);
	RangeTblEntry  *target = NULL;
	ListCell	   *lc;
	Relation		rel;
	TupleDesc		tupdesc;
	int				attno;

	/*
	 * The delta SELECT must compute per-group aggregate deltas for EVERY group
	 * the transition rows touch, including groups that currently fail HAVING —
	 * HAVING is maintained separately (the __mv_having_ok__ flag recomputed by
	 * hav_sql).  Applying it here would drop deltas for failing groups and
	 * corrupt their running totals, so strip it from the copy.  (No-op when the
	 * view has no HAVING.)  Any aggregate that existed only for HAVING is a
	 * resjunk target and is skipped by both the deparse and the INSERT column
	 * list, exactly as in the hand builders.
	 */
	q->havingQual = NULL;

	foreach(lc, q->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		if (rte->rtekind == RTE_RELATION && rte->relid == srctable)
		{
			target = rte;
			break;
		}
	}
	if (target == NULL)
		elog(ERROR,
			 "incr_build_delta_select_query: source relation %u not found",
			 srctable);

	rel = table_open(srctable, AccessShareLock);
	tupdesc = RelationGetDescr(rel);

	target->rtekind = RTE_NAMEDTUPLESTORE;
	target->enrname = pstrdup(enrName);
	target->enrtuples = 0;
	target->coltypes = NIL;
	target->coltypmods = NIL;
	target->colcollations = NIL;
	for (attno = 1; attno <= tupdesc->natts; attno++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, attno - 1);

		if (att->attisdropped)
		{
			/* zeroes for a dropped column, matching addRangeTableEntryForENR */
			target->coltypes = lappend_oid(target->coltypes, InvalidOid);
			target->coltypmods = lappend_int(target->coltypmods, 0);
			target->colcollations = lappend_oid(target->colcollations, InvalidOid);
		}
		else
		{
			target->coltypes = lappend_oid(target->coltypes, att->atttypid);
			target->coltypmods = lappend_int(target->coltypmods, att->atttypmod);
			target->colcollations = lappend_oid(target->colcollations,
												att->attcollation);
		}
	}
	table_close(rel, AccessShareLock);

	/* ENRs carry no permission info; deparse ignores rteperminfos either way. */
	target->perminfoindex = 0;
	/* keep relid (plan invalidation) and eref (column names) unchanged */

	return q;
}

/*
 * incr_build_ins_sql_deparse — INSERT delta via the deparse core.
 * Shell-identical to incr_build_ins_sql_gen; only the SELECT body is produced
 * by ruleutils instead of hand-deparsing each target expression.
 */
static char *
incr_build_ins_sql_deparse(Oid mvrelid, Query *viewQuery,
						   Oid srctable, const char *enrName)
{
	StringInfoData	buf;
	Query		   *dq = incr_build_delta_select_query(viewQuery, srctable, enrName);
	char		   *sel = dbblue_deparse_query(dq);

	initStringInfo(&buf);
	incr_emit_ins_head(&buf, mvrelid, viewQuery);
	appendStringInfoString(&buf, sel);
	incr_emit_ins_conflict_tail(&buf, mvrelid, viewQuery);
	return buf.data;
}

/*
 * incr_build_del_sql_deparse — DELETE delta via the deparse core.
 */
static char *
incr_build_del_sql_deparse(Oid mvrelid, Query *viewQuery,
						   Oid srctable, const char *enrName)
{
	StringInfoData	buf;
	Query		   *dq = incr_build_delta_select_query(viewQuery, srctable, enrName);
	char		   *sel = dbblue_deparse_query(dq);

	initStringInfo(&buf);
	appendStringInfoString(&buf, "WITH d AS (");
	appendStringInfoString(&buf, sel);
	appendStringInfoString(&buf, ") ");
	incr_emit_del_update_tail(&buf, mvrelid, viewQuery);
	return buf.data;
}

/*
 * incr_emit_conflict_do_nothing — " ON CONFLICT (group_cols) DO NOTHING"
 */
static void
incr_emit_conflict_do_nothing(StringInfo buf, Query *viewQuery)
{
	List	   *groupColNames = NIL;
	ListCell   *gcl;
	bool		first = true;

	incr_collect_group_cols(viewQuery, &groupColNames);
	appendStringInfoString(buf, " ON CONFLICT (");
	foreach(gcl, groupColNames)
	{
		if (!first)
			appendStringInfoChar(buf, ',');
		appendStringInfoString(buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}
	appendStringInfoString(buf, ") DO NOTHING");
}

/*
 * incr_build_backfill_sql_deparse — one-time HAVING failing-group backfill via
 * the deparse core (counterpart of incr_build_backfill_sql_gen).
 *
 *   INSERT INTO mv (cols) <SELECT over the REAL source tables, no HAVING,
 *                          __mv_having_ok__ = false> ON CONFLICT (g) DO NOTHING
 *
 * Unlike the delta builders this reads the real base relations (it deparses the
 * view query with no ENR swap), so it can seed groups that initially fail
 * HAVING; DO NOTHING leaves the already-populated passing groups (having_ok =
 * true) intact.  havingQual is stripped so failing groups are included, and the
 * __mv_having_ok__ Const is flipped from true to false.
 */
static char *
incr_build_backfill_sql_deparse(Oid mvrelid, Query *viewQuery)
{
	StringInfoData	buf;
	Query		   *q = copyObject(viewQuery);
	ListCell	   *lc;
	char		   *sel;

	q->havingQual = NULL;
	foreach(lc, q->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (!te->resjunk && te->resname != NULL &&
			strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0 &&
			IsA(te->expr, Const))
		{
			Const *c = (Const *) te->expr;

			c->constvalue = BoolGetDatum(false);
			c->constisnull = false;
		}
	}

	sel = dbblue_deparse_query(q);

	initStringInfo(&buf);
	incr_emit_ins_head(&buf, mvrelid, viewQuery);
	appendStringInfoString(&buf, sel);
	incr_emit_conflict_do_nothing(&buf, viewQuery);
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
	ListCell   *lc;
	bool		first;

	initStringInfo(&buf);

	/* INSERT INTO mv (cols) SELECT ... (shell shared with the deparse path) */
	incr_emit_ins_head(&buf, mvrelid, viewQuery);
	appendStringInfoString(&buf, "SELECT ");

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

	/* ON CONFLICT (group_cols) DO UPDATE SET ... (shell shared with deparse) */
	incr_emit_ins_conflict_tail(&buf, mvrelid, viewQuery);

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
				/* Render DISTINCT so a failing COUNT(DISTINCT ...) group is seeded
				 * with its true value — otherwise hav_sql could later mark it
				 * visible from a wrong (non-distinct) count. */
				appendStringInfo(&buf, "%s(%s%s)", fname,
								 agg->aggdistinct != NIL ? "DISTINCT " : "",
								 ebuf.data);
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
	ListCell   *lc;
	const char *cntcol = quote_identifier(MATVIEW_INCR_COUNT_COL);
	bool		first;

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

	/* UPDATE mv SET ... FROM d WHERE ... (shell shared with the deparse path) */
	incr_emit_del_update_tail(&buf, mvrelid, viewQuery);

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
			 *
			 * If none was found, this table is a TRUE CROSS JOIN only when it is
			 * referenced by NO join qual at all.  The leftmost leaf has
			 * je->quals == NULL (its ON lives in another entry), but it IS
			 * referenced by that ON — so when its neighbour is not known yet we
			 * must DEFER it, not emit a bogus CROSS JOIN that drops the
			 * connecting condition (which would cartesian-join the delta to every
			 * row of the far table, e.g. a 3-table chain whose delta is the far
			 * end).  Checking je->quals == NULL instead got this wrong for 3+
			 * table joins.
			 */
			{
				bool		add_it = (connecting_qual != NULL);

				if (!add_it)
				{
					bool		referenced = false;
					ListCell   *qlc2;

					foreach(qlc2, all_quals)
					{
						Bitmapset  *refs = NULL;

						incr_qual_varnos_walker(lfirst(qlc2), &refs);
						referenced = bms_is_member(je->varno, refs);
						bms_free(refs);
						if (referenced)
							break;
					}
					add_it = !referenced;	/* unreferenced ⇒ genuine CROSS JOIN */
				}

				if (add_it)
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
 * incr_build_affected_sql
 *
 * Append the _affected_ CTE arms for one delta source: the GROUP BY key tuples
 * whose aggregates could change.  This is the ONLY per-shape part of the
 * recompute strategy; incr_build_recompute_sql wraps it with the generic
 * _new_agg_/UPSERT/DELETE tail.  Works for a single table, INNER JOIN, and
 * LEFT/RIGHT/FULL outer joins (the orphan and all-NULL arms simply never fire
 * for the non-outer shapes):
 *
 *   1. Find the GROUP BY keys touched by this delta (_affected_ CTE).
 *      - Arm 1: deparse the viewQuery with the delta table swapped to the
 *        transition-table ENR.  Covers groups in the delta rows' join result.
 *      - Arm 2 (optional-side group key only): detect preserved rows that
 *        changed join status (newly-orphaned on del_sql; newly-matched on
 *        ins_sql).  These rows may join/leave the NULL group which arm 1
 *        cannot see.
 *   2. Re-run the full join query on live tables for those groups only
 *      (_new_agg_ CTE).
 *   3. UPSERT (REPLACE semantics) those groups into the matview.
 *   4. DELETE groups absent from _new_agg_: always for del_sql; also for
 *      ins_sql when arm 2 is present (the NULL group can vanish when every
 *      orphaned preserved row gains an optional match).
 *
 * delta_varno:       varno of the delta table in viewQuery->rtable.
 * delta_table:       transition table name ("__mv_newtable" / "__mv_oldtable").
 * all_tables:        flat list of IncrJoinEntry*, left-to-right join order.
 *
 * buf must already hold "WITH _affected_ AS ("; the arms are appended (no
 * closing paren).  Returns true when the DELETE step must run even on ins_sql
 * (an orphan/all-NULL arm is present, so the NULL group can vanish).
 */
static bool
incr_build_affected_sql(StringInfo buf, Query *viewQuery,
						int delta_varno, const char *delta_table,
						List *all_tables)
{
	ListCell	   *lc;
	bool			first;

	/* ----------------------------------------------------------------
	 * Pre-compute preserved/delta metadata needed for arm 2.
	 * ---------------------------------------------------------------- */
	int				preserved_varno;
	IncrJoinEntry  *preserved_entry = NULL;
	IncrJoinEntry  *delta_entry     = NULL;
	bool			need_orphan_arm  = false;
	bool			full_null_arm    = false;
	bool			is_full_join     = false;
	bool			is_del           = (strcmp(delta_table,
											  MATVIEW_INCR_OLDTABLE) == 0);

	preserved_varno = incr_outer_preserved_varno(all_tables);

	foreach(lc, all_tables)
	{
		IncrJoinEntry *je = lfirst(lc);

		if (je->varno == preserved_varno)
			preserved_entry = je;
		if (je->varno == delta_varno)
			delta_entry = je;
	}
	Assert(delta_entry != NULL && preserved_entry != NULL);

	/*
	 * Arm 2 is needed when:
	 *   (a) the delta is an optional-side (LEFT or RIGHT joined) table,
	 *   (b) it is DIRECTLY connected to the preserved anchor (its ON condition
	 *       references preserved_varno on one side — multi-hop chains are not
	 *       supported and are rejected at CREATE time by incr_recompute_outer_shape),
	 *   (c) at least one GROUP BY key comes from the delta table (optional side),
	 *   (d) all GROUP BY keys are plain Vars from either the preserved anchor or
	 *       the delta table itself (no third-table or expression keys).
	 */
	if (delta_varno != preserved_varno &&
		(delta_entry->join_type == JOIN_LEFT ||
		 delta_entry->join_type == JOIN_RIGHT) &&
		incr_qual_get_other_varno(delta_entry->quals, delta_varno) == preserved_varno)
	{
		bool	has_opt_key   = false;
		bool	has_third_key = false;

		foreach(lc, viewQuery->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
			TargetEntry     *te  = get_sortgroupclause_tle(sgc,
														   viewQuery->targetList);
			int				 rv;

			if (!IsA(te->expr, Var))
			{
				has_third_key = true;
				break;
			}
			incr_resolve_var_colname((Var *) te->expr, viewQuery->rtable, &rv);
			if (rv == delta_varno)
				has_opt_key = true;
			else if (rv != preserved_varno)
			{
				has_third_key = true;
				break;
			}
		}
		need_orphan_arm = has_opt_key && !has_third_key;
	}

	/*
	 * FULL OUTER JOIN NULL arm.
	 *
	 * A FULL JOIN preserves rows from BOTH sides, so both can spawn orphans.
	 * CREATE-time gating (incr_full_join_single_side_keys) guarantees this is a
	 * two-table FULL join whose GROUP BY keys are all plain Vars from ONE side
	 * (the "key side").  The only group arm 1 cannot see is the all-NULL group:
	 * the OTHER side's orphans, whose key-side columns are NULL.
	 *
	 * A delta on the KEY-side table flips the other side's orphan status:
	 *   • delete: an other-side row that matched only the deleted key-side rows
	 *     becomes orphaned → it enters the all-NULL group;
	 *   • insert: a previously-orphaned other-side row that matches an inserted
	 *     key-side row leaves the all-NULL group.
	 * Arm 1 misses both (the flipping other-side row appears MATCHED against the
	 * delta ENR, not as an orphan).  So for any delta on the key side we add the
	 * all-NULL group row to _affected_ and let _new_agg_ recompute it.
	 *
	 * A delta on the OTHER (non-key) side is fully covered by arm 1: an orphan
	 * row in the delta ENR surfaces as an orphan there, contributing the NULL
	 * key directly.
	 */
	{
		ListCell *flc;

		foreach(flc, all_tables)
		{
			if (((IncrJoinEntry *) lfirst(flc))->join_type == JOIN_FULL)
			{
				is_full_join = true;
				break;
			}
		}
	}
	if (is_full_join && viewQuery->groupClause != NIL)
	{
		SortGroupClause *sgc0 = lfirst_node(SortGroupClause,
											list_head(viewQuery->groupClause));
		TargetEntry	    *te0  = get_sortgroupclause_tle(sgc0,
														viewQuery->targetList);
		Node			*g0   = incr_group_key_expr(viewQuery, te0);
		int				 key_side_varno;

		/*
		 * Single-side plain-Var keys have an all-NULL group (the other side's
		 * orphans) that needs the NULL arm on key-side deltas.  COALESCE-of-
		 * join-keys keys have NO all-NULL group (invariant under orphan flip),
		 * so no NULL arm — arm 1 alone is correct.  The gate admits only these
		 * two FULL shapes.
		 */
		if (g0 != NULL && IsA(g0, Var))
		{
			incr_resolve_var_colname((Var *) g0, viewQuery->rtable,
									 &key_side_varno);
			full_null_arm = (delta_varno == key_side_varno);
		}
	}

	/* ----------------------------------------------------------------
	 * _affected_ CTE — arm 1: GROUP BY key combinations touched by this
	 * delta.  Strategy: deparse the viewQuery with the delta table swapped
	 * to the named transition-table ENR (_dg_ subquery), then SELECT DISTINCT
	 * only the GROUP BY column names from that result.
	 * ---------------------------------------------------------------- */
	{
		Query  *aff_dq;
		char   *aff_sel;

		aff_dq = incr_build_delta_select_query(viewQuery, delta_entry->oid,
											   delta_table);
		aff_dq->havingQual = NULL;
		aff_sel = dbblue_deparse_query(aff_dq);

		appendStringInfoString(buf, "\n  SELECT DISTINCT ");
		first = true;
		foreach(lc, viewQuery->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
			TargetEntry     *te  = get_sortgroupclause_tle(sgc,
														   viewQuery->targetList);

			if (!first) appendStringInfoString(buf, ", ");
			appendStringInfoString(buf, quote_identifier(te->resname));
			first = false;
		}
		appendStringInfo(buf, "\n  FROM (%s) _dg_", aff_sel);
	}

	/* ----------------------------------------------------------------
	 * _affected_ CTE — arm 2 (orphan detection for optional-side group
	 * keys).
	 *
	 * A DELETE on the optional side can BIRTH a new NULL group (preserved
	 * rows that lost their last optional match become orphaned and join the
	 * NULL group).  An INSERT on the optional side can SHRINK or VANISH the
	 * NULL group (previously-orphaned preserved rows now have a match).
	 * Arm 1 only sees the old/new key values of the delta rows themselves;
	 * it cannot see these NULL-group changes.  Arm 2 covers them.
	 *
	 * del_sql arm 2:
	 *   JOIN preserved to OLDTABLE (find which preserved rows were affected),
	 *   then LEFT JOIN to the live optional table.  For orphaned rows the
	 *   LEFT JOIN produces NULL — that is their new group key.  For rows
	 *   that still have optional matches the arm produces those keys (a
	 *   harmless superset already captured by arm 1).
	 *
	 * ins_sql arm 2:
	 *   JOIN preserved to NEWTABLE (find which preserved rows gained a new
	 *   optional match), filtered to rows that had NO other optional match
	 *   before (= rows that were previously in the NULL group).  Detected via
	 *   ctid: a live optional row whose ctid does not appear in NEWTABLE is
	 *   a pre-existing row.  If no such row exists for a given preserved row,
	 *   that preserved row was previously unmatched → its old group key was
	 *   NULL → NULL is now in _affected_.
	 * ---------------------------------------------------------------- */
	if (need_orphan_arm)
	{
		char   *live_cond;
		char   *enr_cond;

		live_cond = qual_to_live_sql(delta_entry->quals, viewQuery->rtable,
									 all_tables, preserved_varno);
		/* ENR condition: same as live_cond but with the optional-table alias
		 * replaced by _jd_ (the delta ENR alias). */
		enr_cond = str_replace_all(live_cond,
								   psprintf("_lt%d_", delta_varno), "_jd_");

		appendStringInfoString(buf, "\n  UNION\n  SELECT DISTINCT ");
		first = true;
		foreach(lc, viewQuery->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
			TargetEntry     *te  = get_sortgroupclause_tle(sgc,
														   viewQuery->targetList);
			const char	   *resname;
			int				 rv;
			const char	   *colname;

			/* need_orphan_arm guarantees all keys are plain Vars from
			 * preserved_varno or delta_varno. */
			Assert(IsA(te->expr, Var));
			colname = incr_resolve_var_colname((Var *) te->expr,
											   viewQuery->rtable, &rv);
			resname = quote_identifier(te->resname);

			if (!first) appendStringInfoString(buf, ", ");
			first = false;

			if (rv == delta_varno)
			{
				if (is_del)
					/* del_sql: read current optional value (NULL if orphaned) */
					appendStringInfo(buf, "_lt%d_.%s AS %s",
									 delta_varno,
									 quote_identifier(colname), resname);
				else
				{
					/*
					 * ins_sql: these rows were in the NULL group before the
					 * insert.  Cast NULL to the column's actual type so that
					 * UNION type resolution works for any column type (bare
					 * NULL is typed as "unknown" which becomes "text" under
					 * SELECT DISTINCT, causing a type mismatch in the UNION).
					 */
					appendStringInfo(buf, "CAST(NULL AS %s) AS %s",
									 format_type_be(((Var *) te->expr)->vartype),
									 resname);
				}
			}
			else
			{
				/* preserved_varno: always read from _ltp_ */
				appendStringInfo(buf, "_ltp_.%s AS %s",
								 quote_identifier(colname), resname);
			}
		}

		if (is_del)
		{
			/*
			 * del_sql arm 2: for each preserved row that had a deleted optional
			 * row, look up its CURRENT optional group key.  The LEFT JOIN
			 * returns NULL for orphaned rows (no current optional match).
			 */
			appendStringInfo(buf,
							 "\n  FROM %s _ltp_\n"
							 "  JOIN %s _jd_ ON (%s)\n"
							 "  LEFT JOIN %s _lt%d_ ON (%s)",
							 mv_qname(preserved_entry->oid),
							 delta_table, enr_cond,
							 mv_qname(delta_entry->oid), delta_varno, live_cond);
		}
		else
		{
			/*
			 * ins_sql arm 2: find preserved rows that were PREVIOUSLY unmatched
			 * (= were in the NULL group before this insert).
			 *
			 * A preserved row was previously unmatched iff ALL current optional
			 * rows matching it come from NEWTABLE (no pre-existing rows).
			 * Equivalently: COUNT(live optional matching p) = COUNT(NEWTABLE
			 * matching p).  After the INSERT, COUNT(live) = COUNT(pre-existing)
			 * + COUNT(NEWTABLE), so the equality holds iff COUNT(pre-existing)=0.
			 *
			 * Transition table ENRs do not expose system columns (ctid, oid),
			 * so we use the COUNT comparison rather than a ctid-based filter.
			 */
			char *live_cond_x =
				str_replace_all(live_cond,
								psprintf("_lt%d_", delta_varno), "_ltx_");
			char *enr_cond_jn =
				str_replace_all(enr_cond, "_jd_", "_jn_");

			appendStringInfo(buf,
							 "\n  FROM %s _ltp_\n"
							 "  JOIN %s _jd_ ON (%s)\n"
							 "  WHERE (SELECT COUNT(*) FROM %s _ltx_\n"
							 "         WHERE %s)\n"
							 "      = (SELECT COUNT(*) FROM %s _jn_\n"
							 "         WHERE %s)",
							 mv_qname(preserved_entry->oid),
							 delta_table, enr_cond,
							 mv_qname(delta_entry->oid), live_cond_x,
							 delta_table, enr_cond_jn);
		}
	}

	/* ----------------------------------------------------------------
	 * _affected_ CTE — FULL-join NULL arm.
	 *
	 * For a delta on the key side of a single-side FULL-join aggregate, add
	 * the all-NULL group row (every key column NULL) so _new_agg_ recomputes
	 * the other side's orphan group.  A constant SELECT (no FROM) — it is
	 * unconditional and idempotent: if there is no all-NULL group before or
	 * after the delta, _new_agg_ simply omits it and the DELETE step is a
	 * no-op.  See the full_null_arm comment above for why arm 1 misses it.
	 * ---------------------------------------------------------------- */
	if (full_null_arm)
	{
		appendStringInfoString(buf, "\n  UNION\n  SELECT ");
		first = true;
		foreach(lc, viewQuery->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
			TargetEntry     *te  = get_sortgroupclause_tle(sgc,
														   viewQuery->targetList);

			if (!first) appendStringInfoString(buf, ", ");
			first = false;
			appendStringInfo(buf, "CAST(NULL AS %s) AS %s",
							 format_type_be(((Var *) te->expr)->vartype),
							 quote_identifier(te->resname));
		}
	}

	return need_orphan_arm || full_null_arm;
}

/*
 * incr_build_recompute_sql — the general recompute engine.
 *
 * Build the complete maintenance statement for one delta source of a
 * recompute-strategy matview:
 *
 *   WITH _affected_ AS ( <incr_build_affected_sql arms> ),
 *        _new_agg_  AS ( <live recompute of those groups> ),
 *        _upd_      AS ( <UPSERT> )
 *   <DELETE vanished groups | SELECT 1>
 *
 * Aggregate- and join-agnostic: _new_agg_ re-runs the view query verbatim via
 * the deparse core, so any aggregate the view contains (DISTINCT, stddev, bool,
 * additive) is recomputed exactly as a full REFRESH would.  Used for single
 * tables, INNER joins, and LEFT/RIGHT/FULL outer joins; the self-join variants
 * (incr_build_self_recompute_sql, hand self-agg builders) build their own
 * _affected_ but share the same tail.
 *
 * include_delete_step: true for del_sql; forced internally when an orphan or
 * all-NULL arm is present (ins_sql can then vanish the NULL group).
 */
static char *
incr_build_recompute_sql(Oid mvrelid, Query *viewQuery,
						 int delta_varno, const char *delta_table,
						 List *all_tables, bool include_delete_step)
{
	StringInfoData	buf;
	bool			force_delete;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "WITH _affected_ AS (");
	force_delete = incr_build_affected_sql(&buf, viewQuery, delta_varno,
										   delta_table, all_tables);
	appendStringInfoString(&buf, "\n),\n");
	incr_append_recompute_tail(&buf, mv_qname(mvrelid), viewQuery,
							   include_delete_step || force_delete);
	return buf.data;
}

/*
 * incr_inject_affected_filter
 *
 * Add, to live_q's WHERE, a per-key restriction "<groupkey_i> IN (SELECT
 * <col_i> FROM _affected_ WHERE <col_i> IS NOT NULL)" for every GROUP BY key.
 * Because the restriction sits at the aggregate's OWN query level (a sibling of
 * GROUP BY, applied before grouping), the planner filters the base rows via an
 * index on the group key — the affected-group recompute becomes index-driven
 * instead of aggregating the whole table (measured 3s → 0.2ms on a 1M-row
 * count(DISTINCT) matview; a wrapping-subquery/LATERAL filter can't push a
 * parameter through a DISTINCT-aggregate GROUP BY and falls back to a full
 * scan).
 *
 * Per-key IN is a SUPERSET of the exact affected (k1,k2,…) tuples, which is
 * correct for recompute: extra groups are recomputed to their unchanged value
 * (idempotent) and the DELETE step keys only on _affected_.  Because IN never
 * matches NULL and the subselect excludes NULL keys, the injected arm covers
 * only non-NULL groups — the caller pairs it with the existing NULL-group arm
 * (single nullable key) exactly as the old fast form did.
 *
 * Returns true on success; false (leaving live_q unchanged) if a key lacks a
 * default equality operator, so the caller can fall back to the NULL-safe
 * EXISTS form.  Applied only where the caller's fast_form holds: a single key,
 * or a multi-key set all provably NOT NULL — so a partial-NULL multi-key group
 * (which per-key IN would wrongly drop) never reaches here.
 */
static bool
incr_inject_affected_filter(Query *live_q)
{
	List	   *newquals = NIL;
	List	   *keytypes = NIL;
	List	   *keytypmods = NIL;
	List	   *keycolls = NIL;
	List	   *keynames = NIL;
	ListCell   *lc;
	int			attno;

	if (live_q->groupClause == NIL)
		return false;

	/* Collect group-key exprs + the _affected_ column metadata (all keys). */
	foreach(lc, live_q->groupClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
		TargetEntry	    *te  = get_sortgroupclause_tle(sgc, live_q->targetList);
		Node			*ge  = incr_group_key_expr(live_q, te);

		if (ge == NULL)
			return false;
		keytypes  = lappend_oid(keytypes, exprType(ge));
		keytypmods = lappend_int(keytypmods, exprTypmod(ge));
		keycolls  = lappend_oid(keycolls, exprCollation(ge));
		keynames  = lappend(keynames, makeString(pstrdup(te->resname)));
	}

	/* One "keyexpr IN (SELECT col FROM _affected_ WHERE col IS NOT NULL)". */
	attno = 1;
	foreach(lc, live_q->groupClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
		TargetEntry	    *te  = get_sortgroupclause_tle(sgc, live_q->targetList);
		Node			*ge  = incr_group_key_expr(live_q, te);
		Oid				 ktype = exprType(ge);
		Oid				 kcoll = exprCollation(ge);
		Oid				 eqop  = lookup_type_cache(ktype, TYPECACHE_EQ_OPR)->eq_opr;
		RangeTblEntry   *cte;
		RangeTblRef	    *rtr;
		Query		    *sub;
		Var			    *subvar;
		NullTest	    *nt;
		SubLink		    *sl;
		OpExpr		    *op;
		Param		    *prm;

		if (!OidIsValid(eqop))
			return false;

		/* RTE_CTE describing _affected_ (all group-key columns). */
		cte = makeNode(RangeTblEntry);
		cte->rtekind = RTE_CTE;
		cte->ctename = pstrdup("_affected_");
		cte->ctelevelsup = 1;
		cte->self_reference = false;
		cte->coltypes = list_copy(keytypes);
		cte->coltypmods = list_copy(keytypmods);
		cte->colcollations = list_copy(keycolls);
		cte->alias = NULL;
		cte->eref = makeAlias("_affected_", list_copy(keynames));
		cte->lateral = false;
		cte->inFromCl = true;

		subvar = makeVar(1, attno, ktype, exprTypmod(ge), kcoll, 0);

		nt = makeNode(NullTest);
		nt->arg = (Expr *) copyObject(subvar);
		nt->nulltesttype = IS_NOT_NULL;
		nt->argisrow = false;
		nt->location = -1;

		rtr = makeNode(RangeTblRef);
		rtr->rtindex = 1;

		sub = makeNode(Query);
		sub->commandType = CMD_SELECT;
		sub->canSetTag = false;
		sub->rtable = list_make1(cte);
		sub->jointree = makeFromExpr(list_make1(rtr), (Node *) nt);
		sub->targetList = list_make1(makeTargetEntry((Expr *) subvar, 1,
									 pstrdup(strVal(list_nth(keynames, attno - 1))),
									 false));

		prm = makeNode(Param);
		prm->paramkind = PARAM_SUBLINK;
		prm->paramid = 1;
		prm->paramtype = ktype;
		prm->paramtypmod = exprTypmod(ge);
		prm->paramcollid = kcoll;
		prm->location = -1;

		op = makeNode(OpExpr);
		op->opno = eqop;
		op->opfuncid = get_opcode(eqop);
		op->opresulttype = BOOLOID;
		op->opretset = false;
		op->opcollid = InvalidOid;
		op->inputcollid = kcoll;
		op->args = list_make2(copyObject(ge), prm);
		op->location = -1;

		sl = makeNode(SubLink);
		sl->subLinkType = ANY_SUBLINK;
		sl->subLinkId = 0;
		sl->testexpr = (Node *) op;
		sl->operName = NIL;
		sl->subselect = (Node *) sub;
		sl->location = -1;

		newquals = lappend(newquals, sl);
		attno++;
	}

	if (live_q->jointree->quals != NULL)
		newquals = lcons(live_q->jointree->quals, newquals);
	live_q->jointree->quals = (list_length(newquals) == 1)
		? (Node *) linitial(newquals)
		: (Node *) makeBoolExpr(AND_EXPR, newquals, -1);
	live_q->hasSubLinks = true;
	return true;
}

/*
 * incr_append_recompute_tail
 *
 * Append the shared recompute tail of an outer-join delta statement: the
 * _new_agg_ CTE (live recompute of the affected groups), the _upd_ UPSERT CTE,
 * and the final DELETE-vanished / benign-SELECT step.  buf must already hold
 * "WITH _affected_ AS ( ... ),\n"; this appends the rest, leaving a complete
 * statement.  Shared by incr_build_recompute_sql (single delta table) and
 * incr_build_self_recompute_sql (a self-joined table in both roles), which differ
 * only in how they build _affected_.
 */
static void
incr_append_recompute_tail(StringInfo buf, const char *mvname,
								 Query *viewQuery, bool actual_delete_step)
{
	ListCell   *lc;
	bool		first;

	/* ----------------------------------------------------------------
	 * _new_agg_ CTE: recompute the full join for affected groups.
	 *
	 * Strategy: deparse the viewQuery against the LIVE base tables
	 * (no ENR swap), strip HAVING so failing groups are included (the
	 * hav_sql step re-derives __mv_having_ok__ afterwards), then wrap
	 * in a subquery restricted to the _affected_ keys.  The deparse core
	 * renders the correct join types (INNER/LEFT/FULL) and all column
	 * origins automatically.
	 *
	 * The restriction has two forms (measured on a 1M-row table, one
	 * affected group, index on the key):
	 *
	 *   FAST — CROSS JOIN LATERAL on plain equality:
	 *     FROM (SELECT DISTINCT keys FROM _affected_) _ak_
	 *     CROSS JOIN LATERAL (SELECT ... FROM (<live>) __live__
	 *                         WHERE __live__.k = _ak_.k) _x_
	 *     The = qual is pushed into the deparsed subquery down to the scan
	 *     as a parameterized index condition (~13ms; reads only the
	 *     affected groups' rows).  Plain = misses NULL keys, so this form
	 *     needs a NULL-group arm (single nullable key) or provably
	 *     NOT NULL keys (multi-key).
	 *
	 *   GENERAL — WHERE EXISTS (... IS NOT DISTINCT FROM ...):
	 *     NULL-safe for any key set, but IS NOT DISTINCT FROM is not an
	 *     indexable operator, so the base table is seq-scanned (~46ms at
	 *     1M rows).  Fallback for multi-key sets with nullable or
	 *     expression keys.  (A redundant indexable prefilter with
	 *     "IN (...) OR IS NULL" measured far WORSE — 6.3s — because the
	 *     OR forces a full index scan; do not "optimize" that way.)
	 * ---------------------------------------------------------------- */
	{
		Query  *live_q = copyObject(viewQuery);
		char   *live_sel;
		int		nkeys = list_length(viewQuery->groupClause);
		bool	all_notnull = (nkeys > 0);
		bool	fast_form;
		bool	injected = false;

		live_q->havingQual = NULL;
		live_sel = dbblue_deparse_query(live_q);	/* plain form, for NULL arm */

		foreach(lc, viewQuery->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
			TargetEntry     *te  = get_sortgroupclause_tle(sgc,
														   viewQuery->targetList);
			Node			*gexpr = incr_group_key_expr(viewQuery, te);

			if (gexpr == NULL || !IsA(gexpr, Var) ||
				incr_key_var_nullable((Var *) gexpr, viewQuery->rtable))
			{
				all_notnull = false;
				break;
			}
		}
		/* single key: the NULL arm covers nullability; multi-key: all keys
		 * must be provably NOT NULL for the non-NULL arm to be exhaustive */
		fast_form = (nkeys == 1) || all_notnull;

		if (fast_form)
		{
			/*
			 * Non-NULL arm: recompute restricted to the affected groups with
			 * the key filter injected into the aggregate's own WHERE, so the
			 * scan is index-driven (see incr_inject_affected_filter).
			 */
			Query  *inj_q = copyObject(viewQuery);

			inj_q->havingQual = NULL;
			injected = incr_inject_affected_filter(inj_q);
			if (injected)
			{
				const char *inj_sel = dbblue_deparse_query(inj_q);

				appendStringInfo(buf, "_new_agg_ AS (\n  %s", inj_sel);

				if (nkeys == 1 && !all_notnull)
				{
					const char *k1 =
						quote_identifier(get_sortgroupclause_tle(
							linitial_node(SortGroupClause, viewQuery->groupClause),
							viewQuery->targetList)->resname);

					/* NULL-group arm: executes only when NULL is in _affected_ */
					appendStringInfo(buf,
									 "\n  UNION ALL\n"
									 "  SELECT __live__.*\n"
									 "  FROM (%s) __live__\n"
									 "  WHERE __live__.%s IS NULL\n"
									 "    AND EXISTS (SELECT 1 FROM _affected_ "
									 "WHERE %s IS NULL)",
									 live_sel, k1, k1);
				}
				appendStringInfoString(buf, "\n),\n");
			}
		}

		/*
		 * Fallback (multi-key with nullable/expression keys, or a key type
		 * without a default equality operator): NULL-safe EXISTS.  Correct for
		 * any key set; not index-driven (see the comment above).
		 */
		if (!injected)
		{
			appendStringInfo(buf,
							 "_new_agg_ AS (\n"
							 "  SELECT __live__.*\n"
							 "  FROM (%s) __live__\n"
							 "  WHERE EXISTS (\n"
							 "    SELECT 1 FROM _affected_ WHERE ",
							 live_sel);
			first = true;
			foreach(lc, viewQuery->groupClause)
			{
				SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
				TargetEntry     *te  = get_sortgroupclause_tle(sgc,
															   viewQuery->targetList);
				const char      *col = quote_identifier(te->resname);

				if (!first) appendStringInfoString(buf, " AND ");
				appendStringInfo(buf,
								 "_affected_.%s IS NOT DISTINCT FROM __live__.%s",
								 col, col);
				first = false;
			}
			appendStringInfoString(buf, "\n  )\n),\n");
		}
	}

	/* ----------------------------------------------------------------
	 * _upd_ CTE: UPSERT the recomputed rows into the matview.
	 * ---------------------------------------------------------------- */
	appendStringInfo(buf, "_upd_ AS (\n  INSERT INTO %s (", mvname);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk) continue;
		if (!first) appendStringInfoString(buf, ", ");
		appendStringInfoString(buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(buf, ")\n  SELECT ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk) continue;
		if (!first) appendStringInfoString(buf, ", ");
		appendStringInfoString(buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(buf, " FROM _new_agg_\n  ON CONFLICT (");
	first = true;
	foreach(lc, viewQuery->groupClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
		TargetEntry     *te  = get_sortgroupclause_tle(sgc, viewQuery->targetList);

		if (!first) appendStringInfoString(buf, ", ");
		appendStringInfoString(buf, quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(buf, ") DO UPDATE SET ");
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

		if (!first) appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "%s=EXCLUDED.%s",
						 quote_identifier(te->resname),
						 quote_identifier(te->resname));
		first = false;
	}
	appendStringInfoString(buf, "\n)\n");

	/* ----------------------------------------------------------------
	 * Final statement.
	 * Delete groups that were in _affected_ but no longer in _new_agg_.
	 * Runs for del_sql always; also for ins_sql when arm 2 is present
	 * (the NULL group can completely vanish).  Otherwise a benign SELECT
	 * forces _upd_ to execute.
	 * ---------------------------------------------------------------- */
	if (actual_delete_step)
	{
		/*
		 * DELETE groups that were in _affected_ but no longer appear in
		 * _new_agg_.  Using _new_agg_ as the existence check is correct
		 * for every outer-join shape:
		 *
		 *  • Preserved-side delete: the preserved row is gone → no row in
		 *    _new_agg_ → NOT EXISTS fires.
		 *  • Inner-dim delete (3-table INNER+LEFT): _new_agg_ uses the
		 *    viewQuery's real INNER join type, so unmatched groups produce
		 *    no output → NOT EXISTS fires.
		 *  • Optional-side delete with preserved group key: preserved rows
		 *    remain → _new_agg_ has all groups → NOT EXISTS never fires
		 *    (harmless, correct).
		 *  • Optional-side delete with optional group key (arm 2 active):
		 *    arm 2 adds NULL to _affected_; if _new_agg_ has no NULL group
		 *    (no remaining orphans), NOT EXISTS fires and removes the stale
		 *    NULL row from the matview.
		 *  • ins_sql with arm 2: if the NULL group vanished (every orphaned
		 *    preserved row gained a match), NOT EXISTS fires for NULL.
		 *  • FULL JOIN: _new_agg_ covers both sides uniformly.
		 *  • Self-outer: the two role arms of _affected_ cover every group a
		 *    delta touches in either role; vanished groups fire NOT EXISTS.
		 */
		appendStringInfo(buf, "DELETE FROM %s _mv_\nUSING _affected_\nWHERE ",
						 mvname);
		first = true;
		foreach(lc, viewQuery->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
			TargetEntry     *te  = get_sortgroupclause_tle(sgc, viewQuery->targetList);
			const char      *col = quote_identifier(te->resname);

			if (!first) appendStringInfoString(buf, " AND ");
			appendStringInfo(buf, "_mv_.%s IS NOT DISTINCT FROM _affected_.%s",
							 col, col);
			first = false;
		}

		appendStringInfoString(buf,
							   "\n  AND NOT EXISTS (\n"
							   "    SELECT 1 FROM _new_agg_ WHERE ");
		first = true;
		foreach(lc, viewQuery->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
			TargetEntry     *te  = get_sortgroupclause_tle(sgc, viewQuery->targetList);
			const char      *col = quote_identifier(te->resname);

			if (!first) appendStringInfoString(buf, " AND ");
			appendStringInfo(buf, "_new_agg_.%s IS NOT DISTINCT FROM _mv_.%s",
							 col, col);
			first = false;
		}
		appendStringInfoString(buf, "\n  )");
	}
	else
	{
		/* DML CTEs (_upd_) always execute; this SELECT just terminates the WITH. */
		appendStringInfoString(buf, "SELECT 1");
	}
}

/*
 * incr_build_delta_select_query_at_varno
 *
 * Like incr_build_delta_select_query, but swaps the RTE at a SPECIFIC varno
 * (range-table position) to the transition-table ENR, rather than the first
 * relation matching an OID.  Needed for self-joins, where the same OID appears
 * at two varnos and each role must be swapped independently.
 */
static Query *
incr_build_delta_select_query_at_varno(Query *viewQuery, int target_varno,
									   const char *enrName)
{
	Query		   *q = copyObject(viewQuery);
	RangeTblEntry  *target;
	Relation		rel;
	TupleDesc		tupdesc;
	int				attno;

	q->havingQual = NULL;

	target = rt_fetch(target_varno, q->rtable);
	if (target == NULL || target->rtekind != RTE_RELATION)
		elog(ERROR,
			 "incr_build_delta_select_query_at_varno: varno %d is not a base relation",
			 target_varno);

	rel = table_open(target->relid, AccessShareLock);
	tupdesc = RelationGetDescr(rel);

	target->rtekind = RTE_NAMEDTUPLESTORE;
	target->enrname = pstrdup(enrName);
	target->enrtuples = 0;
	target->coltypes = NIL;
	target->coltypmods = NIL;
	target->colcollations = NIL;
	for (attno = 1; attno <= tupdesc->natts; attno++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, attno - 1);

		if (att->attisdropped)
		{
			target->coltypes = lappend_oid(target->coltypes, InvalidOid);
			target->coltypmods = lappend_int(target->coltypmods, 0);
			target->colcollations = lappend_oid(target->colcollations, InvalidOid);
		}
		else
		{
			target->coltypes = lappend_oid(target->coltypes, att->atttypid);
			target->coltypmods = lappend_int(target->coltypmods, att->atttypmod);
			target->colcollations = lappend_oid(target->colcollations,
												att->attcollation);
		}
	}
	table_close(rel, AccessShareLock);

	target->perminfoindex = 0;
	return q;
}

/*
 * incr_build_self_recompute_sql
 *
 * Recompute-strategy delta for a two-way SELF join with GROUP BY — INNER (any
 * plain-column keys, additive aggregates) or LEFT/RIGHT OUTER (single-side
 * keys).  A delta on
 * the table affects groups where a changed row participates in EITHER role, so
 * _affected_ is the UNION of a per-role arm (each deparses the view with that
 * role's RTE swapped to the transition-table ENR, leaving the other occurrence
 * live).  The shared recompute tail then recomputes those groups from the live
 * self-join and UPSERTs / DELETEs them.
 *
 * If the group keys are on the PRESERVED anchor role, the two role arms are
 * sufficient: a preserved-side key is captured by its role arm regardless of
 * the other role's match state (the LEFT/RIGHT join keeps the preserved row).
 *
 * If the group keys are on the OPTIONAL role, a delta on the table flips the
 * preserved rows' orphan status (deleting a row orphans the rows it was the
 * join partner of; inserting a row de-orphans previously-unmatched rows), and
 * those rows move into / out of the all-NULL group.  As in the FULL-join case,
 * the role arms miss that all-NULL group in the minimal case (the flipping rows
 * appear matched against the delta ENR), so an unconditional all-NULL arm is
 * added and the DELETE step is forced (an insert can vanish the all-NULL group).
 *
 * Optional-side DELETEs additionally need a delta⋈delta arm (both role RTEs
 * swapped to the OLDTABLE ENR): when one statement deletes a preserved row AND
 * its optional partner (possible only in a self-join — both live in the same
 * table), the group they formed appears in neither role arm.  The preserved-
 * role arm LEFT-joins the deleted row to the LIVE optional side (partner gone →
 * NULL, not the old key) and the optional-role arm joins LIVE preserved rows
 * (the deleted one is gone).  Same reasoning as the INNER self-join builder's
 * third arm.  Preserved-side keys don't need it: the preserved-role arm keeps
 * every ENR row via the LEFT join, so its key is always captured.  Inserts
 * don't need it either: new rows are live, so the role arms already cover
 * delta⋈delta combinations.
 *
 * The eligibility gate (incr_self_outer_supported_shape) guarantees a single-
 * side plain-column key set; v1, v2 are the two varnos of the self-joined table.
 */
static char *
incr_build_self_recompute_sql(Oid mvrelid, Query *viewQuery,
						  int v1, int v2, const char *delta_table,
						  List *all_tables, bool include_delete_step)
{
	StringInfoData	buf;
	const char	   *mvname = mv_qname(mvrelid);
	int				roles[2];
	int				ri;
	int				preserved_varno = incr_outer_preserved_varno(all_tables);
	int				key_side_varno;
	bool			opt_side;
	bool			actual_delete_step;
	bool			is_del = (strcmp(delta_table, MATVIEW_INCR_OLDTABLE) == 0);
	SortGroupClause *sgc0 = lfirst_node(SortGroupClause,
										list_head(viewQuery->groupClause));
	TargetEntry	   *te0  = get_sortgroupclause_tle(sgc0, viewQuery->targetList);
	Node		   *gexpr0 = incr_group_key_expr(viewQuery, te0);

	roles[0] = v1;
	roles[1] = v2;

	/*
	 * Optional-side detection applies only to self OUTER joins (the gate there
	 * guarantees single-side plain-Var keys).  A self INNER join has no
	 * preserved/optional roles — its keys may even mix roles — and needs no
	 * orphan machinery, only the delta⋈delta arm on deletes.
	 */
	if (incr_has_outer_join(all_tables))
	{
		Assert(IsA(gexpr0, Var));
		incr_try_resolve_var_to_rel((Var *) gexpr0, viewQuery->rtable,
									&key_side_varno);
		opt_side = (key_side_varno != preserved_varno);
	}
	else
		opt_side = false;
	actual_delete_step = include_delete_step || opt_side;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "WITH _affected_ AS (");

	for (ri = 0; ri < 2; ri++)
	{
		Query	   *aff_dq = incr_build_delta_select_query_at_varno(viewQuery,
																   roles[ri],
																   delta_table);
		char	   *aff_sel = dbblue_deparse_query(aff_dq);
		ListCell   *lc;
		bool		first;

		if (ri == 1)
			appendStringInfoString(&buf, "\n  UNION");
		appendStringInfoString(&buf, "\n  SELECT DISTINCT ");
		first = true;
		foreach(lc, viewQuery->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
			TargetEntry     *te  = get_sortgroupclause_tle(sgc,
														   viewQuery->targetList);

			if (!first) appendStringInfoString(&buf, ", ");
			appendStringInfoString(&buf, quote_identifier(te->resname));
			first = false;
		}
		appendStringInfo(&buf, "\n  FROM (%s) _dg_", aff_sel);
	}

	/*
	 * delta⋈delta arm — both roles swapped to the ENR, capturing group keys
	 * formed entirely among deleted rows (see the function comment).  Needed on
	 * the delete path for optional-side keys of a self OUTER join, and for ANY
	 * key of a self INNER join (a whole join-key partition removed in one
	 * statement appears in neither role arm: each joins the delta to the LIVE
	 * other role, whose partner rows are already gone).  Swapping sequentially
	 * is safe: each call copies the query and only requires ITS target varno to
	 * still be a plain relation RTE.
	 */
	if (is_del && (opt_side || !incr_has_outer_join(all_tables)))
	{
		Query	   *dd_q = incr_build_delta_select_query_at_varno(viewQuery,
																 roles[0],
																 delta_table);
		char	   *dd_sel;
		ListCell   *lc;
		bool		first;

		dd_q = incr_build_delta_select_query_at_varno(dd_q, roles[1],
													  delta_table);
		dd_sel = dbblue_deparse_query(dd_q);

		appendStringInfoString(&buf, "\n  UNION\n  SELECT DISTINCT ");
		first = true;
		foreach(lc, viewQuery->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
			TargetEntry     *te  = get_sortgroupclause_tle(sgc,
														   viewQuery->targetList);

			if (!first) appendStringInfoString(&buf, ", ");
			appendStringInfoString(&buf, quote_identifier(te->resname));
			first = false;
		}
		appendStringInfo(&buf, "\n  FROM (%s) _dd_", dd_sel);
	}

	/*
	 * Optional-side key: add the all-NULL group (every key column NULL) so the
	 * recompute covers preserved rows moving into / out of the orphan group.
	 * Unconditional and idempotent, as for the FULL-join NULL arm.
	 */
	if (opt_side)
	{
		ListCell   *lc;
		bool		first = true;

		appendStringInfoString(&buf, "\n  UNION\n  SELECT ");
		foreach(lc, viewQuery->groupClause)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
			TargetEntry     *te  = get_sortgroupclause_tle(sgc,
														   viewQuery->targetList);

			if (!first) appendStringInfoString(&buf, ", ");
			first = false;
			appendStringInfo(&buf, "CAST(NULL AS %s) AS %s",
							 format_type_be(exprType((Node *) te->expr)),
							 quote_identifier(te->resname));
		}
	}

	appendStringInfoString(&buf, "\n),\n");

	incr_append_recompute_tail(&buf, mvname, viewQuery,
									 actual_delete_step);
	return buf.data;
}

/*
 * incr_self_outer_supported_shape — true iff viewQuery is a self OUTER join the
 * self-outer recompute builder can maintain: a two-way self join (exactly two
 * base-table RTEs, both the SAME relation), joined with LEFT or RIGHT (not
 * FULL) OUTER JOIN, with GROUP BY, and every GROUP BY key a plain column from a
 * SINGLE one of the two roles (preserved anchor OR optional side).  For an
 * optional-side key incr_build_self_recompute_sql adds an all-NULL arm.  FULL self
 * joins, 3+-table shapes, mixed-side / expression keys, and non-GROUP-BY
 * (row-level) self outer joins are not supported.
 */
static bool
incr_self_outer_supported_shape(Query *viewQuery)
{
	List	   *tabs;
	Oid			shared_oid;
	ListCell   *lc;
	bool		has_full = false;
	int			key_varno = -1;

	if (viewQuery->groupClause == NIL)
		return false;

	tabs = incr_collect_tables(viewQuery);
	if (list_length(tabs) != 2)
		return false;
	if (!incr_has_self_join(tabs) || !incr_has_outer_join(tabs))
		return false;

	/* Both entries must be the SAME relation (pure two-way self join). */
	shared_oid = ((IncrJoinEntry *) linitial(tabs))->oid;
	foreach(lc, tabs)
	{
		IncrJoinEntry *je = lfirst(lc);

		if (je->oid != shared_oid)
			return false;
		if (je->join_type == JOIN_FULL)
			has_full = true;
	}
	if (has_full)
		return false;			/* FULL self join not supported */

	/*
	 * Every GROUP BY key must be a plain column resolving to the SAME role
	 * (all preserved-side or all optional-side).  Mixed-side / expression keys
	 * can relocate a row between non-NULL groups on an orphan flip, which the
	 * recompute arms do not track.
	 */
	foreach(lc, viewQuery->groupClause)
	{
		SortGroupClause *sgc   = lfirst_node(SortGroupClause, lc);
		TargetEntry	    *te    = get_sortgroupclause_tle(sgc,
														 viewQuery->targetList);
		Node			*gexpr = incr_group_key_expr(viewQuery, te);
		int				 rv;

		if (gexpr == NULL || !IsA(gexpr, Var))
			return false;			/* expression key — not supported */
		if (!incr_try_resolve_var_to_rel((Var *) gexpr, viewQuery->rtable, &rv))
			return false;
		if (key_varno == -1)
			key_varno = rv;
		else if (rv != key_varno)
			return false;			/* mixed-side keys — not supported */
	}
	return true;
}

/*
 * incr_self_recompute_shape — true for a two-way self join (both RTEs the same
 * relation) with GROUP BY that the self recompute builder can maintain for a
 * recompute aggregate (DISTINCT / stddev / collect / FILTER / float): self INNER
 * join (any plain-column keys), or self LEFT/RIGHT join meeting the single-side
 * shape (incr_self_outer_supported_shape).  Used to widen the recompute-shape
 * gate to self joins, which incr_recompute_outer_shape and
 * incr_inner_join_deparse_shape both exclude.
 */
static bool
incr_self_recompute_shape(Query *viewQuery)
{
	List	   *tabs;
	Oid			shared;
	ListCell   *lc;

	if (viewQuery->groupClause == NIL)
		return false;
	tabs = incr_collect_tables(viewQuery);
	if (list_length(tabs) != 2 || !incr_has_self_join(tabs))
		return false;
	shared = ((IncrJoinEntry *) linitial(tabs))->oid;
	foreach(lc, tabs)
		if (((IncrJoinEntry *) lfirst(lc))->oid != shared)
			return false;
	if (incr_has_outer_join(tabs))
		return incr_self_outer_supported_shape(viewQuery);	/* single-side keys */
	return true;										/* self INNER: any keys */
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

	/*
	 * FULL JOIN: the affected region also contains rows matched on the OTHER
	 * side's key — in particular a previously unmatched (NULL-extended) row that
	 * the delta now matches has a NULL delta-side key and would be missed by the
	 * delete above, leaving a stale phantom.  Mirror the INSERT's region filter
	 * by also deleting via each non-preserved table's join-key matview column.
	 */
	if (has_full_join)
	{
		foreach(lc, all_tables)
		{
			IncrJoinEntry *je = lfirst(lc);
			Node		  *q;
			const char	  *other_fk_col;
			const char	  *mv_other_col = NULL;
			ListCell	  *tlc;

			if (je->varno == preserved_varno)
				continue;
			q = find_connecting_qual(all_tables, je->varno, preserved_varno);
			if (q == NULL)
				q = je->quals;
			other_fk_col = incr_qual_get_colname_for_varno(q, viewQuery->rtable,
														   je->varno);
			if (other_fk_col == NULL)
				continue;
			foreach(tlc, viewQuery->targetList)
			{
				TargetEntry *te = lfirst_node(TargetEntry, tlc);
				Var		   *v;
				int			rv;

				if (te->resjunk || !IsA(te->expr, Var))
					continue;
				v = (Var *) te->expr;
				if (v->varno != je->varno)
					continue;
				if (strcmp(incr_resolve_var_colname(v, viewQuery->rtable, &rv),
						   other_fk_col) == 0)
				{
					mv_other_col = te->resname;
					break;
				}
			}
			if (mv_other_col != NULL && strcmp(mv_other_col, mv_peer_col) != 0)
				appendStringInfo(&buf,
								 "\n     OR %s IN (SELECT jkey FROM _aff_)",
								 quote_identifier(mv_other_col));
		}
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
	char	   *idxname;
	Oid			idxoid;

	/*
	 * Deterministic name so we can locate the index afterward to record its
	 * dependency.  One incremental matview has exactly one such index.
	 */
	idxname = psprintf("__mv_uniq_%u", mvrelid);

	initStringInfo(&sql);
	appendStringInfo(&sql, "CREATE UNIQUE INDEX %s ON %s (",
					 quote_identifier(idxname), mv_qname(mvrelid));
	foreach(lc, groupColNames)
	{
		if (!first) appendStringInfoChar(&sql, ',');
		appendStringInfoString(&sql, quote_identifier(strVal(lfirst(lc))));
		first = false;
	}
	/*
	 * NULLS NOT DISTINCT so a NULL (or partial-NULL) group key is a single
	 * arbiter row for ON CONFLICT — otherwise NULL keys would never conflict and
	 * the delta upsert would pile up duplicate rows.  This is what lets NULL
	 * group keys be maintained with full fidelity (matching a REFRESH) instead
	 * of being excluded.  Identical to a plain unique index for non-NULL keys.
	 */
	appendStringInfoString(&sql, ") NULLS NOT DISTINCT");

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "incr_create_unique_index: SPI_connect failed");
	ret = SPI_execute(sql.data, false, 0);
	SPI_finish();

	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "incr_create_unique_index: failed (%d)", ret);

	/*
	 * Record an INTERNAL dependency from the index to the matview.  This marks
	 * the index as engine-managed infrastructure — just like the delta
	 * triggers — so pg_dump skips dumping it as a standalone CREATE INDEX
	 * (see getIndexes() in pg_dump.c).  On restore the index is recreated by
	 * MatviewIncrSetup instead, avoiding a duplicate-index conflict.
	 */
	CommandCounterIncrement();		/* make the new index visible to syscache */
	idxoid = get_relname_relid(idxname,
							   get_rel_namespace(mvrelid));
	if (OidIsValid(idxoid))
	{
		ObjectAddress idxaddr,
					  mvaddr;

		ObjectAddressSet(idxaddr, RelationRelationId, idxoid);
		ObjectAddressSet(mvaddr, RelationRelationId, mvrelid);
		recordDependencyOn(&idxaddr, &mvaddr, DEPENDENCY_INTERNAL);
	}

	pfree(idxname);
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
				is_update,
				is_truncate;
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
	is_truncate = TRIGGER_FIRED_BY_TRUNCATE(trigdata->tg_event);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "matview_delta_apply: SPI_connect failed");

	/*
	 * Skip maintenance while the matview is unpopulated.  An unpopulated
	 * incremental matview has no baseline to apply deltas to — for example
	 * during pg_dump/restore the delta triggers are installed before the
	 * dump's COPY loads the source tables and before the matview is populated
	 * by the dump's REFRESH.  Any DML in that window is captured wholesale by
	 * the eventual REFRESH, so applying deltas now is both pointless and
	 * (for the HAVING/MIN-MAX paths, which read the matview) an error.
	 */
	{
		HeapTuple	cltup = SearchSysCache1(RELOID, ObjectIdGetDatum(mvrelid));
		bool		populated = true;

		if (HeapTupleIsValid(cltup))
		{
			populated = ((Form_pg_class) GETSTRUCT(cltup))->relispopulated;
			ReleaseSysCache(cltup);
		}
		if (!populated)
		{
			SPI_finish();
			return PointerGetDatum(NULL);
		}
	}

	/* ----- TRUNCATE: no transition data exists, fall back to full refresh ----- */
	if (is_truncate)
	{
		StringInfoData refresh_sql;
		char	   *nspname = get_namespace_name(get_rel_namespace(mvrelid));
		char	   *relname = get_rel_name(mvrelid);

		if (relname == NULL)		/* matview already dropped in this txn */
		{
			SPI_finish();
			return PointerGetDatum(NULL);
		}

		/*
		 * The stored view query carries the hidden __mv_count__ target
		 * (injected at CREATE time), so a standard non-concurrent REFRESH
		 * rebuilds every column — visible and hidden — from the current
		 * source-table contents.  REFRESH performs a heap swap on the matview
		 * and issues no DML against the source tables, so it does not re-enter
		 * this trigger.
		 */
		initStringInfo(&refresh_sql);
		appendStringInfo(&refresh_sql,
						 "REFRESH MATERIALIZED VIEW %s",
						 quote_qualified_identifier(nspname, relname));

		ret = SPI_execute(refresh_sql.data, false, 0);
		if (ret != SPI_OK_UTILITY)
			elog(ERROR, "matview_delta_apply: TRUNCATE refresh failed: %s",
				 SPI_result_code_string(ret));

		pfree(refresh_sql.data);
		SPI_finish();
		return PointerGetDatum(NULL);
	}

	/* Register __mv_newtable / __mv_oldtable as ENRs visible to SPI queries */
	SPI_register_trigger_data(trigdata);

	/* Allow DML on the matview during delta application */
	OpenMatViewIncrementalMaintenance();

	/* RowExclusiveLock — sufficient for non-conflicting group keys */
	LockRelationOid(mvrelid, RowExclusiveLock);

	/*
	 * ----- serialization lock (recompute / multiset shapes) -----
	 *
	 * Recompute/absolute-overwrite and multiset shapes (row-level projections,
	 * UNION ALL, outer join, self-join, MIN/MAX) store a matview-level advisory
	 * lock in lock_sql.  Run it FIRST, as its own SPI statement, for every
	 * event (insert/update/delete): a concurrent maintainer of the same matview
	 * blocks here until we commit, and because it is a separate statement, the
	 * delta statements that follow take fresh READ COMMITTED snapshots that
	 * already include our committed changes — eliminating lost updates without
	 * requiring REPEATABLE READ.  Additive shapes store NULL here and skip it,
	 * keeping their per-group write concurrency.
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

	/*
	 * UNION ALL keeps duplicates, so the matview is the multiset union of the
	 * branches: just INSERT the branch's delta rows verbatim (one matview row
	 * per delta row).  No __mv_count__, no GROUP BY, no ON CONFLICT dedup.
	 *
	 *   INSERT INTO mv (col1, ...) SELECT expr1, ... FROM delta [JOIN ...] [WHERE]
	 */
	appendStringInfo(&buf, "INSERT INTO %s (", mvname);
	first = true;
	foreach(vlc, view_cols)
	{
		TargetEntry *vte = lfirst_node(TargetEntry, vlc);

		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(vte->resname));
		first = false;
	}
	appendStringInfoString(&buf, ") SELECT ");

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

	/*
	 * UNION ALL keeps duplicates, so a delete must remove exactly the deleted
	 * MULTIPLICITY of each tuple from the matview (mirror of the row-level
	 * multiset delete): aggregate the branch delta into one row per distinct
	 * output tuple with its count _k, number the matview's copies of that tuple,
	 * and drop the first _k.
	 *
	 *   DELETE FROM mv WHERE ctid IN (
	 *     SELECT s.ctid FROM (
	 *       SELECT _m.ctid, row_number() OVER (PARTITION BY _m.col...) _rn, _rd._k
	 *       FROM mv _m JOIN (SELECT <expr AS col>..., count(*) _k
	 *                        FROM delta [JOIN] [WHERE] GROUP BY <expr>) _rd
	 *         ON (_m.col IS NOT DISTINCT FROM _rd.col AND ...)
	 *     ) s WHERE s._rn <= s._k)
	 */
	{
		StringInfoData	part,
						sel,
						grp,
						joincond;
		Node		   *wq = incr_get_where_qual(branchQuery);

		initStringInfo(&part);
		initStringInfo(&sel);
		initStringInfo(&grp);
		initStringInfo(&joincond);

		first = true;
		forboth(vlc, view_cols, blc, branch_cols)
		{
			TargetEntry    *vte = lfirst_node(TargetEntry, vlc);
			TargetEntry    *bte = lfirst_node(TargetEntry, blc);
			const char	   *cq = quote_identifier(vte->resname);
			StringInfoData	ebuf;

			initStringInfo(&ebuf);
			incr_deparse_where_qual((Node *) bte->expr, branchQuery->rtable,
									delta_varno, &ebuf);
			if (!first)
			{
				appendStringInfoChar(&part, ',');
				appendStringInfoChar(&sel, ',');
				appendStringInfoChar(&grp, ',');
				appendStringInfoString(&joincond, " AND ");
			}
			appendStringInfo(&part, "_m.%s", cq);
			appendStringInfo(&sel, "%s AS %s", ebuf.data, cq);
			appendStringInfoString(&grp, ebuf.data);
			appendStringInfo(&joincond, "_m.%s IS NOT DISTINCT FROM _rd.%s", cq, cq);
			first = false;
		}

		appendStringInfo(&buf,
						 "DELETE FROM %s WHERE ctid IN ("
						 "SELECT s.ctid FROM ("
						 "SELECT _m.ctid, row_number() OVER (PARTITION BY %s) AS _rn, _rd._k "
						 "FROM %s _m JOIN (SELECT %s, count(*) AS _k",
						 mvname, part.data, mvname, sel.data);
		incr_append_from_join(&buf, branchQuery, delta_varno, delta_table, join_list);
		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, branchQuery->rtable, delta_varno, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
		appendStringInfo(&buf,
						 " GROUP BY %s) _rd ON (%s)) s WHERE s._rn <= s._k)",
						 grp.data, joincond.data);
	}

	return buf.data;
}


/*
 * incr_setup_union_all
 *
 * Set up incremental maintenance for a UNION ALL matview.  UNION ALL keeps
 * duplicates, so the matview is maintained as the plain multiset union of its
 * branches: every branch's source-table change inserts/deletes the matching
 * matview rows verbatim (one matview row per branch row), with no __mv_count__
 * column, no dedup, and no unique index.  A dump/restore reloads the raw rows
 * (duplicates included), which is already correct, so MatviewIncrPostRefresh
 * has nothing to do for this shape.
 *
 * For each UNION ALL branch, install per-source-table triggers carrying the
 * multiset INSERT/DELETE delta SQL.
 */
static void
incr_setup_union_all(Oid mvrelid, Query *viewQuery, bool mv_populated)
{
	List	   *branches = NIL;
	ListCell   *lc;

	(void) mv_populated;		/* nothing populated-state-specific to do */

	incr_collect_union_branches(viewQuery, &branches);

	foreach(lc, branches)
	{
		Query	   *branchQuery = (Query *) lfirst(lc);
		List	   *all_tables  = incr_collect_tables(branchQuery);
		ListCell   *jlc;
		char	   *ins_sql,
				   *del_sql;

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

			/* multiset maintenance needs no cleanup step (no __mv_count__) */
			incr_store_catalog(mvrelid, delta->oid,
							   ins_sql, del_sql, "SELECT 1", NULL,
								   incr_build_mv_lock_sql(mvrelid));
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
 * incr_has_distinct_agg
 * Returns true if viewQuery's target list contains any DISTINCT aggregate
 * (e.g. COUNT(DISTINCT x)).  These can't be maintained by a per-row delta, so
 * the matview is routed to the recompute-affected-groups path.
 */
static bool
incr_has_distinct_agg(Query *viewQuery)
{
	ListCell *lc;

	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (!te->resjunk && IsA(te->expr, Aggref) &&
			((Aggref *) te->expr)->aggdistinct != NIL)
			return true;
	}
	return false;
}

/*
 * incr_is_recompute_only_func
 * True for aggregates that can't be maintained additively or by a per-row delta
 * and so must be recomputed from the live table(s): the stddev/variance family
 * and bool_and/bool_or.  The recompute path renders fname(arg) directly, so once
 * a matview is on that path these come for free.
 */
static bool
incr_is_recompute_only_func(const char *fname)
{
	return (strcmp(fname, "stddev") == 0 || strcmp(fname, "stddev_samp") == 0 ||
			strcmp(fname, "stddev_pop") == 0 || strcmp(fname, "variance") == 0 ||
			strcmp(fname, "var_samp") == 0 || strcmp(fname, "var_pop") == 0 ||
			strcmp(fname, "bool_and") == 0 || strcmp(fname, "bool_or") == 0 ||
			strcmp(fname, "every") == 0 ||
			/* collect aggregates — no additive delta; recomputed verbatim.  The
			 * unordered result's multiset matches a full REFRESH (element order
			 * is unspecified, as SQL allows).  Ordered variants (aggorder) are
			 * rejected by the recompute-shape gate. */
			strcmp(fname, "string_agg") == 0 || strcmp(fname, "array_agg") == 0 ||
			strcmp(fname, "json_agg") == 0 || strcmp(fname, "jsonb_agg") == 0);
}

/*
 * incr_needs_recompute
 * True if the matview must be maintained by recomputing the affected groups from
 * the live table(s): it has a DISTINCT aggregate or a recompute-only function
 * (stddev/variance/bool_and/bool_or).  Such a matview is routed entirely to
 * the deparse-based recompute engine (incr_build_recompute_sql).
 */
static bool
incr_needs_recompute(Query *viewQuery)
{
	ListCell *lc;

	if (incr_has_distinct_agg(viewQuery))
		return true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		Aggref	   *agg;
		char	   *fname;

		if (te->resjunk || !IsA(te->expr, Aggref))
			continue;
		agg = (Aggref *) te->expr;
		fname = get_func_name(agg->aggfnoid);

		/* recompute-only funcs (stddev/bool/collect), FILTERed aggregates, and
		 * float SUM/AVG (recompute avoids running-total rounding drift) all use
		 * the recompute engine rather than an additive delta. */
		if (incr_is_recompute_only_func(fname))
			return true;
		if (agg->aggfilter != NULL)
			return true;
		if ((strcmp(fname, "sum") == 0 || strcmp(fname, "avg") == 0) &&
			(agg->aggtype == FLOAT4OID || agg->aggtype == FLOAT8OID))
			return true;
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
			/* SUM accumulates NULL-safely (a NULL running total must not be
			 * poisoned by + ins, and vice versa); COUNT is never NULL. */
			char	   *sum_expr = incr_nullsafe_accum(
				psprintf("%s.%s", mvname, sum_q),
				psprintf("ins.%s", sum_q), false);

			appendStringInfo(&buf,
							 "%s=%s"
							 ",%s=%s.%s+ins.%s"
							 ",%s=(%s::%s/NULLIF(%s.%s+ins.%s,0))",
							 sum_q, sum_expr,
							 cnt_q, mvname, cnt_q, cnt_q,
							 colq, sum_expr, type_name,
							 mvname, cnt_q, cnt_q);
		}
		else if (IsA(te->expr, Aggref))
		{
			char	   *fn = get_func_name(((Aggref *) te->expr)->aggfnoid);
			const char *scq = (strcmp(fn, "sum") == 0)
				? incr_sumcnt_sibling(viewQuery, te->resname) : NULL;

			if (strcmp(fn, "min") == 0)
				appendStringInfo(&buf, "%s=LEAST(%s.%s,ins.%s)",
								 colq, mvname, colq, colq);
			else if (strcmp(fn, "max") == 0)
				appendStringInfo(&buf, "%s=GREATEST(%s.%s,ins.%s)",
								 colq, mvname, colq, colq);
			else if (scq != NULL)
				/* SUM with a non-NULL counter: the counter itself is maintained
				 * by its own (standalone) entry in this SET; here we only render
				 * the visible SUM as NULL when the running counter reaches 0. */
				appendStringInfo(&buf,
								 "%s=CASE WHEN %s.%s+ins.%s=0 THEN NULL ELSE %s END",
								 colq, mvname, scq, scq,
								 incr_nullsafe_accum(psprintf("%s.%s", mvname, colq),
													 psprintf("ins.%s", colq), false));
			else
				/* COUNT (and SUM without a counter): NULL-safe so a NULL running
				 * total left by an earlier delete is not poisoned by "+ ins". */
				appendStringInfo(&buf, "%s=%s", colq,
								 incr_nullsafe_accum(psprintf("%s.%s", mvname, colq),
													 psprintf("ins.%s", colq), false));
		}
		else
			appendStringInfo(&buf, "%s=%s", colq,
							 incr_nullsafe_accum(psprintf("%s.%s", mvname, colq),
												 psprintf("ins.%s", colq), false));
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

			char	   *sum_expr = incr_nullsafe_accum(
				psprintf("%s.%s", mvname, sum_q),
				psprintf("EXCLUDED.%s", sum_q), false);

			appendStringInfo(&buf,
							 "%s=%s"
							 ",%s=%s.%s+EXCLUDED.%s"
							 ",%s=(%s::%s/NULLIF(%s.%s+EXCLUDED.%s,0))",
							 sum_q, sum_expr,
							 cnt_q, mvname, cnt_q, cnt_q,
							 colq, sum_expr, type_name,
							 mvname, cnt_q, cnt_q);
		}
		else if (IsA(te->expr, Aggref))
		{
			char	   *fn = get_func_name(((Aggref *) te->expr)->aggfnoid);
			const char *scq = (strcmp(fn, "sum") == 0)
				? incr_sumcnt_sibling(viewQuery, te->resname) : NULL;

			if (strcmp(fn, "min") == 0)
				appendStringInfo(&buf, "%s=LEAST(%s.%s,EXCLUDED.%s)",
								 colq, mvname, colq, colq);
			else if (strcmp(fn, "max") == 0)
				appendStringInfo(&buf, "%s=GREATEST(%s.%s,EXCLUDED.%s)",
								 colq, mvname, colq, colq);
			else if (scq != NULL)
				/* SUM: counter (maintained by its own SET entry) drives NULL display */
				appendStringInfo(&buf,
								 "%s=CASE WHEN %s.%s+EXCLUDED.%s=0 THEN NULL ELSE %s END",
								 colq, mvname, scq, scq,
								 incr_nullsafe_accum(psprintf("%s.%s", mvname, colq),
													 psprintf("EXCLUDED.%s", colq), false));
			else
				appendStringInfo(&buf, "%s=%s", colq,
								 incr_nullsafe_accum(psprintf("%s.%s", mvname, colq),
													 psprintf("EXCLUDED.%s", colq), false));
		}
		else
			appendStringInfo(&buf, "%s=%s", colq,
							 incr_nullsafe_accum(psprintf("%s.%s", mvname, colq),
												 psprintf("EXCLUDED.%s", colq), false));
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
		/*
		 * Always alias the group column to its matview output name.  Later
		 * CTEs and the upd/DELETE reference these keys by output name
		 * (groupColNames = resname), so the source column must be exposed under
		 * that name even when it differs (e.g. "SELECT g AS k").
		 */
		appendStringInfo(&buf, "%s AS %s", ebuf.data,
						 quote_identifier(te->resname));
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
			/* Always alias to output name (see affected CTE above) */
			appendStringInfo(&buf, "%s AS %s", ebuf2.data,
							 quote_identifier(te2->resname));
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
	/*
	 * COUNT(arg) delta columns: COUNT(arg) AS del_<resname> for each
	 * count-of-an-argument aggregate — both the visible count(col) and the
	 * hidden AVG count (__mv_avgcnt_*).  These exclude NULL arguments, so they
	 * must be decremented by COUNT(arg) of the delta, NOT by del_cnt (COUNT(*)),
	 * which would over-subtract whenever the argument is NULL.  COUNT(*) and
	 * __mv_count__ keep using del_cnt.
	 */
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
			if (strcmp(te2->resname, MATVIEW_INCR_COUNT_COL) == 0)
				continue;
			agg2 = (Aggref *) te2->expr;
			if (strcmp(get_func_name(agg2->aggfnoid), "count") != 0)
				continue;
			if (agg2->aggstar || agg2->args == NIL)		/* COUNT(*) uses del_cnt */
				continue;

			arg_te = linitial_node(TargetEntry, agg2->args);
			initStringInfo(&ebuf2);
			incr_deparse_where_qual((Node *) arg_te->expr, viewQuery->rtable,
									delta_varno, &ebuf2);
			appendStringInfo(&buf, ",COUNT(%s) AS %s",
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

		/* JOIN affected ON (alias.col IS NOT DISTINCT FROM affected.resname AND ...)
		 * — NULL-safe so a NULL/partial-NULL group key is rescanned, not dropped. */
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
			appendStringInfo(&buf, "%s IS NOT DISTINCT FROM affected.%s",
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

			/*
			 * Restrict to the affected groups NULL-safely:
			 *   WHERE EXISTS (SELECT 1 FROM affected
			 *                 WHERE <src1> IS NOT DISTINCT FROM affected.<out1> AND ...)
			 * The left side references the live source table (source column name),
			 * the right side the affected CTE (output name).  IS NOT DISTINCT FROM
			 * (not IN/=) so a NULL/partial-NULL key group is rescanned, not dropped.
			 */
			appendStringInfoString(&buf, " WHERE EXISTS (SELECT 1 FROM affected WHERE ");
			first = true;
			foreach(glc, grp_tes)
			{
				TargetEntry   *te = lfirst(glc);
				StringInfoData srcbuf;

				if (!first)
					appendStringInfoString(&buf, " AND ");
				first = false;
				initStringInfo(&srcbuf);
				incr_deparse_where_qual((Node *) te->expr, viewQuery->rtable,
										delta_varno, &srcbuf);
				appendStringInfo(&buf, "%s IS NOT DISTINCT FROM affected.%s",
								 srcbuf.data, quote_identifier(te->resname));
			}
			appendStringInfoChar(&buf, ')');
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
					/* COUNT(*) uses del_cnt; COUNT(arg) excludes NULLs and has
					 * its own COUNT(arg) delta column. */
					delta_colname = (agg2->aggstar || agg2->args == NIL)
						? "del_cnt"
						: psprintf("del_%s", te->resname);
				}
				else if (strcmp(fname2, "sum") == 0)
				{
					is_delta = true;
					delta_colname = psprintf("del_%s", te->resname);
				}
			}

			if (is_delta)
			{
				/* SUM with a non-NULL counter: keep delta arithmetic for the
				 * running total (so it composes with the insert delta) but render
				 * the visible value as NULL once the counter (also delta-
				 * maintained, by its own standalone entry) reaches 0. */
				const char *scq = (IsA(te->expr, Aggref) &&
								   strcmp(get_func_name(((Aggref *) te->expr)->aggfnoid),
										  "sum") == 0)
					? incr_sumcnt_sibling(viewQuery, te->resname) : NULL;
				char	   *run = incr_nullsafe_accum(
					psprintf("%s.%s", mvname, colq),
					psprintf("old_delta.%s", quote_identifier(delta_colname)),
					true);

				if (scq != NULL)
				{
					const char *del_scnt = quote_identifier(
						psprintf("del_%s%s", MATVIEW_INCR_SUMCNT_PREFIX,
								 te->resname));

					appendStringInfo(&buf,
									 "%s=CASE WHEN %s.%s-old_delta.%s=0 THEN NULL ELSE %s END",
									 colq, mvname, scq, del_scnt, run);
				}
				else
					appendStringInfo(&buf, "%s=%s", colq, run);
			}
			else
				appendStringInfo(&buf, "%s=new_agg.%s", colq, colq);
		}
	}
	/* JOIN old_delta ON new_agg.k IS NOT DISTINCT FROM old_delta.k (NULL-safe,
	 * not USING, so a NULL/partial-NULL key group still joins). */
	appendStringInfoString(&buf, " FROM new_agg JOIN old_delta ON (");
	first = true;
	foreach(gcl, groupColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(gcl)));

		if (!first)
			appendStringInfoString(&buf, " AND ");
		first = false;
		appendStringInfo(&buf, "new_agg.%s IS NOT DISTINCT FROM old_delta.%s",
						 colq, colq);
	}
	appendStringInfoString(&buf, ") WHERE ");
	first = true;
	foreach(gcl, groupColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(gcl)));

		if (!first)
			appendStringInfoString(&buf, " AND ");
		first = false;
		appendStringInfo(&buf, "%s.%s IS NOT DISTINCT FROM new_agg.%s",
						 mvname, colq, colq);
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
		appendStringInfo(&buf, "%s.%s IS NOT DISTINCT FROM affected.%s",
						 mvname, colq, colq);
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
		appendStringInfo(&buf, "new_agg.%s IS NOT DISTINCT FROM %s.%s",
						 colq, mvname, colq);
	}
	appendStringInfoChar(&buf, ')');

	return buf.data;
}

/*
 * incr_build_mv_lock_sql — serialize all incremental maintenance of a matview
 *
 * Returns a SELECT that takes a transaction-scoped advisory lock keyed on the
 * matview OID.  matview_delta_apply() runs this as its own SPI statement at the
 * very start of maintenance (before the INSERT delta), so a transaction
 * maintaining this matview holds the lock until commit and concurrent
 * maintainers run one at a time.  Combined with READ COMMITTED's per-statement
 * snapshots, this means every recompute/delete statement that runs after the
 * lock sees a fresh snapshot that already includes whatever the previous
 * maintainer committed.  That makes the recompute / absolute-overwrite and
 * multiset shapes — row-level projections, UNION ALL, outer join, self-join and
 * MIN/MAX — correct under concurrent writers at any isolation level, not just
 * REPEATABLE READ and above.  (Embedding the lock inside the delta statement
 * would not help: READ COMMITTED fixes that statement's snapshot before the
 * lock is acquired, so the recompute would still read pre-lock data.)
 *
 * Additive shapes — single-table and INNER JOIN SUM/COUNT/AVG — are left
 * lock-free: their ON CONFLICT upserts already serialize on the matview row
 * lock and compose correctly under READ COMMITTED, so they keep full per-group
 * write concurrency.  Those store NULL for lock_sql and skip this step.
 *
 * Lock key space: the single-argument pg_advisory_xact_lock(int8), which is a
 * different space from the (int4,int4) two-key form, so it never collides with
 * any other advisory lock.
 */
static char *
incr_build_mv_lock_sql(Oid mvrelid)
{
	return psprintf("SELECT pg_advisory_xact_lock(%u::bigint)",
					(unsigned) mvrelid);
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

	/*
	 * Set-operation (UNION ALL) queries: the rtable subqueries are the union
	 * branches, not FROM-subqueries to inline.  Normalizing them would rewrite
	 * the branch RTEs out from under the setOperations tree and corrupt it
	 * (incr_collect_union_branches would then find a non-subquery leaf).  The
	 * UNION ALL setup path handles each branch directly, so leave the query as
	 * is.
	 */
	if (viewQuery->setOperations != NULL)
		return viewQuery;

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
