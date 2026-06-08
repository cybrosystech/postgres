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
 * Four plans per matview:
 *   INCR_PLAN_INS — apply __mv_newtable delta to matview (INSERT ON CONFLICT)
 *   INCR_PLAN_DEL — subtract __mv_oldtable delta from matview (UPDATE)
 *   INCR_PLAN_CLN — remove zero-count groups (DELETE WHERE __mv_count__ <= 0)
 *   INCR_PLAN_HAV — recompute __mv_having_ok__ for all active groups (HAVING)
 * ----------
 */
#define INCR_PLAN_INS	0
#define INCR_PLAN_DEL	1
#define INCR_PLAN_CLN	2
#define INCR_PLAN_HAV	3
#define INCR_NUM_PLANS	4

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
	int		varno;		/* varno of this table in viewQuery->rtable */
	Oid		oid;		/* table OID */
	Node   *quals;		/* ON condition for this join step */
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
static void incr_collect_group_cols(Query *viewQuery, List **groupColNames);
static void incr_append_from_join(StringInfo buf, Query *viewQuery,
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
static void incr_store_catalog(Oid mvrelid, Oid srctable,
							   const char *ins_sql,
							   const char *del_sql,
							   const char *cln_sql,
							   const char *having_sql);
static void incr_create_unique_index(Oid mvrelid, List *groupColNames);
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

	if (viewQuery->groupClause == NIL)
	{
		*reason = "query has no GROUP BY clause";
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
		*reason = "set operations are not supported";
		return false;
	}
	if (viewQuery->hasSubLinks)
	{
		*reason = "subqueries are not supported";
		return false;
	}
	if (viewQuery->distinctClause != NIL)
	{
		*reason = "DISTINCT is not supported";
		return false;
	}

	/* Count base relations: 1 = Phase 1 (single table), 2 = Phase 2 (INNER JOIN) */
	foreach(lc, viewQuery->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		/* PG19 RTE_GROUP, RTE_RESULT, and explicit-JOIN's RTE_JOIN are bookkeeping — skip */
		if (rte->rtekind == RTE_GROUP || rte->rtekind == RTE_RESULT ||
			rte->rtekind == RTE_JOIN)
			continue;

		if (rte->rtekind == RTE_RELATION)
			nbasetables++;
		else
		{
			*reason = "only plain table references are supported (no functions, VALUES, etc.)";
			return false;
		}
	}

	if (nbasetables == 1)
	{
		/* Phase 1: single source table — nothing extra to check */
	}
	else if (nbasetables >= 2)
	{
		/*
		 * Phase 2+: N-table INNER JOIN.  Require an explicit JOIN ... ON tree
		 * rooted at the single FromExpr.fromlist entry.  Walk the JoinExpr
		 * tree to confirm every step is INNER with a non-null ON condition.
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

		/* Walk every JoinExpr in the tree: must all be INNER with quals */
		{
			List	   *stack = list_make1(jtree_root);
			ListCell   *slc;

			foreach(slc, stack)
			{
				JoinExpr   *je = lfirst(slc);

				if (!IsA(je, JoinExpr))
					continue;
				if (je->jointype != JOIN_INNER)
				{
					*reason = "only INNER JOINs are supported for incremental refresh";
					return false;
				}
				if (je->quals == NULL)
				{
					*reason = "each JOIN step must have an ON condition";
					return false;
				}
				if (IsA(je->larg, JoinExpr))
					stack = lappend(stack, je->larg);
				if (IsA(je->rarg, JoinExpr))
					stack = lappend(stack, je->rarg);
			}
		}
	}

	/* Only Var (group col) or Aggref (SUM/COUNT/AVG) in target list */
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk)
			continue;
		if (incr_is_hidden_col(te->resname))	/* skip hidden maintenance columns */
			continue;
		if (IsA(te->expr, Var))
			continue;
		if (IsA(te->expr, Aggref))
		{
			Aggref	   *agg = (Aggref *) te->expr;
			char	   *fname = get_func_name(agg->aggfnoid);

			if (strcmp(fname, "sum") == 0 || strcmp(fname, "count") == 0 ||
				strcmp(fname, "avg") == 0)
			{
				/* Validate aggregate argument expressions (Phase 6) */
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
							   "(supported: SUM, COUNT, AVG)", fname);
			return false;
		}
		*reason = "only column references and SUM/COUNT aggregates are allowed";
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

	/* Count base tables to determine phase */
	foreach(lc, viewQuery->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		if (rte->rtekind == RTE_RELATION)
			nbasetables++;
	}

	hasHaving = (viewQuery->havingQual != NULL);

	/* Step 1: unique index on GROUP BY columns (same for all phases) */
	incr_collect_group_cols(viewQuery, &groupColNames);
	incr_create_unique_index(mvrelid, groupColNames);

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
		/* ---- Phase 1: single source table ---- */
		Oid			srctable = incr_get_source_table(viewQuery);

		ins_sql = incr_build_ins_sql_gen(mvrelid, viewQuery, -1,
										 MATVIEW_INCR_NEWTABLE, NIL);
		del_sql = incr_build_del_sql_gen(mvrelid, viewQuery, -1,
										 MATVIEW_INCR_OLDTABLE, NIL);
		incr_store_catalog(mvrelid, srctable, ins_sql, del_sql, cln_sql, hav_sql);

		/*
		 * HAVING backfill: the initial CREATE MATERIALIZED VIEW only inserted
		 * HAVING-passing groups.  Insert all remaining groups (with
		 * __mv_having_ok__ = false) so their running totals are tracked from
		 * the start.  ON CONFLICT DO NOTHING leaves passing groups intact.
		 */
		if (hasHaving)
		{
			char	   *backfill_sql = incr_build_backfill_sql_gen(
				mvrelid, viewQuery, -1, mv_qname(srctable), NIL);
			int			spi_ret;

			OpenMatViewIncrementalMaintenance();
			SPI_connect();
			spi_ret = SPI_execute(backfill_sql, false, 0);
			SPI_finish();
			CloseMatViewIncrementalMaintenance();
			if (spi_ret < 0)
				elog(ERROR, "DBblue: HAVING backfill failed (code %d)", spi_ret);
		}

		incr_install_triggers(mvrelid, srctable);
	}
	else
	{
		/* ---- Phase 2+: N-table INNER JOIN ---- */
		List	   *all_tables = incr_collect_tables(viewQuery);
		ListCell   *jlc;

		foreach(jlc, all_tables)
		{
			IncrJoinEntry *delta = lfirst(jlc);
			List		  *join_list = incr_build_join_list_for_delta(all_tables,
																	 delta->varno);

			ins_sql = incr_build_ins_sql_gen(mvrelid, viewQuery,
											 delta->varno, MATVIEW_INCR_NEWTABLE,
											 join_list);
			del_sql = incr_build_del_sql_gen(mvrelid, viewQuery,
											 delta->varno, MATVIEW_INCR_OLDTABLE,
											 join_list);
			incr_store_catalog(mvrelid, delta->oid, ins_sql, del_sql, cln_sql, hav_sql);
			incr_install_triggers(mvrelid, delta->oid);
		}

		/*
		 * HAVING backfill: seed all groups from the real tables so groups that
		 * initially fail HAVING are tracked.  Pick the first table as the
		 * "anchor"; its join_list covers all others.
		 */
		if (hasHaving)
		{
			IncrJoinEntry *first = linitial(all_tables);
			List		  *join_list = incr_build_join_list_for_delta(all_tables,
																	 first->varno);
			char		  *backfill_sql = incr_build_backfill_sql_gen(
				mvrelid, viewQuery,
				first->varno, mv_qname(first->oid), join_list);
			int			   spi_ret;

			OpenMatViewIncrementalMaintenance();
			SPI_connect();
			spi_ret = SPI_execute(backfill_sql, false, 0);
			SPI_finish();
			CloseMatViewIncrementalMaintenance();
			if (spi_ret < 0)
				elog(ERROR, "DBblue: HAVING backfill (JOIN) failed (code %d)", spi_ret);
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
		StringInfoData	jbuf;

		initStringInfo(&jbuf);
		incr_deparse_where_qual(je->quals, viewQuery->rtable, delta_varno, &jbuf);
		appendStringInfo(buf, " JOIN %s _j%d_ ON (%s)",
						 mv_qname(je->oid), je->varno, jbuf.data);
	}
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

			if (connecting_qual != NULL)
			{
				IncrJoinEntry *new_je = palloc(sizeof(IncrJoinEntry));

				new_je->varno = je->varno;
				new_je->oid = je->oid;
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

/* ============================================================
 * Catalog helpers
 * ============================================================
 */

static void
incr_store_catalog(Oid mvrelid, Oid srctable,
				   const char *ins_sql,
				   const char *del_sql,
				   const char *cln_sql,
				   const char *having_sql)
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

	attnum = (plan_type == INCR_PLAN_INS) ? Anum_pg_dbblue_matview_ins_sql :
			 (plan_type == INCR_PLAN_DEL) ? Anum_pg_dbblue_matview_del_sql :
			 (plan_type == INCR_PLAN_CLN) ? Anum_pg_dbblue_matview_cln_sql :
											Anum_pg_dbblue_matview_having_sql;

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
