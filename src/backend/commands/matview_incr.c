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
#include "catalog/pg_dbblue_matview.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/matview.h"
#include "commands/matview_incr.h"
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
 * Three plans per matview:
 *   INCR_PLAN_INS — apply __mv_newtable delta to matview (INSERT ON CONFLICT)
 *   INCR_PLAN_DEL — subtract __mv_oldtable delta from matview (UPDATE)
 *   INCR_PLAN_CLN — remove zero-count groups (DELETE WHERE __mv_count__ <= 0)
 * ----------
 */
#define INCR_PLAN_INS	0
#define INCR_PLAN_DEL	1
#define INCR_PLAN_CLN	2
#define INCR_NUM_PLANS	3

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
 * Fixed aliases used in Phase 2 delta SQL.
 * The transition table (__mv_newtable/__mv_oldtable) gets alias _d_;
 * the other (non-changing) source table gets alias _j_.
 */
#define INCR_DELTA_ALIAS	"_d_"
#define INCR_JOIN_ALIAS		"_j_"

/* ----------
 * Forward declarations
 * ----------
 */
static Oid	incr_get_source_table(Query *viewQuery);
static void incr_get_join_info(Query *viewQuery,
							   int *tbl1_varno, Oid *tbl1_oid,
							   int *tbl2_varno, Oid *tbl2_oid,
							   Node **join_quals);
static void incr_deparse_expr(Node *expr, List *rtable,
							  int delta_varno, StringInfo buf);
static void incr_collect_group_cols(Query *viewQuery, List **groupColNames);
static void incr_collect_agg_info(Query *viewQuery,
								  List **aggColNames,
								  List **aggFuncNames,
								  List **aggArgColNames);
static char *incr_build_ins_sql(Oid mvrelid, Query *viewQuery);
static char *incr_build_del_sql(Oid mvrelid, Query *viewQuery);
static char *incr_build_cln_sql(Oid mvrelid);
static char *incr_build_ins_sql_join(Oid mvrelid, Query *viewQuery,
									 int delta_varno, Oid other_oid,
									 Node *join_quals);
static char *incr_build_del_sql_join(Oid mvrelid, Query *viewQuery,
									 int delta_varno, Oid other_oid,
									 Node *join_quals);
static void incr_store_catalog(Oid mvrelid, Oid srctable,
							   const char *ins_sql,
							   const char *del_sql,
							   const char *cln_sql);
static void incr_create_unique_index(Oid mvrelid, List *groupColNames);
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
		*reason = "HAVING is not supported";
		return false;
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
		/* Phase 1: nothing extra to check */
	}
	else if (nbasetables == 2)
	{
		/* Phase 2: require an INNER JOIN condition */
		bool		has_join = false;

		if (IsA(viewQuery->jointree, FromExpr))
		{
			FromExpr   *fe = (FromExpr *) viewQuery->jointree;

			if (fe->quals != NULL)
				has_join = true;	/* implicit join: FROM T1, T2 WHERE ... */
			else if (fe->fromlist != NIL && IsA(linitial(fe->fromlist), JoinExpr))
			{
				JoinExpr   *je = (JoinExpr *) linitial(fe->fromlist);

				if (je->jointype == JOIN_INNER && je->quals != NULL)
					has_join = true;
			}
		}

		if (!has_join)
		{
			*reason = "two source tables require an INNER JOIN condition (Phase 2);"
				" LEFT/OUTER JOINs are Phase 3";
			return false;
		}
	}
	else
	{
		*reason = "more than two source tables not supported; Phase 2 maximum is two tables";
		return false;
	}

	/* Only Var (group col) or Aggref (SUM/COUNT) in target list */
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (te->resjunk)
			continue;
		if (IsA(te->expr, Var))
			continue;
		if (IsA(te->expr, Aggref))
		{
			Aggref	   *agg = (Aggref *) te->expr;
			char	   *fname = get_func_name(agg->aggfnoid);

			if (strcmp(fname, "sum") == 0 || strcmp(fname, "count") == 0)
				continue;

			*reason = psprintf("aggregate \"%s\" not supported in Phase 1 "
							   "(supported: SUM, COUNT)", fname);
			return false;
		}
		*reason = "only column references and SUM/COUNT aggregates are allowed";
		return false;
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
			   *cln_sql;
	int			nbasetables = 0;
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

	/* Step 1: unique index on GROUP BY columns (same for all phases) */
	incr_collect_group_cols(viewQuery, &groupColNames);
	incr_create_unique_index(mvrelid, groupColNames);

	cln_sql = incr_build_cln_sql(mvrelid);

	if (nbasetables == 1)
	{
		/* ---- Phase 1: single source table ---- */
		Oid			srctable = incr_get_source_table(viewQuery);

		ins_sql = incr_build_ins_sql(mvrelid, viewQuery);
		del_sql = incr_build_del_sql(mvrelid, viewQuery);
		incr_store_catalog(mvrelid, srctable, ins_sql, del_sql, cln_sql);
		incr_install_triggers(mvrelid, srctable);
	}
	else
	{
		/* ---- Phase 2: two-table INNER JOIN ---- */
		int			tbl1_varno,
					tbl2_varno;
		Oid			tbl1_oid,
					tbl2_oid;
		Node	   *join_quals;

		incr_get_join_info(viewQuery,
						   &tbl1_varno, &tbl1_oid,
						   &tbl2_varno, &tbl2_oid,
						   &join_quals);

		/* Delta SQL when T1 rows change — join transition table with current T2 */
		ins_sql = incr_build_ins_sql_join(mvrelid, viewQuery,
										  tbl1_varno, tbl2_oid, join_quals);
		del_sql = incr_build_del_sql_join(mvrelid, viewQuery,
										  tbl1_varno, tbl2_oid, join_quals);
		incr_store_catalog(mvrelid, tbl1_oid, ins_sql, del_sql, cln_sql);
		incr_install_triggers(mvrelid, tbl1_oid);

		/* Delta SQL when T2 rows change — join transition table with current T1 */
		ins_sql = incr_build_ins_sql_join(mvrelid, viewQuery,
										  tbl2_varno, tbl1_oid, join_quals);
		del_sql = incr_build_del_sql_join(mvrelid, viewQuery,
										  tbl2_varno, tbl1_oid, join_quals);
		incr_store_catalog(mvrelid, tbl2_oid, ins_sql, del_sql, cln_sql);
		incr_install_triggers(mvrelid, tbl2_oid);
	}

	ereport(DEBUG1,
			(errmsg("DBblue: incremental refresh (Phase %d) set up for matview %s",
					nbasetables, get_rel_name(mvrelid))));
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
 * Append "COUNT(*) AS __mv_count__" to the query's target list.
 * Called before matview creation so the hidden tracking column is populated
 * naturally by the initial SELECT — no ALTER TABLE needed afterward.
 */
void
MatviewIncrAddCountTarget(Query *q)
{
	Aggref	   *aggref;
	TargetEntry *te;

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

	te = makeTargetEntry((Expr *) aggref,
						 list_length(q->targetList) + 1,
						 pstrdup(MATVIEW_INCR_COUNT_COL),
						 false);

	q->targetList = lappend(q->targetList, te);
}

/* ============================================================
 * Internal helpers — query introspection
 * ============================================================
 */

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

/*
 * Collect aggregate column info: output name, function name, arg column name.
 * For COUNT(*), argColName is NULL.
 */
static void
incr_collect_agg_info(Query *viewQuery,
					  List **aggColNames,
					  List **aggFuncNames,
					  List **aggArgColNames)
{
	ListCell   *lc;

	*aggColNames = NIL;
	*aggFuncNames = NIL;
	*aggArgColNames = NIL;

	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		Aggref	   *agg;
		char	   *fname,
				   *argcol = NULL;

		if (te->resjunk || !IsA(te->expr, Aggref))
			continue;

		/* __mv_count__ is handled explicitly by the SQL builders — skip it here */
		if (te->resname && strcmp(te->resname, MATVIEW_INCR_COUNT_COL) == 0)
			continue;

		agg = (Aggref *) te->expr;
		fname = get_func_name(agg->aggfnoid);

		if (agg->args != NIL)
		{
			TargetEntry *arg_te = linitial_node(TargetEntry, agg->args);

			if (IsA(arg_te->expr, Var))
			{
				Var		   *v = (Var *) arg_te->expr;
				RangeTblEntry *rte = rt_fetch(v->varno, viewQuery->rtable);

				argcol = get_attname(rte->relid, v->varattno, false);
			}
		}

		*aggColNames = lappend(*aggColNames, makeString(pstrdup(te->resname)));
		*aggFuncNames = lappend(*aggFuncNames, makeString(pstrdup(fname)));
				/* NULL pointer signals COUNT(*) — no argument column */
		*aggArgColNames = lappend(*aggArgColNames,
								  argcol ? (Node *) makeString(pstrdup(argcol))
										 : NULL);
	}
}

/* ============================================================
 * SQL builders
 * ============================================================
 */

/*
 * incr_build_ins_sql — INSERT delta
 *
 *   INSERT INTO mv (g1, g2, sum_col, cnt_col, __mv_count__)
 *   SELECT g1, g2, SUM(col), COUNT(*), COUNT(*)
 *   FROM __mv_newtable
 *   GROUP BY g1, g2
 *   ON CONFLICT (g1, g2) DO UPDATE SET
 *     sum_col      = mv.sum_col      + EXCLUDED.sum_col,
 *     cnt_col      = mv.cnt_col      + EXCLUDED.cnt_col,
 *     __mv_count__ = mv.__mv_count__ + EXCLUDED.__mv_count__
 */
static char *
incr_build_ins_sql(Oid mvrelid, Query *viewQuery)
{
	StringInfoData buf;
	List	   *groupColNames = NIL;
	List	   *aggColNames = NIL,
			   *aggFuncNames = NIL,
			   *aggArgColNames = NIL;
	ListCell   *gcl,
			   *acl,
			   *fcl,
			   *arcl;
	const char *mvname = mv_qname(mvrelid);
	const char *cntcol = quote_identifier(MATVIEW_INCR_COUNT_COL);
	bool		first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	incr_collect_agg_info(viewQuery, &aggColNames, &aggFuncNames, &aggArgColNames);

	initStringInfo(&buf);

	/* INSERT INTO mv (group_cols, agg_cols, __mv_count__) */
	appendStringInfo(&buf, "INSERT INTO %s (", mvname);
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}
	foreach(acl, aggColNames)
	{
		appendStringInfo(&buf, ",%s", quote_identifier(strVal(lfirst(acl))));
	}
	appendStringInfo(&buf, ",%s) ", cntcol);

	/* SELECT group_cols, agg_exprs, COUNT(*) FROM __mv_newtable GROUP BY group_cols */
	appendStringInfoString(&buf, "SELECT ");
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}

	forthree(acl, aggColNames, fcl, aggFuncNames, arcl, aggArgColNames)
	{
		const char *fname = strVal(lfirst(fcl));
		Node	   *argnode = lfirst(arcl);

		if (strcmp(fname, "count") == 0 && argnode == NULL)
			appendStringInfoString(&buf, ",COUNT(*)");
		else
			appendStringInfo(&buf, ",%s(%s)",
							 fname,
							 quote_identifier(strVal((String *) argnode)));
	}
	appendStringInfo(&buf, ",COUNT(*) FROM %s GROUP BY ", MATVIEW_INCR_NEWTABLE);
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}

	/* ON CONFLICT (group_cols) DO UPDATE SET ... */
	appendStringInfoString(&buf, " ON CONFLICT (");
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}
	appendStringInfoString(&buf, ") DO UPDATE SET ");

	first = true;
	foreach(acl, aggColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(acl)));

		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%s=%s.%s+EXCLUDED.%s", colq, mvname, colq, colq);
		first = false;
	}
	if (!first) appendStringInfoChar(&buf, ',');
	appendStringInfo(&buf, "%s=%s.%s+EXCLUDED.%s", cntcol, mvname, cntcol, cntcol);

	return buf.data;
}

/*
 * incr_build_del_sql — DELETE delta
 *
 *   WITH d AS (
 *     SELECT g1, g2, SUM(col) AS sum_col, COUNT(*) AS __mv_count__
 *     FROM __mv_oldtable GROUP BY g1, g2
 *   )
 *   UPDATE mv SET
 *     sum_col      = mv.sum_col      - d.sum_col,
 *     __mv_count__ = mv.__mv_count__ - d.__mv_count__
 *   FROM d
 *   WHERE mv.g1 = d.g1 AND mv.g2 = d.g2
 */
static char *
incr_build_del_sql(Oid mvrelid, Query *viewQuery)
{
	StringInfoData buf;
	List	   *groupColNames = NIL;
	List	   *aggColNames = NIL,
			   *aggFuncNames = NIL,
			   *aggArgColNames = NIL;
	ListCell   *gcl,
			   *acl,
			   *fcl,
			   *arcl;
	const char *mvname = mv_qname(mvrelid);
	const char *cntcol = quote_identifier(MATVIEW_INCR_COUNT_COL);
	bool		first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	incr_collect_agg_info(viewQuery, &aggColNames, &aggFuncNames, &aggArgColNames);

	initStringInfo(&buf);

	/* WITH d AS (SELECT group_cols, agg_exprs, COUNT(*) FROM __mv_oldtable GROUP BY ...) */
	appendStringInfoString(&buf, "WITH d AS (SELECT ");
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}
	forthree(acl, aggColNames, fcl, aggFuncNames, arcl, aggArgColNames)
	{
		const char *fname = strVal(lfirst(fcl));
		Node	   *argnode = lfirst(arcl);
		const char *colq = quote_identifier(strVal(lfirst(acl)));

		if (strcmp(fname, "count") == 0 && argnode == NULL)
			appendStringInfo(&buf, ",COUNT(*) AS %s", colq);
		else
			appendStringInfo(&buf, ",%s(%s) AS %s",
							 fname,
							 quote_identifier(strVal((String *) argnode)),
							 colq);
	}
	appendStringInfo(&buf, ",COUNT(*) AS %s FROM %s GROUP BY ",
					 cntcol, MATVIEW_INCR_OLDTABLE);
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}
	appendStringInfoString(&buf, ") ");

	/* UPDATE mv SET agg = mv.agg - d.agg, __mv_count__ = mv.__mv_count__ - d.__mv_count__ */
	appendStringInfo(&buf, "UPDATE %s SET ", mvname);
	first = true;
	foreach(acl, aggColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(acl)));

		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%s=%s.%s-d.%s", colq, mvname, colq, colq);
		first = false;
	}
	if (!first) appendStringInfoChar(&buf, ',');
	appendStringInfo(&buf, "%s=%s.%s-d.%s", cntcol, mvname, cntcol, cntcol);

	/* FROM d WHERE mv.g = d.g */
	appendStringInfo(&buf, " FROM d WHERE ");
	first = true;
	foreach(gcl, groupColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(gcl)));

		if (!first) appendStringInfoString(&buf, " AND ");
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
 * Phase 2 helpers
 * ============================================================
 */

/*
 * incr_get_join_info
 * Walk the rtable to find the two RTE_RELATION entries and extract the
 * INNER JOIN qualification from either a JoinExpr or FromExpr->quals.
 */
static void
incr_get_join_info(Query *viewQuery,
				   int *tbl1_varno, Oid *tbl1_oid,
				   int *tbl2_varno, Oid *tbl2_oid,
				   Node **join_quals)
{
	ListCell   *lc;
	int			varno = 0;

	*tbl1_varno = *tbl2_varno = 0;
	*tbl1_oid = *tbl2_oid = InvalidOid;
	*join_quals = NULL;

	foreach(lc, viewQuery->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		varno++;
		if (rte->rtekind != RTE_RELATION)
			continue;

		if (*tbl1_varno == 0)
		{
			*tbl1_varno = varno;
			*tbl1_oid = rte->relid;
		}
		else
		{
			*tbl2_varno = varno;
			*tbl2_oid = rte->relid;
		}
	}

	if (IsA(viewQuery->jointree, FromExpr))
	{
		FromExpr   *fe = (FromExpr *) viewQuery->jointree;

		if (fe->quals != NULL)
			*join_quals = fe->quals;	/* implicit: FROM T1, T2 WHERE ... */
		else if (fe->fromlist != NIL &&
				 IsA(linitial(fe->fromlist), JoinExpr))
		{
			JoinExpr   *je = (JoinExpr *) linitial(fe->fromlist);

			*join_quals = je->quals;	/* explicit: FROM T1 JOIN T2 ON ... */
		}
	}

	if (*join_quals == NULL)
		elog(ERROR, "incr_get_join_info: no join condition found");
}

/*
 * incr_deparse_expr
 * Render a query-tree Node to SQL, substituting aliases:
 *   Var(varno == delta_varno)  →  _d_."colname"
 *   Var(other varno)           →  _j_."colname"
 * Handles Var, OpExpr (binary operators), and BoolExpr (AND/OR/NOT).
 *
 * PG17+ wraps GROUP BY columns in an RTE_GROUP entry; explicit JOINs add an
 * RTE_JOIN.  Both have relid=0, so we follow the indirection chain until we
 * reach an RTE_RELATION.
 */
static void
incr_deparse_expr(Node *expr, List *rtable, int delta_varno, StringInfo buf)
{
	if (expr == NULL)
		return;

	if (IsA(expr, Var))
	{
		Var		   *v = (Var *) expr;
		RangeTblEntry *rte;
		const char *colname;
		const char *alias;

		/* Chase RTE_JOIN/RTE_GROUP indirection until we find the base table */
		for (;;)
		{
			rte = rt_fetch(v->varno, rtable);
			if (rte->rtekind == RTE_RELATION)
				break;
			if (rte->rtekind == RTE_JOIN)
			{
				/* joinaliasvars maps output col position → underlying Var */
				Node *av = list_nth(rte->joinaliasvars, v->varattno - 1);
				if (!IsA(av, Var))
					elog(ERROR, "incr_deparse_expr: non-Var in joinaliasvars");
				v = (Var *) av;
			}
			else if (rte->rtekind == RTE_GROUP)
			{
				/* groupexprs maps output position → grouping expression */
				Node *ge = list_nth(rte->groupexprs, v->varattno - 1);
				if (!IsA(ge, Var))
					elog(ERROR, "incr_deparse_expr: non-Var in groupexprs");
				v = (Var *) ge;
			}
			else
				elog(ERROR,
					 "incr_deparse_expr: unexpected RTE kind %d at varno %d",
					 (int) rte->rtekind, v->varno);
		}

		colname = get_attname(rte->relid, v->varattno, false);
		alias = (v->varno == delta_varno) ? INCR_DELTA_ALIAS : INCR_JOIN_ALIAS;
		appendStringInfo(buf, "%s.%s", alias, quote_identifier(colname));
	}
	else if (IsA(expr, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) expr;
		HeapTuple	tup;
		Form_pg_operator opform;
		char	   *opname;

		tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "incr_deparse_expr: operator %u not found", op->opno);
		opform = (Form_pg_operator) GETSTRUCT(tup);
		opname = pstrdup(NameStr(opform->oprname));
		ReleaseSysCache(tup);

		appendStringInfoChar(buf, '(');
		incr_deparse_expr(linitial(op->args), rtable, delta_varno, buf);
		appendStringInfo(buf, " %s ", opname);
		incr_deparse_expr(lsecond(op->args), rtable, delta_varno, buf);
		appendStringInfoChar(buf, ')');
	}
	else if (IsA(expr, BoolExpr))
	{
		BoolExpr   *boolexpr = (BoolExpr *) expr;
		const char *opstr;
		ListCell   *lc;
		bool		first = true;

		opstr = (boolexpr->boolop == AND_EXPR) ? " AND " :
				(boolexpr->boolop == OR_EXPR)  ? " OR "  : " NOT ";

		appendStringInfoChar(buf, '(');
		foreach(lc, boolexpr->args)
		{
			if (!first)
				appendStringInfoString(buf, opstr);
			incr_deparse_expr(lfirst(lc), rtable, delta_varno, buf);
			first = false;
		}
		appendStringInfoChar(buf, ')');
	}
	else
		elog(ERROR,
			 "incr_deparse_expr: unsupported expression type %d in join condition",
			 (int) nodeTag(expr));
}

/*
 * incr_build_ins_sql_join — Phase 2 INSERT delta
 *
 * When rows in the delta-side table change, join the transition table with the
 * current state of the other table:
 *
 *   INSERT INTO mv (g1, agg_col, __mv_count__)
 *   SELECT _d_.g1, SUM(_j_.agg_arg), COUNT(*)
 *   FROM __mv_newtable _d_ JOIN schema.other _j_ ON (join_cond)
 *   GROUP BY _d_.g1
 *   ON CONFLICT (g1) DO UPDATE SET
 *     agg_col      = mv.agg_col      + EXCLUDED.agg_col,
 *     __mv_count__ = mv.__mv_count__ + EXCLUDED.__mv_count__
 */
static char *
incr_build_ins_sql_join(Oid mvrelid, Query *viewQuery,
						int delta_varno, Oid other_oid,
						Node *join_quals)
{
	StringInfoData buf;
	StringInfoData jbuf;
	List	   *groupColNames = NIL;
	ListCell   *lc,
			   *gcl;
	const char *mvname = mv_qname(mvrelid);
	const char *other_qname = mv_qname(other_oid);
	bool		first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	initStringInfo(&buf);
	initStringInfo(&jbuf);

	incr_deparse_expr(join_quals, viewQuery->rtable, delta_varno, &jbuf);

	/* INSERT INTO mv (output_cols) */
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

	/* SELECT: group cols as _d_.col/_j_.col, aggs with aliased args */
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
		{
			appendStringInfoString(&buf, "COUNT(*)");
		}
		else if (IsA(te->expr, Var))
		{
			StringInfoData ebuf;

			initStringInfo(&ebuf);
			incr_deparse_expr((Node *) te->expr, viewQuery->rtable,
							  delta_varno, &ebuf);
			appendStringInfoString(&buf, ebuf.data);
		}
		else if (IsA(te->expr, Aggref))
		{
			Aggref	   *agg = (Aggref *) te->expr;
			char	   *fname = get_func_name(agg->aggfnoid);

			if (strcmp(fname, "count") == 0 && agg->aggstar)
				appendStringInfoString(&buf, "COUNT(*)");
			else if (agg->args != NIL)
			{
				TargetEntry *arg_te = linitial_node(TargetEntry, agg->args);
				StringInfoData ebuf;

				initStringInfo(&ebuf);
				incr_deparse_expr((Node *) arg_te->expr, viewQuery->rtable,
								  delta_varno, &ebuf);
				appendStringInfo(&buf, "%s(%s)", fname, ebuf.data);
			}
			else
				appendStringInfo(&buf, "%s(*)", fname);
		}
	}

	/* FROM __mv_newtable _d_ JOIN other _j_ ON (condition) */
	appendStringInfo(&buf, " FROM %s %s JOIN %s %s ON (%s)",
					 MATVIEW_INCR_NEWTABLE, INCR_DELTA_ALIAS,
					 other_qname, INCR_JOIN_ALIAS,
					 jbuf.data);

	/* GROUP BY _d_.g1, ... */
	appendStringInfoString(&buf, " GROUP BY ");
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		StringInfoData ebuf;

		if (te->resjunk || !IsA(te->expr, Var))
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		initStringInfo(&ebuf);
		incr_deparse_expr((Node *) te->expr, viewQuery->rtable,
						  delta_varno, &ebuf);
		appendStringInfoString(&buf, ebuf.data);
	}

	/* ON CONFLICT (group_cols) DO UPDATE SET agg = mv.agg + EXCLUDED.agg */
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
		const char *colq;

		if (te->resjunk || IsA(te->expr, Var))
			continue;
		colq = quote_identifier(te->resname);
		if (!first)
			appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%s=%s.%s+EXCLUDED.%s", colq, mvname, colq, colq);
		first = false;
	}

	return buf.data;
}

/*
 * incr_build_del_sql_join — Phase 2 DELETE delta
 *
 * When rows in the delta-side table are removed, join the old transition
 * table with the current state of the other table and subtract:
 *
 *   WITH d AS (
 *     SELECT _d_.g1 AS g1, SUM(_j_.agg_arg) AS agg_col,
 *            COUNT(*) AS __mv_count__
 *     FROM __mv_oldtable _d_ JOIN schema.other _j_ ON (join_cond)
 *     GROUP BY _d_.g1
 *   )
 *   UPDATE mv SET
 *     agg_col      = mv.agg_col      - d.agg_col,
 *     __mv_count__ = mv.__mv_count__ - d.__mv_count__
 *   FROM d
 *   WHERE mv.g1 = d.g1
 */
static char *
incr_build_del_sql_join(Oid mvrelid, Query *viewQuery,
						int delta_varno, Oid other_oid,
						Node *join_quals)
{
	StringInfoData buf;
	StringInfoData jbuf;
	List	   *groupColNames = NIL;
	ListCell   *lc,
			   *gcl;
	const char *mvname = mv_qname(mvrelid);
	const char *other_qname = mv_qname(other_oid);
	const char *cntcol = quote_identifier(MATVIEW_INCR_COUNT_COL);
	bool		first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	initStringInfo(&buf);
	initStringInfo(&jbuf);

	incr_deparse_expr(join_quals, viewQuery->rtable, delta_varno, &jbuf);

	/* WITH d AS (SELECT ...) */
	appendStringInfoString(&buf, "WITH d AS (SELECT ");
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
		{
			appendStringInfo(&buf, "COUNT(*) AS %s", cntcol);
		}
		else if (IsA(te->expr, Var))
		{
			StringInfoData ebuf;

			initStringInfo(&ebuf);
			incr_deparse_expr((Node *) te->expr, viewQuery->rtable,
							  delta_varno, &ebuf);
			appendStringInfo(&buf, "%s AS %s",
							 ebuf.data, quote_identifier(te->resname));
		}
		else if (IsA(te->expr, Aggref))
		{
			Aggref	   *agg = (Aggref *) te->expr;
			char	   *fname = get_func_name(agg->aggfnoid);
			const char *colq = quote_identifier(te->resname);

			if (strcmp(fname, "count") == 0 && agg->aggstar)
				appendStringInfo(&buf, "COUNT(*) AS %s", colq);
			else if (agg->args != NIL)
			{
				TargetEntry *arg_te = linitial_node(TargetEntry, agg->args);
				StringInfoData ebuf;

				initStringInfo(&ebuf);
				incr_deparse_expr((Node *) arg_te->expr, viewQuery->rtable,
								  delta_varno, &ebuf);
				appendStringInfo(&buf, "%s(%s) AS %s", fname, ebuf.data, colq);
			}
			else
				appendStringInfo(&buf, "%s(*) AS %s", fname, colq);
		}
	}

	/* FROM __mv_oldtable _d_ JOIN other _j_ ON (cond) GROUP BY ... ) */
	appendStringInfo(&buf, " FROM %s %s JOIN %s %s ON (%s) GROUP BY ",
					 MATVIEW_INCR_OLDTABLE, INCR_DELTA_ALIAS,
					 other_qname, INCR_JOIN_ALIAS,
					 jbuf.data);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		StringInfoData ebuf;

		if (te->resjunk || !IsA(te->expr, Var))
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		initStringInfo(&ebuf);
		incr_deparse_expr((Node *) te->expr, viewQuery->rtable,
						  delta_varno, &ebuf);
		appendStringInfoString(&buf, ebuf.data);
	}
	appendStringInfoString(&buf, ") ");

	/* UPDATE mv SET agg = mv.agg - d.agg ... */
	appendStringInfo(&buf, "UPDATE %s SET ", mvname);
	first = true;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		const char *colq;

		if (te->resjunk || IsA(te->expr, Var))
			continue;
		colq = quote_identifier(te->resname);
		if (!first)
			appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%s=%s.%s-d.%s", colq, mvname, colq, colq);
		first = false;
	}

	/* FROM d WHERE mv.g = d.g */
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

/* ============================================================
 * Catalog helpers
 * ============================================================
 */

static void
incr_store_catalog(Oid mvrelid, Oid srctable,
				   const char *ins_sql,
				   const char *del_sql,
				   const char *cln_sql)
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
											Anum_pg_dbblue_matview_cln_sql;

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

	CloseMatViewIncrementalMaintenance();
	SPI_finish();
	return PointerGetDatum(NULL);
}
