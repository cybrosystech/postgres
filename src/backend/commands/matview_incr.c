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
 * Per-AVG-column metadata collected from the view's query tree.
 * Used by SQL builders to emit correct hidden-column maintenance.
 */
typedef struct AvgColInfo
{
	char   *avg_col;		/* visible output column name */
	char   *sum_col;		/* __mv_avgsum_<avg_col> hidden column */
	char   *cnt_col;		/* __mv_avgcnt_<avg_col> hidden column */
	Oid		avg_rettype;	/* return type of the AVG (numeric, float8, …) */
	Node   *arg_expr;		/* AVG argument expression (Var) — for Phase 2 deparse */
	char   *arg_col;		/* simple column name for Phase 1 (may be NULL) */
} AvgColInfo;

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
static bool incr_is_hidden_col(const char *resname);
static Oid	incr_find_sum_agg(Oid avg_fnoid, Oid *rettype_out);
static void incr_collect_avg_info(Query *viewQuery, List **avgInfoList);
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
static char *incr_build_backfill_sql(Oid mvrelid, Query *viewQuery,
									 Oid srctable);
static char *incr_build_del_sql(Oid mvrelid, Query *viewQuery);
static char *incr_build_cln_sql(Oid mvrelid);
static char *incr_build_ins_sql_join(Oid mvrelid, Query *viewQuery,
									 int delta_varno, Oid other_oid,
									 Node *join_quals);
static char *incr_build_backfill_sql_join(Oid mvrelid, Query *viewQuery,
										  int delta_varno, Oid delta_oid,
										  Oid other_oid, Node *join_quals);
static char *incr_build_del_sql_join(Oid mvrelid, Query *viewQuery,
									 int delta_varno, Oid other_oid,
									 Node *join_quals);
static void incr_store_catalog(Oid mvrelid, Oid srctable,
							   const char *ins_sql,
							   const char *del_sql,
							   const char *cln_sql,
							   const char *having_sql);
static void incr_create_unique_index(Oid mvrelid, List *groupColNames);
static bool incr_validate_having(Node *expr, Query *viewQuery);
static bool incr_validate_where_qual(Node *qual);
static Node *incr_get_where_qual(Query *viewQuery);
static void incr_deparse_where_qual(Node *qual, List *rtable, int delta_varno,
									StringInfo buf);
static const char *incr_resolve_var_colname(Var *v, List *rtable);
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
		if (!incr_validate_having(viewQuery->havingQual, viewQuery))
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
		/* Phase 1: nothing extra to check */
	}
	else if (nbasetables == 2)
	{
		/* Phase 2: require an INNER JOIN condition */
		bool		has_join = false;

		if (IsA(viewQuery->jointree, FromExpr))
		{
			FromExpr   *fe = (FromExpr *) viewQuery->jointree;

			if (fe->fromlist != NIL && IsA(linitial(fe->fromlist), JoinExpr))
			{
				JoinExpr   *je = (JoinExpr *) linitial(fe->fromlist);

				if (je->jointype == JOIN_INNER && je->quals != NULL)
					has_join = true;
			}
			else if (fe->quals != NULL)
				has_join = true;	/* implicit join: FROM T1, T2 WHERE join_cond */
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
				continue;

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

		if (where_qual != NULL && !incr_validate_where_qual(where_qual))
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

		ins_sql = incr_build_ins_sql(mvrelid, viewQuery);
		del_sql = incr_build_del_sql(mvrelid, viewQuery);
		incr_store_catalog(mvrelid, srctable, ins_sql, del_sql, cln_sql, hav_sql);

		/*
		 * HAVING backfill: the initial CREATE MATERIALIZED VIEW only inserted
		 * HAVING-passing groups.  Insert all remaining groups (with
		 * __mv_having_ok__ = false) so their running totals are tracked from
		 * the start.  ON CONFLICT DO NOTHING leaves passing groups intact.
		 */
		if (hasHaving)
		{
			char	   *backfill_sql = incr_build_backfill_sql(mvrelid, viewQuery, srctable);
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
		incr_store_catalog(mvrelid, tbl1_oid, ins_sql, del_sql, cln_sql, hav_sql);
		incr_install_triggers(mvrelid, tbl1_oid);

		/* Delta SQL when T2 rows change — join transition table with current T1 */
		ins_sql = incr_build_ins_sql_join(mvrelid, viewQuery,
										  tbl2_varno, tbl1_oid, join_quals);
		del_sql = incr_build_del_sql_join(mvrelid, viewQuery,
										  tbl2_varno, tbl1_oid, join_quals);
		incr_store_catalog(mvrelid, tbl2_oid, ins_sql, del_sql, cln_sql, hav_sql);
		incr_install_triggers(mvrelid, tbl2_oid);

		/*
		 * HAVING backfill for JOIN: seed all groups (not just HAVING-passing
		 * ones) so groups that initially fail HAVING are tracked from the start.
		 * We join the real tables instead of transition tables.  DO NOTHING
		 * leaves the already-passing groups intact.
		 */
		if (hasHaving)
		{
			char	   *backfill_sql = incr_build_backfill_sql_join(
				mvrelid, viewQuery,
				tbl1_varno, tbl1_oid, tbl2_oid, join_quals);
			int			spi_ret;

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

/*
 * incr_collect_avg_info
 * Build a list of AvgColInfo for every AVG() column in the target list.
 * arg_col is the plain column name (for Phase 1 single-table SQL builders);
 * arg_expr is the raw Var node (for Phase 2 builders that call incr_deparse_expr).
 */
static void
incr_collect_avg_info(Query *viewQuery, List **avgInfoList)
{
	ListCell   *lc;

	*avgInfoList = NIL;
	foreach(lc, viewQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		Aggref	   *agg;
		AvgColInfo *info;

		if (te->resjunk || !IsA(te->expr, Aggref))
			continue;
		agg = (Aggref *) te->expr;
		if (strcmp(get_func_name(agg->aggfnoid), "avg") != 0)
			continue;

		info = palloc(sizeof(AvgColInfo));
		info->avg_col = pstrdup(te->resname);
		info->sum_col = psprintf("%s%s", MATVIEW_INCR_AVGSUM_PREFIX, te->resname);
		info->cnt_col = psprintf("%s%s", MATVIEW_INCR_AVGCNT_PREFIX, te->resname);
		info->avg_rettype = agg->aggtype;
		info->arg_expr = NULL;
		info->arg_col = NULL;

		if (agg->args != NIL)
		{
			TargetEntry *arg_te = linitial_node(TargetEntry, agg->args);
			Node	   *argnode = (Node *) arg_te->expr;

			info->arg_expr = argnode;

			/* For Phase 1: resolve the arg Var to a plain column name */
			if (IsA(argnode, Var))
			{
				Var		   *v = (Var *) argnode;
				RangeTblEntry *rte = rt_fetch(v->varno, viewQuery->rtable);

				if (rte->rtekind == RTE_RELATION)
					info->arg_col = get_attname(rte->relid, v->varattno, false);
			}
		}

		*avgInfoList = lappend(*avgInfoList, info);
	}
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
 * incr_validate_where_qual
 * Return true if qual only uses expressions safe for WHERE maintenance:
 * Var, Const, OpExpr, BoolExpr, FuncExpr (stable/immutable), NullTest,
 * ScalarArrayOpExpr (col IN/NOT IN list), ArrayExpr.
 * Aggregates and sublinks are not permitted in WHERE.
 */
static bool
incr_validate_where_qual(Node *qual)
{
	ListCell   *lc;

	if (qual == NULL)
		return true;

	if (IsA(qual, Var) || IsA(qual, Const))
		return true;

	if (IsA(qual, NullTest))
		return incr_validate_where_qual((Node *) ((NullTest *) qual)->arg);

	if (IsA(qual, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) qual;

		foreach(lc, op->args)
			if (!incr_validate_where_qual(lfirst(lc)))
				return false;
		return true;
	}

	if (IsA(qual, BoolExpr))
	{
		BoolExpr   *be = (BoolExpr *) qual;

		foreach(lc, be->args)
			if (!incr_validate_where_qual(lfirst(lc)))
				return false;
		return true;
	}

	if (IsA(qual, FuncExpr))
	{
		FuncExpr   *fe = (FuncExpr *) qual;

		if (fe->funcretset || func_volatile(fe->funcid) == PROVOLATILE_VOLATILE)
			return false;
		foreach(lc, fe->args)
			if (!incr_validate_where_qual(lfirst(lc)))
				return false;
		return true;
	}

	if (IsA(qual, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *sao = (ScalarArrayOpExpr *) qual;

		foreach(lc, sao->args)
			if (!incr_validate_where_qual(lfirst(lc)))
				return false;
		return true;
	}

	if (IsA(qual, ArrayExpr))
	{
		ArrayExpr  *ae = (ArrayExpr *) qual;

		foreach(lc, ae->elements)
			if (!incr_validate_where_qual(lfirst(lc)))
				return false;
		return true;
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

		if (delta_varno < 0)
		{
			/* Phase 1: bare column name — the transition table has no alias */
			appendStringInfoString(buf,
								   quote_identifier(incr_resolve_var_colname(v, rtable)));
		}
		else
		{
			/* Phase 2: use incr_deparse_expr for _d_ / _j_ aliasing */
			incr_deparse_expr(qual, rtable, delta_varno, buf);
		}
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
 * name for a Var node.  Shared by incr_deparse_expr and incr_deparse_having_cond.
 */
static const char *
incr_resolve_var_colname(Var *v, List *rtable)
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
	return get_attname(rte->relid, v->varattno, false);
}

/*
 * incr_validate_having
 * Return true if expr uses only maintained aggregates (COUNT/SUM/AVG),
 * group-by Vars, constants, comparisons, and boolean operators.
 * Used by MatviewIncrIsEligible to gate Phase 4 eligibility.
 */
static bool
incr_validate_having(Node *expr, Query *viewQuery)
{
	if (expr == NULL)
		return true;

	if (IsA(expr, Aggref))
	{
		Aggref	   *agg = (Aggref *) expr;
		char	   *fname = get_func_name(agg->aggfnoid);
		ListCell   *lc;

		/* COUNT(*) — always maintained via __mv_count__ */
		if (strcmp(fname, "count") == 0 && agg->aggstar)
			return true;

		/* Other aggregates must match a SELECT target */
		foreach(lc, viewQuery->targetList)
		{
			TargetEntry *te = lfirst_node(TargetEntry, lc);

			if (!IsA(te->expr, Aggref))
				continue;
			if (((Aggref *) te->expr)->aggfnoid == agg->aggfnoid)
				return true;	/* same function — good enough for eligibility */
		}
		return false;
	}
	else if (IsA(expr, Var))
		return true;
	else if (IsA(expr, Const))
		return true;
	else if (IsA(expr, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) expr;
		ListCell   *lc;

		foreach(lc, op->args)
		{
			if (!incr_validate_having(lfirst(lc), viewQuery))
				return false;
		}
		return true;
	}
	else if (IsA(expr, BoolExpr))
	{
		BoolExpr   *be = (BoolExpr *) expr;
		ListCell   *lc;

		foreach(lc, be->args)
		{
			if (!incr_validate_having(lfirst(lc), viewQuery))
				return false;
		}
		return true;
	}
	else if (IsA(expr, NullTest))
		return true;			/* IS NULL / IS NOT NULL on group cols is fine */
	else if (IsA(expr, FuncExpr))
	{
		/* Allow implicit type-coercion functions (e.g. int4 → numeric) */
		FuncExpr   *fe = (FuncExpr *) expr;
		ListCell   *lc;

		if (!fe->funcretset)
		{
			foreach(lc, fe->args)
			{
				if (!incr_validate_having(lfirst(lc), viewQuery))
					return false;
			}
			return true;
		}
	}
	return false;
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
		const char *colname = incr_resolve_var_colname(v, viewQuery->rtable);
		ListCell   *lc;

		foreach(lc, viewQuery->targetList)
		{
			TargetEntry *te = lfirst_node(TargetEntry, lc);

			if (!IsA(te->expr, Var))
				continue;
			if (strcmp(incr_resolve_var_colname((Var *) te->expr,
												viewQuery->rtable),
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

		/* Hidden maintenance columns and AVG are handled separately */
		if (incr_is_hidden_col(te->resname))
			continue;

		agg = (Aggref *) te->expr;
		fname = get_func_name(agg->aggfnoid);

		/* AVG is handled by incr_collect_avg_info, not here */
		if (strcmp(fname, "avg") == 0)
			continue;

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
	List	   *avgInfoList = NIL;
	ListCell   *gcl,
			   *acl,
			   *fcl,
			   *arcl,
			   *lc;
	const char *mvname = mv_qname(mvrelid);
	const char *cntcol = quote_identifier(MATVIEW_INCR_COUNT_COL);
	bool		first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	incr_collect_agg_info(viewQuery, &aggColNames, &aggFuncNames, &aggArgColNames);
	incr_collect_avg_info(viewQuery, &avgInfoList);

	initStringInfo(&buf);

	/* INSERT INTO mv (group_cols, agg_cols, avg_cols, hidden_avg_cols, __mv_count__) */
	appendStringInfo(&buf, "INSERT INTO %s (", mvname);
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}
	foreach(acl, aggColNames)
		appendStringInfo(&buf, ",%s", quote_identifier(strVal(lfirst(acl))));
	foreach(lc, avgInfoList)
	{
		AvgColInfo *ai = lfirst(lc);

		appendStringInfo(&buf, ",%s,%s,%s",
						 quote_identifier(ai->avg_col),
						 quote_identifier(ai->sum_col),
						 quote_identifier(ai->cnt_col));
	}
	if (viewQuery->havingQual != NULL)
		appendStringInfo(&buf, ",%s", quote_identifier(MATVIEW_INCR_HAVING_COL));
	appendStringInfo(&buf, ",%s) ", cntcol);

	/* SELECT group_cols, agg_exprs, avg+hidden exprs, COUNT(*) FROM __mv_newtable GROUP BY ... */
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
	foreach(lc, avgInfoList)
	{
		AvgColInfo *ai = lfirst(lc);
		const char *argq = ai->arg_col ? quote_identifier(ai->arg_col) : "NULL";

		appendStringInfo(&buf, ",AVG(%s),SUM(%s),COUNT(%s)", argq, argq, argq);
	}
	if (viewQuery->havingQual != NULL)
		appendStringInfoString(&buf, ",true");
	appendStringInfo(&buf, ",COUNT(*) FROM %s", MATVIEW_INCR_NEWTABLE);
	{
		Node	   *wq = incr_get_where_qual(viewQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, viewQuery->rtable, -1, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
	}
	appendStringInfoString(&buf, " GROUP BY ");
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

	/* Regular SUM/COUNT aggregates: simple addition */
	first = true;
	foreach(acl, aggColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(acl)));

		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%s=%s.%s+EXCLUDED.%s", colq, mvname, colq, colq);
		first = false;
	}

	/* AVG aggregates: update hidden sum/cnt, then recompute visible avg */
	foreach(lc, avgInfoList)
	{
		AvgColInfo *ai = lfirst(lc);
		const char *avg_q = quote_identifier(ai->avg_col);
		const char *sum_q = quote_identifier(ai->sum_col);
		const char *cnt_q = quote_identifier(ai->cnt_col);
		const char *type_name = format_type_be(ai->avg_rettype);

		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf,
						 "%s=%s.%s+EXCLUDED.%s"
						 ",%s=%s.%s+EXCLUDED.%s"
						 ",%s=((%s.%s+EXCLUDED.%s)::%s/NULLIF(%s.%s+EXCLUDED.%s,0))",
						 sum_q, mvname, sum_q, sum_q,
						 cnt_q, mvname, cnt_q, cnt_q,
						 avg_q,
						 mvname, sum_q, sum_q, type_name,
						 mvname, cnt_q, cnt_q);
		first = false;
	}

	if (!first) appendStringInfoChar(&buf, ',');
	appendStringInfo(&buf, "%s=%s.%s+EXCLUDED.%s", cntcol, mvname, cntcol, cntcol);

	return buf.data;
}

/*
 * incr_build_backfill_sql — one-time setup for HAVING matviews
 *
 * Inserts all groups (not just HAVING-passing ones) into the base table so
 * that groups which initially fail HAVING are still tracked.  Groups already
 * present (they passed HAVING during initial population) are left untouched
 * via ON CONFLICT DO NOTHING.
 *
 *   INSERT INTO base (g1, ..., agg_cols, ..., __mv_having_ok__, __mv_count__)
 *   SELECT g1, ..., agg_exprs, ..., false, COUNT(*)
 *   FROM <srctable>
 *   GROUP BY g1, ...
 *   ON CONFLICT (g1, ...) DO NOTHING
 */
static char *
incr_build_backfill_sql(Oid mvrelid, Query *viewQuery, Oid srctable)
{
	StringInfoData buf;
	List	   *groupColNames = NIL;
	List	   *aggColNames = NIL,
			   *aggFuncNames = NIL,
			   *aggArgColNames = NIL;
	List	   *avgInfoList = NIL;
	ListCell   *gcl,
			   *acl,
			   *fcl,
			   *arcl,
			   *lc;
	const char *mvname = mv_qname(mvrelid);
	const char *cntcol = quote_identifier(MATVIEW_INCR_COUNT_COL);
	const char *srctable_name;
	bool		first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	incr_collect_agg_info(viewQuery, &aggColNames, &aggFuncNames, &aggArgColNames);
	incr_collect_avg_info(viewQuery, &avgInfoList);

	srctable_name = quote_qualified_identifier(
		get_namespace_name(get_rel_namespace(srctable)),
		get_rel_name(srctable));

	initStringInfo(&buf);

	/* INSERT INTO base (group_cols, agg_cols, ..., __mv_having_ok__, __mv_count__) */
	appendStringInfo(&buf, "INSERT INTO %s (", mvname);
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}
	foreach(acl, aggColNames)
		appendStringInfo(&buf, ",%s", quote_identifier(strVal(lfirst(acl))));
	foreach(lc, avgInfoList)
	{
		AvgColInfo *ai = lfirst(lc);

		appendStringInfo(&buf, ",%s,%s,%s",
						 quote_identifier(ai->avg_col),
						 quote_identifier(ai->sum_col),
						 quote_identifier(ai->cnt_col));
	}
	appendStringInfo(&buf, ",%s,%s) ",
					 quote_identifier(MATVIEW_INCR_HAVING_COL), cntcol);

	/* SELECT group_cols, agg_exprs, ..., false, COUNT(*) FROM srctable GROUP BY ... */
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
	foreach(lc, avgInfoList)
	{
		AvgColInfo *ai = lfirst(lc);
		const char *argq = ai->arg_col ? quote_identifier(ai->arg_col) : "NULL";

		appendStringInfo(&buf, ",AVG(%s),SUM(%s),COUNT(%s)", argq, argq, argq);
	}
	/* __mv_having_ok__ = false for all backfill rows; DO NOTHING leaves passing rows alone */
	appendStringInfo(&buf, ",false,COUNT(*) FROM %s", srctable_name);
	{
		Node	   *wq = incr_get_where_qual(viewQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, viewQuery->rtable, -1, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
	}
	appendStringInfoString(&buf, " GROUP BY ");
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}

	/* ON CONFLICT DO NOTHING — don't overwrite already-passing groups */
	appendStringInfoString(&buf, " ON CONFLICT (");
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}
	appendStringInfoString(&buf, ") DO NOTHING");

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
	List	   *avgInfoList = NIL;
	ListCell   *gcl,
			   *acl,
			   *fcl,
			   *arcl,
			   *lc;
	const char *mvname = mv_qname(mvrelid);
	const char *cntcol = quote_identifier(MATVIEW_INCR_COUNT_COL);
	bool		first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	incr_collect_agg_info(viewQuery, &aggColNames, &aggFuncNames, &aggArgColNames);
	incr_collect_avg_info(viewQuery, &avgInfoList);

	initStringInfo(&buf);

	/* WITH d AS (SELECT group_cols, agg_exprs, hidden avg cols, COUNT(*) FROM __mv_oldtable ...) */
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
	/* Hidden AVG support cols in CTE */
	foreach(lc, avgInfoList)
	{
		AvgColInfo *ai = lfirst(lc);
		const char *argq = ai->arg_col ? quote_identifier(ai->arg_col) : "NULL";

		appendStringInfo(&buf, ",SUM(%s) AS %s,COUNT(%s) AS %s",
						 argq, quote_identifier(ai->sum_col),
						 argq, quote_identifier(ai->cnt_col));
	}
	appendStringInfo(&buf, ",COUNT(*) AS %s FROM %s", cntcol, MATVIEW_INCR_OLDTABLE);
	{
		Node	   *wq = incr_get_where_qual(viewQuery);

		if (wq != NULL)
		{
			StringInfoData wbuf;

			initStringInfo(&wbuf);
			incr_deparse_where_qual(wq, viewQuery->rtable, -1, &wbuf);
			appendStringInfo(&buf, " WHERE %s", wbuf.data);
		}
	}
	appendStringInfoString(&buf, " GROUP BY ");
	first = true;
	foreach(gcl, groupColNames)
	{
		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfoString(&buf, quote_identifier(strVal(lfirst(gcl))));
		first = false;
	}
	appendStringInfoString(&buf, ") ");

	/* UPDATE mv SET agg=mv.agg-d.agg, hidden_avg, avg_recompute, __mv_count__ */
	appendStringInfo(&buf, "UPDATE %s SET ", mvname);
	first = true;
	foreach(acl, aggColNames)
	{
		const char *colq = quote_identifier(strVal(lfirst(acl)));

		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%s=%s.%s-d.%s", colq, mvname, colq, colq);
		first = false;
	}
	foreach(lc, avgInfoList)
	{
		AvgColInfo *ai = lfirst(lc);
		const char *avg_q = quote_identifier(ai->avg_col);
		const char *sum_q = quote_identifier(ai->sum_col);
		const char *cnt_q = quote_identifier(ai->cnt_col);
		const char *type_name = format_type_be(ai->avg_rettype);

		if (!first) appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf,
						 "%s=%s.%s-d.%s"
						 ",%s=%s.%s-d.%s"
						 ",%s=((%s.%s-d.%s)::%s/NULLIF(%s.%s-d.%s,0))",
						 sum_q, mvname, sum_q, sum_q,
						 cnt_q, mvname, cnt_q, cnt_q,
						 avg_q,
						 mvname, sum_q, sum_q, type_name,
						 mvname, cnt_q, cnt_q);
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

		if (fe->fromlist != NIL &&
			IsA(linitial(fe->fromlist), JoinExpr))
		{
			JoinExpr   *je = (JoinExpr *) linitial(fe->fromlist);

			*join_quals = je->quals;	/* explicit: FROM T1 JOIN T2 ON ... */
			/* fe->quals is the WHERE clause — handled separately */
		}
		else if (fe->quals != NULL)
			*join_quals = fe->quals;	/* implicit: FROM T1, T2 WHERE join_cond */
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
		else if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
		{
			/* new groups start as passing; hav_sql corrects after delta */
			appendStringInfoString(&buf, "true");
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

	/* FROM __mv_newtable _d_ JOIN other _j_ ON (condition) [WHERE ...] */
	appendStringInfo(&buf, " FROM %s %s JOIN %s %s ON (%s)",
					 MATVIEW_INCR_NEWTABLE, INCR_DELTA_ALIAS,
					 other_qname, INCR_JOIN_ALIAS,
					 jbuf.data);
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

		if (te->resjunk || IsA(te->expr, Var))
			continue;
		/* hidden avgsum/avgcnt and having cols handled separately — skip generic path */
		if (strncmp(te->resname, MATVIEW_INCR_AVGSUM_PREFIX,
					sizeof(MATVIEW_INCR_AVGSUM_PREFIX) - 1) == 0 ||
			strncmp(te->resname, MATVIEW_INCR_AVGCNT_PREFIX,
					sizeof(MATVIEW_INCR_AVGCNT_PREFIX) - 1) == 0 ||
			strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			continue;

		if (IsA(te->expr, Aggref))
		{
			Aggref	   *agg = (Aggref *) te->expr;
			char	   *fname = get_func_name(agg->aggfnoid);

			if (strcmp(fname, "avg") == 0)
			{
				char	   *sum_col = psprintf("%s%s",
											  MATVIEW_INCR_AVGSUM_PREFIX,
											  te->resname);
				char	   *cnt_col = psprintf("%s%s",
											  MATVIEW_INCR_AVGCNT_PREFIX,
											  te->resname);
				const char *avg_q = quote_identifier(te->resname);
				const char *sum_q = quote_identifier(sum_col);
				const char *cnt_q = quote_identifier(cnt_col);
				const char *type_name = format_type_be(agg->aggtype);

				if (!first) appendStringInfoChar(&buf, ',');
				appendStringInfo(&buf,
								 "%s=%s.%s+EXCLUDED.%s"
								 ",%s=%s.%s+EXCLUDED.%s"
								 ",%s=((%s.%s+EXCLUDED.%s)::%s/NULLIF(%s.%s+EXCLUDED.%s,0))",
								 sum_q, mvname, sum_q, sum_q,
								 cnt_q, mvname, cnt_q, cnt_q,
								 avg_q,
								 mvname, sum_q, sum_q, type_name,
								 mvname, cnt_q, cnt_q);
				first = false;
				continue;
			}
		}

		{
			const char *colq = quote_identifier(te->resname);

			if (!first) appendStringInfoChar(&buf, ',');
			appendStringInfo(&buf, "%s=%s.%s+EXCLUDED.%s", colq, mvname, colq, colq);
			first = false;
		}
	}

	return buf.data;
}

/*
 * incr_build_backfill_sql_join — one-time setup for Phase 2 HAVING matviews
 *
 * Same idea as incr_build_backfill_sql but for a two-table JOIN:
 * replaces __mv_newtable with the actual delta table so all groups are
 * seeded before triggers are installed.
 *
 *   INSERT INTO base (group_cols, agg_cols, ..., __mv_having_ok__, __mv_count__)
 *   SELECT _d_.g1, agg_exprs, ..., false, COUNT(*)
 *   FROM delta_table _d_ JOIN other_table _j_ ON (join_cond)
 *   GROUP BY _d_.g1, ...
 *   ON CONFLICT (group_cols) DO NOTHING
 */
static char *
incr_build_backfill_sql_join(Oid mvrelid, Query *viewQuery,
							 int delta_varno, Oid delta_oid,
							 Oid other_oid, Node *join_quals)
{
	StringInfoData buf;
	StringInfoData jbuf;
	List	   *groupColNames = NIL;
	ListCell   *lc,
			   *gcl;
	const char *mvname = mv_qname(mvrelid);
	const char *delta_qname = quote_qualified_identifier(
		get_namespace_name(get_rel_namespace(delta_oid)),
		get_rel_name(delta_oid));
	const char *other_qname = mv_qname(other_oid);
	bool		first;

	incr_collect_group_cols(viewQuery, &groupColNames);
	initStringInfo(&buf);
	initStringInfo(&jbuf);

	incr_deparse_expr(join_quals, viewQuery->rtable, delta_varno, &jbuf);

	/* INSERT INTO base (output_cols) */
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

	/* SELECT: same structure as ins_sql_join but __mv_having_ok__ = false */
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
			appendStringInfoString(&buf, "false");	/* DO NOTHING leaves t=true intact */
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

	/* FROM actual_delta_table _d_ JOIN other _j_ ON (cond) [WHERE ...] */
	appendStringInfo(&buf, " FROM %s %s JOIN %s %s ON (%s)",
					 delta_qname, INCR_DELTA_ALIAS,
					 other_qname, INCR_JOIN_ALIAS,
					 jbuf.data);
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

	/* ON CONFLICT DO NOTHING — passing groups already in base */
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
		/* skip avg visible col (recomputed from hidden sum/cnt) and having col */
		if (strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			continue;
		if (IsA(te->expr, Aggref) &&
			strcmp(get_func_name(((Aggref *) te->expr)->aggfnoid), "avg") == 0)
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

	/* FROM __mv_oldtable _d_ JOIN other _j_ ON (cond) [WHERE ...] GROUP BY ... ) */
	appendStringInfo(&buf, " FROM %s %s JOIN %s %s ON (%s)",
					 MATVIEW_INCR_OLDTABLE, INCR_DELTA_ALIAS,
					 other_qname, INCR_JOIN_ALIAS,
					 jbuf.data);
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

		if (te->resjunk || IsA(te->expr, Var))
			continue;
		/* hidden avgsum/avgcnt and having cols handled separately — skip generic path */
		if (strncmp(te->resname, MATVIEW_INCR_AVGSUM_PREFIX,
					sizeof(MATVIEW_INCR_AVGSUM_PREFIX) - 1) == 0 ||
			strncmp(te->resname, MATVIEW_INCR_AVGCNT_PREFIX,
					sizeof(MATVIEW_INCR_AVGCNT_PREFIX) - 1) == 0 ||
			strcmp(te->resname, MATVIEW_INCR_HAVING_COL) == 0)
			continue;

		if (IsA(te->expr, Aggref))
		{
			Aggref	   *agg = (Aggref *) te->expr;
			char	   *fname = get_func_name(agg->aggfnoid);

			if (strcmp(fname, "avg") == 0)
			{
				char	   *sum_col = psprintf("%s%s",
											  MATVIEW_INCR_AVGSUM_PREFIX,
											  te->resname);
				char	   *cnt_col = psprintf("%s%s",
											  MATVIEW_INCR_AVGCNT_PREFIX,
											  te->resname);
				const char *avg_q = quote_identifier(te->resname);
				const char *sum_q = quote_identifier(sum_col);
				const char *cnt_q = quote_identifier(cnt_col);
				const char *type_name = format_type_be(agg->aggtype);

				if (!first) appendStringInfoChar(&buf, ',');
				appendStringInfo(&buf,
								 "%s=%s.%s-d.%s"
								 ",%s=%s.%s-d.%s"
								 ",%s=((%s.%s-d.%s)::%s/NULLIF(%s.%s-d.%s,0))",
								 sum_q, mvname, sum_q, sum_q,
								 cnt_q, mvname, cnt_q, cnt_q,
								 avg_q,
								 mvname, sum_q, sum_q, type_name,
								 mvname, cnt_q, cnt_q);
				first = false;
				continue;
			}
		}

		{
			const char *colq = quote_identifier(te->resname);

			if (!first) appendStringInfoChar(&buf, ',');
			appendStringInfo(&buf, "%s=%s.%s-d.%s", colq, mvname, colq, colq);
			first = false;
		}
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
