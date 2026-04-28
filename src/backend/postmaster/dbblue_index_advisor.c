/*-------------------------------------------------------------------------
 *
 * dbblue_index_advisor.c
 *	  dbblue Auto Index Suggestion background worker.
 *
 * The advisor periodically inspects pg_stat_statements to find expensive
 * queries, parses each one to derive candidate indexes, uses HypoPG to
 * estimate the cost reduction of each candidate, and records winners in
 * the dbblue_index_suggestions table for the DBA to review.
 *
 * Phase 4 (this file):
 *	 - Worker connects to dbblue_auto_index_suggestion_database at startup,
 *	   lazy-creates public.dbblue_index_suggestions, and detects whether
 *	   the hypopg extension is installed.
 *	 - Each tick reads top-N expensive queries from pg_stat_statements.
 *	 - Each query is parsed (raw_parser + parse_analyze_varparams) under
 *	   its own subtransaction; Vars from WHERE / JOIN / HAVING become
 *	   single-column B-tree CREATE INDEX candidates.
 *	 - For every candidate the worker EXPLAINs the query (GENERIC_PLAN)
 *	   to obtain a baseline cost, asks hypopg to materialise a
 *	   hypothetical index, EXPLAINs again to obtain a hypothetical cost,
 *	   and resets hypopg.  Each candidate runs in its own nested
 *	   subtransaction so a failure (e.g. column type lacks btree
 *	   support) does not poison sibling candidates.
 *	 - Candidates whose cost reduction is at least
 *	   dbblue_auto_index_suggestion_min_cost_improvement are recorded in
 *	   public.dbblue_index_suggestions via INSERT ... ON CONFLICT
 *	   (relation_oid, index_columns) DO UPDATE.
 *
 * Composite candidates, write-amp filtering, and the status lifecycle
 * arrive in Phase 5.
 *
 * Portions Copyright (c) 2026, dbblue / Cybrosys.
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/dbblue_index_advisor.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_index.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/dbblue_index_advisor.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"

/* GUC-backed variables (defaults; real values come from guc_parameters.dat) */
bool	dbblue_auto_index_suggestion_enabled = false;
int		dbblue_auto_index_suggestion_interval = 3600;
int		dbblue_auto_index_suggestion_min_calls = 100;
double	dbblue_auto_index_suggestion_min_cost_improvement = 0.30;
int		dbblue_auto_index_suggestion_max_index_columns = 3;
int		dbblue_auto_index_suggestion_top_n_queries = 50;
char   *dbblue_auto_index_suggestion_database = NULL;

/*
 * One candidate index, derived from a single query.
 *
 * Phase 3 only emits single-column candidates; Phase 5 extends this
 * struct (or its replacement) with a column array.
 */
typedef struct IndexCandidate
{
	Oid			relid;
	AttrNumber	attno;
	char	   *qualified_relname;	/* schema.table, palloc'd */
	char	   *attname;			/* column name, palloc'd */
	char	   *ddl;				/* CREATE INDEX ... statement */
} IndexCandidate;

/* Walker context for extract_var_walker. */
typedef struct CandidateWalkerCtx
{
	Query	   *query;				/* root analysed query (for rtable) */
	List	   *candidates;			/* List of IndexCandidate * */
} CandidateWalkerCtx;

/*
 * One row pulled from pg_stat_statements that we plan to analyse.
 *
 * Built in a per-tick MemoryContext so we can SPI_finish before doing
 * the heavier parse/analyse work, which avoids holding SPI_tuptable
 * across subtransactions.
 */
typedef struct StatStatementsRow
{
	int64		queryid;
	int64		calls;
	double		total_exec_time_ms;
	char	   *query_text;
} StatStatementsRow;

/*
 * Cached at worker startup: true iff the hypopg extension is installed
 * in the connected database.  When false, the advisor still parses and
 * lists candidates but skips cost evaluation and writes no rows.
 */
static bool dbblue_hypopg_available = false;

/*
 * Defensive cap on the size of a pg_stat_statements query text we will
 * parse.  Some workloads (notably Odoo's ORM) emit very large queries,
 * and recursive parser/analyzer paths inside a bgworker have proved
 * crash-prone on them.  Anything larger is logged and skipped.  16 KiB
 * is comfortably above ordinary OLTP queries while still well below the
 * stack-overflow danger zone.
 */
#define DBBLUE_MAX_QUERY_TEXT_LEN 16384

static void dbblue_advisor_ensure_results_table(void);
static void dbblue_advisor_detect_hypopg(void);
static void dbblue_advisor_run_tick(void);
static void dbblue_advisor_process_query(StatStatementsRow *row);
static bool dbblue_query_should_skip(Query *query);
static bool dbblue_relation_should_skip(Oid relid);
static bool dbblue_first_col_index_exists(Oid relid, AttrNumber attno);
static bool dbblue_candidate_already_listed(List *candidates,
											Oid relid, AttrNumber attno);
static bool extract_var_walker(Node *node, CandidateWalkerCtx *ctx);
static double dbblue_advisor_get_plan_cost(const char *query_text);
static void dbblue_advisor_reset_hypotheticals(void);
static void dbblue_advisor_evaluate_candidates(StatStatementsRow *row,
											   List *candidates);
static void dbblue_advisor_record_suggestion(StatStatementsRow *row,
											 IndexCandidate *cand,
											 double baseline,
											 double hypothetical,
											 double improvement_pct);

/*
 * SQL run on first startup to create the suggestions table if missing.
 *
 * Schema: public.dbblue_index_suggestions.  Phase 2 only creates the
 * table; rows are populated starting in Phase 4.
 */
static const char *const dbblue_create_results_table_sql =
	"CREATE TABLE IF NOT EXISTS public.dbblue_index_suggestions ("
	"    id                   bigserial PRIMARY KEY,"
	"    relation_oid         oid NOT NULL,"
	"    relation_name        text NOT NULL,"
	"    index_columns        text[] NOT NULL,"
	"    include_columns      text[],"
	"    index_method         text NOT NULL DEFAULT 'btree',"
	"    queryid              bigint,"
	"    sample_query         text,"
	"    baseline_cost        double precision,"
	"    hypothetical_cost    double precision,"
	"    cost_improvement_pct double precision,"
	"    total_calls          bigint,"
	"    total_exec_time_ms   double precision,"
	"    ddl                  text NOT NULL,"
	"    status               text NOT NULL DEFAULT 'new',"
	"    created_at           timestamptz NOT NULL DEFAULT now(),"
	"    last_seen_at         timestamptz NOT NULL DEFAULT now(),"
	"    UNIQUE (relation_oid, index_columns)"
	")";

/*
 * dbblue_advisor_ensure_results_table
 *		Create public.dbblue_index_suggestions if it does not exist.
 */
static void
dbblue_advisor_ensure_results_table(void)
{
	int			ret;

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue index advisor: ensuring results table");

	ret = SPI_execute(dbblue_create_results_table_sql, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(WARNING,
				(errmsg("dbblue index advisor: could not create dbblue_index_suggestions (SPI rc=%d)",
						ret)));

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * dbblue_relation_should_skip
 *		True for relations the advisor must never recommend indexes on:
 *		system catalogs, information_schema, toast and temp namespaces,
 *		and anything that is not a regular or partitioned table.
 */
static bool
dbblue_relation_should_skip(Oid relid)
{

	Oid			nspoid;
	char	   *nspname;
	char		relkind;
	bool		skip;

	if (!OidIsValid(relid))
		return true;

	relkind = get_rel_relkind(relid);
	if (relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE)
		return true;

	nspoid = get_rel_namespace(relid);
	nspname = get_namespace_name(nspoid);
	if (nspname == NULL)
		return true;

	skip = (strcmp(nspname, "pg_catalog") == 0 ||
			strcmp(nspname, "information_schema") == 0 ||
			strcmp(nspname, "pg_toast") == 0 ||
			strncmp(nspname, "pg_temp_", 8) == 0 ||
			strncmp(nspname, "pg_toast_temp_", 14) == 0);

	pfree(nspname);
	return skip;
}

/*
 * dbblue_query_should_skip
 *		True if a Query is uninteresting for index suggestion: not a
 *		SELECT, or its rangetable contains no user relation we would
 *		ever index.
 */
static bool
dbblue_query_should_skip(Query *query)
{

	ListCell   *lc;

	if (query->commandType != CMD_SELECT)
		return true;

	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_RELATION &&
			!dbblue_relation_should_skip(rte->relid))
			return false;
	}
	return true;
}

/*
 * dbblue_first_col_index_exists
 *		True if some existing index on `relid` has `attno` as its leading
 *		column.  We use this to suppress redundant single-column
 *		suggestions.
 *
 * Implementation note: this used to scan pg_index directly via
 * table_open(IndexRelationId) + systable_beginscan, but that pattern
 * SIGSEGV'd reproducibly inside the bgworker on PG19 when the
 * surrounding workload was doing concurrent DDL (Odoo bootstrap).
 * The crash always landed in systable_beginscan itself, even after we
 * lifted the call out of expression_tree_walker.  Going through SPI
 * instead is robust: the SPI machinery handles its own snapshot,
 * resource owner, and catalog-cache concerns and is already exercised
 * elsewhere in the worker.
 */
static bool
dbblue_first_col_index_exists(Oid relid, AttrNumber attno)
{
	StringInfoData q;
	int			ret;
	bool		found = false;

	ereport(DEBUG1,
			(errmsg("dbblue advisor[trace]: fcie(SPI): relid=%u attno=%d",
					relid, attno)));

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		ereport(WARNING,
				(errmsg("dbblue index advisor: SPI_connect failed in fcie")));
		return false;
	}

	initStringInfo(&q);
	/*
	 * indkey is int2vector.  Casting it to int2[] doesn't reliably give
	 * a 1-based array (the historical lower bound is 0), so we parse the
	 * text form instead — int2vector text is always space-separated
	 * decimal integers, so split_part(indkey::text, ' ', 1) is the
	 * leading-column attno.
	 */
	appendStringInfo(&q,
					 "SELECT 1 FROM pg_catalog.pg_index "
					 "WHERE indrelid = %u "
					 "  AND indnatts > 0 "
					 "  AND split_part(indkey::text, ' ', 1)::int2 = %d::int2 "
					 "LIMIT 1",
					 relid, (int) attno);

	ret = SPI_execute(q.data, true, 1);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
		found = true;

	pfree(q.data);
	SPI_finish();

	ereport(DEBUG1,
			(errmsg("dbblue advisor[trace]: fcie(SPI): returning %d for relid=%u attno=%d",
					(int) found, relid, attno)));

	return found;
}

/*
 * dbblue_candidate_already_listed
 *		Linear-scan dedup for in-memory candidates.  Candidate counts per
 *		query are small (a few dozen at most), so O(n) is fine.
 */
static bool
dbblue_candidate_already_listed(List *candidates, Oid relid, AttrNumber attno)
{

	ListCell   *lc;

	foreach(lc, candidates)
	{
		IndexCandidate *c = (IndexCandidate *) lfirst(lc);

		if (c->relid == relid && c->attno == attno)
			return true;
	}
	return false;
}

/*
 * extract_var_walker
 *		Tree walker that collects every Var referencing a column on a
 *		user table and, if it is not already covered by an existing
 *		index, records it as an IndexCandidate.
 *
 * Visits subqueries and expression children automatically.  Whole-row
 * Vars and references to system columns are ignored.
 */
static bool
extract_var_walker(Node *node, CandidateWalkerCtx *ctx)
{

	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;
		RangeTblEntry *rte;
		Oid			relid;
		AttrNumber	attno;
		char	   *nspname;
		char	   *relname;
		IndexCandidate *cand;

		if (var->varlevelsup != 0)
			return false;
		if (var->varno <= 0 ||
			var->varno > list_length(ctx->query->rtable))
			return false;

		rte = rt_fetch(var->varno, ctx->query->rtable);
		if (rte->rtekind != RTE_RELATION)
			return false;

		relid = rte->relid;
		attno = var->varattno;

		/* Skip system columns (attno < 0) and whole-row refs (attno == 0). */
		if (attno <= 0)
			return false;

		if (dbblue_relation_should_skip(relid))
			return false;

		if (dbblue_candidate_already_listed(ctx->candidates, relid, attno))
			return false;

		/*
		 * Note: the existing-index check is intentionally NOT performed
		 * here.  Calling table_open(IndexRelationId) + systable_beginscan
		 * from inside an expression_tree_walker (itself running inside a
		 * BeginInternalSubTransaction in a bgworker, after SPI_finish)
		 * has been observed to SIGSEGV reproducibly on PG19 against an
		 * Odoo workload.  Filtering is deferred to the caller, which runs
		 * the check on the collected list once the walker returns.
		 */

		ereport(DEBUG1,
				(errmsg("dbblue advisor[trace]: walker: relid=%u attno=%d -> looking up names",
						relid, attno)));

		nspname = get_namespace_name(get_rel_namespace(relid));
		relname = get_rel_name(relid);
		if (nspname == NULL || relname == NULL)
		{
			ereport(DEBUG1,
					(errmsg("dbblue advisor[trace]: walker: relid=%u attno=%d name lookup returned NULL, skipping",
							relid, attno)));
			return false;
		}

		ereport(DEBUG1,
				(errmsg("dbblue advisor[trace]: walker: relid=%u attno=%d nsp=%s rel=%s -> building candidate",
						relid, attno, nspname, relname)));

		cand = (IndexCandidate *) palloc0(sizeof(*cand));
		cand->relid = relid;
		cand->attno = attno;
		cand->qualified_relname =
			pstrdup(quote_qualified_identifier(nspname, relname));
		cand->attname = pstrdup(get_attname(relid, attno, false));
		cand->ddl = psprintf("CREATE INDEX ON %s (%s)",
							 cand->qualified_relname,
							 quote_identifier(cand->attname));
		ctx->candidates = lappend(ctx->candidates, cand);

		ereport(DEBUG1,
				(errmsg("dbblue advisor[trace]: walker: candidate appended ddl=%s",
						cand->ddl)));

		return false;
	}

	if (IsA(node, Query))
		return query_tree_walker((Query *) node, extract_var_walker,
								 ctx, 0);

	return expression_tree_walker(node, extract_var_walker, ctx);
}

/*
 * dbblue_advisor_detect_hypopg
 *		Set dbblue_hypopg_available based on whether the hypopg extension
 *		is installed in our connected database.
 *
 * Called once at worker startup.  If hypopg is missing, the advisor
 * still parses queries and lists candidates in the log but skips cost
 * evaluation and writes no rows.
 */
static void
dbblue_advisor_detect_hypopg(void)
{

	int			ret;

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue index advisor: detecting hypopg");

	ret = SPI_execute("SELECT 1 FROM pg_extension WHERE extname = 'hypopg'",
					  true, 0);
	dbblue_hypopg_available = (ret == SPI_OK_SELECT && SPI_processed > 0);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	if (dbblue_hypopg_available)
		ereport(LOG,
				(errmsg("dbblue index advisor: hypopg extension detected; cost evaluation enabled")));
	else
		ereport(WARNING,
				(errmsg("dbblue index advisor: hypopg extension not installed in database \"%s\"; candidates will be logged but no rows will be written",
						dbblue_auto_index_suggestion_database),
				 errhint("Run CREATE EXTENSION hypopg in that database to enable cost-based suggestions.")));
}

/*
 * dbblue_advisor_get_plan_cost
 *		EXPLAIN (FORMAT JSON, GENERIC_PLAN) the query and return the
 *		planner-estimated total cost of the top-level plan.
 *
 * Returns -1.0 on any failure (planner error, parse failure, missing
 * relation, etc.) so callers can skip the query without raising.
 *
 * Caller must already be inside SPI_connect.  GENERIC_PLAN is required
 * because pg_stat_statements stores normalised query text containing
 * $1/$2 parameter placeholders; without GENERIC_PLAN the planner
 * complains about missing parameters.
 */
static double
dbblue_advisor_get_plan_cost(const char *query_text)
{

	StringInfoData explain_sql;
	int			ret;
	double		cost = -1.0;

	initStringInfo(&explain_sql);
	appendStringInfo(&explain_sql,
					 "EXPLAIN (FORMAT JSON, GENERIC_PLAN) %s",
					 query_text);

	/*
	 * NOTE: read_only must be false here.  EXPLAIN is rejected by SPI
	 * when run in a read-only ("non-volatile") context with the error
	 * "EXPLAIN is not allowed in a non-volatile function".  We're not
	 * actually executing the inner query (no ANALYZE), just planning it,
	 * but SPI's check fires before that distinction is made.
	 */
	ret = SPI_execute(explain_sql.data, false, 0);

	if ((ret == SPI_OK_SELECT || ret == SPI_OK_UTILITY) &&
		SPI_processed >= 1 && SPI_tuptable != NULL)
	{
		bool		isnull;
		Datum		d;

		d = SPI_getbinval(SPI_tuptable->vals[0],
						  SPI_tuptable->tupdesc, 1, &isnull);
		if (!isnull)
		{
			char	   *json = TextDatumGetCString(d);
			const char *p;

			/*
			 * The first occurrence of "Total Cost" in the JSON belongs
			 * to the topmost Plan node — that is the cost we want.
			 * Sub-plans, sub-queries and Inner/Outer plans appear later.
			 */
			p = strstr(json, "\"Total Cost\":");
			if (p != NULL)
			{
				p += strlen("\"Total Cost\":");
				while (*p == ' ' || *p == '\t' || *p == '\n')
					p++;
				cost = strtod(p, NULL);
			}
			pfree(json);
		}
	}

	pfree(explain_sql.data);
	return cost;
}

/*
 * dbblue_advisor_reset_hypotheticals
 *		Drop every hypothetical index registered in this backend.
 *
 * HypoPG state is per-backend memory, not transactional, so we must
 * explicitly reset between candidates to ensure each evaluation sees
 * exactly one hypothetical index.
 */
static void
dbblue_advisor_reset_hypotheticals(void)
{
	SPI_execute("SELECT hypopg_reset()", false, 0);
}

/*
 * dbblue_advisor_record_suggestion
 *		Insert a winning candidate into public.dbblue_index_suggestions,
 *		or refresh the existing row's metrics if we have already seen
 *		this (relation, index_columns) combination.
 *
 * String values from untrusted sources (the pg_stat_statements query
 * text in particular) are passed through quote_literal_cstr to avoid
 * any chance of SQL injection from a hostile query body.
 */
static void
dbblue_advisor_record_suggestion(StatStatementsRow *row,
								 IndexCandidate *cand,
								 double baseline,
								 double hypothetical,
								 double improvement_pct)
{

	StringInfoData sql;
	char	   *q_relname;
	char	   *q_attname;
	char	   *q_sample;
	char	   *q_ddl;

	q_relname = quote_literal_cstr(cand->qualified_relname);
	q_attname = quote_literal_cstr(cand->attname);
	q_sample = quote_literal_cstr(row->query_text);
	q_ddl = quote_literal_cstr(cand->ddl);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO public.dbblue_index_suggestions "
					 "(relation_oid, relation_name, index_columns, queryid, "
					 " sample_query, baseline_cost, hypothetical_cost, "
					 " cost_improvement_pct, total_calls, total_exec_time_ms, ddl) "
					 "VALUES (%u, %s, ARRAY[%s]::text[], %lld, "
					 "        %s, %.6f, %.6f, %.4f, %lld, %.6f, %s) "
					 "ON CONFLICT (relation_oid, index_columns) DO UPDATE SET "
					 "  last_seen_at = now(), "
					 "  queryid = EXCLUDED.queryid, "
					 "  sample_query = EXCLUDED.sample_query, "
					 "  baseline_cost = EXCLUDED.baseline_cost, "
					 "  hypothetical_cost = EXCLUDED.hypothetical_cost, "
					 "  cost_improvement_pct = EXCLUDED.cost_improvement_pct, "
					 "  total_calls = EXCLUDED.total_calls, "
					 "  total_exec_time_ms = EXCLUDED.total_exec_time_ms",
					 cand->relid, q_relname, q_attname,
					 (long long) row->queryid, q_sample,
					 baseline, hypothetical, improvement_pct,
					 (long long) row->calls, row->total_exec_time_ms, q_ddl);

	SPI_execute(sql.data, false, 0);

	pfree(sql.data);
	pfree(q_relname);
	pfree(q_attname);
	pfree(q_sample);
	pfree(q_ddl);
}

/*
 * dbblue_advisor_evaluate_candidates
 *		Drive the EXPLAIN baseline / hypopg / EXPLAIN hypothetical /
 *		threshold cycle for every candidate produced by Phase 3, and
 *		record the winners.
 *
 * Each candidate runs in a nested subtransaction so that a failure in
 * one (e.g. the column type does not have a btree opclass, or the
 * relation has been dropped) does not invalidate sibling candidates
 * or already-recorded winners.
 */
static void
dbblue_advisor_evaluate_candidates(StatStatementsRow *row, List *candidates)
{

	double		baseline;
	int			winners = 0;
	ListCell   *lc;

	if (!dbblue_hypopg_available)
		return;

	ereport(DEBUG1,
			(errmsg("dbblue advisor[trace]: evaluate_candidates: queryid=%lld, %d candidate(s) — Phase 4 cost-eval ENABLED",
					(long long) row->queryid, list_length(candidates))));

	SPI_connect();

	ereport(DEBUG1, (errmsg("dbblue advisor[trace]: SPI_connect ok; resetting hypopg")));
	dbblue_advisor_reset_hypotheticals();

	ereport(DEBUG1, (errmsg("dbblue advisor[trace]: getting baseline cost")));
	baseline = dbblue_advisor_get_plan_cost(row->query_text);
	ereport(DEBUG1, (errmsg("dbblue advisor[trace]: baseline=%.2f", baseline)));

	if (baseline < 0.0)
	{
		ereport(LOG,
				(errmsg("dbblue index advisor: queryid=%lld baseline EXPLAIN failed; skipping cost evaluation",
						(long long) row->queryid)));
		SPI_finish();
		return;
	}

	foreach(lc, candidates)
	{
		IndexCandidate *cand = (IndexCandidate *) lfirst(lc);
		MemoryContext oldcxt = CurrentMemoryContext;
		ResourceOwner oldowner = CurrentResourceOwner;

		ereport(DEBUG1,
				(errmsg("dbblue advisor[trace]: candidate begin: %s", cand->ddl)));

		BeginInternalSubTransaction(NULL);

		PG_TRY();
		{
			StringInfoData create_sql;
			char	   *q_ddl;
			double		new_cost;
			double		improvement_pct;

			q_ddl = quote_literal_cstr(cand->ddl);
			initStringInfo(&create_sql);
			appendStringInfo(&create_sql,
							 "SELECT hypopg_create_index(%s)", q_ddl);

			ereport(DEBUG1,
					(errmsg("dbblue advisor[trace]: calling hypopg_create_index")));
			SPI_execute(create_sql.data, false, 0);
			ereport(DEBUG1,
					(errmsg("dbblue advisor[trace]: hypopg_create_index returned")));
			pfree(create_sql.data);
			pfree(q_ddl);

			ereport(DEBUG1,
					(errmsg("dbblue advisor[trace]: getting hypothetical cost")));
			new_cost = dbblue_advisor_get_plan_cost(row->query_text);
			ereport(DEBUG1,
					(errmsg("dbblue advisor[trace]: hypothetical_cost=%.2f", new_cost)));

			/*
			 * Always reset, win or lose, so the next candidate starts
			 * from a clean slate.
			 */
			ereport(DEBUG1, (errmsg("dbblue advisor[trace]: resetting hypopg post-candidate")));
			dbblue_advisor_reset_hypotheticals();

			if (new_cost >= 0.0 && new_cost < baseline)
			{
				improvement_pct = ((baseline - new_cost) / baseline) * 100.0;

				if (improvement_pct >=
					dbblue_auto_index_suggestion_min_cost_improvement * 100.0)
				{
					dbblue_advisor_record_suggestion(row, cand, baseline,
													 new_cost, improvement_pct);
					winners++;
					ereport(LOG,
							(errmsg("dbblue index advisor: WINNER queryid=%lld %s baseline=%.2f -> %.2f (%.1f%% improvement)",
									(long long) row->queryid, cand->ddl,
									baseline, new_cost, improvement_pct)));
				}
				else
				{
					ereport(LOG,
							(errmsg("dbblue index advisor: queryid=%lld %s below threshold: baseline=%.2f -> %.2f (%.1f%% < %.1f%% required)",
									(long long) row->queryid, cand->ddl,
									baseline, new_cost, improvement_pct,
									dbblue_auto_index_suggestion_min_cost_improvement * 100.0)));
				}
			}

			ReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcxt);
			CurrentResourceOwner = oldowner;
		}
		PG_CATCH();
		{
			ErrorData  *edata;

			MemoryContextSwitchTo(oldcxt);
			edata = CopyErrorData();
			FlushErrorState();
			RollbackAndReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcxt);
			CurrentResourceOwner = oldowner;

			ereport(LOG,
					(errmsg("dbblue index advisor: queryid=%lld candidate %s failed: %s",
							(long long) row->queryid, cand->ddl,
							edata->message)));
			FreeErrorData(edata);
		}
		PG_END_TRY();
	}

	if (winners > 0)
		ereport(LOG,
				(errmsg("dbblue index advisor: queryid=%lld recorded %d winning suggestion(s)",
						(long long) row->queryid, winners)));

	SPI_finish();
}

/*
 * dbblue_advisor_process_query
 *		Parse, analyse, and walk a single query under its own internal
 *		subtransaction.  An error in any one query (e.g. it references
 *		a relation that has since been dropped) is logged but does not
 *		abort the surrounding tick.
 */
static void
dbblue_advisor_process_query(StatStatementsRow *row)
{

	MemoryContext oldcontext = CurrentMemoryContext;
	ResourceOwner oldowner = CurrentResourceOwner;
	size_t		qlen;

	qlen = strlen(row->query_text);

	/*
	 * Hard length cap.  See DBBLUE_MAX_QUERY_TEXT_LEN.  Skip large queries
	 * before we even start a subtransaction, so a pathological text cannot
	 * crash the worker via parse-analyze recursion / stack overflow.
	 */
	if (qlen > DBBLUE_MAX_QUERY_TEXT_LEN)
	{
		ereport(LOG,
				(errmsg("dbblue index advisor: queryid=%lld skipped (text length %zu > %d byte cap)",
						(long long) row->queryid, qlen,
						DBBLUE_MAX_QUERY_TEXT_LEN)));
		return;
	}

	/*
	 * Diagnostic breadcrumb.  Emitted before every parse so that if the
	 * worker SIGSEGVs the immediately preceding LOG line identifies the
	 * offending queryid and the start of its text.  Truncated to 200 chars
	 * so the log stays readable.  Remove or downgrade once the parser-path
	 * crash is understood.
	 */
	ereport(DEBUG1,
			(errmsg("dbblue advisor[trace]: about to parse queryid=%lld len=%zu text=%.200s",
					(long long) row->queryid, qlen, row->query_text)));

	BeginInternalSubTransaction(NULL);

	PG_TRY();
	{
		List	   *parsetree_list;

		parsetree_list = pg_parse_query(row->query_text);

		if (parsetree_list == NIL)
		{
			ereport(LOG,
					(errmsg("dbblue index advisor: queryid=%lld unparseable, skipped",
							(long long) row->queryid)));
		}
		else
		{
			RawStmt    *raw = (RawStmt *) linitial(parsetree_list);
			Oid		   *paramTypes = NULL;
			int			numParams = 0;
			Query	   *query;

			query = parse_analyze_varparams(raw, row->query_text,
											&paramTypes, &numParams,
											NULL);

			ereport(DEBUG1,
					(errmsg("dbblue advisor[trace]: parsed queryid=%lld cmdtype=%d rtable_len=%d",
							(long long) row->queryid,
							(int) query->commandType,
							list_length(query->rtable))));

			if (dbblue_query_should_skip(query))
			{
				ereport(LOG,
						(errmsg("dbblue index advisor: queryid=%lld skipped (non-SELECT or only catalog tables)",
								(long long) row->queryid)));
			}
			else
			{
				CandidateWalkerCtx ctx;
				ListCell   *lc;

				memset(&ctx, 0, sizeof(ctx));
				ctx.query = query;
				ctx.candidates = NIL;

				ereport(DEBUG1,
						(errmsg("dbblue advisor[trace]: walking queryid=%lld jointree=%s havingQual=%s",
								(long long) row->queryid,
								query->jointree ? "yes" : "no",
								query->havingQual ? "yes" : "no")));

				/*
				 * Walk WHERE / JOIN clauses (jointree carries both) and
				 * HAVING.  Targetlist Vars are intentionally not visited:
				 * SELECT-list columns rarely benefit from indexing and
				 * including them would generate noise.
				 */
				if (query->jointree != NULL)
					expression_tree_walker((Node *) query->jointree,
										   extract_var_walker, &ctx);

				ereport(DEBUG1,
						(errmsg("dbblue advisor[trace]: walked jointree queryid=%lld candidates=%d",
								(long long) row->queryid,
								list_length(ctx.candidates))));

				if (query->havingQual != NULL)
					expression_tree_walker(query->havingQual,
										   extract_var_walker, &ctx);

				ereport(DEBUG1,
						(errmsg("dbblue advisor[trace]: walked all queryid=%lld candidates=%d",
								(long long) row->queryid,
								list_length(ctx.candidates))));

				/*
				 * Post-walker filter: drop candidates whose attno is
				 * already the leading column of an existing index.  We
				 * deferred this check out of the walker callback because
				 * doing systable_beginscan from inside an
				 * expression_tree_walker callback (under
				 * BeginInternalSubTransaction in a bgworker, after
				 * SPI_finish) has been seen to SIGSEGV reproducibly on
				 * PG19 with the Odoo workload.  Running the check from
				 * the surrounding subxact level — outside any tree
				 * walker — is stable.
				 */
				if (ctx.candidates != NIL)
				{
					List	   *kept = NIL;
					ListCell   *lc2;

					foreach(lc2, ctx.candidates)
					{
						IndexCandidate *c = (IndexCandidate *) lfirst(lc2);

						if (dbblue_first_col_index_exists(c->relid, c->attno))
						{
							ereport(DEBUG1,
									(errmsg("dbblue advisor[trace]: queryid=%lld dropping candidate %s (already covered)",
											(long long) row->queryid, c->ddl)));
							continue;
						}
						kept = lappend(kept, c);
					}
					ctx.candidates = kept;

					ereport(DEBUG1,
							(errmsg("dbblue advisor[trace]: filtered queryid=%lld candidates=%d after existing-index prune",
									(long long) row->queryid,
									list_length(ctx.candidates))));
				}

				if (ctx.candidates == NIL)
				{
					ereport(LOG,
							(errmsg("dbblue index advisor: queryid=%lld no candidates (all columns already indexed or filtered)",
									(long long) row->queryid)));
				}
				else
				{
					ereport(LOG,
							(errmsg("dbblue index advisor: queryid=%lld -> %d candidate(s) (calls=%lld total=%.2fms)",
									(long long) row->queryid,
									list_length(ctx.candidates),
									(long long) row->calls,
									row->total_exec_time_ms)));
					foreach(lc, ctx.candidates)
					{
						IndexCandidate *c = (IndexCandidate *) lfirst(lc);

						ereport(LOG,
								(errmsg("dbblue index advisor:   %s",
										c->ddl)));
					}

					/*
					 * Phase 4: cost-evaluate each candidate via hypopg
					 * and record winners into dbblue_index_suggestions.
					 * No-op when hypopg is not installed.
					 */
					dbblue_advisor_evaluate_candidates(row, ctx.candidates);
				}
			}
		}

		/* Successful path: commit the subtransaction. */
		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcontext);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		/* Capture the error before unwinding the subxact. */
		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcontext);
		CurrentResourceOwner = oldowner;

		ereport(LOG,
				(errmsg("dbblue index advisor: queryid=%lld parse/analyse failed: %s",
						(long long) row->queryid, edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();
}

/*
 * dbblue_advisor_run_tick
 *		One iteration of analysis.
 *
 * Read the top-N expensive queries from pg_stat_statements for our DB,
 * detach from SPI, then analyse each row under its own subtransaction.
 */
static void
dbblue_advisor_run_tick(void)
{
	StringInfoData query;
	MemoryContext per_tick_cxt;
	MemoryContext oldcxt;
	List	   *rows = NIL;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT queryid, calls, total_exec_time, "
					 "       query "
					 "FROM pg_stat_statements "
					 "WHERE dbid = %u "
					 "  AND queryid IS NOT NULL "
					 "  AND calls >= %d "
					 "ORDER BY total_exec_time DESC "
					 "LIMIT %d",
					 MyDatabaseId,
					 dbblue_auto_index_suggestion_min_calls,
					 dbblue_auto_index_suggestion_top_n_queries);

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue index advisor: scanning pg_stat_statements");

	/*
	 * Per-tick context for the rows list and any palloc'd candidate
	 * strings.  Lives until end of tick, then is freed in one shot.
	 */
	per_tick_cxt = AllocSetContextCreate(TopTransactionContext,
										 "dbblue advisor tick",
										 ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(per_tick_cxt);

	PG_TRY();
	{
		int			ret;
		uint64		i;

		SPI_connect();

		ret = SPI_execute(query.data, true, 0);
		if (ret != SPI_OK_SELECT)
		{
			ereport(WARNING,
					(errmsg("dbblue index advisor: pg_stat_statements query failed (SPI rc=%d)",
							ret)));
			SPI_finish();
		}
		else
		{
			/*
			 * SPI_connect switched CurrentMemoryContext to its private
			 * procCxt, which gets freed by SPI_finish.  We need the rows
			 * list (and its cells, allocated by lappend in
			 * CurrentMemoryContext) to outlive SPI_finish, so switch
			 * back to per_tick_cxt for the loop.
			 */
			MemoryContextSwitchTo(per_tick_cxt);

			for (i = 0; i < SPI_processed; i++)
			{
				HeapTuple	tup = SPI_tuptable->vals[i];
				TupleDesc	desc = SPI_tuptable->tupdesc;
				bool		qid_isnull,
							calls_isnull,
							time_isnull,
							text_isnull;
				Datum		qid_d,
							calls_d,
							time_d,
							text_d;
				StatStatementsRow *r;

				qid_d = SPI_getbinval(tup, desc, 1, &qid_isnull);
				calls_d = SPI_getbinval(tup, desc, 2, &calls_isnull);
				time_d = SPI_getbinval(tup, desc, 3, &time_isnull);
				text_d = SPI_getbinval(tup, desc, 4, &text_isnull);

				if (qid_isnull || calls_isnull || time_isnull || text_isnull)
					continue;

				r = (StatStatementsRow *) palloc(sizeof(*r));
				r->queryid = DatumGetInt64(qid_d);
				r->calls = DatumGetInt64(calls_d);
				r->total_exec_time_ms = DatumGetFloat8(time_d);
				r->query_text = TextDatumGetCString(text_d);

				rows = lappend(rows, r);
			}

			SPI_finish();
		}

		ereport(LOG,
				(errmsg("dbblue index advisor: tick examined %d candidate queries",
						list_length(rows))));

		/*
		 * SPI is now closed.  Process each row under its own internal
		 * subtransaction so a parse error does not poison the rest of
		 * the tick.
		 */
		{
			ListCell   *lc;

			foreach(lc, rows)
			{
				StatStatementsRow *r = (StatStatementsRow *) lfirst(lc);

				dbblue_advisor_process_query(r);
			}
		}
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcxt);
		edata = CopyErrorData();
		FlushErrorState();

		ereport(WARNING,
				(errmsg("dbblue index advisor: tick failed: %s",
						edata->message),
				 errhint("Check that pg_stat_statements is loaded via shared_preload_libraries and CREATE EXTENSION pg_stat_statements has been run in database \"%s\".",
						 dbblue_auto_index_suggestion_database)));
		FreeErrorData(edata);

		/*
		 * SPI_finish is idempotent — safe even if we never connected.
		 * AbortCurrentTransaction tears down TopTransactionContext and
		 * everything below it (including per_tick_cxt), so we must NOT
		 * try to free per_tick_cxt ourselves.
		 */
		SPI_finish();
		AbortCurrentTransaction();
		pgstat_report_activity(STATE_IDLE, NULL);
		pfree(query.data);
		return;
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcxt);
	/* per_tick_cxt is a child of TopTransactionContext; it gets freed
	 * along with the surrounding transaction below. */
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
	pfree(query.data);
}

/*
 * DbblueIndexAdvisorMain
 *		Entry point for the dbblue index advisor background worker.
 */
void
DbblueIndexAdvisorMain(Datum main_arg)
{
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(dbblue_auto_index_suggestion_database,
										 NULL, 0);

	ereport(LOG,
			(errmsg("dbblue index advisor started (database=\"%s\")",
					dbblue_auto_index_suggestion_database)));

	dbblue_advisor_ensure_results_table();
	dbblue_advisor_detect_hypopg();

	while (!ShutdownRequestPending)
	{
		long		timeout_ms;
		int			rc;

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (dbblue_auto_index_suggestion_enabled)
			dbblue_advisor_run_tick();

		timeout_ms = (long) dbblue_auto_index_suggestion_interval * 1000L;
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   timeout_ms,
					   WAIT_EVENT_DBBLUE_INDEX_ADVISOR_MAIN);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	ereport(LOG, (errmsg("dbblue index advisor shutting down")));
	proc_exit(0);
}

/*
 * DbblueIndexAdvisorRegister
 *		Register the dbblue index advisor as a static background worker.
 */
void
DbblueIndexAdvisorRegister(void)
{
	BackgroundWorker bgw;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
					BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_restart_time = 60;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "DbblueIndexAdvisorMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "dbblue index advisor");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "dbblue index advisor");
	bgw.bgw_main_arg = (Datum) 0;
	bgw.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&bgw);
}
