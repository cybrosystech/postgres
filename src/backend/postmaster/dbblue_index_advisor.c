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
 * dbblue_check_advisor_enabled
 *		GUC check_hook for dbblue_auto_index_suggestion_enabled.
 *
 * Fires synchronously in the backend that runs ALTER SYSTEM SET (or
 * any other source that changes the GUC), so any ereport() the hook
 * emits is delivered straight to the client (psql, the application,
 * etc.) — not just to the server log.
 *
 * The hook does NOT block the value change.  It only complains when
 * the operator is turning the feature ON while pg_stat_statements is
 * not yet in shared_preload_libraries — a configuration that would
 * cause every advisor tick to fail.  shared_preload_libraries is a
 * PGC_POSTMASTER GUC and a backend cannot change it; the only fix is
 * a server restart, so we surface the warning as early and as visibly
 * as possible.
 *
 * Returning true unconditionally means the value is always accepted;
 * the operator can turn the feature on without pg_stat_statements
 * preloaded if they want (the worker will keep parking and warning
 * each tick), and they can turn it off freely.
 */
bool
dbblue_check_advisor_enabled(bool *newval, void **extra, GucSource source)
{
	const char *spl;

	/* Only complain when the operator is turning the feature ON. */
	if (!(*newval))
		return true;

	/*
	 * Inspect the current shared_preload_libraries setting.  We pass
	 * missing_ok=true defensively even though this GUC always exists.
	 * restrict_privileged=false because we don't need superuser-only
	 * values; SPL is readable by anyone.
	 */
	spl = GetConfigOption("shared_preload_libraries", true, false);

	if (spl == NULL || *spl == '\0' ||
		strstr(spl, "pg_stat_statements") == NULL)
	{
		printf(_("WARNING: dbblue_auto_index_suggestion_enabled is being set to on, but pg_stat_statements is not in shared_preload_libraries"));
		ereport(NOTICE,
				(errmsg("dbblue_auto_index_suggestion_enabled is being set to on, but pg_stat_statements is not in shared_preload_libraries"),
				 errdetail("The advisor reads workload statistics from pg_stat_statements; without that library preloaded into shared memory, every advisor tick will fail and no index suggestions will be produced."),
				 errhint("Run: ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';\n"
						 "Then restart the server (a restart is required because shared_preload_libraries is a postmaster-only GUC).")));
	}

	return true;
}

/*
 * One key column of a candidate index.  Order in the parent
 * IndexCandidate's `cols` array is the order in which the column will
 * appear in the generated CREATE INDEX statement (leading column first).
 */
typedef struct IndexColumn
{
	AttrNumber	attno;
	char	   *attname;			/* palloc'd */
} IndexColumn;

/*
 * One candidate index, derived from a single query.
 *
 * `access_method` is "btree" by default; "hash" for hash variants.
 * `partial_predicate` is NULL for non-partial indexes, otherwise the
 * SQL fragment to splice into "CREATE INDEX ... WHERE <predicate>"
 * (e.g. "active IS TRUE").
 */
typedef struct IndexCandidate
{
	Oid			relid;
	char	   *qualified_relname;	/* schema.table, palloc'd */
	int			n_cols;				/* >= 1 */
	IndexColumn *cols;				/* palloc'd array of length n_cols */
	char	   *access_method;		/* "btree" or "hash"; palloc'd */
	char	   *partial_predicate;	/* NULL or "<col> IS TRUE/FALSE/NULL" */
	char	   *ddl;				/* CREATE INDEX ... statement */
} IndexCandidate;

/*
 * Phase 5.2: per-column role classification for one relation
 * referenced by a query.
 *
 * Each column may play several roles in a single query (e.g., the
 * same column referenced in WHERE and ORDER BY).  The classifier
 * walks the parse tree and records a (relid, attno, role) tuple in
 * the appropriate per-relid attno-list.  Lists are dedup'd so each
 * (attno, role) pair appears at most once per relid.
 *
 * 5.2 only populates the inventory; the candidate generator still
 * emits single-column DDL.  5.3 will consume rel_infos to emit
 * composite, partial, INCLUDE, and hash variants.
 */
typedef enum DbblueColRole
{
	DBBLUE_ROLE_EQ,				/* col = / IN / ANY-array */
	DBBLUE_ROLE_RANGE,			/* col <, <=, >, >= */
	DBBLUE_ROLE_SORT,			/* ORDER BY col */
	DBBLUE_ROLE_GROUP,			/* GROUP BY col */
} DbblueColRole;

/*
 * Kinds of constant-comparison clauses we recognise as candidates
 * for a partial-index predicate.  Order is important: the formatter
 * indexes into a small string table.
 */
typedef enum DbbluePartialKind
{
	DBBLUE_PARTIAL_IS_TRUE = 0,
	DBBLUE_PARTIAL_IS_NOT_TRUE,
	DBBLUE_PARTIAL_IS_FALSE,
	DBBLUE_PARTIAL_IS_NOT_FALSE,
	DBBLUE_PARTIAL_IS_NULL,
	DBBLUE_PARTIAL_IS_NOT_NULL,
} DbbluePartialKind;

typedef struct DbbluePartialClause
{
	AttrNumber			attno;
	DbbluePartialKind	kind;
} DbbluePartialClause;

typedef struct DbblueRelInfo
{
	Oid			relid;
	List	   *eq_attnos;		/* List of int (AttrNumber) */
	List	   *range_attnos;
	List	   *sort_attnos;
	List	   *group_attnos;
	List	   *partial_clauses;	/* List of DbbluePartialClause * */
} DbblueRelInfo;

/* Walker context for extract_var_walker and the predicate classifier. */
typedef struct CandidateWalkerCtx
{
	Query	   *query;				/* root analysed query (for rtable) */
	List	   *candidates;			/* List of IndexCandidate * */
	List	   *rel_infos;			/* List of DbblueRelInfo * (one per relid) */
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
static void dbblue_advisor_ensure_extensions(void);
static bool dbblue_try_create_extension(const char *extname);
static void dbblue_advisor_run_tick(void);
static void dbblue_advisor_process_query(StatStatementsRow *row);
static bool dbblue_query_should_skip(Query *query);
static bool dbblue_relation_should_skip(Oid relid);
static bool dbblue_first_col_index_exists(Oid relid, AttrNumber attno);
static bool dbblue_candidate_already_listed(List *candidates, Oid relid,
											int key_cols,
											const AttrNumber *key_attnos,
											const char *access_method,
											const char *partial_predicate);
static bool extract_var_walker(Node *node, CandidateWalkerCtx *ctx);
static DbblueRelInfo *dbblue_relinfo_for(CandidateWalkerCtx *ctx, Oid relid);
static void dbblue_relinfo_add(DbblueRelInfo *ri, AttrNumber attno,
							   DbblueColRole role);
static bool dbblue_op_role(Oid opno, DbblueColRole *role);
static void dbblue_classify_var(Node *arg, DbblueColRole role,
								CandidateWalkerCtx *ctx);
static void dbblue_classify_predicate(Node *clause, CandidateWalkerCtx *ctx);
static void dbblue_classify_sortgroup(Query *query, CandidateWalkerCtx *ctx);
static void dbblue_generate_composites(CandidateWalkerCtx *ctx);
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
	"    partial_predicate    text,"
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
	"    last_seen_at         timestamptz NOT NULL DEFAULT now()"
	")";

/*
 * Schema migration applied unconditionally on each ensure call so
 * fresh installs and upgrades converge on the same shape.  All steps
 * are idempotent.
 *
 * 1. Add partial_predicate column if the table existed without it.
 * 2. Drop any pre-Phase-5 unique constraint, then re-add one that
 *    distinguishes partial / non-partial and btree / hash variants
 *    (PG15+ NULLS NOT DISTINCT means NULL partial_predicate values
 *    still collide with each other, so ON CONFLICT works correctly).
 */
static const char *const dbblue_migrate_results_table_sql =
	"ALTER TABLE public.dbblue_index_suggestions "
	"  ADD COLUMN IF NOT EXISTS partial_predicate text; "
	"ALTER TABLE public.dbblue_index_suggestions "
	"  DROP CONSTRAINT IF EXISTS dbblue_index_suggestions_relation_oid_index_columns_key; "
	"ALTER TABLE public.dbblue_index_suggestions "
	"  DROP CONSTRAINT IF EXISTS dbblue_index_suggestions_unique_key; "
	"ALTER TABLE public.dbblue_index_suggestions "
	"  ADD CONSTRAINT dbblue_index_suggestions_unique_key "
	"    UNIQUE NULLS NOT DISTINCT "
	"    (relation_oid, index_columns, index_method, partial_predicate); ";

/*
 * dbblue_advisor_ensure_results_table
 *		Create public.dbblue_index_suggestions if it does not exist,
 *		and apply any schema migrations needed by newer phases.
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

	/*
	 * Apply schema migrations.  SPI_execute on a multi-statement
	 * string runs them sequentially; each ALTER is itself
	 * idempotent.  Wrap in a subtransaction so a partial failure
	 * (e.g. UNIQUE NULLS NOT DISTINCT not supported on older
	 * server) doesn't poison the parent transaction.
	 */
	{
		MemoryContext oldcxt = CurrentMemoryContext;
		ResourceOwner oldowner = CurrentResourceOwner;

		BeginInternalSubTransaction(NULL);
		PG_TRY();
		{
			SPI_execute(dbblue_migrate_results_table_sql, false, 0);
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

			ereport(WARNING,
					(errmsg("dbblue index advisor: schema migration failed: %s",
							edata->message)));
			FreeErrorData(edata);
		}
		PG_END_TRY();
	}

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
 *		True if a Query is uninteresting for index suggestion.
 *
 * Accepted command types:
 *   - CMD_SELECT — the obvious read path.
 *   - CMD_UPDATE — the executor still has to *find* matching rows
 *	   before modifying them; an index on the WHERE columns helps the
 *	   read sub-plan exactly the same way it helps a SELECT.
 *   - CMD_DELETE — same reasoning as UPDATE.
 *
 * Rejected:
 *   - CMD_INSERT — INSERTs into a table aren't sped up by indexes on
 *	   that table.  (INSERT ... SELECT has an embedded SELECT that
 *	   could benefit, but pulling that out is Phase 6+ work.)
 *   - CMD_UTILITY — ALTER, CREATE, BEGIN, SET, EXPLAIN, etc.
 *   - CMD_MERGE / CMD_NOTHING / others — not yet supported.
 *
 * Also rejects queries whose rangetable has no user relation we would
 * ever index (e.g. catalog-only or pg_temp queries).
 */
static bool
dbblue_query_should_skip(Query *query)
{
	ListCell   *lc;

	if (query->commandType != CMD_SELECT &&
		query->commandType != CMD_UPDATE &&
		query->commandType != CMD_DELETE)
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

	// ereport(DEBUG1,
	// 		(errmsg("dbblue advisor[trace]: fcie(SPI): returning %d for relid=%u attno=%d",
	// 				(int) found, relid, attno)));

	return found;
}

/*
 * dbblue_candidate_already_listed
 *		Linear-scan dedup for in-memory candidates.  Candidate counts per
 *		query are small (a few dozen at most), so O(n×k) where k is
 *		index width is fine.
 *
 * For Phase 5.1 the walker still emits single-column candidates, so
 * `key_cols` is always 1 and `key_attnos` holds a single attno; the
 * comparison degenerates to the original (relid, attno) check.  When
 * Phase 5.3 lands and emits composites, this same loop already does
 * the right thing because we compare the full ordered column list.
 */
static bool
dbblue_candidate_already_listed(List *candidates, Oid relid,
								int key_cols, const AttrNumber *key_attnos,
								const char *access_method,
								const char *partial_predicate)
{
	ListCell   *lc;

	foreach(lc, candidates)
	{
		IndexCandidate *c = (IndexCandidate *) lfirst(lc);
		int			i;

		if (c->relid != relid)
			continue;
		if (c->n_cols != key_cols)
			continue;
		for (i = 0; i < key_cols; i++)
			if (c->cols[i].attno != key_attnos[i])
				break;
		if (i != key_cols)
			continue;
		/* access method and partial predicate must also match for dedup. */
		if (strcmp(c->access_method, access_method) != 0)
			continue;
		if ((c->partial_predicate == NULL) != (partial_predicate == NULL))
			continue;
		if (c->partial_predicate != NULL &&
			strcmp(c->partial_predicate, partial_predicate) != 0)
			continue;
		return true;			/* full match */
	}
	return false;
}

/*
 * dbblue_relinfo_for
 *		Get the per-relid DbblueRelInfo entry for `relid`, creating an
 *		empty one if this is the first time we see this relation.
 */
static DbblueRelInfo *
dbblue_relinfo_for(CandidateWalkerCtx *ctx, Oid relid)
{
	ListCell   *lc;
	DbblueRelInfo *ri;

	foreach(lc, ctx->rel_infos)
	{
		ri = (DbblueRelInfo *) lfirst(lc);
		if (ri->relid == relid)
			return ri;
	}

	ri = (DbblueRelInfo *) palloc0(sizeof(*ri));
	ri->relid = relid;
	ctx->rel_infos = lappend(ctx->rel_infos, ri);
	return ri;
}

/*
 * dbblue_relinfo_add
 *		Append `attno` to the role list it belongs to, dedup'd.
 */
static void
dbblue_relinfo_add(DbblueRelInfo *ri, AttrNumber attno, DbblueColRole role)
{
	List	  **listp;
	ListCell   *lc;

	switch (role)
	{
		case DBBLUE_ROLE_EQ:
			listp = &ri->eq_attnos;
			break;
		case DBBLUE_ROLE_RANGE:
			listp = &ri->range_attnos;
			break;
		case DBBLUE_ROLE_SORT:
			listp = &ri->sort_attnos;
			break;
		case DBBLUE_ROLE_GROUP:
			listp = &ri->group_attnos;
			break;
		default:
			return;
	}

	foreach(lc, *listp)
	{
		if (lfirst_int(lc) == (int) attno)
			return;
	}
	*listp = lappend_int(*listp, (int) attno);
}

/*
 * dbblue_op_role
 *		Heuristic operator → role classifier based on the operator's
 *		name.  Covers the >99% case (`=`, `<`, `<=`, `>`, `>=`) without
 *		needing a full pg_amop opfamily lookup.
 *
 * Returns true and sets *role on a recognised operator; returns false
 * for anything else (custom types, geometric ops, etc.) — those
 * predicates are silently dropped from the inventory in 5.2 and may
 * be revisited in Phase 6.
 */
static bool
dbblue_op_role(Oid opno, DbblueColRole *role)
{
	char	   *opname = get_opname(opno);
	bool		got = false;

	if (opname == NULL)
		return false;

	if (strcmp(opname, "=") == 0)
	{
		*role = DBBLUE_ROLE_EQ;
		got = true;
	}
	else if (strcmp(opname, "<") == 0 || strcmp(opname, "<=") == 0 ||
			 strcmp(opname, ">") == 0 || strcmp(opname, ">=") == 0)
	{
		*role = DBBLUE_ROLE_RANGE;
		got = true;
	}

	pfree(opname);
	return got;
}

/*
 * dbblue_classify_var
 *		If `arg` is a Var on a real, user-relation column, register it
 *		under `role` in the per-relid info.  Same Var-acceptance checks
 *		as extract_var_walker.
 */
static void
dbblue_classify_var(Node *arg, DbblueColRole role, CandidateWalkerCtx *ctx)
{
	Var		   *v;
	RangeTblEntry *rte;
	DbblueRelInfo *ri;

	if (arg == NULL || !IsA(arg, Var))
		return;
	v = (Var *) arg;

	if (v->varlevelsup != 0)
		return;
	if (v->varno <= 0 ||
		v->varno > list_length(ctx->query->rtable))
		return;
	rte = rt_fetch(v->varno, ctx->query->rtable);
	if (rte->rtekind != RTE_RELATION)
		return;
	if (v->varattno <= 0)
		return;
	if (dbblue_relation_should_skip(rte->relid))
		return;

	ri = dbblue_relinfo_for(ctx, rte->relid);
	dbblue_relinfo_add(ri, v->varattno, role);
}

/*
 * dbblue_classify_predicate
 *		Recursively classify a WHERE/HAVING predicate node.
 *
 * Handled: BoolExpr (AND/OR/NOT — recurses into args), OpExpr
 * (=, <, <=, >, >= recognised), ScalarArrayOpExpr (`col IN (...)` /
 * `col = ANY(arr)` recorded as equality).  Other nodes silently
 * skipped — Phase 5.3 may extend to NullTest / FuncExpr.
 */
static void
dbblue_classify_predicate(Node *clause, CandidateWalkerCtx *ctx)
{
	if (clause == NULL)
		return;

	if (IsA(clause, BoolExpr))
	{
		BoolExpr   *be = (BoolExpr *) clause;
		ListCell   *lc;

		foreach(lc, be->args)
			dbblue_classify_predicate((Node *) lfirst(lc), ctx);
		return;
	}

	if (IsA(clause, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) clause;
		DbblueColRole role;
		ListCell   *lc;

		if (!dbblue_op_role(op->opno, &role))
			return;

		foreach(lc, op->args)
			dbblue_classify_var((Node *) lfirst(lc), role, ctx);
		return;
	}

	if (IsA(clause, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *sao = (ScalarArrayOpExpr *) clause;
		DbblueColRole role;

		if (!dbblue_op_role(sao->opno, &role))
			return;
		if (role != DBBLUE_ROLE_EQ)
			return;

		if (list_length(sao->args) >= 1)
			dbblue_classify_var((Node *) linitial(sao->args),
								DBBLUE_ROLE_EQ, ctx);
		return;
	}

	/*
	 * NullTest: `col IS NULL` / `col IS NOT NULL`.  We treat the column
	 * as equality (it's a selective filter) AND record the clause as a
	 * candidate partial-index predicate so the generator can emit a
	 * partial variant.
	 */
	if (IsA(clause, NullTest))
	{
		NullTest   *nt = (NullTest *) clause;

		ereport(DEBUG1,
				(errmsg("dbblue advisor[trace]: classifier saw NullTest type=%d arg_tag=%d",
						(int) nt->nulltesttype,
						nt->arg ? (int) nodeTag(nt->arg) : -1)));

		if (nt->arg != NULL && IsA(nt->arg, Var))
		{
			Var		   *v = (Var *) nt->arg;
			RangeTblEntry *rte;

			if (v->varlevelsup != 0 ||
				v->varno <= 0 ||
				v->varno > list_length(ctx->query->rtable))
				return;
			rte = rt_fetch(v->varno, ctx->query->rtable);
			if (rte->rtekind != RTE_RELATION)
				return;
			if (v->varattno <= 0)
				return;
			if (dbblue_relation_should_skip(rte->relid))
				return;

			dbblue_classify_var((Node *) v, DBBLUE_ROLE_EQ, ctx);

			/* Record the partial-index candidate clause. */
			{
				DbblueRelInfo *ri = dbblue_relinfo_for(ctx, rte->relid);
				DbbluePartialClause *pc = palloc(sizeof(*pc));

				pc->attno = v->varattno;
				pc->kind = (nt->nulltesttype == IS_NULL)
					? DBBLUE_PARTIAL_IS_NULL
					: DBBLUE_PARTIAL_IS_NOT_NULL;
				ri->partial_clauses = lappend(ri->partial_clauses, pc);
			}
		}
		return;
	}

	/*
	 * BooleanTest: `col IS TRUE` / `IS FALSE` / `IS NOT TRUE` /
	 * `IS NOT FALSE`.  Treated the same way as NullTest — record a
	 * partial clause and tag the column as equality.
	 */
	if (IsA(clause, BooleanTest))
	{
		BooleanTest *bt = (BooleanTest *) clause;
		DbbluePartialKind kind;
		bool		recognised = true;

		ereport(DEBUG1,
				(errmsg("dbblue advisor[trace]: classifier saw BooleanTest type=%d arg_tag=%d",
						(int) bt->booltesttype,
						bt->arg ? (int) nodeTag(bt->arg) : -1)));

		switch (bt->booltesttype)
		{
			case IS_TRUE:
				kind = DBBLUE_PARTIAL_IS_TRUE;
				break;
			case IS_NOT_TRUE:
				kind = DBBLUE_PARTIAL_IS_NOT_TRUE;
				break;
			case IS_FALSE:
				kind = DBBLUE_PARTIAL_IS_FALSE;
				break;
			case IS_NOT_FALSE:
				kind = DBBLUE_PARTIAL_IS_NOT_FALSE;
				break;
			default:
				recognised = false;
				break;
		}
		if (!recognised)
			return;

		if (bt->arg != NULL && IsA(bt->arg, Var))
		{
			Var		   *v = (Var *) bt->arg;
			RangeTblEntry *rte;

			if (v->varlevelsup != 0 ||
				v->varno <= 0 ||
				v->varno > list_length(ctx->query->rtable))
				return;
			rte = rt_fetch(v->varno, ctx->query->rtable);
			if (rte->rtekind != RTE_RELATION)
				return;
			if (v->varattno <= 0)
				return;
			if (dbblue_relation_should_skip(rte->relid))
				return;

			dbblue_classify_var((Node *) v, DBBLUE_ROLE_EQ, ctx);

			{
				DbblueRelInfo *ri = dbblue_relinfo_for(ctx, rte->relid);
				DbbluePartialClause *pc = palloc(sizeof(*pc));

				pc->attno = v->varattno;
				pc->kind = kind;
				ri->partial_clauses = lappend(ri->partial_clauses, pc);
			}
		}
		return;
	}

	/* Other node types: not classified. */
}

/*
 * dbblue_classify_sortgroup
 *		Walk query->sortClause and query->groupClause to record each
 *		referenced Var as SORT or GROUP, respectively.  Each clause
 *		entry's tleSortGroupRef points back into query->targetList.
 */
static void
dbblue_classify_sortgroup(Query *query, CandidateWalkerCtx *ctx)
{
	ListCell   *lc_clause;
	ListCell   *lc_tle;

	foreach(lc_clause, query->sortClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc_clause);

		foreach(lc_tle, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc_tle);

			if (tle->ressortgroupref == sgc->tleSortGroupRef)
			{
				dbblue_classify_var((Node *) tle->expr,
									DBBLUE_ROLE_SORT, ctx);
				break;
			}
		}
	}

	foreach(lc_clause, query->groupClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc_clause);

		foreach(lc_tle, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc_tle);

			if (tle->ressortgroupref == sgc->tleSortGroupRef)
			{
				dbblue_classify_var((Node *) tle->expr,
									DBBLUE_ROLE_GROUP, ctx);
				break;
			}
		}
	}
}

/*
 * dbblue_partial_predicate_text
 *		Format a (relid, attno, kind) partial clause as the SQL fragment
 *		that goes after WHERE in CREATE INDEX ... WHERE <pred>.
 *		The result is palloc'd in CurrentMemoryContext.  Returns NULL
 *		on lookup failure.
 */
static char *
dbblue_partial_predicate_text(Oid relid, AttrNumber attno,
							  DbbluePartialKind kind)
{
	static const char *const suffix[] = {
		"IS TRUE",			/* DBBLUE_PARTIAL_IS_TRUE */
		"IS NOT TRUE",		/* DBBLUE_PARTIAL_IS_NOT_TRUE */
		"IS FALSE",			/* DBBLUE_PARTIAL_IS_FALSE */
		"IS NOT FALSE",		/* DBBLUE_PARTIAL_IS_NOT_FALSE */
		"IS NULL",			/* DBBLUE_PARTIAL_IS_NULL */
		"IS NOT NULL",		/* DBBLUE_PARTIAL_IS_NOT_NULL */
	};
	char	   *attname = get_attname(relid, attno, false);

	if (attname == NULL)
		return NULL;
	return psprintf("%s %s", quote_identifier(attname), suffix[kind]);
}

/*
 * dbblue_emit_one_candidate
 *		Helper used by the generator: take an ordered key list, an
 *		access method ("btree" / "hash"), and an optional partial
 *		predicate, and append the IndexCandidate to ctx->candidates.
 *
 *		Skips emission if the same (relid, cols, access_method,
 *		partial_predicate) combination is already on the list.
 */
static void
dbblue_emit_one_candidate(CandidateWalkerCtx *ctx, Oid relid,
						  const char *qualified_relname,
						  int n_keys, const AttrNumber *key_attnos,
						  const char *access_method,
						  const char *partial_predicate)
{
	IndexCandidate *cand;
	StringInfoData ddl;
	int			i;
	bool		ok = true;

	if (dbblue_candidate_already_listed(ctx->candidates, relid,
										n_keys, key_attnos,
										access_method, partial_predicate))
		return;

	cand = (IndexCandidate *) palloc0(sizeof(*cand));
	cand->relid = relid;
	cand->qualified_relname = pstrdup(qualified_relname);
	cand->n_cols = n_keys;
	cand->cols = (IndexColumn *) palloc(n_keys * sizeof(IndexColumn));
	cand->access_method = pstrdup(access_method);
	cand->partial_predicate = partial_predicate
		? pstrdup(partial_predicate) : NULL;

	initStringInfo(&ddl);
	appendStringInfo(&ddl, "CREATE INDEX ON %s", cand->qualified_relname);
	if (strcmp(access_method, "btree") != 0)
		appendStringInfo(&ddl, " USING %s", access_method);
	appendStringInfoString(&ddl, " (");
	for (i = 0; i < n_keys; i++)
	{
		char	   *attname = get_attname(relid, key_attnos[i], false);

		if (attname == NULL)
		{
			ok = false;
			break;
		}
		cand->cols[i].attno = key_attnos[i];
		cand->cols[i].attname = pstrdup(attname);
		if (i > 0)
			appendStringInfoString(&ddl, ", ");
		appendStringInfoString(&ddl, quote_identifier(attname));
	}
	appendStringInfoString(&ddl, ")");
	if (partial_predicate != NULL)
		appendStringInfo(&ddl, " WHERE %s", partial_predicate);

	if (!ok)
	{
		pfree(ddl.data);
		pfree(cand->cols);
		pfree(cand->access_method);
		if (cand->partial_predicate)
			pfree(cand->partial_predicate);
		pfree(cand);
		return;
	}

	cand->ddl = ddl.data;
	ctx->candidates = lappend(ctx->candidates, cand);

	ereport(DEBUG1,
			(errmsg("dbblue advisor[trace]: emitted candidate: %s",
					cand->ddl)));
}

/*
 * dbblue_generate_composites
 *		For each relid in ctx->rel_infos, build a single composite
 *		candidate using the canonical column ordering:
 *
 *		    leading <- equality columns
 *		    middle  <- range columns
 *		    trailing<- sort columns
 *
 *		The result is capped at dbblue_auto_index_suggestion_max_index_columns
 *		(default 3).  Length-1 composites are skipped because the
 *		single-column candidates are already produced by extract_var_walker.
 *
 *		The composite goes through the same hypopg-evaluation pipeline
 *		as a single-column candidate; the planner picks the cheapest.
 *		Composites are intentionally NOT subjected to the leading-column
 *		"already covered" filter — a (a, b) index can still help even
 *		when (a) is independently indexed.
 */
static void
dbblue_generate_composites(CandidateWalkerCtx *ctx)
{
	ListCell   *lc_ri;
	int			max_cols = dbblue_auto_index_suggestion_max_index_columns;

	if (max_cols < 2)
		return;					/* user disabled composites */

	foreach(lc_ri, ctx->rel_infos)
	{
		DbblueRelInfo *ri = (DbblueRelInfo *) lfirst(lc_ri);
		List	   *ordered = NIL;
		ListCell   *lc;
		int			n_keys;
		AttrNumber *key_attnos = NULL;
		char	   *nspname;
		char	   *relname;
		char	   *qualified_relname;
		int			i;

		/*
		 * Build the ordered attno list: equality cols → range cols →
		 * sort cols.  Each role list is already dedup'd internally;
		 * across-roles we dedup with list_member_int so a column
		 * appearing in both eq and sort doesn't show up twice.
		 */
		foreach(lc, ri->eq_attnos)
			ordered = lappend_int(ordered, lfirst_int(lc));
		foreach(lc, ri->range_attnos)
		{
			int			a = lfirst_int(lc);

			if (!list_member_int(ordered, a))
				ordered = lappend_int(ordered, a);
		}
		foreach(lc, ri->sort_attnos)
		{
			int			a = lfirst_int(lc);

			if (!list_member_int(ordered, a))
				ordered = lappend_int(ordered, a);
		}

		/* Cap at the configured maximum. */
		if (list_length(ordered) > max_cols)
			ordered = list_truncate(ordered, max_cols);

		/* Resolve schema-qualified relation name (used by all variants). */
		nspname = get_namespace_name(get_rel_namespace(ri->relid));
		relname = get_rel_name(ri->relid);
		if (nspname == NULL || relname == NULL)
		{
			list_free(ordered);
			continue;
		}
		qualified_relname = quote_qualified_identifier(nspname, relname);

		n_keys = list_length(ordered);
		if (n_keys >= 1)
		{
			key_attnos = (AttrNumber *) palloc(n_keys * sizeof(AttrNumber));
			i = 0;
			foreach(lc, ordered)
				key_attnos[i++] = (AttrNumber) lfirst_int(lc);
		}

		/*
		 * (1) Composite btree candidate (length >= 2).  Single-column
		 * candidates are emitted by the walker, not here.
		 */
		if (n_keys >= 2)
		{
			dbblue_emit_one_candidate(ctx, ri->relid, qualified_relname,
									  n_keys, key_attnos,
									  "btree", NULL);

			/*
			 * (2) Partial-composite variants — same key list but with
			 * each detected partial-clause appended as a WHERE.
			 */
			foreach(lc, ri->partial_clauses)
			{
				DbbluePartialClause *pc =
					(DbbluePartialClause *) lfirst(lc);
				char	   *predicate;

				predicate = dbblue_partial_predicate_text(ri->relid,
														  pc->attno,
														  pc->kind);
				if (predicate == NULL)
					continue;
				dbblue_emit_one_candidate(ctx, ri->relid, qualified_relname,
										  n_keys, key_attnos,
										  "btree", predicate);
				pfree(predicate);
			}
		}

		/*
		 * (3) Partial single-column variants — for the *first* eq column
		 * (most common shape), pair it with each partial clause.  This
		 * adds candidates like CREATE INDEX ON tbl (id) WHERE active IS TRUE
		 * even when no composite is generated.
		 */
		if (list_length(ri->eq_attnos) >= 1)
		{
			AttrNumber	first_eq = (AttrNumber) lfirst_int(list_head(ri->eq_attnos));

			foreach(lc, ri->partial_clauses)
			{
				DbbluePartialClause *pc =
					(DbbluePartialClause *) lfirst(lc);
				char	   *predicate;

				predicate = dbblue_partial_predicate_text(ri->relid,
														  pc->attno,
														  pc->kind);
				if (predicate == NULL)
					continue;
				dbblue_emit_one_candidate(ctx, ri->relid, qualified_relname,
										  1, &first_eq,
										  "btree", predicate);
				pfree(predicate);
			}
		}

		/*
		 * (4) Hash candidates.  Postgres only supports single-column
		 * hash indexes, and hash is only competitive for columns used
		 * exclusively in equality predicates (no range, no sort).
		 */
		foreach(lc, ri->eq_attnos)
		{
			AttrNumber	a = (AttrNumber) lfirst_int(lc);

			if (list_member_int(ri->range_attnos, a))
				continue;
			if (list_member_int(ri->sort_attnos, a))
				continue;
			dbblue_emit_one_candidate(ctx, ri->relid, qualified_relname,
									  1, &a,
									  "hash", NULL);
		}

		if (key_attnos != NULL)
			pfree(key_attnos);
		list_free(ordered);
	}
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

		if (dbblue_candidate_already_listed(ctx->candidates, relid,
											1, &attno,
											"btree", NULL))
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

		/*
		 * Build a single-column candidate using the Phase 5 multi-column
		 * shape.  n_cols == 1 today; the same struct accommodates >1
		 * once Phase 5.3's composite generator lands.
		 */
		cand = (IndexCandidate *) palloc0(sizeof(*cand));
		cand->relid = relid;
		cand->qualified_relname =
			pstrdup(quote_qualified_identifier(nspname, relname));
		cand->n_cols = 1;
		cand->cols = (IndexColumn *) palloc(sizeof(IndexColumn));
		cand->cols[0].attno = attno;
		cand->cols[0].attname = pstrdup(get_attname(relid, attno, false));
		cand->access_method = pstrdup("btree");
		cand->partial_predicate = NULL;
		cand->ddl = psprintf("CREATE INDEX ON %s (%s)",
							 cand->qualified_relname,
							 quote_identifier(cand->cols[0].attname));
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
 * dbblue_try_create_extension
 *		Run "CREATE EXTENSION IF NOT EXISTS <extname>" via SPI inside an
 *		internal subtransaction, so a permission error or a missing
 *		extension package warns rather than aborting the surrounding
 *		ensure_extensions() transaction.
 *
 * Caller must already be inside SPI_connect.  `extname` is spliced
 * directly into SQL — only call with trusted constants.
 *
 * Returns true if the extension is installed in the connected
 * database after the call (either it was already there, or we just
 * created it).
 */
static bool
dbblue_try_create_extension(const char *extname)
{
	StringInfoData sql;
	MemoryContext oldcontext = CurrentMemoryContext;
	ResourceOwner oldowner = CurrentResourceOwner;
	bool		ok = false;

	initStringInfo(&sql);
	appendStringInfo(&sql, "CREATE EXTENSION IF NOT EXISTS %s", extname);

	BeginInternalSubTransaction(NULL);

	PG_TRY();
	{
		SPI_execute(sql.data, false, 0);

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcontext);
		CurrentResourceOwner = oldowner;
		ok = true;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcontext);
		CurrentResourceOwner = oldowner;

		ereport(WARNING,
				(errmsg("dbblue index advisor: could not CREATE EXTENSION %s in database \"%s\": %s",
						extname,
						dbblue_auto_index_suggestion_database,
						edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();

	pfree(sql.data);
	return ok;
}

/*
 * dbblue_advisor_ensure_extensions
 *		Install pg_stat_statements and hypopg in the connected database
 *		(idempotent via CREATE EXTENSION IF NOT EXISTS), and update
 *		dbblue_hypopg_available accordingly.
 *
 * Called only when the operator has the feature switched on (gated in
 * the main loop), and only the first tick after each off→on transition.
 *
 * pg_stat_statements requires its shared library to be loaded via
 * shared_preload_libraries; that is a postmaster-only GUC and a
 * backend cannot change it.  We log a clear WARNING + errhint when we
 * see it missing, then continue — CREATE EXTENSION pg_stat_statements
 * still creates the SQL view, it just won't have any data to report.
 */
static void
dbblue_advisor_ensure_extensions(void)
{
	int			ret;
	bool		preloaded = false;
	bool		psl_ok;
	bool		hypo_ok;

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING,
						   "dbblue index advisor: ensuring extensions");

	/* Step 1.  Verify pg_stat_statements is preloaded. */
	ret = SPI_execute("SELECT 1 FROM pg_settings "
					  "WHERE name = 'shared_preload_libraries' "
					  "  AND setting LIKE '%pg_stat_statements%'",
					  true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
		preloaded = true;

	if (!preloaded)
		ereport(WARNING,
				(errmsg("dbblue index advisor: pg_stat_statements is not in shared_preload_libraries"),
				 errdetail("The pg_stat_statements SQL view will be created but will report no query statistics until the library is preloaded."),
				 errhint("Add 'pg_stat_statements' to shared_preload_libraries in postgresql.conf and restart the server.")));

	/* Step 2.  Install (or confirm) both extensions. */
	psl_ok = dbblue_try_create_extension("pg_stat_statements");
	hypo_ok = dbblue_try_create_extension("hypopg");

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	if (psl_ok)
		ereport(LOG,
				(errmsg("dbblue index advisor: pg_stat_statements extension is installed in database \"%s\"",
						dbblue_auto_index_suggestion_database)));

	if (hypo_ok)
	{
		dbblue_hypopg_available = true;
		ereport(LOG,
				(errmsg("dbblue index advisor: hypopg extension is installed; cost evaluation enabled")));
	}
	else
	{
		dbblue_hypopg_available = false;
		ereport(WARNING,
				(errmsg("dbblue index advisor: hypopg extension is unavailable; candidates will be logged but no rows will be written"),
				 errhint("Install the hypopg package on the server, then re-enable dbblue_auto_index_suggestion_enabled.")));
	}
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
	ereport(LOG,errmsg("dbblue query for cost estimation: %s", explain_sql.data));

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
	StringInfoData cols_sql;
	char	   *q_relname;
	char	   *q_sample;
	char	   *q_ddl;
	char	   *q_method;
	char	   *q_partial;
	int			i;

	q_relname = quote_literal_cstr(cand->qualified_relname);
	q_sample = quote_literal_cstr(row->query_text);
	q_ddl = quote_literal_cstr(cand->ddl);
	q_method = quote_literal_cstr(cand->access_method
								  ? cand->access_method : "btree");
	q_partial = cand->partial_predicate
		? quote_literal_cstr(cand->partial_predicate)
		: pstrdup("NULL");

	/*
	 * Build the index_columns text[] literal: ARRAY['c1', 'c2', ...].
	 * Each name is passed through quote_literal_cstr to neutralise any
	 * weird identifiers (the column names are sourced from the catalog
	 * so they should be safe, but we stay defensive on principle).
	 */
	initStringInfo(&cols_sql);
	appendStringInfoString(&cols_sql, "ARRAY[");
	for (i = 0; i < cand->n_cols; i++)
	{
		char	   *q_name = quote_literal_cstr(cand->cols[i].attname);

		if (i > 0)
			appendStringInfoString(&cols_sql, ", ");
		appendStringInfoString(&cols_sql, q_name);
		pfree(q_name);
	}
	appendStringInfoString(&cols_sql, "]::text[]");

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO public.dbblue_index_suggestions "
					 "(relation_oid, relation_name, index_columns, "
					 " index_method, partial_predicate, queryid, "
					 " sample_query, baseline_cost, hypothetical_cost, "
					 " cost_improvement_pct, total_calls, total_exec_time_ms, ddl) "
					 "VALUES (%u, %s, %s, %s, %s, %lld, "
					 "        %s, %.6f, %.6f, %.4f, %lld, %.6f, %s) "
					 "ON CONFLICT (relation_oid, index_columns, "
					 "             index_method, partial_predicate) "
					 "DO UPDATE SET "
					 "  last_seen_at = now(), "
					 "  queryid = EXCLUDED.queryid, "
					 "  sample_query = EXCLUDED.sample_query, "
					 "  baseline_cost = EXCLUDED.baseline_cost, "
					 "  hypothetical_cost = EXCLUDED.hypothetical_cost, "
					 "  cost_improvement_pct = EXCLUDED.cost_improvement_pct, "
					 "  total_calls = EXCLUDED.total_calls, "
					 "  total_exec_time_ms = EXCLUDED.total_exec_time_ms",
					 cand->relid, q_relname, cols_sql.data,
					 q_method, q_partial,
					 (long long) row->queryid, q_sample,
					 baseline, hypothetical, improvement_pct,
					 (long long) row->calls, row->total_exec_time_ms, q_ddl);

	SPI_execute(sql.data, false, 0);

	pfree(sql.data);
	pfree(cols_sql.data);
	pfree(q_relname);
	pfree(q_sample);
	pfree(q_ddl);
	pfree(q_method);
	pfree(q_partial);
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
						(errmsg("dbblue index advisor: queryid=%lld skipped (cmdtype=%d not SELECT/UPDATE/DELETE, or only catalog tables)",
								(long long) row->queryid,
								(int) query->commandType)));
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
				 * Phase 5.2: build the per-relid predicate inventory
				 * (eq / range / sort / group attno lists).  Behavior
				 * unchanged in 5.2 — the inventory is observable via
				 * DEBUG1 below but isn't yet consumed by the candidate
				 * generator.  Phase 5.3 will use it to emit composite
				 * and partial DDL variants.
				 */
				if (query->jointree != NULL && query->jointree->quals != NULL)
					dbblue_classify_predicate(query->jointree->quals, &ctx);
				if (query->havingQual != NULL)
					dbblue_classify_predicate(query->havingQual, &ctx);
				dbblue_classify_sortgroup(query, &ctx);

				{
					ListCell   *lc_ri;

					foreach(lc_ri, ctx.rel_infos)
					{
						DbblueRelInfo *ri = (DbblueRelInfo *) lfirst(lc_ri);

						ereport(DEBUG1,
								(errmsg("dbblue advisor[trace]: queryid=%lld relid=%u inventory eq=%d range=%d sort=%d group=%d partial=%d",
										(long long) row->queryid, ri->relid,
										list_length(ri->eq_attnos),
										list_length(ri->range_attnos),
										list_length(ri->sort_attnos),
										list_length(ri->group_attnos),
										list_length(ri->partial_clauses))));
					}
				}

				/*
				 * Phase 5.3: feed the predicate inventory into the
				 * composite generator.  This appends one composite
				 * candidate per relid that has >=2 distinct columns
				 * across eq/range/sort.  Single-column candidates
				 * already on the list are kept too — the planner +
				 * threshold filter pick the winners.
				 */
				dbblue_generate_composites(&ctx);

				ereport(DEBUG1,
						(errmsg("dbblue advisor[trace]: post-composite queryid=%lld total candidates=%d",
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

						/*
						 * Only filter "already covered" for length-1
						 * plain-btree candidates with no partial
						 * predicate.  Composites, partial indexes and
						 * hash variants all express something the
						 * existing single-column btree on the leading
						 * column does NOT cover, so they must be
						 * evaluated by hypopg even when the leading
						 * column is independently indexed.
						 */
						if (c->n_cols == 1 &&
							c->partial_predicate == NULL &&
							c->access_method != NULL &&
							strcmp(c->access_method, "btree") == 0 &&
							dbblue_first_col_index_exists(c->relid,
														  c->cols[0].attno))
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
	bool		extensions_ensured = false;

	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(dbblue_auto_index_suggestion_database,
										 NULL, 0);

	ereport(LOG,
			(errmsg("dbblue index advisor started (database=\"%s\")",
					dbblue_auto_index_suggestion_database)));

	/*
	 * The results table is needed regardless of the on/off flag (so the
	 * DBA can still inspect past suggestions while the feature is paused),
	 * so create it unconditionally at startup.  pg_stat_statements and
	 * hypopg are only auto-installed once the operator switches the
	 * feature on — see the loop body.
	 */
	dbblue_advisor_ensure_results_table();

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
		{
			/*
			 * First tick after each off→on transition: install (or
			 * verify) pg_stat_statements and hypopg in the connected
			 * database.  Cleared on flip-to-off below so a subsequent
			 * re-enable picks up any extension drops the operator made
			 * in the interim.
			 */
			if (!extensions_ensured)
			{
				dbblue_advisor_ensure_extensions();
				extensions_ensured = true;
			}
			dbblue_advisor_run_tick();
		}
		else
		{
			extensions_ensured = false;
		}

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
