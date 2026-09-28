/*-------------------------------------------------------------------------
 *
 * autoprepare.c
 *	  Automatic server-side plan caching for repeated query shapes.
 *
 * See src/include/tcop/autoprepare.h for the rationale.
 *
 * Design summary
 * --------------
 *	- The "notebook" is a per-backend hash table keyed by the query's 64-bit
 *	  fingerprint (Query->queryId, already computed by the core jumbler when
 *	  compute_query_id is on).  Each entry counts sightings and, once promoted,
 *	  owns a saved CachedPlanSource plus the parameter types captured at
 *	  promotion time.
 *	- Storage is process-local (CacheMemoryContext), NOT shared buffers: a plan
 *	  is a tree of C structs, not a disk page.
 *	- Reuse rides entirely on the existing plancache machinery
 *	  (CreateCachedPlanForQuery / CompleteCachedPlan / SaveCachedPlan /
 *	  GetCachedPlan), so DDL/stat invalidation and the custom-vs-generic plan
 *	  decision come for free.
 *
 * Correctness stance
 * ------------------
 *	The 64-bit queryId only selects a candidate entry; it is not trusted for
 *	shape identity, since it is built for pg_stat_statements grouping and
 *	ignores aliases and all constants.  On every reuse we re-parameterize the
 *	incoming query and require it to be equal() to the cached parameterized
 *	query, and then verify that extracting this query's constants reproduces
 *	the exact parameter count and types recorded at promotion.  If either check
 *	fails (a queryId collision, or any build/extract divergence) we fall back
 *	to normal planning.  Wrong results are therefore not possible from a
 *	mismatch -- only a missed optimization.
 *
 * src/backend/tcop/autoprepare.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "tcop/autoprepare.h"

#include "fmgr.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "nodes/queryjumble.h"
#include "rewrite/rewriteHandler.h"
#include "storage/dsm.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/shm_mq.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "storage/subsystems.h"
#include "utils/array.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"
#include "utils/wait_event.h"

/* Decline shapes with more parameters than this (e.g. huge multi-row INSERTs). */
#define APREP_MAX_PARAMS 100

/* ---- GUCs ---- */
bool		autoprepare_enabled = false;	/* off by default -- opt in */
int			autoprepare_threshold = 2;		/* cache once seen this many times */
int			autoprepare_limit = 1024;		/* cap entries per backend */

/* ---- the notebook ---- */
typedef struct AutoprepareEntry
{
	uint64		fingerprint;	/* hash key: Query->queryId */
	uint32		seen_count;		/* how many times this shape has appeared */
	bool		promoted;		/* true once plansource is built */
	bool		declined;		/* true if this shape can't be cached -- never
								 * re-attempt the (expensive) build */

	/* NULL until promoted; lives in CacheMemoryContext via SaveCachedPlan(). */
	CachedPlanSource *plansource;

	/* Parameter types captured at promotion, in $1..$n order, for binding and
	 * for the reuse-time consistency check.  Allocated in AutoprepareContext. */
	Oid		   *param_types;
	int			num_params;
}			AutoprepareEntry;

static HTAB *autoprepare_table = NULL;
static MemoryContext AutoprepareContext = NULL;

/*
 * Per-backend counters for the lifetime of the backend (not cleared by
 * DISCARD PLANS), reported by dbblue_autoprepare_stats() and
 * dbblue_log_autoprepare_shapes().
 */
static uint64 aprep_hits = 0;			/* cached plan reused */
static uint64 aprep_fallbacks = 0;		/* promoted shape, reuse checks failed */
static uint64 aprep_promotions = 0;		/* plan built and cached */
static uint64 aprep_declines = 0;		/* shape found uncacheable at promotion */
static uint64 aprep_rejected_full = 0;	/* statements of untracked shapes
										 * turned away: limit reached */

/* guards against recursion via CHECK_FOR_INTERRUPTS() inside ereport() */
static bool LogAutoprepareShapesInProgress = false;

/* Longest query text written per shape by dbblue_log_autoprepare_shapes(). */
#define APREP_LOG_QUERY_MAXLEN 1024

/* ---- forward decls ---- */
static bool query_is_cacheable(Query *query);
static CachedPlanSource *build_parameterized_plansource(Query *query,
														const char *query_string,
														Oid **param_types_out,
														int *num_params_out);
static ParamListInfo extract_bound_params(Query *query,
										  Oid *param_types, int num_params);
static CommandTag aprep_cmdtag(CmdType c);


/* ----------------------------------------------------------------
 *		shape transform: shared predicates (build & extract agree)
 * ---------------------------------------------------------------- */

/* A scalar literal we are willing to fold into a parameter. */
static inline bool
const_is_parameterizable(Const *c)
{
	/* Folding a NULL gains nothing and can perturb NULL-aware plans. */
	if (c->constisnull)
		return false;
	return true;
}

/*
 * Is this "x op ANY (ARRAY[const, const, ...])" with >= 2 constant elements?
 * That is exactly what the core jumbler squashes into one fingerprint, so we
 * collapse it into a single array-typed parameter to match.
 */
static bool
saop_is_squashable(ScalarArrayOpExpr *s, ArrayExpr **arr_out)
{
	ArrayExpr  *a;
	ListCell   *lc;

	if (list_length(s->args) != 2)
		return false;
	if (!IsA(lsecond(s->args), ArrayExpr))
		return false;
	a = (ArrayExpr *) lsecond(s->args);
	if (a->multidims)
		return false;
	if (list_length(a->elements) < 2)	/* matches IsSquashableConstantList */
		return false;
	foreach(lc, a->elements)
	{
		if (!IsA(lfirst(lc), Const))
			return false;
	}
	*arr_out = a;
	return true;
}

static Param *
make_extern_param(int paramid, Oid type, int32 typmod, Oid collid)
{
	Param	   *p = makeNode(Param);

	p->paramkind = PARAM_EXTERN;
	p->paramid = paramid;
	p->paramtype = type;
	p->paramtypmod = typmod;
	p->paramcollid = collid;
	p->location = -1;
	return p;
}

/*
 * A few SQL constructs are represented internally as ordinary function calls
 * whose *deparse* (get_func_sql_syntax() in ruleutils.c) requires a particular
 * argument to remain a bare literal Const -- the field name of
 * EXTRACT('epoch' FROM x), and the form argument of NORMALIZE(x, NFC) /
 * x IS NFC NORMALIZED.  If we fold that literal into a $n Param, deparsing any
 * plan derived from the cached shape (auto_explain, EXPLAIN VERBOSE,
 * pg_get_viewdef, a stored view, ...) casts the Param to Const and trips an
 * Assert on assert builds -- or reads a bad pointer in TextDatumGetCString()
 * on production builds -- crashing the backend and restarting the cluster.
 *
 * So these particular arguments must be left literal.  Returns the 0-based
 * index of the argument that must stay Const, or -1 if this function imposes
 * no such requirement.
 *
 * MUST be kept in sync with the Const-asserting cases of get_func_sql_syntax()
 * in src/backend/utils/adt/ruleutils.c.
 */
static int
aprep_literal_only_argno(Oid funcid)
{
	switch (funcid)
	{
		case F_EXTRACT_TEXT_DATE:
		case F_EXTRACT_TEXT_TIME:
		case F_EXTRACT_TEXT_TIMETZ:
		case F_EXTRACT_TEXT_TIMESTAMP:
		case F_EXTRACT_TEXT_TIMESTAMPTZ:
		case F_EXTRACT_TEXT_INTERVAL:
			return 0;			/* EXTRACT(field FROM x) -- the field name */
		case F_IS_NORMALIZED:
		case F_NORMALIZE:
			return 1;			/* NORMALIZE(x, form) / x IS form NORMALIZED */
		default:
			return -1;
	}
}


/* ----------------------------------------------------------------
 *		shape transform: BUILD (Const -> Param, collect types)
 * ---------------------------------------------------------------- */

typedef struct AprepBuildCtx
{
	int			next_paramid;	/* 0-based running counter */
	Oid			types[APREP_MAX_PARAMS];
	bool		too_many;
}			AprepBuildCtx;

static Node *
aprep_build_mutator(Node *node, void *context)
{
	AprepBuildCtx *ctx = (AprepBuildCtx *) context;

	if (node == NULL || ctx->too_many)
		return node;

	/* Descend into sub-queries explicitly. */
	if (IsA(node, Query))
		return (Node *) query_tree_mutator((Query *) node,
										   aprep_build_mutator, ctx, 0);

	if (IsA(node, Const))
	{
		Const	   *c = (Const *) node;
		int			id;

		if (!const_is_parameterizable(c))
			return node;
		if (ctx->next_paramid >= APREP_MAX_PARAMS)
		{
			ctx->too_many = true;
			return node;
		}
		id = ++ctx->next_paramid;
		ctx->types[id - 1] = c->consttype;
		return (Node *) make_extern_param(id, c->consttype,
										  c->consttypmod, c->constcollid);
	}

	if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *s = (ScalarArrayOpExpr *) node;
		ArrayExpr  *arr;

		if (saop_is_squashable(s, &arr))
		{
			Node	   *newleft;
			Param	   *p;
			ScalarArrayOpExpr *ns;
			int			id;

			if (ctx->next_paramid >= APREP_MAX_PARAMS)
			{
				ctx->too_many = true;
				return node;
			}
			/* Walk the left arg FIRST so its param ids precede the array's. */
			newleft = aprep_build_mutator((Node *) linitial(s->args), ctx);
			if (ctx->too_many)
				return node;
			id = ++ctx->next_paramid;
			ctx->types[id - 1] = arr->array_typeid;
			p = make_extern_param(id, arr->array_typeid, -1, InvalidOid);
			ns = copyObject(s);
			ns->args = list_make2(newleft, p);
			return (Node *) ns;		/* do NOT recurse into array elements */
		}
	}

	if (IsA(node, FuncExpr))
	{
		FuncExpr   *f = (FuncExpr *) node;
		int			litarg = aprep_literal_only_argno(f->funcid);

		if (litarg >= 0)
		{
			FuncExpr   *nf = copyObject(f);
			ListCell   *lc;
			int			i = 0;

			/*
			 * Parameterize the value arguments but leave the literal-only
			 * argument (e.g. EXTRACT's field name) exactly as its original
			 * Const -- see aprep_literal_only_argno().
			 */
			foreach(lc, nf->args)
			{
				if (i != litarg)
				{
					lfirst(lc) = aprep_build_mutator((Node *) lfirst(lc), ctx);
					if (ctx->too_many)
						return node;
				}
				i++;
			}
			return (Node *) nf;
		}
	}

	return expression_tree_mutator(node, aprep_build_mutator, ctx);
}

/*
 * Parameterize an analyzed query.  Returns the parameterized copy and fills
 * types_out and nparams_out, or NULL to decline (no constants, or too many).
 * LIMIT/OFFSET literals are parameterized like any other constant.
 */
static Query *
aprep_parameterize_build(Query *analyzed, Oid **types_out, int *nparams_out)
{
	AprepBuildCtx ctx;
	Query	   *mutated;

	ctx.next_paramid = 0;
	ctx.too_many = false;

	/*
	 * Parameterize the ENTIRE query, including LIMIT/OFFSET.  We must
	 * parameterize these (not keep them literal) because the queryId we key on
	 * normalizes constants away -- so "LIMIT 10" and "LIMIT 20" share a
	 * queryId.  If the limit stayed literal, the cached plan would bake in one
	 * limit and hand it to the other query, returning the wrong number of
	 * rows.  Turning the limit into a $n bound fresh at execution keeps the
	 * result correct while the shapes still share one cached plan.
	 */
	mutated = query_tree_mutator(analyzed, aprep_build_mutator, &ctx, 0);

	if (ctx.too_many || ctx.next_paramid == 0)
		return NULL;

	/*
	 * Detach the source-text bounds of the promoting statement.  This
	 * parameterized query is cached once and then reused across many different
	 * literal statements that share its shape (e.g. Odoo's varying-length IN
	 * lists, or the same query written with a shorter literal).  Those source
	 * strings differ in length, but query_tree_mutator() copied the promoter's
	 * stmt_location/stmt_len onto this tree -- and those bounds flow into every
	 * PlannedStmt derived from the cached plan.  At reuse time
	 * pg_stat_statements / the query jumbler feed (current source string, the
	 * cached stmt_location/stmt_len) into CleanQuerytext(); if the cached
	 * stmt_len exceeds the length of a shorter current statement, CleanQuerytext
	 * reads past end-of-string -- an assertion failure ("query_len <=
	 * strlen(query)") on assert builds and an out-of-bounds read otherwise.
	 *
	 * Odoo (and exec_simple_query in general for our hook) runs one statement
	 * per query, so 0 location / 0 length ("the whole current source string is
	 * the statement") is always the correct, safe bound for the reused plan.
	 */
	mutated->stmt_location = 0;
	mutated->stmt_len = 0;

	*types_out = (Oid *) palloc(sizeof(Oid) * ctx.next_paramid);
	memcpy(*types_out, ctx.types, sizeof(Oid) * ctx.next_paramid);
	*nparams_out = ctx.next_paramid;
	return mutated;
}


/* ----------------------------------------------------------------
 *		shape transform: EXTRACT (collect this query's values)
 * ---------------------------------------------------------------- */

typedef struct AprepExtractCtx
{
	int			next_paramid;
	ParamListInfo params;
	Oid		   *expected;
	int			nexpected;
	bool		mismatch;
}			AprepExtractCtx;

static void
set_param(ParamListInfo p, int idx, Oid type, Datum value, bool isnull)
{
	ParamExternData *prm = &p->params[idx];

	prm->value = value;
	prm->isnull = isnull;
	prm->pflags = PARAM_FLAG_CONST;
	prm->ptype = type;
}

/* Build a 1-D array Datum from a squashable ArrayExpr's constant elements. */
static Datum
build_array_datum(ArrayExpr *arr)
{
	int			n = list_length(arr->elements);
	Datum	   *elems = (Datum *) palloc(sizeof(Datum) * n);
	bool	   *nulls = (bool *) palloc(sizeof(bool) * n);
	int			dims[1];
	int			lbs[1];
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	ListCell   *lc;
	int			i = 0;

	foreach(lc, arr->elements)
	{
		Const	   *c = lfirst_node(Const, lc);

		elems[i] = c->constvalue;
		nulls[i] = c->constisnull;
		i++;
	}
	get_typlenbyvalalign(arr->element_typeid, &elmlen, &elmbyval, &elmalign);
	dims[0] = n;
	lbs[0] = 1;
	return PointerGetDatum(construct_md_array(elems, nulls, 1, dims, lbs,
											  arr->element_typeid,
											  elmlen, elmbyval, elmalign));
}

static bool
aprep_extract_walker(Node *node, void *context)
{
	AprepExtractCtx *ctx = (AprepExtractCtx *) context;

	if (node == NULL || ctx->mismatch)
		return ctx->mismatch;	/* true aborts the walk */

	if (IsA(node, Query))
		return query_tree_walker((Query *) node,
								 aprep_extract_walker, ctx, 0);

	if (IsA(node, Const))
	{
		Const	   *c = (Const *) node;
		int			id;

		if (!const_is_parameterizable(c))
			return false;
		id = ++ctx->next_paramid;
		if (id > ctx->nexpected || ctx->expected[id - 1] != c->consttype)
		{
			ctx->mismatch = true;
			return true;
		}
		set_param(ctx->params, id - 1, c->consttype, c->constvalue,
				  c->constisnull);
		return false;
	}

	if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *s = (ScalarArrayOpExpr *) node;
		ArrayExpr  *arr;

		if (saop_is_squashable(s, &arr))
		{
			int			id;

			/* Match build order: left arg first, then the array param. */
			if (aprep_extract_walker((Node *) linitial(s->args), ctx))
				return true;
			id = ++ctx->next_paramid;
			if (id > ctx->nexpected || ctx->expected[id - 1] != arr->array_typeid)
			{
				ctx->mismatch = true;
				return true;
			}
			set_param(ctx->params, id - 1, arr->array_typeid,
					  build_array_datum(arr), false);
			return false;		/* do NOT walk into array elements */
		}
	}

	if (IsA(node, FuncExpr))
	{
		FuncExpr   *f = (FuncExpr *) node;
		int			litarg = aprep_literal_only_argno(f->funcid);

		if (litarg >= 0)
		{
			ListCell   *lc;
			int			i = 0;

			/* Mirror aprep_build_mutator: skip the literal-only argument. */
			foreach(lc, f->args)
			{
				if (i != litarg &&
					aprep_extract_walker((Node *) lfirst(lc), ctx))
					return true;
				i++;
			}
			return false;
		}
	}

	return expression_tree_walker(node, aprep_extract_walker, ctx);
}

/*
 * Recover this query's literal values into a ParamListInfo matching the
 * promoted plan's $1..$n.  Returns NULL (fail-safe) if the recovered parameter
 * count/types don't exactly match what was recorded at promotion.
 */
static ParamListInfo
extract_bound_params(Query *query, Oid *param_types, int num_params)
{
	AprepExtractCtx ctx;

	ctx.next_paramid = 0;
	ctx.params = makeParamList(num_params);
	ctx.expected = param_types;
	ctx.nexpected = num_params;
	ctx.mismatch = false;

	/* Walk the whole query (incl. LIMIT/OFFSET) so the limit value is bound,
	 * exactly mirroring aprep_parameterize_build. */
	(void) query_tree_walker(query, aprep_extract_walker, &ctx, 0);

	if (ctx.mismatch || ctx.next_paramid != num_params)
		return NULL;			/* fail safe -> caller plans normally */
	return ctx.params;
}


/* ----------------------------------------------------------------
 *		plansource construction
 * ---------------------------------------------------------------- */

/*
 * Does the query (or any subquery/CTE) use a GRAPH_TABLE clause?  Our
 * parameterization walk relies on expression_tree_mutator(), which does not
 * handle SQL/PGQ graph-pattern node types and would elog(ERROR) on them.  Such
 * queries must therefore be declined rather than parameterized.  We only scan
 * range tables (GRAPH_TABLE is always a table source), avoiding any walk over
 * the graph-pattern expression nodes themselves.
 */
static bool
query_has_graph_table(Query *query)
{
	ListCell   *lc;

	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		if (rte->rtekind == RTE_GRAPH_TABLE)
			return true;
		if (rte->rtekind == RTE_SUBQUERY && rte->subquery != NULL &&
			query_has_graph_table(rte->subquery))
			return true;
	}
	foreach(lc, query->cteList)
	{
		CommonTableExpr *cte = lfirst_node(CommonTableExpr, lc);

		if (cte->ctequery != NULL && IsA(cte->ctequery, Query) &&
			query_has_graph_table((Query *) cte->ctequery))
			return true;
	}
	return false;
}

static bool
query_is_cacheable(Query *query)
{
	if (query->utilityStmt != NULL)
		return false;
	switch (query->commandType)
	{
		case CMD_SELECT:
		case CMD_INSERT:
		case CMD_UPDATE:
		case CMD_DELETE:
		case CMD_MERGE:
			break;
		default:
			return false;
	}
	/* SQL/PGQ GRAPH_TABLE nodes aren't handled by our parameterization walk. */
	if (query_has_graph_table(query))
		return false;
	return true;
}

static CommandTag
aprep_cmdtag(CmdType c)
{
	switch (c)
	{
		case CMD_SELECT:
			return CMDTAG_SELECT;
		case CMD_INSERT:
			return CMDTAG_INSERT;
		case CMD_UPDATE:
			return CMDTAG_UPDATE;
		case CMD_DELETE:
			return CMDTAG_DELETE;
		case CMD_MERGE:
			return CMDTAG_MERGE;
		default:
			return CMDTAG_UNKNOWN;
	}
}

/*
 * Build a reusable, parameterized CachedPlanSource for this shape.
 * Returns NULL to decline (nothing to parameterize, too many params, or a
 * query whose rewrite does not yield exactly one query).
 *
 * We are called with the analyzed but not-yet-rewritten query (see the hook in
 * exec_simple_query).  CreateCachedPlanForQuery stores the parameterized tree
 * as the "analyzed" tree, and the plancache re-runs the rewrite itself on
 * invalidation.  We rewrite a copy here only to supply the initial query list
 * to CompleteCachedPlan.  We conservatively DECLINE any query whose rewrite
 * expands to other than one query (DO ALSO / INSTEAD rules and the like).
 */
static CachedPlanSource *
build_parameterized_plansource(Query *analyzed, const char *query_string,
							   Oid **param_types_out, int *num_params_out)
{
	Query	   *pquery;
	Oid		   *ptypes;
	int			nparams;
	List	   *rewritten;
	CachedPlanSource *plansource;

	pquery = aprep_parameterize_build(analyzed, &ptypes, &nparams);
	if (pquery == NULL)
		return NULL;

	rewritten = QueryRewrite(copyObject(pquery));
	if (list_length(rewritten) != 1)
		return NULL;			/* rule-rewritten / multi -> skip (see caveat) */

	plansource = CreateCachedPlanForQuery(pquery, query_string,
										  aprep_cmdtag(analyzed->commandType));
	CompleteCachedPlan(plansource,
					   rewritten,
					   NULL,	/* querytree_context: use current */
					   ptypes,
					   nparams,
					   NULL,	/* no parserSetup */
					   NULL,
					   CURSOR_OPT_PARALLEL_OK,
					   true);	/* fixed_result */

	*param_types_out = ptypes;
	*num_params_out = nparams;
	return plansource;
}


/* ----------------------------------------------------------------
 *		infrastructure
 * ---------------------------------------------------------------- */

static void
autoprepare_init(void)
{
	HASHCTL		ctl;

	if (autoprepare_table != NULL)
		return;

	AutoprepareContext = AllocSetContextCreate(CacheMemoryContext,
											   "Autoprepare cache",
											   ALLOCSET_DEFAULT_SIZES);
	ctl.keysize = sizeof(uint64);
	ctl.entrysize = sizeof(AutoprepareEntry);
	ctl.hcxt = AutoprepareContext;
	autoprepare_table = hash_create("Autoprepare shapes", 64, &ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

void
AutoprepareReset(void)
{
	HASH_SEQ_STATUS seq;
	AutoprepareEntry *entry;

	if (autoprepare_table == NULL)
		return;

	hash_seq_init(&seq, autoprepare_table);
	while ((entry = (AutoprepareEntry *) hash_seq_search(&seq)) != NULL)
	{
		if (entry->plansource)
			DropCachedPlan(entry->plansource);
		hash_search(autoprepare_table, &entry->fingerprint, HASH_REMOVE, NULL);
	}
}


/* ----------------------------------------------------------------
 *		main entry point
 * ---------------------------------------------------------------- */

AutoprepareResult
AutoprepareConsult(Query *analyzed_query, const char *query_string,
				   CachedPlanSource **plansource_out,
				   ParamListInfo *boundParams_out)
{
	AutoprepareEntry *entry;
	uint64		fp;
	bool		found;
	bool		dbg;

	*plansource_out = NULL;
	*boundParams_out = NULL;

	/* dbblue diagnostic: focus logging on ir_attachment queries only */
	dbg = (query_string != NULL && strstr(query_string, "ir_attachment") != NULL);

	if (!autoprepare_enabled)
		return APREP_UNCACHEABLE;
	if (!query_is_cacheable(analyzed_query))
	{
		if (dbg)
			elog(LOG, "[autoprep] UNCACHEABLE(not-cacheable: utility/cmdtype/graph) :: %.160s", query_string);
		return APREP_UNCACHEABLE;
	}

	/* The fingerprint is the queryId the core jumbler already computed. */
	fp = (uint64) analyzed_query->queryId;
	if (fp == UINT64CONST(0))
	{
		if (dbg)
			elog(LOG, "[autoprep] UNCACHEABLE(queryId=0; compute_query_id off?) :: %.160s", query_string);
		return APREP_UNCACHEABLE;	/* query-id computation disabled */
	}

	if (autoprepare_table == NULL)
		autoprepare_init();

	entry = (AutoprepareEntry *) hash_search(autoprepare_table, &fp,
											 HASH_FIND, &found);

	/* ---- first sighting ---- */
	if (!found)
	{
		if (hash_get_num_entries(autoprepare_table) >= autoprepare_limit)
		{
			aprep_rejected_full++;
			return APREP_MISS;	/* cap reached; TODO: LRU-evict instead */
		}

		entry = (AutoprepareEntry *) hash_search(autoprepare_table, &fp,
												 HASH_ENTER, &found);
		entry->seen_count = 1;
		entry->promoted = false;
		entry->declined = false;
		entry->plansource = NULL;
		entry->param_types = NULL;
		entry->num_params = 0;
		if (dbg)
			elog(LOG, "[autoprep] MISS(first-sighting) qid=%llu seen=1 :: %.160s",
				 (unsigned long long) fp, query_string);
		return APREP_MISS;
	}

	/* ---- already promoted: try to reuse ---- */
	if (entry->promoted && entry->plansource != NULL)
	{
		ParamListInfo boundParams;
		Query	   *ipquery;
		Oid		   *itypes = NULL;
		int			inparams = 0;

		entry->seen_count++;

		/*
		 * Comprehensive collision guard.  Our hash key is Query->queryId (the
		 * core jumble), which is built for pg_stat_statements *grouping*: it
		 * ignores column aliases and normalizes out ALL constants -- including
		 * the ones we deliberately keep literal (NULLs, and the literal-only
		 * arguments of aprep_literal_only_argno()).  So two genuinely
		 * different queries can share a queryId, and reusing the cached plan
		 * for the wrong one yields wrong column names or wrong results.
		 * Defend against that here: re-parameterize the incoming query and
		 * require it to be equal() to the cached parameterized query
		 * (plansource->analyzed_parse_tree).  That holds exactly when the two
		 * differ only in the values we bind as parameters.  equal() ignores
		 * token locations and queryId but compares aliases, parameter types
		 * and non-parameterized literals, so it catches alias-, type- and
		 * NULL-style collisions.  On any mismatch we fall back to normal
		 * planning rather than return a wrong answer.
		 */
		ipquery = aprep_parameterize_build(analyzed_query, &itypes, &inparams);
		(void) itypes;
		(void) inparams;
		if (ipquery == NULL ||
			!equal(ipquery, entry->plansource->analyzed_parse_tree))
		{
			aprep_fallbacks++;
			if (dbg)
				elog(LOG, "[autoprep] MISS(reuse-fail: %s) qid=%llu seen=%u -> REPLAN :: %.160s",
					 (ipquery == NULL) ? "reparameterize-returned-null(0-params/too-many)"
									   : "equal()-shape-mismatch(query text/aliases/kept-literals differ)",
					 (unsigned long long) fp, entry->seen_count, query_string);
			return APREP_MISS;	/* queryId collision -> plan normally */
		}

		boundParams = extract_bound_params(analyzed_query,
										   entry->param_types,
										   entry->num_params);
		if (boundParams == NULL)
		{
			aprep_fallbacks++;
			if (dbg)
				elog(LOG, "[autoprep] MISS(reuse-fail: extract-mismatch, param count/types diverged) qid=%llu -> REPLAN :: %.160s",
					 (unsigned long long) fp, query_string);
			return APREP_MISS;	/* divergence -> plan normally */
		}

		*plansource_out = entry->plansource;
		*boundParams_out = boundParams;
		aprep_hits++;
		if (dbg)
			elog(LOG, "[autoprep] HIT (reusing cached plan) qid=%llu nparams=%d seen=%u :: %.160s",
				 (unsigned long long) fp, entry->num_params, entry->seen_count, query_string);
		return APREP_HIT;
	}

	/* ---- known-uncacheable shape: never re-attempt the build ---- */
	if (entry->declined)
	{
		if (dbg)
			elog(LOG, "[autoprep] MISS(previously-declined; won't rebuild) qid=%llu -> REPLAN :: %.160s",
				 (unsigned long long) fp, query_string);
		return APREP_MISS;
	}

	/* ---- seen before, not yet promoted: bump and maybe promote ---- */
	entry->seen_count++;
	if (entry->seen_count >= autoprepare_threshold)
	{
		/*
		 * Build in a short-lived context so the scratch produced while
		 * parameterizing the query (a copyObject of the whole query tree plus
		 * the QueryRewrite output) is freed immediately.  Only two things must
		 * outlive this block: the finished CachedPlanSource -- which
		 * SaveCachedPlan reparents to CacheMemoryContext -- and a copy of the
		 * parameter types, which we stash in AutoprepareContext.  Without this,
		 * every promotion (and every promotion attempt) leaked its scratch
		 * into the long-lived cache context.
		 */
		MemoryContext build_cxt = AllocSetContextCreate(CurrentMemoryContext,
														"Autoprepare build",
														ALLOCSET_DEFAULT_SIZES);
		MemoryContext old = MemoryContextSwitchTo(build_cxt);
		Oid		   *ptypes = NULL;
		int			nparams = 0;
		CachedPlanSource *ps;

		ps = build_parameterized_plansource(analyzed_query, query_string,
											&ptypes, &nparams);
		if (ps != NULL)
		{
			SaveCachedPlan(ps); /* reparents the plan to CacheMemoryContext +
								 * registers it for invalidation callbacks */
			entry->plansource = ps;
			entry->num_params = nparams;
			/* copy param types into the long-lived cache context */
			entry->param_types = (Oid *) MemoryContextAlloc(AutoprepareContext,
															sizeof(Oid) * nparams);
			memcpy(entry->param_types, ptypes, sizeof(Oid) * nparams);
			entry->promoted = true;
			aprep_promotions++;
			if (dbg)
				elog(LOG, "[autoprep] PROMOTED (cached now; future runs can HIT) qid=%llu nparams=%d :: %.160s",
					 (unsigned long long) fp, nparams, query_string);
		}
		else
		{
			/*
			 * This shape can't be parameterized/cached (too many params,
			 * rule-rewritten, etc.).  Mark it so we never pay the build cost
			 * again -- otherwise every future execution would redo the
			 * copyObject + QueryRewrite.
			 */
			entry->declined = true;
			aprep_declines++;
			if (dbg)
				elog(LOG, "[autoprep] DECLINED(build returned NULL: 0-params, >%d params, or QueryRewrite expanded to !=1 query) qid=%llu -> always REPLAN :: %.160s",
					 APREP_MAX_PARAMS, (unsigned long long) fp, query_string);
		}
		MemoryContextSwitchTo(old);
		MemoryContextDelete(build_cxt);		/* frees all build scratch */
	}

	return APREP_MISS;			/* plan normally on the promoting call */
}


/* ----------------------------------------------------------------
 *		backend startup
 * ---------------------------------------------------------------- */

void
AutoprepareRegisterGUCs(void)
{
	/*
	 * The autoprepare parameters (dbblue_autoprepare_enabled,
	 * dbblue_autoprepare_threshold, dbblue_autoprepare_limit) are now core
	 * GUCs defined in src/backend/utils/misc/guc_parameters.dat, so there is
	 * nothing to register here at backend start.
	 *
	 * We still force query-id computation on:
	 *
	 * Our fingerprint is the query jumble (Query->queryId).  Force it on so
	 * the feature works even under the default compute_query_id = auto when no
	 * other consumer (e.g. pg_stat_statements) has requested it.
	 */
	EnableQueryId();
}


/* ----------------------------------------------------------------
 *		introspection
 *
 * The shape table is process-local, so reporting another backend's table
 * works by request and reply:
 *
 *	- The requester creates a DSM segment holding a shm_mq, makes itself the
 *	  receiver, posts the segment handle into the target's AprepReportSlot
 *	  (one per ProcNumber, in shared memory) and signals the target with
 *	  PROCSIG_AUTOPREPARE_REPORT.
 *	- At its next CHECK_FOR_INTERRUPTS() -- which idle backends reach too --
 *	  the target takes the handle, attaches as sender, and streams a summary
 *	  message, one message per shape (unless only the summary was asked for)
 *	  and an end marker.
 *
 * The requesting backend's own table goes through the same emit code
 * without the queue.  A backend waiting for a reply defers any request made
 * to it meanwhile, so two backends asking each other cannot deadlock (both
 * time out instead).  To keep concurrent all-backends requests from stalling
 * on each other that way, such a request skips backends that are themselves
 * waiting on a reply -- those are monitoring sessions, not application ones.
 * ---------------------------------------------------------------- */

/* Give up on a target that makes no progress for this long. */
#define APREP_REPORT_TIMEOUT_MS		5000
#define APREP_REPORT_QUEUE_SIZE		65536

typedef struct AprepReportSlot
{
	slock_t		mutex;			/* protects the fields below */
	dsm_handle	handle;			/* DSM_HANDLE_INVALID: no request pending */
	int			target_pid;		/* backend the request is addressed to */
	int			requester_pid;	/* backend waiting for the reply */
	bool		want_shapes;	/* false: summary only */
	int			requesting_pid; /* owner of this slot while it waits on
								 * another backend's reply, else 0 */
} AprepReportSlot;

static AprepReportSlot *AprepReportSlots = NULL;	/* MaxBackends entries */

static bool AprepRequestInProgress = false;	/* waiting on another backend */
static bool AprepReportInProgress = false;	/* sending our own report */

/* report message kinds */
#define APREP_MSG_SUMMARY	'S'
#define APREP_MSG_SHAPE		'E'
#define APREP_MSG_END		'Z'

typedef struct AprepMsgSummary
{
	char		kind;
	bool		enabled;
	int32		limit;
	int64		entries;
	int64		promoted;
	int64		tracking;
	int64		declined;
	uint64		hits;
	uint64		fallbacks;
	uint64		promotions;
	uint64		declines;
	uint64		rejected_full;
} AprepMsgSummary;

typedef struct AprepMsgShape
{
	char		kind;
	char		state;			/* 'p'romoted, 't'racking, 'd'eclined */
	int32		num_params;
	uint32		seen_count;
	int64		queryid;
	int32		query_len;		/* -1: no text; else text follows */
} AprepMsgShape;

/* Where a report goes: into a shm_mq, or straight to a consumer. */
typedef bool (*AprepEmitFn) (void *arg, const void *data, Size len);

/* Receiving side: turns report messages into result rows. */
typedef struct AprepConsumer
{
	ReturnSetInfo *rsinfo;
	bool		want_shapes;	/* shape rows, else one stats row */
	int			pid;			/* backend currently being reported */
	bool		got_end;
} AprepConsumer;

typedef enum AprepRequestResult
{
	APREP_REQ_OK,
	APREP_REQ_GONE,				/* target exited before it was signalled */
	APREP_REQ_NO_REPLY,			/* timed out, or reply was cut short */
} AprepRequestResult;


static void
AutoprepareShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "Autoprepare report slots",
					   .size = mul_size(MaxBackends, sizeof(AprepReportSlot)),
					   .ptr = (void **) &AprepReportSlots,
		);
}

static void
AutoprepareShmemInit(void *arg)
{
	for (int i = 0; i < MaxBackends; i++)
	{
		AprepReportSlot *slot = &AprepReportSlots[i];

		SpinLockInit(&slot->mutex);
		slot->handle = DSM_HANDLE_INVALID;
		slot->target_pid = 0;
		slot->requester_pid = 0;
		slot->want_shapes = false;
		slot->requesting_pid = 0;
	}
}

/*
 * Advertise in our own slot that we are waiting on another backend, so that
 * an all-backends request skips us instead of waiting out our deferral.
 */
static void
aprep_set_requesting(bool on)
{
	AprepReportSlot *slot;

	if (MyProcNumber < 0 || MyProcNumber >= MaxBackends)
		return;
	slot = &AprepReportSlots[MyProcNumber];
	SpinLockAcquire(&slot->mutex);
	slot->requesting_pid = on ? MyProcPid : 0;
	SpinLockRelease(&slot->mutex);
}

/* Is backend pid (at procno) currently waiting on another backend's reply? */
static bool
aprep_is_requesting(int pid, ProcNumber procno)
{
	AprepReportSlot *slot = &AprepReportSlots[procno];
	bool		requesting;

	SpinLockAcquire(&slot->mutex);
	requesting = (slot->requesting_pid == pid);
	SpinLockRelease(&slot->mutex);
	return requesting;
}

const ShmemCallbacks AutoprepareShmemCallbacks = {
	.request_fn = AutoprepareShmemRequest,
	.init_fn = AutoprepareShmemInit,
};

static char
aprep_entry_state_code(AutoprepareEntry *entry)
{
	if (entry->promoted)
		return 'p';
	if (entry->declined)
		return 'd';
	return 't';
}

static const char *
aprep_state_name(char code)
{
	switch (code)
	{
		case 'p':
			return "promoted";
		case 'd':
			return "declined";
		default:
			return "tracking";
	}
}

static void
aprep_fill_summary(AprepMsgSummary *s)
{
	HASH_SEQ_STATUS seq;
	AutoprepareEntry *entry;

	memset(s, 0, sizeof(*s));
	s->kind = APREP_MSG_SUMMARY;
	s->enabled = autoprepare_enabled;
	s->limit = autoprepare_limit;
	s->hits = aprep_hits;
	s->fallbacks = aprep_fallbacks;
	s->promotions = aprep_promotions;
	s->declines = aprep_declines;
	s->rejected_full = aprep_rejected_full;

	if (autoprepare_table == NULL)
		return;

	s->entries = hash_get_num_entries(autoprepare_table);
	hash_seq_init(&seq, autoprepare_table);
	while ((entry = (AutoprepareEntry *) hash_seq_search(&seq)) != NULL)
	{
		switch (aprep_entry_state_code(entry))
		{
			case 'p':
				s->promoted++;
				break;
			case 'd':
				s->declined++;
				break;
			default:
				s->tracking++;
				break;
		}
	}
}

/*
 * Produce this backend's report through emit(): the summary, then (if
 * want_shapes) one message per shape, then the end marker.  Stops early if
 * emit() returns false, i.e. the receiver went away.
 */
static void
aprep_emit_report(bool want_shapes, AprepEmitFn emit, void *arg)
{
	AprepMsgSummary summary;
	char		end = APREP_MSG_END;

	aprep_fill_summary(&summary);
	if (!emit(arg, &summary, sizeof(summary)))
		return;

	if (want_shapes && autoprepare_table != NULL)
	{
		HASH_SEQ_STATUS seq;
		AutoprepareEntry *entry;
		StringInfoData buf;

		initStringInfo(&buf);
		hash_seq_init(&seq, autoprepare_table);
		while ((entry = (AutoprepareEntry *) hash_seq_search(&seq)) != NULL)
		{
			AprepMsgShape hdr;
			const char *qs = NULL;

			if (entry->plansource != NULL)
				qs = entry->plansource->query_string;

			memset(&hdr, 0, sizeof(hdr));
			hdr.kind = APREP_MSG_SHAPE;
			hdr.state = aprep_entry_state_code(entry);
			hdr.num_params = entry->num_params;
			hdr.seen_count = entry->seen_count;
			hdr.queryid = (int64) entry->fingerprint;
			hdr.query_len = (qs != NULL) ? (int32) strlen(qs) : -1;

			resetStringInfo(&buf);
			appendBinaryStringInfo(&buf, &hdr, sizeof(hdr));
			if (qs != NULL)
				appendBinaryStringInfo(&buf, qs, hdr.query_len);

			if (!emit(arg, buf.data, buf.len))
			{
				hash_seq_term(&seq);
				pfree(buf.data);
				return;
			}
		}
		pfree(buf.data);
	}

	(void) emit(arg, &end, sizeof(end));
}

/* AprepEmitFn for the target side: send over the requester's shm_mq. */
static bool
aprep_emit_mq(void *arg, const void *data, Size len)
{
	shm_mq_handle *mqh = (shm_mq_handle *) arg;
	bool		is_end = (*(const char *) data == APREP_MSG_END);

	return shm_mq_send(mqh, len, data, false, is_end) == SHM_MQ_SUCCESS;
}

/*
 * AprepEmitFn for the receiving side: add a result row.  Returns false on a
 * malformed message.
 */
static bool
aprep_consume(void *arg, const void *data, Size len)
{
	AprepConsumer *c = (AprepConsumer *) arg;

	if (len < 1)
		return false;

	switch (*(const char *) data)
	{
		case APREP_MSG_SUMMARY:
			{
				AprepMsgSummary s;
				Datum		values[12];
				bool		nulls[12] = {0};

				if (len != sizeof(s))
					return false;
				if (c->want_shapes)
					return true;
				memcpy(&s, data, sizeof(s));

				values[0] = Int32GetDatum(c->pid);
				values[1] = BoolGetDatum(s.enabled);
				values[2] = Int64GetDatum(s.entries);
				values[3] = Int32GetDatum(s.limit);
				values[4] = Int64GetDatum(s.promoted);
				values[5] = Int64GetDatum(s.tracking);
				values[6] = Int64GetDatum(s.declined);
				values[7] = Int64GetDatum((int64) s.hits);
				values[8] = Int64GetDatum((int64) s.fallbacks);
				values[9] = Int64GetDatum((int64) s.promotions);
				values[10] = Int64GetDatum((int64) s.declines);
				values[11] = Int64GetDatum((int64) s.rejected_full);
				tuplestore_putvalues(c->rsinfo->setResult, c->rsinfo->setDesc,
									 values, nulls);
				return true;
			}

		case APREP_MSG_SHAPE:
			{
				AprepMsgShape h;
				Datum		values[6];
				bool		nulls[6] = {0};

				if (len < sizeof(h))
					return false;
				memcpy(&h, data, sizeof(h));
				if (h.query_len >= 0 ? len != sizeof(h) + h.query_len
					: len != sizeof(h))
					return false;
				if (!c->want_shapes)
					return true;

				values[0] = Int32GetDatum(c->pid);
				values[1] = Int64GetDatum(h.queryid);
				values[2] = CStringGetTextDatum(aprep_state_name(h.state));
				values[3] = Int64GetDatum((int64) h.seen_count);
				values[4] = Int32GetDatum(h.num_params);
				if (h.query_len >= 0)
					values[5] = PointerGetDatum(cstring_to_text_with_len((const char *) data + sizeof(h),
																		 h.query_len));
				else
					nulls[5] = true;
				tuplestore_putvalues(c->rsinfo->setResult, c->rsinfo->setDesc,
									 values, nulls);
				return true;
			}

		case APREP_MSG_END:
			c->got_end = true;
			return true;

		default:
			return false;
	}
}

/*
 * Ask backend pid (with ProcNumber procno) for its report and feed the reply
 * to the consumer.
 */
static AprepRequestResult
aprep_request_report(int pid, ProcNumber procno, AprepConsumer *consumer)
{
	AprepReportSlot *slot = &AprepReportSlots[procno];
	dsm_segment *seg;
	shm_mq	   *mq;
	shm_mq_handle *mqh;
	dsm_handle	handle;
	AprepRequestResult result = APREP_REQ_NO_REPLY;

	seg = dsm_create(APREP_REPORT_QUEUE_SIZE, 0);
	handle = dsm_segment_handle(seg);
	mq = shm_mq_create(dsm_segment_address(seg), APREP_REPORT_QUEUE_SIZE);
	shm_mq_set_receiver(mq, MyProc);
	mqh = shm_mq_attach(mq, seg, NULL);

	AprepRequestInProgress = true;
	aprep_set_requesting(true);
	PG_TRY();
	{
		TimestampTz last_progress = GetCurrentTimestamp();
		bool		posted = false;

		/* Post the request, waiting while another requester holds the slot. */
		for (;;)
		{
			dsm_handle	busy_handle;
			int			busy_requester;

			SpinLockAcquire(&slot->mutex);
			busy_handle = slot->handle;
			busy_requester = slot->requester_pid;
			if (busy_handle == DSM_HANDLE_INVALID)
			{
				slot->handle = handle;
				slot->target_pid = pid;
				slot->requester_pid = MyProcPid;
				slot->want_shapes = consumer->want_shapes;
				posted = true;
			}
			SpinLockRelease(&slot->mutex);

			if (posted)
				break;

			/* A requester that died mid-request leaves its request behind. */
			if (BackendPidGetProc(busy_requester) == NULL)
			{
				SpinLockAcquire(&slot->mutex);
				if (slot->handle == busy_handle)
					slot->handle = DSM_HANDLE_INVALID;
				SpinLockRelease(&slot->mutex);
				continue;
			}

			if (TimestampDifferenceExceeds(last_progress, GetCurrentTimestamp(),
										   APREP_REPORT_TIMEOUT_MS))
				break;
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 10, WAIT_EVENT_MESSAGE_QUEUE_RECEIVE);
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		if (posted &&
			SendProcSignal(pid, PROCSIG_AUTOPREPARE_REPORT, procno) < 0)
		{
			result = APREP_REQ_GONE;
			posted = false;
		}

		/* Collect the reply. */
		last_progress = GetCurrentTimestamp();
		while (posted)
		{
			Size		nbytes;
			void	   *data;
			shm_mq_result res;

			res = shm_mq_receive(mqh, &nbytes, &data, true);
			if (res == SHM_MQ_SUCCESS)
			{
				if (!aprep_consume(consumer, data, nbytes))
					break;		/* malformed */
				if (consumer->got_end)
				{
					result = APREP_REQ_OK;
					break;
				}
				last_progress = GetCurrentTimestamp();
				continue;
			}
			if (res == SHM_MQ_DETACHED)
				break;			/* sender went away before the end marker */

			/* SHM_MQ_WOULD_BLOCK */
			if (TimestampDifferenceExceeds(last_progress, GetCurrentTimestamp(),
										   APREP_REPORT_TIMEOUT_MS))
				break;
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 100, WAIT_EVENT_MESSAGE_QUEUE_RECEIVE);
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}
	}
	PG_FINALLY();
	{
		AprepRequestInProgress = false;
		aprep_set_requesting(false);

		/* Withdraw the request if the target never picked it up. */
		SpinLockAcquire(&slot->mutex);
		if (slot->handle == handle)
			slot->handle = DSM_HANDLE_INVALID;
		SpinLockRelease(&slot->mutex);

		/* Serve a request made to us while we were waiting. */
		if (AutoprepareReportPending)
			InterruptPending = true;
	}
	PG_END_TRY();

	shm_mq_detach(mqh);
	dsm_detach(seg);
	return result;
}

/*
 * Add backend pid's rows to the consumer's result.  explicit_pid: the caller
 * named this pid, so report a pid that is not a backend.
 */
static void
aprep_report_backend(int pid, AprepConsumer *consumer, bool explicit_pid)
{
	PGPROC	   *proc;
	ProcNumber	procno = INVALID_PROC_NUMBER;
	AprepRequestResult res;

	consumer->pid = pid;
	consumer->got_end = false;

	if (pid == MyProcPid)
	{
		aprep_emit_report(consumer->want_shapes, aprep_consume, consumer);
		return;
	}

	proc = BackendPidGetProc(pid);
	if (proc != NULL)
		procno = GetNumberFromPGProc(proc);
	if (proc == NULL || procno < 0 || procno >= MaxBackends)
		res = APREP_REQ_GONE;
	else if (!explicit_pid && aprep_is_requesting(pid, procno))
	{
		/*
		 * Another monitoring session, itself waiting on a report: it would
		 * only answer after its own wait, so skip it rather than stall.
		 */
		return;
	}
	else
		res = aprep_request_report(pid, procno, consumer);

	if (res == APREP_REQ_GONE && explicit_pid)
		ereport(WARNING,
				(errmsg("PID %d is not a PostgreSQL backend process", pid)));
	else if (res == APREP_REQ_NO_REPLY)
		ereport(WARNING,
				(errmsg("backend with PID %d did not answer the autoprepare report request",
						pid),
				 errdetail("Its rows are missing or incomplete.  A backend answers at its next interrupt check; one that is itself waiting on another backend's report answers after that.")));
}

/* Shared body of dbblue_autoprepare_shapes() and dbblue_autoprepare_stats(). */
static void
aprep_report_srf(FunctionCallInfo fcinfo, bool want_shapes)
{
	AprepConsumer consumer;
	int			nbackends;

	InitMaterializedSRF(fcinfo, 0);
	consumer.rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	consumer.want_shapes = want_shapes;

	if (!PG_ARGISNULL(0))
	{
		aprep_report_backend(PG_GETARG_INT32(0), &consumer, true);
		return;
	}

	/* No pid given: every client backend, ourselves included. */
	nbackends = pgstat_fetch_stat_numbackends();
	for (int i = 1; i <= nbackends; i++)
	{
		LocalPgBackendStatus *local = pgstat_get_local_beentry_by_index(i);

		if (local == NULL ||
			local->backendStatus.st_backendType != B_BACKEND ||
			local->backendStatus.st_procpid <= 0)
			continue;
		aprep_report_backend(local->backendStatus.st_procpid, &consumer, false);
	}
}

/*
 * dbblue_autoprepare_shapes([target_pid])
 *		One row per shape in the autoprepare table of target_pid, or of every
 *		client backend when target_pid is NULL.
 *
 * queryid matches pg_stat_statements.queryid.  query is the text of the
 * statement that promoted the shape, or NULL if it has no cached plan.
 */
Datum
dbblue_autoprepare_shapes(PG_FUNCTION_ARGS)
{
	aprep_report_srf(fcinfo, true);
	return (Datum) 0;
}

/*
 * dbblue_autoprepare_stats([target_pid])
 *		One summary row for target_pid, or for every client backend when
 *		target_pid is NULL: table size against the limit, per-state counts
 *		and lifetime counters.
 */
Datum
dbblue_autoprepare_stats(PG_FUNCTION_ARGS)
{
	aprep_report_srf(fcinfo, false);
	return (Datum) 0;
}

/*
 * HandleAutoprepareReportInterrupt
 *		Signal-handler side of a report request: just set the flag.
 */
void
HandleAutoprepareReportInterrupt(void)
{
	InterruptPending = true;
	AutoprepareReportPending = true;
	/* latch will be set by procsignal_sigusr1_handler */
}

/*
 * ProcessAutoprepareReportInterrupt
 *		Answer a pending report request, called from ProcessInterrupts().
 */
void
ProcessAutoprepareReportInterrupt(void)
{
	AprepReportSlot *slot;
	dsm_handle	handle = DSM_HANDLE_INVALID;
	bool		want_shapes = false;
	MemoryContext report_cxt;
	MemoryContext oldcxt;

	/*
	 * While we are ourselves waiting on another backend, or already sending a
	 * report, leave the flag set; the request is served once that finishes.
	 */
	if (AprepRequestInProgress || AprepReportInProgress)
		return;
	AutoprepareReportPending = false;

	if (AprepReportSlots == NULL || MyProcNumber < 0 ||
		MyProcNumber >= MaxBackends)
		return;

	slot = &AprepReportSlots[MyProcNumber];
	SpinLockAcquire(&slot->mutex);
	if (slot->handle != DSM_HANDLE_INVALID && slot->target_pid == MyProcPid)
	{
		handle = slot->handle;
		want_shapes = slot->want_shapes;
		slot->handle = DSM_HANDLE_INVALID;
	}
	SpinLockRelease(&slot->mutex);

	if (handle == DSM_HANDLE_INVALID)
		return;

	AprepReportInProgress = true;
	report_cxt = AllocSetContextCreate(TopMemoryContext,
									   "Autoprepare report",
									   ALLOCSET_SMALL_SIZES);
	oldcxt = MemoryContextSwitchTo(report_cxt);
	PG_TRY();
	{
		/* NULL if the requester already gave up and destroyed the segment */
		dsm_segment *seg = dsm_attach(handle);

		if (seg != NULL)
		{
			shm_mq	   *mq = (shm_mq *) dsm_segment_address(seg);
			shm_mq_handle *mqh;

			shm_mq_set_sender(mq, MyProc);
			mqh = shm_mq_attach(mq, seg, NULL);
			aprep_emit_report(want_shapes, aprep_emit_mq, mqh);
			shm_mq_detach(mqh);
			dsm_detach(seg);
		}
	}
	PG_FINALLY();
	{
		MemoryContextSwitchTo(oldcxt);
		MemoryContextDelete(report_cxt);
		AprepReportInProgress = false;
		if (AutoprepareReportPending)
			InterruptPending = true;
	}
	PG_END_TRY();
}

/*
 * dbblue_log_autoprepare_shapes
 *		Signal a backend to write its autoprepare table to the server log.
 *
 * Modeled on pg_log_backend_memory_contexts(): superuser-only by default
 * (the dump can be long), and the target does the work at its next
 * CHECK_FOR_INTERRUPTS().  Only regular backends run queries, so auxiliary
 * processes are not accepted.
 */
Datum
dbblue_log_autoprepare_shapes(PG_FUNCTION_ARGS)
{
	int			pid = PG_GETARG_INT32(0);
	PGPROC	   *proc;

	proc = BackendPidGetProc(pid);
	if (proc == NULL)
	{
		/* just a warning, so a loop over pg_stat_activity won't abort */
		ereport(WARNING,
				(errmsg("PID %d is not a PostgreSQL backend process", pid)));
		PG_RETURN_BOOL(false);
	}

	if (SendProcSignal(pid, PROCSIG_LOG_AUTOPREPARE_SHAPES,
					   GetNumberFromPGProc(proc)) < 0)
	{
		ereport(WARNING,
				(errmsg("could not send signal to process %d: %m", pid)));
		PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(true);
}

/*
 * HandleLogAutoprepareShapesInterrupt
 *		Signal-handler side: just set the flag; logging happens later.
 */
void
HandleLogAutoprepareShapesInterrupt(void)
{
	InterruptPending = true;
	LogAutoprepareShapesPending = true;
	/* latch will be set by procsignal_sigusr1_handler */
}

/*
 * ProcessLogAutoprepareShapesInterrupt
 *		Write this backend's autoprepare table to the server log.
 *
 * One summary line (entries vs. limit, per-state counts, lifetime counters),
 * then one line per shape.  Query text is clipped to APREP_LOG_QUERY_MAXLEN
 * bytes; dbblue_autoprepare_shapes() returns the full text.
 */
void
ProcessLogAutoprepareShapesInterrupt(void)
{
	LogAutoprepareShapesPending = false;

	if (LogAutoprepareShapesInProgress)
		return;
	LogAutoprepareShapesInProgress = true;

	PG_TRY();
	{
		AprepMsgSummary s;

		aprep_fill_summary(&s);
		ereport(LOG_SERVER_ONLY,
				(errhidestmt(true),
				 errhidecontext(true),
				 errmsg("autoprepare shapes of PID %d: %lld entries (limit %d, enabled %s): %lld promoted, %lld tracking, %lld declined",
						MyProcPid, (long long) s.entries, s.limit,
						s.enabled ? "on" : "off",
						(long long) s.promoted, (long long) s.tracking,
						(long long) s.declined),
				 errdetail("Since backend start: %llu hits, %llu reuse fallbacks, %llu promotions, %llu declines, %llu statements not tracked because the limit was reached.",
						   (unsigned long long) s.hits,
						   (unsigned long long) s.fallbacks,
						   (unsigned long long) s.promotions,
						   (unsigned long long) s.declines,
						   (unsigned long long) s.rejected_full)));

		if (autoprepare_table != NULL)
		{
			HASH_SEQ_STATUS seq;
			AutoprepareEntry *entry;

			hash_seq_init(&seq, autoprepare_table);
			while ((entry = (AutoprepareEntry *) hash_seq_search(&seq)) != NULL)
			{
				const char *state = aprep_state_name(aprep_entry_state_code(entry));

				if (entry->plansource != NULL)
				{
					const char *qs = entry->plansource->query_string;
					int			qlen = strlen(qs);
					int			cliplen = pg_mbcliplen(qs, qlen,
													   APREP_LOG_QUERY_MAXLEN);

					ereport(LOG_SERVER_ONLY,
							(errhidestmt(true),
							 errhidecontext(true),
							 errmsg_internal("autoprepare shape: queryid=%lld state=%s seen=%u params=%d query: %.*s%s",
											 (long long) entry->fingerprint,
											 state,
											 entry->seen_count,
											 entry->num_params,
											 cliplen, qs,
											 cliplen < qlen ? "..." : "")));
				}
				else
					ereport(LOG_SERVER_ONLY,
							(errhidestmt(true),
							 errhidecontext(true),
							 errmsg_internal("autoprepare shape: queryid=%lld state=%s seen=%u",
											 (long long) entry->fingerprint,
											 state,
											 entry->seen_count)));
			}
		}
	}
	PG_FINALLY();
	{
		LogAutoprepareShapesInProgress = false;
	}
	PG_END_TRY();
}
