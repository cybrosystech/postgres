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
 *	We trust the 64-bit queryId for shape identity (the same thing
 *	pg_stat_statements does), but we additionally *verify* on every reuse that
 *	extracting this query's constants reproduces the exact parameter count and
 *	types recorded at promotion.  If it does not (a hash collision, or any
 *	build/extract divergence), extract_bound_params() returns NULL and we fall
 *	back to normal planning.  Wrong results are therefore not possible from a
 *	mismatch -- only a missed optimization.
 *
 * src/backend/tcop/autoprepare.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "tcop/autoprepare.h"

#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "nodes/queryjumble.h"
#include "rewrite/rewriteHandler.h"
#include "utils/array.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

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
 * LIMIT/OFFSET are temporarily detached so their literals are never folded.
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
 * rule-rewritten query -- see caveat).
 *
 * CAVEAT (analyze-vs-rewrite): we are called with the post-rewrite analyzed
 * query (exec_simple_query gives rewritten trees).  CreateCachedPlanForQuery
 * stores it as the "analyzed" tree and re-rewrites on invalidation.  For the
 * overwhelmingly common Odoo case -- direct table queries with no ON
 * SELECT/INSERT/... rules -- re-rewrite is a no-op, so this is correct.  We
 * conservatively DECLINE any query whose rewrite expands to other than one
 * query, which excludes the rule cases.  Moving the hook to the pre-rewrite
 * point would remove the caveat entirely.
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

	*plansource_out = NULL;
	*boundParams_out = NULL;

	if (!autoprepare_enabled)
		return APREP_UNCACHEABLE;
	if (!query_is_cacheable(analyzed_query))
		return APREP_UNCACHEABLE;

	/* The fingerprint is the queryId the core jumbler already computed. */
	fp = (uint64) analyzed_query->queryId;
	if (fp == UINT64CONST(0))
		return APREP_UNCACHEABLE;	/* query-id computation disabled */

	if (autoprepare_table == NULL)
		autoprepare_init();

	entry = (AutoprepareEntry *) hash_search(autoprepare_table, &fp,
											 HASH_FIND, &found);

	/* ---- first sighting ---- */
	if (!found)
	{
		if (hash_get_num_entries(autoprepare_table) >= autoprepare_limit)
			return APREP_MISS;	/* cap reached; TODO: LRU-evict instead */

		entry = (AutoprepareEntry *) hash_search(autoprepare_table, &fp,
												 HASH_ENTER, &found);
		entry->seen_count = 1;
		entry->promoted = false;
		entry->declined = false;
		entry->plansource = NULL;
		entry->param_types = NULL;
		entry->num_params = 0;
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
		 * the ones we deliberately keep literal (LIMIT/OFFSET, NULLs).  So two
		 * genuinely different queries can share a queryId, and reusing the
		 * cached plan for the wrong one yields wrong column names or wrong
		 * results.  Defend against that here: re-parameterize the incoming
		 * query and require it to be equal() to the cached parameterized query
		 * (plansource->analyzed_parse_tree).  That holds exactly when the two
		 * differ only in the values we bind as parameters.  equal() ignores
		 * token locations but compares aliases and non-parameterized literals,
		 * so it catches alias- and LIMIT-style collisions.  On any mismatch we
		 * fall back to normal planning rather than return a wrong answer.
		 */
		ipquery = aprep_parameterize_build(analyzed_query, &itypes, &inparams);
		(void) itypes;
		(void) inparams;
		if (ipquery == NULL ||
			!equal(ipquery, entry->plansource->analyzed_parse_tree))
			return APREP_MISS;	/* queryId collision -> plan normally */

		boundParams = extract_bound_params(analyzed_query,
										   entry->param_types,
										   entry->num_params);
		if (boundParams == NULL)
			return APREP_MISS;	/* divergence -> plan normally */

		*plansource_out = entry->plansource;
		*boundParams_out = boundParams;
		return APREP_HIT;
	}

	/* ---- known-uncacheable shape: never re-attempt the build ---- */
	if (entry->declined)
		return APREP_MISS;

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
		}
		MemoryContextSwitchTo(old);
		MemoryContextDelete(build_cxt);		/* frees all build scratch */
	}

	return APREP_MISS;			/* plan normally on the promoting call */
}


/* ----------------------------------------------------------------
 *		GUC registration
 * ---------------------------------------------------------------- */

void
AutoprepareRegisterGUCs(void)
{
	DefineCustomBoolVariable("db_blue.autoprepare_enabled",
							 "Cache and reuse plans for repeated query shapes.",
							 NULL,
							 &autoprepare_enabled,
							 false,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("db_blue.autoprepare_threshold",
							"Cache a query shape after it is seen this many times.",
							NULL,
							&autoprepare_threshold,
							2, 1, INT_MAX,
							PGC_SUSET, 0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("db_blue.autoprepare_limit",
							"Maximum number of cached query shapes per backend.",
							NULL,
							&autoprepare_limit,
							1024, 1, INT_MAX,
							PGC_SUSET, 0,
							NULL, NULL, NULL);

	/*
	 * Reserve the whole "db_blue" GUC prefix. This is the correct single home
	 * for the reservation: AutoprepareRegisterGUCs() runs in PostgresMain()
	 * after process_shared_preload_libraries(), so any db_blue.* GUCs owned by
	 * preloaded modules (e.g. the pg_prewarm soft-pin pinner:
	 * db_blue.pinned_tables, db_blue.ring_buffer_tables, ...) are already
	 * defined by this point and are left untouched — only unrecognized
	 * db_blue.* placeholders (typos) are flagged.
	 */
	MarkGUCPrefixReserved("db_blue");

	/*
	 * Our fingerprint is the query jumble (Query->queryId).  Force it on so
	 * the feature works even under the default compute_query_id = auto when no
	 * other consumer (e.g. pg_stat_statements) has requested it.
	 */
	EnableQueryId();
}
