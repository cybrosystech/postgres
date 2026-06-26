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
	Node	   *save_lo = analyzed->limitOffset;
	Node	   *save_lc = analyzed->limitCount;

	ctx.next_paramid = 0;
	ctx.too_many = false;

	analyzed->limitOffset = NULL;
	analyzed->limitCount = NULL;
	mutated = query_tree_mutator(analyzed, aprep_build_mutator, &ctx, 0);
	analyzed->limitOffset = save_lo;	/* restore caller's tree */
	analyzed->limitCount = save_lc;

	if (ctx.too_many || ctx.next_paramid == 0)
		return NULL;

	/* Re-attach the original (un-parameterized) LIMIT/OFFSET literals. */
	mutated->limitOffset = copyObject(save_lo);
	mutated->limitCount = copyObject(save_lc);

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
	Node	   *save_lo = query->limitOffset;
	Node	   *save_lc = query->limitCount;

	ctx.next_paramid = 0;
	ctx.params = makeParamList(num_params);
	ctx.expected = param_types;
	ctx.nexpected = num_params;
	ctx.mismatch = false;

	/* Mirror build: hide LIMIT/OFFSET so their literals aren't counted. */
	query->limitOffset = NULL;
	query->limitCount = NULL;
	(void) query_tree_walker(query, aprep_extract_walker, &ctx, 0);
	query->limitOffset = save_lo;
	query->limitCount = save_lc;

	if (ctx.mismatch || ctx.next_paramid != num_params)
		return NULL;			/* fail safe -> caller plans normally */
	return ctx.params;
}


/* ----------------------------------------------------------------
 *		plansource construction
 * ---------------------------------------------------------------- */

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
			return true;
		default:
			return false;
	}
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

		entry->seen_count++;
		boundParams = extract_bound_params(analyzed_query,
										   entry->param_types,
										   entry->num_params);
		if (boundParams == NULL)
			return APREP_MISS;	/* collision / divergence -> plan normally */

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
		MemoryContext old = MemoryContextSwitchTo(AutoprepareContext);
		Oid		   *ptypes = NULL;
		int			nparams = 0;
		CachedPlanSource *ps;

		ps = build_parameterized_plansource(analyzed_query, query_string,
											&ptypes, &nparams);
		if (ps != NULL)
		{
			SaveCachedPlan(ps); /* move to CacheMemoryContext + register for
								 * invalidation callbacks */
			entry->plansource = ps;
			entry->param_types = ptypes;
			entry->num_params = nparams;
			entry->promoted = true;
		}
		else
		{
			/*
			 * This shape can't be parameterized/cached (too many params,
			 * rule-rewritten, etc.).  Mark it so we never pay the build cost
			 * again -- otherwise every future execution would redo the
			 * copyObject + QueryRewrite and leak it into AutoprepareContext.
			 */
			entry->declined = true;
		}
		MemoryContextSwitchTo(old);
	}

	return APREP_MISS;			/* plan normally on the promoting call */
}


/* ----------------------------------------------------------------
 *		GUC registration
 * ---------------------------------------------------------------- */

void
AutoprepareRegisterGUCs(void)
{
	DefineCustomBoolVariable("autoprepare.enabled",
							 "Cache and reuse plans for repeated query shapes.",
							 NULL,
							 &autoprepare_enabled,
							 false,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("autoprepare.threshold",
							"Cache a query shape after it is seen this many times.",
							NULL,
							&autoprepare_threshold,
							2, 1, INT_MAX,
							PGC_SUSET, 0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("autoprepare.limit",
							"Maximum number of cached query shapes per backend.",
							NULL,
							&autoprepare_limit,
							1024, 1, INT_MAX,
							PGC_SUSET, 0,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("autoprepare");

	/*
	 * Our fingerprint is the query jumble (Query->queryId).  Force it on so
	 * the feature works even under the default compute_query_id = auto when no
	 * other consumer (e.g. pg_stat_statements) has requested it.
	 */
	EnableQueryId();
}
