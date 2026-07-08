/*-------------------------------------------------------------------------
 *
 * columnar_scan.c
 *		DBblue Columnar Engine - planner integration + columnar scan executor
 *		(Milestone 2, step 2: the serve path).
 *
 * Planner side: set_rel_pathlist_hook offers a CustomPath for a base relation
 * when every column the scan node references (projection AND quals) is
 * registered and populated in the column store. System columns, whole-row
 * references, samples, and non-base relations fall through to the normal
 * paths. The needed column list travels to the executor in custom_private so
 * eligibility is RE-CHECKED at executor startup - a cached plan whose
 * relation was since repopulated with fewer columns (or dropped from the
 * store) degrades to a pure heap scan inside this same node, never to an
 * error and never to wrong results.
 *
 * Executor side: the scan walks the relation in DBBC_PAGES_PER_BLOCK-page
 * ranges. For each range that has a columnar block, the block is served ONLY
 * if every heap page in the range still has PD_ALL_VISIBLE set AND its
 * current page LSN equals the LSN recorded when the block was built (both
 * checked under a share lock on the page). That proves the page bytes are
 * unchanged since the build and every row in them is visible to every
 * snapshot - so emitting the block's rows is indistinguishable from a heap
 * scan at any isolation level. Ranges that fail the check (or have no block)
 * are read from the heap through the table AM with the query snapshot, which
 * also covers pages beyond the last populate (heap growth) and rows newer
 * than the build.
 *
 * Concurrency: the scan PINS the relation's current store version at
 * BeginCustomScan (refcount; registered with the ResourceOwner so aborted
 * queries unpin too) and unpins at EndCustomScan. A concurrent repopulate or
 * drop swaps the entry to a new version and merely unpins the old one, so
 * pinned blocks can never be freed under a running scan, publishers never
 * wait for readers, and no dshash lock is ever held beyond a momentary
 * lookup (dshash forbids holding one across another lookup - it asserts).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "access/stratnum.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/tupmacs.h"
#include "catalog/pg_aggregate_d.h"
#include "catalog/pg_am_d.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "nodes/nodeFuncs.h"
#include "nodes/value.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/predicate.h"
#include "utils/array.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "dbblue_columnar.h"

/* saved hook */
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;

/* executor state */
typedef struct DbbcColPtrs
{
	uint8	   *values;			/* fixed array or uint32 offsets */
	uint8	   *blob;			/* varlena blob, or NULL */
	uint8	   *nulls;			/* null bitmap, or NULL */
} DbbcColPtrs;

/*
 * A qual simple enough to evaluate against zone maps (block skipping) and
 * against raw column values (the columnar pre-filter). Soundness rules: the
 * operator must belong to the column type's default btree opfamily (the
 * comparison proc is the opfamily's ORDER proc for the actual operand
 * types, so cross-type quals like int4col = int8const are exact), and the
 * qual's collation must equal the column's (zone min/max were computed
 * under the column collation; a different qual collation orders
 * differently). Everything not extractable stays in plan.qual, which
 * ExecScan re-evaluates on every returned row regardless - the pre-filter
 * can only remove rows, never admit wrong ones.
 */
typedef enum DbbcSkipKind
{
	DBBC_SKIP_OP,				/* Var <btree-strategy-op> Const */
	DBBC_SKIP_SAOP_EQ,			/* Var = ANY (const array) */
	DBBC_SKIP_NULLTEST,			/* Var IS [NOT] NULL */
} DbbcSkipKind;

typedef struct DbbcSkipQual
{
	DbbcSkipKind kind;
	AttrNumber	attno;
	Oid			coltype;		/* the Var's vartype: must match chunk->atttypid
								 * before a zone map (built under that type) may
								 * be trusted */
	uint16		strategy;		/* BTLess..BTGreater for OP */
	bool		nulltest_isnull;
	Oid			collation;
	FmgrInfo	cmp;			/* btree ORDER proc (coltype, rhs type) */
	Datum		value;			/* OP: the Const value */
	Datum	   *elems;			/* SAOP: non-null array elements */
	int			nelems;
} DbbcSkipQual;

typedef struct DbbcScanState
{
	CustomScanState css;

	bool		initialized;	/* false under EXPLAIN (no ANALYZE) */
	Snapshot	snapshot;
	List	   *needed_attnos;	/* from custom_private */

	/* pinned store version (refcount held from Begin to End) */
	DbbcRelVersion *version;	/* NULL -> pure heap mode */
	dsa_area   *dsa;
	dsa_pointer *dir;
	uint32		ndirslots;
	int			ncols;
	int16	   *attnums;		/* local copy */

	/* range iteration */
	BlockNumber heap_nblocks;	/* as of BeginCustomScan */
	uint32		total_slots;
	uint32		cur_slot;		/* next range to consider */

	/* columnar block being emitted */
	DbbcBlock  *cur_block;
	DbbcColumnChunk *cur_chunks;
	DbbcColPtrs *col_ptrs;		/* resolved addresses for cur_block */
	uint32		cur_row;

	/* pushed-down simple quals (zone skipping + columnar pre-filter) */
	DbbcSkipQual *skipquals;
	int			nskipquals;
	int		   *attno_to_col;	/* [natts] -> chunk index or -1 */

	/* heap fallback */
	TableScanDesc heap_scan;
	TupleTableSlot *heap_slot;	/* table-AM slot (scan slot is virtual) */

	/*
	 * Degraded mode: no usable store version (stale cached plan after a
	 * repopulate/drop, or a non-heap table AM after ALTER TABLE ... SET
	 * ACCESS METHOD). The whole relation is read through ONE plain table-AM
	 * scan - never heap_setscanlimits, which is heap-AM-only.
	 */
	bool		whole_rel_mode;
	bool		whole_rel_done;

	/* instrumentation */
	uint64		blocks_columnar;
	uint64		blocks_skipped;
	uint64		ranges_heap;
	uint64		rows_columnar;
	uint64		rows_filtered;
} DbbcScanState;

static void dbbc_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
								  Index rti, RangeTblEntry *rte);
static bool dbbc_rel_ready(PlannerInfo *root, RelOptInfo *rel,
						   RangeTblEntry *rte, List **needed_out);
static Plan *dbbc_plan_custom_path(PlannerInfo *root, RelOptInfo *rel,
								   CustomPath *best_path, List *tlist,
								   List *clauses, List *custom_plans);
static Node *dbbc_create_scan_state(CustomScan *cscan);
static void dbbc_begin_scan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *dbbc_exec_scan(CustomScanState *node);
static void dbbc_end_scan(CustomScanState *node);
static void dbbc_rescan_scan(CustomScanState *node);
static void dbbc_explain_scan(CustomScanState *node, List *ancestors,
							  ExplainState *es);
static TupleTableSlot *dbbc_next(ScanState *ss);
static bool dbbc_recheck(ScanState *ss, TupleTableSlot *slot);
static bool dbbc_block_valid(DbbcScanState *s, Relation rel, DbbcBlock *block);
static TupleTableSlot *dbbc_emit_row(DbbcScanState *s);

static const CustomPathMethods dbbc_path_methods = {
	.CustomName = "DBBlueColumnarScan",
	.PlanCustomPath = dbbc_plan_custom_path,
};

static const CustomScanMethods dbbc_scan_methods = {
	.CustomName = "DBBlueColumnarScan",
	.CreateCustomScanState = dbbc_create_scan_state,
};

static const CustomExecMethods dbbc_exec_methods = {
	.CustomName = "DBBlueColumnarScan",
	.BeginCustomScan = dbbc_begin_scan,
	.ExecCustomScan = dbbc_exec_scan,
	.EndCustomScan = dbbc_end_scan,
	.ReScanCustomScan = dbbc_rescan_scan,
	.ExplainCustomScan = dbbc_explain_scan,
};

static void dbbc_agg_init(void);

void
dbbc_scan_init(void)
{
	RegisterCustomScanMethods(&dbbc_scan_methods);

	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = dbbc_set_rel_pathlist;

	dbbc_agg_init();
}

/*
 * Try to turn one qual clause into a DbbcSkipQual. See the struct comment
 * for the soundness rules; anything that doesn't fit is simply left to
 * ExecScan's normal qual evaluation.
 */
static bool
dbbc_extract_one_qual(Node *clause, Index varno, DbbcSkipQual *q)
{
	memset(q, 0, sizeof(DbbcSkipQual));

	if (IsA(clause, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) clause;
		Node	   *l,
				   *r;
		Oid			opno = op->opno;
		Oid			ltype,
					rtype;
		Var		   *var;
		Const	   *cst;
		Oid			opclass,
					opfam;
		int			strategy;
		Oid			cmpproc;

		if (list_length(op->args) != 2)
			return false;
		l = (Node *) linitial(op->args);
		r = (Node *) lsecond(op->args);

		/* normalize to Var op Const, commuting if needed */
		if (IsA(r, Const) || (IsA(r, RelabelType) &&
							  IsA(((RelabelType *) r)->arg, Const)))
		{
			/* keep as is */
		}
		else
		{
			Node	   *tmp;

			opno = get_commutator(opno);
			if (!OidIsValid(opno))
				return false;
			tmp = l;
			l = r;
			r = tmp;
		}

		ltype = exprType(l);
		rtype = exprType(r);

		while (IsA(l, RelabelType))
			l = (Node *) ((RelabelType *) l)->arg;
		while (IsA(r, RelabelType))
			r = (Node *) ((RelabelType *) r)->arg;
		if (!IsA(l, Var) || !IsA(r, Const))
			return false;
		var = (Var *) l;
		cst = (Const *) r;
		if (var->varno != varno || var->varlevelsup != 0 ||
			var->varattno <= 0)
			return false;
		if (cst->constisnull)
			return false;

		/* qual collation must match the column's (zone-map ordering) */
		if (op->inputcollid != var->varcollid)
			return false;

		opclass = GetDefaultOpClass(var->vartype, BTREE_AM_OID);
		if (!OidIsValid(opclass))
			return false;
		opfam = get_opclass_family(opclass);
		strategy = get_op_opfamily_strategy(opno, opfam);
		if (strategy < BTLessStrategyNumber ||
			strategy > BTGreaterStrategyNumber)
			return false;
		cmpproc = get_opfamily_proc(opfam, ltype, rtype, BTORDER_PROC);
		if (!OidIsValid(cmpproc))
			return false;

		q->kind = DBBC_SKIP_OP;
		q->attno = var->varattno;
		q->coltype = var->vartype;
		q->strategy = (uint16) strategy;
		q->collation = op->inputcollid;
		q->value = cst->constvalue;
		fmgr_info(cmpproc, &q->cmp);
		return true;
	}

	if (IsA(clause, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
		Node	   *l,
				   *r;
		Oid			ltype;
		Var		   *var;
		Const	   *cst;
		ArrayType  *arr;
		Oid			elemtype;
		int16		elemlen;
		bool		elembyval;
		char		elemalign;
		Datum	   *elems;
		bool	   *nulls;
		int			nelems,
					nkeep,
					i;
		Oid			opclass,
					opfam;
		Oid			cmpproc;

		if (!saop->useOr || list_length(saop->args) != 2)
			return false;
		l = (Node *) linitial(saop->args);
		r = (Node *) lsecond(saop->args);
		ltype = exprType(l);
		while (IsA(l, RelabelType))
			l = (Node *) ((RelabelType *) l)->arg;
		if (!IsA(l, Var) || !IsA(r, Const))
			return false;
		var = (Var *) l;
		cst = (Const *) r;
		if (var->varno != varno || var->varlevelsup != 0 ||
			var->varattno <= 0 || cst->constisnull)
			return false;
		if (saop->inputcollid != var->varcollid)
			return false;

		opclass = GetDefaultOpClass(var->vartype, BTREE_AM_OID);
		if (!OidIsValid(opclass))
			return false;
		opfam = get_opclass_family(opclass);
		if (get_op_opfamily_strategy(saop->opno, opfam) !=
			BTEqualStrategyNumber)
			return false;

		arr = DatumGetArrayTypeP(cst->constvalue);
		elemtype = ARR_ELEMTYPE(arr);
		cmpproc = get_opfamily_proc(opfam, ltype, elemtype, BTORDER_PROC);
		if (!OidIsValid(cmpproc))
			return false;

		get_typlenbyvalalign(elemtype, &elemlen, &elembyval, &elemalign);
		deconstruct_array(arr, elemtype, elemlen, elembyval, elemalign,
						  &elems, &nulls, &nelems);

		/* NULL elements can never match '=', drop them */
		nkeep = 0;
		for (i = 0; i < nelems; i++)
		{
			if (!nulls[i])
				elems[nkeep++] = elems[i];
		}

		q->kind = DBBC_SKIP_SAOP_EQ;
		q->attno = var->varattno;
		q->coltype = var->vartype;
		q->strategy = BTEqualStrategyNumber;
		q->collation = saop->inputcollid;
		q->elems = elems;
		q->nelems = nkeep;
		fmgr_info(cmpproc, &q->cmp);
		return true;
	}

	if (IsA(clause, NullTest))
	{
		NullTest   *nt = (NullTest *) clause;
		Node	   *arg = (Node *) nt->arg;
		Var		   *var;

		if (nt->argisrow)
			return false;
		while (IsA(arg, RelabelType))
			arg = (Node *) ((RelabelType *) arg)->arg;
		if (!IsA(arg, Var))
			return false;
		var = (Var *) arg;
		if (var->varno != varno || var->varlevelsup != 0 ||
			var->varattno <= 0)
			return false;

		q->kind = DBBC_SKIP_NULLTEST;
		q->attno = var->varattno;
		q->nulltest_isnull = (nt->nulltesttype == IS_NULL);
		return true;
	}

	return false;
}

/* extract all simple quals from a list of bare clause expressions */
static int
dbbc_extract_skip_quals(List *clauses, Index varno, DbbcSkipQual **out)
{
	DbbcSkipQual *quals;
	int			n = 0;
	ListCell   *lc;

	if (clauses == NIL)
	{
		*out = NULL;
		return 0;
	}

	quals = (DbbcSkipQual *)
		palloc(list_length(clauses) * sizeof(DbbcSkipQual));
	foreach(lc, clauses)
	{
		if (dbbc_extract_one_qual((Node *) lfirst(lc), varno, &quals[n]))
			n++;
	}

	*out = quals;
	return n;
}

/* cmp(col-side datum, qual-side datum) via the qual's btree ORDER proc */
static inline int32
dbbc_skip_cmp(DbbcSkipQual *q, Datum coldatum, Datum qualdatum)
{
	return DatumGetInt32(FunctionCall2Coll(&q->cmp, q->collation,
										   coldatum, qualdatum));
}

/*
 * Can this (valid!) block be skipped entirely - i.e. can we prove from the
 * zone maps that NO row in it satisfies all the quals? A skipped block is
 * sound because a valid block provably contains every row of its range.
 * Only ever called on blocks that passed dbbc_block_valid: a stale block's
 * zone maps prove nothing.
 */
static bool
dbbc_zone_block_skippable(DbbcSkipQual *quals, int nquals,
						  DbbcBlock *block, DbbcColumnChunk *chunks,
						  int ncols)
{
	int			i;

	for (i = 0; i < nquals; i++)
	{
		DbbcSkipQual *q = &quals[i];
		DbbcColumnChunk *chunk = NULL;
		Datum		zmin,
					zmax;
		int			c;

		for (c = 0; c < ncols; c++)
		{
			if (chunks[c].attnum == q->attno)
			{
				chunk = &chunks[c];
				break;
			}
		}
		if (chunk == NULL)
			continue;

		if (q->kind == DBBC_SKIP_NULLTEST)
		{
			if (q->nulltest_isnull)
			{
				if (chunk->null_count == 0)
					return true;	/* no NULLs: IS NULL matches nothing */
			}
			else
			{
				if (chunk->null_count == block->nrows)
					return true;	/* all NULL: IS NOT NULL matches nothing */
			}
			continue;
		}

		/* strict operators match no all-NULL column (ordering-independent) */
		if (chunk->null_count == block->nrows)
			return true;

		/*
		 * The min/max are only extrema under the exact type + collation they
		 * were built with. A non-rewriting ALTER COLUMN TYPE (binary
		 * coercible) or COLLATE leaves heap pages / LSN / VM untouched - so
		 * the block still passes validity - but changes the current
		 * catalog's ordering out from under these bytes. Comparing them with
		 * the current comparator could skip a block that actually matches
		 * (missing rows), or hand a stale by-value datum to a wider/varlena
		 * comparator (crash) on the plan-time estimate path. If build-time
		 * identity and current identity disagree, treat as no zone map.
		 */
		if (chunk->atttypid != q->coltype ||
			chunk->attcollation != q->collation)
			continue;
		if (!chunk->has_minmax)
			continue;			/* no zone map: cannot prove anything */

		zmin = dbbc_chunk_minmax_datum(chunk, false);
		zmax = dbbc_chunk_minmax_datum(chunk, true);

		if (q->kind == DBBC_SKIP_OP)
		{
			bool		skip = false;

			switch (q->strategy)
			{
				case BTLessStrategyNumber:
					skip = dbbc_skip_cmp(q, zmin, q->value) >= 0;
					break;
				case BTLessEqualStrategyNumber:
					skip = dbbc_skip_cmp(q, zmin, q->value) > 0;
					break;
				case BTEqualStrategyNumber:
					skip = dbbc_skip_cmp(q, zmin, q->value) > 0 ||
						dbbc_skip_cmp(q, zmax, q->value) < 0;
					break;
				case BTGreaterEqualStrategyNumber:
					skip = dbbc_skip_cmp(q, zmax, q->value) < 0;
					break;
				case BTGreaterStrategyNumber:
					skip = dbbc_skip_cmp(q, zmax, q->value) <= 0;
					break;
			}
			if (skip)
				return true;
		}
		else					/* DBBC_SKIP_SAOP_EQ */
		{
			bool		any_possible = false;
			int			e;

			for (e = 0; e < q->nelems; e++)
			{
				if (dbbc_skip_cmp(q, zmin, q->elems[e]) <= 0 &&
					dbbc_skip_cmp(q, zmax, q->elems[e]) >= 0)
				{
					any_possible = true;
					break;
				}
			}
			if (!any_possible)
				return true;
		}
	}

	return false;
}

/*
 * Plan-time skip-fraction estimate: walk (a sample of) the real zone maps
 * with the query's extractable quals. Returns the estimated fraction of
 * BUILT blocks that will be skipped, scaled by build coverage, clamped
 * conservatively.
 */
static double
dbbc_estimate_skip_fraction(Oid relid, List *clauses, Index varno)
{
	DbbcSkipQual *quals;
	int			nquals;
	DbbcRelVersion *version;
	dsa_pointer *dir;
	uint32		step;
	uint32		slot;
	uint32		seen = 0;
	uint32		skipped = 0;
	double		frac;

	nquals = dbbc_extract_skip_quals(clauses, varno, &quals);
	if (nquals == 0)
		return 0.0;

	version = dbbc_version_pin_tracked(relid);
	if (version == NULL)
		return 0.0;
	if (!DsaPointerIsValid(version->blockdir) || version->ndirslots == 0)
	{
		dbbc_version_unpin_tracked(version);
		return 0.0;
	}

	dir = (dsa_pointer *) dsa_get_address(dbbc_store_dsa(),
										  version->blockdir);
	step = Max(1, version->ndirslots / 1024);
	for (slot = 0; slot < version->ndirslots; slot += step)
	{
		DbbcBlock  *block;
		DbbcColumnChunk *chunks;

		if (!DsaPointerIsValid(dir[slot]))
			continue;
		block = (DbbcBlock *) dsa_get_address(dbbc_store_dsa(), dir[slot]);
		chunks = (DbbcColumnChunk *) dsa_get_address(dbbc_store_dsa(),
													 block->chunks);
		seen++;
		if (dbbc_zone_block_skippable(quals, nquals, block, chunks,
									  version->ncols))
			skipped++;
	}

	frac = (seen > 0) ? (double) skipped / seen : 0.0;
	/* scale by how much of the relation is columnarized at all */
	frac *= (double) version->nblocks / Max(version->ndirslots, 1);

	dbbc_version_unpin_tracked(version);

	return Min(frac, 0.95);
}

/*
 * Can this base relation's scan be served by the column store? On success,
 * *needed_out receives the (possibly empty) list of user attnos the scan
 * references, for re-validation at executor startup.
 */
static bool
dbbc_rel_ready(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte,
			   List **needed_out)
{
	Bitmapset  *attrs = NULL;
	List	   *needed = NIL;
	ListCell   *lc;
	int			x;
	DbbcRelVersion *version;
	int16	   *reg;
	bool		ok = true;

	if (rel->reloptkind != RELOPT_BASEREL)
		return false;
	if (rte->rtekind != RTE_RELATION)
		return false;
	if (rte->relkind != RELKIND_RELATION && rte->relkind != RELKIND_MATVIEW)
		return false;
	if (rte->tablesample != NULL)
		return false;

	/*
	 * Laterally-dependent rels would need a parameterized path (their
	 * reltarget can carry PlaceHolderVars referencing other rels); we only
	 * build unparameterized paths, so stay out entirely.
	 */
	if (!bms_is_empty(rel->lateral_relids))
		return false;

	/* the block format and the range-limited fallback are heap-AM-only */
	if (get_rel_relam(rte->relid) != HEAP_TABLE_AM_OID)
		return false;

	/* every column the scan must produce or filter on */
	pull_varattnos((Node *) rel->reltarget->exprs, rel->relid, &attrs);
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, rel->relid, &attrs);
	}

	x = -1;
	while ((x = bms_next_member(attrs, x)) >= 0)
	{
		AttrNumber	attno = x + FirstLowInvalidHeapAttributeNumber;

		/*
		 * System columns and whole-row references are heap-only. This gate
		 * is also LOAD-BEARING for EvalPlanQual: every relation that can
		 * receive an EPQ test tuple (UPDATE/DELETE/MERGE targets, FOR
		 * UPDATE/SHARE rels, rowmarked rels) gets a junk ctid or whole-row
		 * Var injected into its reltarget before this hook runs, so it is
		 * excluded here - which is what keeps foreign EPQ slots away from
		 * our virtual-slot-specialized expressions. Do not weaken this
		 * check without handling EPQ explicitly.
		 */
		if (attno <= 0)
			return false;
		needed = lappend_int(needed, attno);
	}

	/*
	 * Is a populated store present and does it cover the needed columns?
	 * Tracked pin: dsa_get_address can error on segment attach, and an
	 * untracked pin would leak the version forever.
	 */
	version = dbbc_version_pin_tracked(rte->relid);
	if (version == NULL)
		return false;
	if (!DsaPointerIsValid(version->blockdir) || version->nblocks == 0)
	{
		dbbc_version_unpin_tracked(version);
		return false;
	}
	reg = (int16 *) dsa_get_address(dbbc_store_dsa(), version->attnums);
	foreach(lc, needed)
	{
		int			attno = lfirst_int(lc);
		bool		found = false;
		int			c;

		for (c = 0; c < version->ncols; c++)
		{
			if (reg[c] == attno)
			{
				found = true;
				break;
			}
		}
		if (!found)
		{
			ok = false;
			break;
		}
	}
	dbbc_version_unpin_tracked(version);

	if (!ok)
		return false;

	*needed_out = needed;
	return true;
}

static void
dbbc_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
					  Index rti, RangeTblEntry *rte)
{
	List	   *needed = NIL;
	List	   *clauses = NIL;
	ListCell   *lc;
	double		skip_frac;
	CustomPath *cpath;
	QualCost	qcost;

	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);

	if (!dbblue_columnar_enabled || !dbblue_columnar_enable_columnar_scan)
		return;

	if (!dbbc_rel_ready(root, rel, rte, &needed))
		return;

	/* estimate zone-map skipping against the real block metadata */
	foreach(lc, rel->baserestrictinfo)
		clauses = lappend(clauses, ((RestrictInfo *) lfirst(lc))->clause);
	skip_frac = dbbc_estimate_skip_fraction(rte->relid, clauses, rel->relid);

	cpath = makeNode(CustomPath);
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = rel;
	cpath->path.pathtarget = rel->reltarget;
	cpath->path.param_info = NULL;
	cpath->path.parallel_aware = false;
	cpath->path.parallel_safe = false;
	cpath->path.parallel_workers = 0;
	cpath->path.rows = rel->rows;
	cpath->path.pathkeys = NIL;
	cpath->flags = 0;
	cpath->custom_private = list_make1(needed);
	cpath->methods = &dbbc_path_methods;

	/*
	 * Costing: the scan still visits every heap page (validity checks pin
	 * page headers; fallback reads pages in full), so charge full I/O like a
	 * seqscan. The CPU saving has two sources: no tuple deforming on
	 * columnar-served rows, and zone-map skipping - blocks the real zone
	 * maps prove empty for these quals contribute no per-tuple work at all,
	 * so the CPU terms scale by (1 - skip_frac).
	 */
	cost_qual_eval(&qcost, rel->baserestrictinfo, root);
	cpath->path.disabled_nodes = 0;
	cpath->path.startup_cost = qcost.startup;
	cpath->path.total_cost = qcost.startup +
		seq_page_cost * rel->pages +
		(cpu_tuple_cost * 0.75 + qcost.per_tuple) * rel->tuples *
		(1.0 - skip_frac);

	add_path(rel, (Path *) cpath);
}

static Plan *
dbbc_plan_custom_path(PlannerInfo *root, RelOptInfo *rel,
					  CustomPath *best_path, List *tlist,
					  List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = extract_actual_clauses(clauses, false);
	cscan->scan.scanrelid = rel->relid;
	cscan->flags = best_path->flags;
	cscan->custom_private = best_path->custom_private;
	cscan->methods = &dbbc_scan_methods;

	return &cscan->scan.plan;
}

static Node *
dbbc_create_scan_state(CustomScan *cscan)
{
	DbbcScanState *s = (DbbcScanState *) palloc0(sizeof(DbbcScanState));

	NodeSetTag(s, T_CustomScanState);
	s->css.methods = &dbbc_exec_methods;

	return (Node *) s;
}

static void
dbbc_scan_setup(DbbcScanState *s, Relation rel, EState *estate,
				List *needed_attnos, List *qual_clauses, Index qual_varno)
{
	s->initialized = true;
	s->snapshot = estate->es_snapshot;

	/*
	 * SIREAD lock, exactly as a heap seqscan takes in initscan: without it a
	 * fully-columnar-served scan would leave no predicate lock and
	 * SERIALIZABLE write skew would go undetected. (No-op below
	 * SERIALIZABLE.)
	 */
	PredicateLockRelation(rel, s->snapshot);

	dbbc_store_attach();
	s->dsa = dbbc_store_dsa();

	/*
	 * Pin the current store version for the scan's whole lifetime
	 * (abort-safe via the ResourceOwner). Re-validate coverage: the store
	 * may have been repopulated with different columns since this plan was
	 * made; if it no longer covers the scan, run entirely from the heap -
	 * degrade, never error, never wrong. A non-heap table AM (possible via
	 * ALTER TABLE ... SET ACCESS METHOD under a cached plan) also degrades:
	 * the block format and heap_setscanlimits are heap-only.
	 */
	if (rel->rd_rel->relam == HEAP_TABLE_AM_OID)
		s->version = dbbc_version_pin_tracked(RelationGetRelid(rel));
	else
		s->version = NULL;
	if (s->version != NULL)
	{
		bool		usable = DsaPointerIsValid(s->version->blockdir) &&
			s->version->nblocks > 0;

		if (usable)
		{
			int16	   *reg = (int16 *) dsa_get_address(s->dsa,
														s->version->attnums);
			ListCell   *lc;

			foreach(lc, needed_attnos)
			{
				int			attno = lfirst_int(lc);
				bool		found = false;
				int			c;

				for (c = 0; c < s->version->ncols; c++)
				{
					if (reg[c] == attno)
					{
						found = true;
						break;
					}
				}
				if (!found)
				{
					usable = false;
					break;
				}
			}
		}

		if (usable)
		{
			int16	   *reg = (int16 *) dsa_get_address(s->dsa,
														s->version->attnums);
			int			natts = RelationGetDescr(rel)->natts;
			int			c;

			s->dir = (dsa_pointer *) dsa_get_address(s->dsa,
													 s->version->blockdir);
			s->ndirslots = s->version->ndirslots;
			s->ncols = s->version->ncols;
			s->attnums = (int16 *) palloc(s->ncols * sizeof(int16));
			memcpy(s->attnums, reg, s->ncols * sizeof(int16));
			s->col_ptrs = (DbbcColPtrs *)
				palloc(s->ncols * sizeof(DbbcColPtrs));

			s->attno_to_col = (int *) palloc(natts * sizeof(int));
			for (c = 0; c < natts; c++)
				s->attno_to_col[c] = -1;
			for (c = 0; c < s->ncols; c++)
			{
				if (s->attnums[c] >= 1 && s->attnums[c] <= natts)
					s->attno_to_col[s->attnums[c] - 1] = c;
			}

			/* simple quals for zone skipping + the columnar pre-filter */
			s->nskipquals =
				dbbc_extract_skip_quals(qual_clauses, qual_varno,
										&s->skipquals);
		}
		else
		{
			dbbc_version_unpin_tracked(s->version);
			s->version = NULL;
		}
	}

	s->whole_rel_mode = (s->version == NULL);
	s->whole_rel_done = false;

	s->heap_nblocks = RelationGetNumberOfBlocks(rel);
	s->total_slots = (s->heap_nblocks + DBBC_PAGES_PER_BLOCK - 1) /
		DBBC_PAGES_PER_BLOCK;
	s->cur_slot = 0;

	/* heap-fallback rows need the table AM's slot type, not our virtual one */
	s->heap_slot = table_slot_create(rel, &estate->es_tupleTable);
}

static void
dbbc_begin_scan(CustomScanState *node, EState *estate, int eflags)
{
	DbbcScanState *s = (DbbcScanState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	Relation	rel = node->ss.ss_currentRelation;

	s->needed_attnos = (List *) linitial(cscan->custom_private);

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	dbbc_scan_setup(s, rel, estate, s->needed_attnos,
					cscan->scan.plan.qual, cscan->scan.scanrelid);
}

/* the columnar block for a range, or NULL if absent / store unusable */
static inline DbbcBlock *
dbbc_slot_block(DbbcScanState *s, uint32 slot)
{
	if (s->version == NULL || slot >= s->ndirslots ||
		!DsaPointerIsValid(s->dir[slot]))
		return NULL;
	return (DbbcBlock *) dsa_get_address(s->dsa, s->dir[slot]);
}

/*
 * The serve-time validity proof. Under a share lock on each heap page of the
 * block's range: PD_ALL_VISIBLE still set (every row visible to every
 * snapshot) AND the page LSN equals the build-time stamp (the bytes are the
 * ones the block was built from - a vacuum that re-set the VM bit after an
 * intervening change leaves a different LSN, which is exactly the case the
 * equality catches).
 */
static bool
dbbc_block_valid(DbbcScanState *s, Relation rel, DbbcBlock *block)
{
	BlockNumber range_end;
	uint16		p;

	/*
	 * The block must cover its range's ENTIRE current extent. A block
	 * reaching past the current end of the heap (concurrent truncation) is
	 * stale; and a partial trailing block (npages < 32) whose range the heap
	 * has since GROWN into must fall back to the heap for the whole range,
	 * or the pages after its stamped ones would be served by neither path -
	 * silently missing rows.
	 */
	range_end = Min(block->first_page + (BlockNumber) DBBC_PAGES_PER_BLOCK,
					s->heap_nblocks);
	if ((BlockNumber) block->first_page + block->npages != range_end)
		return false;

	for (p = 0; p < block->npages; p++)
	{
		Buffer		buf;
		Page		page;
		bool		ok;

		buf = ReadBuffer(rel, block->first_page + p);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		ok = PageIsAllVisible(page) &&
			BufferGetLSNAtomic(buf) == block->stamps[p].lsn;
		UnlockReleaseBuffer(buf);

		if (!ok)
			return false;
	}
	return true;
}

/* read one value of chunk c at row from the current block */
static inline Datum
dbbc_chunk_read(DbbcScanState *s, int c, uint32 row, bool *isnull)
{
	DbbcColumnChunk *chunk = &s->cur_chunks[c];
	DbbcColPtrs *ptrs = &s->col_ptrs[c];

	if (ptrs->nulls != NULL &&
		(ptrs->nulls[row / 8] & (1 << (row % 8))) != 0)
	{
		*isnull = true;
		return (Datum) 0;
	}

	if (chunk->attlen > 0)
	{
		uint8	   *ptr = ptrs->values + (Size) row * chunk->attlen;

		*isnull = false;
		if (chunk->attbyval)
			return fetch_att(ptr, true, chunk->attlen);
		return PointerGetDatum(ptr);
	}
	else
	{
		uint32		off = ((uint32 *) ptrs->values)[row];

		if (off == DBBC_VAR_NULL_OFFSET)
		{
			/* defensive; the bitmap should have caught it */
			*isnull = true;
			return (Datum) 0;
		}
		*isnull = false;
		return PointerGetDatum(ptrs->blob + off);
	}
}

/*
 * Columnar pre-filter: evaluate the simple quals against raw column values
 * before paying for slot formation. Rows removed here would have been
 * removed by ExecScan's qual anyway (same operator semantics via the btree
 * ORDER proc); rows passed are still re-checked by ExecScan, so this can
 * only ever remove, never wrongly admit.
 */
static bool
dbbc_row_passes(DbbcScanState *s, uint32 row)
{
	int			i;

	for (i = 0; i < s->nskipquals; i++)
	{
		DbbcSkipQual *q = &s->skipquals[i];
		int			c = s->attno_to_col[q->attno - 1];
		Datum		value;
		bool		isnull;

		if (c < 0)
			continue;			/* not served columnar; leave to ExecScan */
		value = dbbc_chunk_read(s, c, row, &isnull);

		if (q->kind == DBBC_SKIP_NULLTEST)
		{
			if (isnull != q->nulltest_isnull)
				return false;
			continue;
		}

		if (isnull)
			return false;		/* strict operators never pass NULL */

		if (q->kind == DBBC_SKIP_OP)
		{
			int32		r = dbbc_skip_cmp(q, value, q->value);
			bool		pass = false;

			switch (q->strategy)
			{
				case BTLessStrategyNumber:
					pass = r < 0;
					break;
				case BTLessEqualStrategyNumber:
					pass = r <= 0;
					break;
				case BTEqualStrategyNumber:
					pass = r == 0;
					break;
				case BTGreaterEqualStrategyNumber:
					pass = r >= 0;
					break;
				case BTGreaterStrategyNumber:
					pass = r > 0;
					break;
			}
			if (!pass)
				return false;
		}
		else					/* DBBC_SKIP_SAOP_EQ */
		{
			bool		any = false;
			int			e;

			for (e = 0; e < q->nelems; e++)
			{
				if (dbbc_skip_cmp(q, value, q->elems[e]) == 0)
				{
					any = true;
					break;
				}
			}
			if (!any)
				return false;
		}
	}

	return true;
}

/* fill the virtual scan slot from cur_block[cur_row] */
static TupleTableSlot *
dbbc_emit_row(DbbcScanState *s)
{
	TupleTableSlot *slot = s->css.ss.ss_ScanTupleSlot;
	uint32		row = s->cur_row;
	int			natts = slot->tts_tupleDescriptor->natts;
	int			c;

	ExecClearTuple(slot);

	/* unregistered columns are never referenced (planner gating): NULL them */
	memset(slot->tts_isnull, true, natts * sizeof(bool));
	memset(slot->tts_values, 0, natts * sizeof(Datum));

	for (c = 0; c < s->ncols; c++)
	{
		int			attidx = s->cur_chunks[c].attnum - 1;
		bool		isnull;
		Datum		value;

		value = dbbc_chunk_read(s, c, row, &isnull);
		if (!isnull)
		{
			slot->tts_values[attidx] = value;
			slot->tts_isnull[attidx] = false;
		}
	}

	ExecStoreVirtualTuple(slot);
	s->cur_row++;
	s->rows_columnar++;

	return slot;
}

/*
 * ExecScan access method: next tuple from the hybrid walk, or NULL at EOF.
 * Quals and projection are applied by ExecScan on whatever slot we return.
 */
static TupleTableSlot *
dbbc_next(ScanState *ss)
{
	DbbcScanState *s = (DbbcScanState *) ss;
	Relation	rel = ss->ss_currentRelation;

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * Degraded mode: one plain, AM-agnostic scan of the whole relation
		 * (no heap_setscanlimits - the AM may not be heap).
		 */
		if (s->whole_rel_mode)
		{
			if (s->heap_scan == NULL)
			{
				if (s->whole_rel_done)
					return NULL;
				s->heap_scan = table_beginscan(rel, s->snapshot, 0, NULL, 0);
			}
			if (table_scan_getnextslot(s->heap_scan, ForwardScanDirection,
									   s->heap_slot))
				return ExecCopySlot(s->css.ss.ss_ScanTupleSlot, s->heap_slot);
			table_endscan(s->heap_scan);
			s->heap_scan = NULL;
			s->whole_rel_done = true;
			return NULL;
		}

		if (s->cur_block != NULL)
		{
			while (s->cur_row < s->cur_block->nrows)
			{
				if (s->nskipquals > 0 && !dbbc_row_passes(s, s->cur_row))
				{
					s->rows_filtered++;
					s->cur_row++;
					continue;
				}
				return dbbc_emit_row(s);
			}
			s->cur_block = NULL;
			s->cur_slot++;
			continue;
		}

		if (s->heap_scan != NULL)
		{
			if (table_scan_getnextslot(s->heap_scan, ForwardScanDirection,
									   s->heap_slot))
			{
				/*
				 * Return every tuple in the node's own (virtual) scan slot:
				 * quals/projection were compiled against that slot type, so
				 * handing them the table AM's buffer-heap slot would trip
				 * the executor's slot-type specialization. The copy
				 * materializes, so it does not depend on the buffer pin.
				 */
				return ExecCopySlot(s->css.ss.ss_ScanTupleSlot, s->heap_slot);
			}
			table_endscan(s->heap_scan);
			s->heap_scan = NULL;
			s->cur_slot++;
			continue;
		}

		if (s->cur_slot >= s->total_slots)
			return NULL;

		/* decide how to serve the next range */
		{
			DbbcBlock  *block = dbbc_slot_block(s, s->cur_slot);

			if (block != NULL && dbbc_block_valid(s, rel, block))
			{
				DbbcColumnChunk *chunks = (DbbcColumnChunk *)
					dsa_get_address(s->dsa, block->chunks);
				int			c;

				/*
				 * Zone-map skip: only after the validity proof (a stale
				 * block's zone maps prove nothing). A valid block contains
				 * every row of its range, so proving no row can match the
				 * quals disposes of the whole range.
				 */
				if (s->nskipquals > 0 &&
					dbbc_zone_block_skippable(s->skipquals, s->nskipquals,
											  block, chunks, s->ncols))
				{
					s->blocks_skipped++;
					s->cur_slot++;
					continue;
				}

				s->cur_block = block;
				s->cur_chunks = chunks;
				for (c = 0; c < s->ncols; c++)
				{
					DbbcColumnChunk *chunk = &s->cur_chunks[c];

					s->col_ptrs[c].values = (uint8 *)
						dsa_get_address(s->dsa, chunk->values);
					s->col_ptrs[c].blob =
						DsaPointerIsValid(chunk->varblob) ? (uint8 *)
						dsa_get_address(s->dsa, chunk->varblob) : NULL;
					s->col_ptrs[c].nulls =
						DsaPointerIsValid(chunk->nulls) ? (uint8 *)
						dsa_get_address(s->dsa, chunk->nulls) : NULL;
				}
				s->cur_row = 0;
				s->blocks_columnar++;
			}
			else
			{
				BlockNumber start = s->cur_slot * DBBC_PAGES_PER_BLOCK;
				BlockNumber nblocks = Min((BlockNumber) DBBC_PAGES_PER_BLOCK,
										  s->heap_nblocks - start);

				s->heap_scan = table_beginscan_strat(rel, s->snapshot,
													 0, NULL, true, false);
				heap_setscanlimits(s->heap_scan, start, nblocks);
				s->ranges_heap++;
			}
		}
	}
}

/*
 * EPQ recheck: the recheck tuple is a real heap tuple and our only quals are
 * plan.qual, which ExecScan re-evaluates itself.
 */
static bool
dbbc_recheck(ScanState *ss, TupleTableSlot *slot)
{
	return true;
}

static TupleTableSlot *
dbbc_exec_scan(CustomScanState *node)
{
	return ExecScan(&node->ss, dbbc_next, dbbc_recheck);
}

static void
dbbc_end_scan(CustomScanState *node)
{
	DbbcScanState *s = (DbbcScanState *) node;

	if (s->heap_scan != NULL)
	{
		table_endscan(s->heap_scan);
		s->heap_scan = NULL;
	}
	if (s->version != NULL)
	{
		dbbc_version_unpin_tracked(s->version);
		s->version = NULL;
	}
}

static void
dbbc_rescan_scan(CustomScanState *node)
{
	DbbcScanState *s = (DbbcScanState *) node;

	if (s->heap_scan != NULL)
	{
		table_endscan(s->heap_scan);
		s->heap_scan = NULL;
	}
	s->cur_block = NULL;
	s->cur_slot = 0;
	s->whole_rel_done = false;

	ExecScanReScan(&node->ss);
}

static void
dbbc_explain_scan(CustomScanState *node, List *ancestors, ExplainState *es)
{
	DbbcScanState *s = (DbbcScanState *) node;

	if (es->analyze)
	{
		ExplainPropertyInteger("Columnar Blocks Served", NULL,
							   (int64) s->blocks_columnar, es);
		ExplainPropertyInteger("Columnar Blocks Skipped", NULL,
							   (int64) s->blocks_skipped, es);
		ExplainPropertyInteger("Heap Fallback Ranges", NULL,
							   (int64) s->ranges_heap, es);
		ExplainPropertyInteger("Columnar Rows", NULL,
							   (int64) s->rows_columnar, es);
		ExplainPropertyInteger("Rows Removed by Columnar Filter", NULL,
							   (int64) s->rows_filtered, es);
	}
}

/*-------------------------------------------------------------------------
 * Scalar aggregate pushdown (Milestone 2 step 4a)
 *
 * At UPPERREL_GROUP_AGG, for a query of the exact shape
 *     SELECT count(*) [, count(col) ...] FROM t
 * (scalar - no GROUP BY, no HAVING, no WHERE, no DISTINCT/FILTER/ORDER in the
 * aggregate) over a single columnar-eligible heap relation, we add a custom
 * path that answers entirely from block metadata: a valid block contributes
 * block->nrows to COUNT(*) and block->nrows - chunk->null_count to COUNT(col),
 * with no value reads at all. Invalid / unbuilt / grown ranges are counted
 * from the heap with the query snapshot. Anything outside this shape adds no
 * path, so the normal Agg-over-columnar-scan plan runs unchanged.
 *
 * Filters and SUM/MIN/MAX/AVG (transition functions + full qual evaluation)
 * are step 4b; this step is deliberately just counting, to prove the
 * upper-path plumbing with the smallest possible correctness surface.
 *-------------------------------------------------------------------------
 */

typedef enum DbbcAggKind
{
	DBBC_AGG_COUNT_STAR,
	DBBC_AGG_COUNT_COL,
} DbbcAggKind;

typedef struct DbbcAggItem
{
	DbbcAggKind kind;
	AttrNumber	attno;			/* COUNT_COL: the counted column */
} DbbcAggItem;

/*
 * custom_private layout (all copyObject-safe Node lists):
 *   linitial : Integer holding the base relation OID (absolute; an RT index
 *              would be wrong - custom_private is opaque to setrefs, so it
 *              never receives the rtoffset applied to subquery/CTE/view
 *              range tables). Bit-preserved through int; read back with (Oid).
 *   lsecond  : List of {Integer kind, Integer attno} per output column
 *   lthird   : physical output tlist (List of TargetEntry)
 */
#define DBBC_AGG_PRIV_RELOID(cp)	((Oid) intVal(linitial(cp)))
#define DBBC_AGG_PRIV_ITEMS(cp)		((List *) lsecond(cp))
#define DBBC_AGG_PRIV_TLIST(cp)		((List *) lthird(cp))

typedef struct DbbcAggScanState
{
	CustomScanState css;
	bool		executed;		/* emitted the single result row yet? */
	int			naggs;
	DbbcAggItem *aggs;
	int64	   *counts;			/* per-agg accumulator */
	Relation	rel;
	bool		rel_opened;
} DbbcAggScanState;

static Node *dbbc_agg_create_scan_state(CustomScan *cscan);
static void dbbc_agg_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *dbbc_agg_exec(CustomScanState *node);
static void dbbc_agg_end(CustomScanState *node);
static void dbbc_agg_rescan(CustomScanState *node);
static void dbbc_agg_explain(CustomScanState *node, List *ancestors,
							 ExplainState *es);

static const CustomScanMethods dbbc_agg_scan_methods = {
	.CustomName = "DBBlueColumnarAgg",
	.CreateCustomScanState = dbbc_agg_create_scan_state,
};

static const CustomExecMethods dbbc_agg_exec_methods = {
	.CustomName = "DBBlueColumnarAgg",
	.BeginCustomScan = dbbc_agg_begin,
	.ExecCustomScan = dbbc_agg_exec,
	.EndCustomScan = dbbc_agg_end,
	.ReScanCustomScan = dbbc_agg_rescan,
	.ExplainCustomScan = dbbc_agg_explain,
};

static Plan *dbbc_agg_plan_custom_path(PlannerInfo *root, RelOptInfo *rel,
									   CustomPath *best_path, List *tlist,
									   List *clauses, List *custom_plans);

static const CustomPathMethods dbbc_agg_path_methods = {
	.CustomName = "DBBlueColumnarAgg",
	.PlanCustomPath = dbbc_agg_plan_custom_path,
};

static create_upper_paths_hook_type prev_create_upper_paths_hook = NULL;

/*
 * Classify one aggregated-target expression. Returns true and fills *item if
 * it is a bare COUNT(*) or COUNT(col) with no DISTINCT/FILTER/ORDER; the
 * counted column (if any) is added to *needcols.
 */
static bool
dbbc_agg_classify(Node *expr, Index relid, DbbcAggItem *item, List **needcols)
{
	Aggref	   *agg;

	if (!IsA(expr, Aggref))
		return false;
	agg = (Aggref *) expr;

	if (agg->aggdistinct != NIL || agg->aggfilter != NULL ||
		agg->aggorder != NIL || agg->aggkind != AGGKIND_NORMAL ||
		agg->aggsplit != AGGSPLIT_SIMPLE)
		return false;

	if (agg->aggfnoid == F_COUNT_)
	{
		/* count(*) */
		item->kind = DBBC_AGG_COUNT_STAR;
		item->attno = InvalidAttrNumber;
		return true;
	}

	if (agg->aggfnoid == F_COUNT_ANY && list_length(agg->args) == 1)
	{
		TargetEntry *tle = (TargetEntry *) linitial(agg->args);
		Node	   *arg = (Node *) tle->expr;

		while (IsA(arg, RelabelType))
			arg = (Node *) ((RelabelType *) arg)->arg;
		if (IsA(arg, Var))
		{
			Var		   *var = (Var *) arg;

			if (var->varno == relid && var->varlevelsup == 0 &&
				var->varattno > 0)
			{
				item->kind = DBBC_AGG_COUNT_COL;
				item->attno = var->varattno;
				*needcols = list_append_unique_int(*needcols, var->varattno);
				return true;
			}
		}
	}

	return false;
}

static void
dbbc_create_upper_paths(PlannerInfo *root, UpperRelationKind stage,
						RelOptInfo *input_rel, RelOptInfo *output_rel,
						void *extra)
{
	Query	   *parse = root->parse;
	GroupPathExtraData *gextra = (GroupPathExtraData *) extra;
	RelOptInfo *base_rel;
	RangeTblEntry *rte;
	int			relid;
	List	   *tlist;
	List	   *items = NIL;
	List	   *needcols = NIL;
	DbbcRelVersion *version;
	ListCell   *lc;
	CustomPath *cpath;
	int16	   *reg;
	bool		ok = true;

	if (prev_create_upper_paths_hook)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);

	if (stage != UPPERREL_GROUP_AGG)
		return;
	if (!dbblue_columnar_enabled || !dbblue_columnar_enable_columnar_scan)
		return;

	/* scalar aggregation only: no GROUP BY, no grouping sets, no HAVING */
	if (parse->groupClause != NIL || parse->groupingSets != NIL ||
		!parse->hasAggs || parse->havingQual != NULL ||
		(gextra != NULL && gextra->havingQual != NULL))
		return;

	/* 4a: no WHERE - counting with filters is step 4b */
	if (input_rel->baserestrictinfo != NIL)
		return;

	/* single plain heap base relation */
	if (bms_membership(input_rel->relids) != BMS_SINGLETON)
		return;
	relid = bms_singleton_member(input_rel->relids);
	if (relid <= 0 || relid > root->simple_rel_array_size)
		return;
	base_rel = root->simple_rel_array[relid];
	rte = root->simple_rte_array[relid];
	if (base_rel == NULL || rte->rtekind != RTE_RELATION)
		return;
	if (rte->relkind != RELKIND_RELATION && rte->relkind != RELKIND_MATVIEW)
		return;
	if (get_rel_relam(rte->relid) != HEAP_TABLE_AM_OID)
		return;

	/* every aggregated-target expr must be a bare COUNT(*) / COUNT(col) */
	foreach(lc, output_rel->reltarget->exprs)
	{
		DbbcAggItem item;

		if (!dbbc_agg_classify((Node *) lfirst(lc), relid, &item, &needcols))
			return;
		items = lappend(items, list_make2(makeInteger((int) item.kind),
										  makeInteger((int) item.attno)));
	}
	if (items == NIL)
		return;

	/* a populated version must cover every counted column */
	version = dbbc_version_pin(rte->relid);
	if (version == NULL)
		return;
	if (!DsaPointerIsValid(version->blockdir) || version->nblocks == 0)
	{
		dbbc_version_unpin(version);
		return;
	}
	reg = (int16 *) dsa_get_address(dbbc_store_dsa(), version->attnums);
	foreach(lc, needcols)
	{
		int			attno = lfirst_int(lc);
		bool		found = false;
		int			c;

		for (c = 0; c < version->ncols; c++)
			if (reg[c] == attno)
			{
				found = true;
				break;
			}
		if (!found)
		{
			ok = false;
			break;
		}
	}
	dbbc_version_unpin(version);
	if (!ok)
		return;

	/* physical output tlist = the aggregated target exprs themselves */
	tlist = make_tlist_from_pathtarget(output_rel->reltarget);
	apply_pathtarget_labeling_to_tlist(tlist, output_rel->reltarget);

	cpath = makeNode(CustomPath);
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = output_rel;
	cpath->path.pathtarget = output_rel->reltarget;
	cpath->path.param_info = NULL;
	cpath->path.parallel_aware = false;
	cpath->path.parallel_safe = false;
	cpath->path.parallel_workers = 0;
	cpath->path.rows = 1;
	cpath->path.pathkeys = NIL;
	cpath->flags = 0;
	cpath->custom_paths = NIL;
	/* store the absolute relation OID (not the RT index; see macro comment) */
	cpath->custom_private = list_make3(makeInteger((int) rte->relid),
									   items, tlist);
	cpath->methods = &dbbc_agg_path_methods;

	/*
	 * Metadata-only: near-zero cost so it wins for count() over a populated
	 * relation (the normal Agg-over-scan path remains as the fallback).
	 */
	cpath->path.startup_cost = 0.0;
	cpath->path.total_cost = 1.0 + cpu_operator_cost * base_rel->pages;
	cpath->path.disabled_nodes = 0;

	add_path(output_rel, (Path *) cpath);
}

static Plan *
dbbc_agg_plan_custom_path(PlannerInfo *root, RelOptInfo *rel,
						  CustomPath *best_path, List *tlist,
						  List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	List	   *phys_tlist = (List *) lthird(best_path->custom_private);

	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = NIL;
	cscan->scan.scanrelid = 0;	/* upper node: not a base-rel scan */
	cscan->flags = best_path->flags;
	cscan->custom_scan_tlist = phys_tlist;	/* describes emitted columns */
	cscan->custom_private = best_path->custom_private;
	cscan->methods = &dbbc_agg_scan_methods;

	return &cscan->scan.plan;
}

static Node *
dbbc_agg_create_scan_state(CustomScan *cscan)
{
	DbbcAggScanState *as = (DbbcAggScanState *) palloc0(sizeof(DbbcAggScanState));

	NodeSetTag(as, T_CustomScanState);
	as->css.methods = &dbbc_agg_exec_methods;
	return (Node *) as;
}

static void
dbbc_agg_begin(CustomScanState *node, EState *estate, int eflags)
{
	DbbcAggScanState *as = (DbbcAggScanState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	List	   *cp = cscan->custom_private;
	List	   *items = DBBC_AGG_PRIV_ITEMS(cp);
	Oid			reloid = DBBC_AGG_PRIV_RELOID(cp);
	ListCell   *lc;
	int			i;

	as->naggs = list_length(items);
	as->aggs = (DbbcAggItem *) palloc(as->naggs * sizeof(DbbcAggItem));
	as->counts = (int64 *) palloc0(as->naggs * sizeof(int64));
	i = 0;
	foreach(lc, items)
	{
		List	   *it = (List *) lfirst(lc);

		as->aggs[i].kind = (DbbcAggKind) intVal(linitial(it));
		as->aggs[i].attno = (AttrNumber) intVal(lsecond(it));
		i++;
	}
	as->executed = false;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* the executor already holds the range-table lock on this relation */
	as->rel = table_open(reloid, NoLock);
	as->rel_opened = true;
}

/* count a heap page range [start, start+nblocks) with the query snapshot */
static void
dbbc_agg_count_heap_range(DbbcAggScanState *as, DbbcScanState *s,
						  BlockNumber start, BlockNumber nblocks)
{
	TableScanDesc scan;

	scan = table_beginscan_strat(as->rel, s->snapshot, 0, NULL, true, false);
	heap_setscanlimits(scan, start, nblocks);
	while (table_scan_getnextslot(scan, ForwardScanDirection, s->heap_slot))
	{
		int			a;

		for (a = 0; a < as->naggs; a++)
		{
			if (as->aggs[a].kind == DBBC_AGG_COUNT_STAR)
				as->counts[a]++;
			else
			{
				bool		isnull;

				(void) slot_getattr(s->heap_slot, as->aggs[a].attno, &isnull);
				if (!isnull)
					as->counts[a]++;
			}
		}
	}
	table_endscan(scan);
}

static TupleTableSlot *
dbbc_agg_exec(CustomScanState *node)
{
	DbbcAggScanState *as = (DbbcAggScanState *) node;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	DbbcScanState scratch;
	uint32		cur;
	int			a;

	List	   *needed = NIL;

	if (as->executed)
		return NULL;
	as->executed = true;

	/*
	 * Counted columns must be covered by the current version, else metadata
	 * null_count is unavailable; pass them so a stale plan (version changed
	 * since planning) degrades to whole-relation heap counting.
	 */
	for (a = 0; a < as->naggs; a++)
		if (as->aggs[a].kind == DBBC_AGG_COUNT_COL)
			needed = list_append_unique_int(needed, as->aggs[a].attno);

	/* reuse the standard scan setup for version pin, dir, heap slot, etc. */
	memset(&scratch, 0, sizeof(scratch));
	scratch.css.ss.ss_currentRelation = as->rel;
	dbbc_scan_setup(&scratch, as->rel, node->ss.ps.state, needed, NIL, 0);

	if (scratch.whole_rel_mode)
	{
		/* no usable version (stale plan): count the whole relation */
		dbbc_agg_count_heap_range(as, &scratch, 0, scratch.heap_nblocks);
	}
	else
	{
		for (cur = 0; cur < scratch.total_slots; cur++)
		{
			DbbcBlock  *block = dbbc_slot_block(&scratch, cur);

			CHECK_FOR_INTERRUPTS();

			if (block != NULL && dbbc_block_valid(&scratch, as->rel, block))
			{
				DbbcColumnChunk *chunks = (DbbcColumnChunk *)
					dsa_get_address(scratch.dsa, block->chunks);

				for (a = 0; a < as->naggs; a++)
				{
					if (as->aggs[a].kind == DBBC_AGG_COUNT_STAR)
						as->counts[a] += block->nrows;
					else
					{
						int			c;

						for (c = 0; c < scratch.ncols; c++)
							if (chunks[c].attnum == as->aggs[a].attno)
							{
								as->counts[a] += block->nrows -
									chunks[c].null_count;
								break;
							}
					}
				}
			}
			else
			{
				BlockNumber start = cur * DBBC_PAGES_PER_BLOCK;
				BlockNumber nblk = Min((BlockNumber) DBBC_PAGES_PER_BLOCK,
									   scratch.heap_nblocks - start);

				dbbc_agg_count_heap_range(as, &scratch, start, nblk);
			}
		}
	}

	if (scratch.version != NULL)
		dbbc_version_unpin_tracked(scratch.version);

	/* emit one row: the counts, in output order */
	ExecClearTuple(slot);
	for (a = 0; a < as->naggs; a++)
	{
		slot->tts_values[a] = Int64GetDatum(as->counts[a]);
		slot->tts_isnull[a] = false;
	}
	ExecStoreVirtualTuple(slot);
	return slot;
}

static void
dbbc_agg_end(CustomScanState *node)
{
	DbbcAggScanState *as = (DbbcAggScanState *) node;

	if (as->rel_opened)
	{
		table_close(as->rel, NoLock);
		as->rel_opened = false;
	}
}

static void
dbbc_agg_rescan(CustomScanState *node)
{
	DbbcAggScanState *as = (DbbcAggScanState *) node;

	as->executed = false;
	if (as->counts)
		memset(as->counts, 0, as->naggs * sizeof(int64));
}

static void
dbbc_agg_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	ExplainPropertyText("Columnar Aggregate", "count (metadata)", es);
}

static void
dbbc_agg_init(void)
{
	RegisterCustomScanMethods(&dbbc_agg_scan_methods);
	prev_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = dbbc_create_upper_paths;
}
