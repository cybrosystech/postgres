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

#include "access/cmptype.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/stratnum.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/tupmacs.h"
#include "access/visibilitymap.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_aggregate_d.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type_d.h"
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
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/predicate.h"
#include "storage/shm_toc.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/expandeddatum.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "lib/stringinfo.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "dbblue_columnar.h"

/* saved hook */
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;

/* executor state */
typedef struct DbbcColPtrs
{
	uint8	   *values;			/* PLAIN: nrows values; DICT: dict entries */
	uint8	   *blob;			/* varlena blob, or NULL */
	uint8	   *nulls;			/* null bitmap, or NULL */
	uint8	   *codes;			/* DICT: nrows codes (code_width bytes); else NULL */
	int16		attlen;			/* hoisted from the chunk: read per value */
	bool		attbyval;		/* hoisted from the chunk: read per value */
	bool		is_dict;		/* DBBC_ENCODING_DICT: translate row -> code */
	uint8		code_width;		/* DICT: 1 or 2 bytes per code */
	uint32		dict_count;		/* DICT: number of dictionary entries */
	int			attidx;			/* attnum - 1: the slot index to write */
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

	/*
	 * Vectorized fast path (Step 4): set only for OP/SAOP over an
	 * integer-family column whose comparand(s) also widen losslessly to int64
	 * (native signed-integer order == btree order for these types). When set,
	 * dbbc_compute_selection compares as int64 inline, skipping the per-row
	 * fmgr call in cmp. Everything else (text/numeric/cross-type) falls back
	 * to cmp, so the fast path can only ever be an optimization, never wrong.
	 */
	bool		fast_int;
	int16		col_ikind;		/* how to widen the COLUMN datum: 2 | 4 | 8 */
	int64		fast_ival;		/* OP: the Const, widened to int64 */
	int64	   *fast_ielems;	/* SAOP: elems widened to int64 (nelems) */
} DbbcSkipQual;

/*
 * Shared state for a parallel columnar scan, in the scan's DSM segment. All
 * participants (leader + workers) claim block-directory slots from one atomic
 * cursor, and pin the SAME immutable store version (identified by version_dp,
 * published by the leader) so a concurrent repopulate cannot split them.
 * heap_nblocks/total_slots are the leader's, shared so the per-slot range math
 * and the "block covers the range's full extent" validity check agree exactly.
 */
typedef struct DbbcParallelState
{
	pg_atomic_uint32 next_slot;
	dsa_pointer version_dp;		/* leader's pinned version, or Invalid */
	BlockNumber heap_nblocks;
	uint32		total_slots;
} DbbcParallelState;

typedef struct DbbcScanState
{
	CustomScanState css;

	bool		initialized;	/* false under EXPLAIN (no ANALYZE) */
	Snapshot	snapshot;
	List	   *needed_attnos;	/* from custom_private */
	bool		prefilter_unsafe;	/* RLS/security qual present: skip the
									 * pre-filter + zone-skip, leave all quals
									 * to ExecScan's security-ordered ExecQual */
	DbbcParallelState *pstate;	/* NULL unless parallel-aware execution */

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

	/*
	 * Per-block SoA decode buffer (Step 3) + precomputed selection (Step 4).
	 * Only the columns referenced by a skip qual are decoded (decode_col[c]);
	 * payload columns stay on-demand in dbbc_emit_row. dbbc_compute_selection
	 * fills sel[] once per block from the decoded arrays, so the row loop is an
	 * O(1) lookup instead of a per-row qual evaluation. Scan-serve path only.
	 */
	Datum	   *dec_val;		/* [ncols * decode_cap]; valid where decode_col[c] */
	bool	   *dec_null;		/* [ncols * decode_cap] */
	bool	   *decode_col;		/* [ncols]: is col c referenced by a skip qual? */
	uint32		decode_cap;		/* rows allocated per column in dec_val/dec_null */
	bool	   *sel;			/* [decode_cap]: row passes prefilter (nskipquals>0) */

	/* heap fallback */
	TableScanDesc heap_scan;
	TupleTableSlot *heap_slot;	/* table-AM slot (scan slot is virtual) */

	/* pinned VM buffer for the M6 fast validity proof (released at End) */
	Buffer		vm_check_buf;

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
	uint64		blocks_vm_validated;	/* validity proven from the VM alone */
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

static Size dbbc_scan_estimate_dsm(CustomScanState *node, ParallelContext *pcxt);
static void dbbc_scan_initialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *coordinate);
static void dbbc_scan_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *coordinate);
static void dbbc_scan_initialize_worker(CustomScanState *node, shm_toc *toc, void *coordinate);

static const CustomExecMethods dbbc_exec_methods = {
	.CustomName = "DBBlueColumnarScan",
	.BeginCustomScan = dbbc_begin_scan,
	.ExecCustomScan = dbbc_exec_scan,
	.EndCustomScan = dbbc_end_scan,
	.ReScanCustomScan = dbbc_rescan_scan,
	.EstimateDSMCustomScan = dbbc_scan_estimate_dsm,
	.InitializeDSMCustomScan = dbbc_scan_initialize_dsm,
	.ReInitializeDSMCustomScan = dbbc_scan_reinitialize_dsm,
	.InitializeWorkerCustomScan = dbbc_scan_initialize_worker,
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
 * Byte width to widen a fast-path integer type's Datum to int64 (native signed
 * order == btree order for these), or 0 if the type is not fast-path eligible.
 */
static inline int16
dbbc_int_kind(Oid typid)
{
	switch (typid)
	{
		case INT2OID:
			return 2;
		case INT4OID:
		case DATEOID:
			return 4;
		case INT8OID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return 8;
	}
	return 0;
}

/*
 * Comparison "domain" of a fast-path type. Two operands may be compared as raw
 * int64 only when their domains match: int2/int4/int8 share one domain (raw
 * integers, cross-comparable by value), but date (days), timestamp and
 * timestamptz (microseconds) are each their own domain - comparing a date's
 * day count against a timestamp's microsecond count as int64 would be wrong,
 * even though they share a btree opfamily. 0 = not fast-path eligible.
 */
static inline int
dbbc_int_domain(Oid typid)
{
	switch (typid)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
			return 1;
		case DATEOID:
			return 2;
		case TIMESTAMPOID:
			return 3;
		case TIMESTAMPTZOID:
			return 4;
	}
	return 0;
}

/* Widen an int-family Datum of type typid to int64; false if not eligible. */
static inline bool
dbbc_datum_to_int64(Datum d, Oid typid, int64 *out)
{
	switch (dbbc_int_kind(typid))
	{
		case 2:
			*out = (int64) DatumGetInt16(d);
			return true;
		case 4:
			*out = (int64) DatumGetInt32(d);
			return true;
		case 8:
			*out = (int64) DatumGetInt64(d);
			return true;
	}
	return false;
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

		/* vectorized fast path when column and comparand share an int domain */
		if (dbbc_int_domain(q->coltype) != 0 &&
			dbbc_int_domain(q->coltype) == dbbc_int_domain(rtype) &&
			dbbc_datum_to_int64(cst->constvalue, rtype, &q->fast_ival))
		{
			q->fast_int = true;
			q->col_ikind = dbbc_int_kind(q->coltype);
		}
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

		/* vectorized fast path: widen all elements to int64 (same domain) */
		if (dbbc_int_domain(q->coltype) != 0 &&
			dbbc_int_domain(q->coltype) == dbbc_int_domain(elemtype))
		{
			int64	   *iv = (int64 *) palloc(Max(nkeep, 1) * sizeof(int64));
			bool		allok = true;

			for (i = 0; i < nkeep; i++)
			{
				if (!dbbc_datum_to_int64(elems[i], elemtype, &iv[i]))
				{
					allok = false;
					break;
				}
			}
			if (allok)
			{
				q->fast_int = true;
				q->col_ikind = dbbc_int_kind(q->coltype);
				q->fast_ielems = iv;
			}
			else
				pfree(iv);
		}
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
 * with the query's extractable quals, and how much of the heap the M6
 * VM-fork validity proof will spare from being read at all. One sampled walk
 * fills both:
 *   *skip_frac - estimated fraction of blocks zone-map-skipped (CPU saving),
 *   *vm_frac   - estimated fraction of the heap whose blocks validate from
 *                the VM alone (IO saving: those heap pages are never read).
 * Both scaled by build coverage and clamped conservatively. rel is the
 * already-locked relation (the planner holds a lock; we only read its VM).
 */
static void
dbbc_estimate_serve_fractions(Relation rel, List *clauses, Index varno,
							  double *skip_frac, double *vm_frac)
{
	DbbcSkipQual *quals;
	int			nquals;
	DbbcRelVersion *version;
	dsa_pointer *dir;
	uint32		step;
	uint32		slot;
	uint32		seen = 0;
	uint32		skipped = 0;
	uint32		vmvalid = 0;
	double		coverage;
	Buffer		vmbuf = InvalidBuffer;

	*skip_frac = 0.0;
	*vm_frac = 0.0;

	nquals = dbbc_extract_skip_quals(clauses, varno, &quals);

	version = dbbc_version_pin_tracked(RelationGetRelid(rel));
	if (version == NULL)
		return;
	if (!DsaPointerIsValid(version->blockdir) || version->ndirslots == 0)
	{
		dbbc_version_unpin_tracked(version);
		return;
	}

	dir = (dsa_pointer *) dsa_get_address(dbbc_store_dsa(),
										  version->blockdir);
	/*
	 * Bounded sample. This is a plan-time COST estimate, so the VM probe is
	 * done lock-free (unlike the authoritative dbbc_block_vm_valid, which
	 * SHARE-locks) - a racy bit/LSN read only mis-estimates a fraction, never
	 * affects correctness, and avoids taking up to 256 buffer content locks
	 * on every plan of a query against a registered table.
	 */
	step = Max(1, version->ndirslots / 256);
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
		if (nquals > 0 &&
			dbbc_zone_block_skippable(quals, nquals, block, chunks,
									  version->ncols))
			skipped++;
		/* lock-free approximation of the VM fast-path eligibility */
		if (!XLogRecPtrIsInvalid(block->vm_lsn))
		{
			bool		av = true;
			uint16		p;

			for (p = 0; p < block->npages; p++)
				if (!(visibilitymap_get_status(rel, block->first_page + p,
											   &vmbuf) & VISIBILITYMAP_ALL_VISIBLE))
				{
					av = false;
					break;
				}
			if (av && BufferIsValid(vmbuf) &&
				BufferGetLSNAtomic(vmbuf) == block->vm_lsn)
				vmvalid++;
		}
	}
	if (BufferIsValid(vmbuf))
		ReleaseBuffer(vmbuf);

	/* scale by how much of the relation is columnarized at all */
	coverage = (double) version->nblocks / Max(version->ndirslots, 1);
	if (seen > 0)
	{
		*skip_frac = Min(((double) skipped / seen) * coverage, 0.95);
		*vm_frac = Min(((double) vmvalid / seen) * coverage, 0.98);
	}

	dbbc_version_unpin_tracked(version);
}

/*
 * Relids for which this backend has already logged a coverage miss. Kept in
 * TopMemoryContext so it survives per-query contexts. Bounds the coverage-miss
 * LOG to at most once per relation per backend, so a partially-covered hot
 * table queried at high volume cannot flood the server log (one line per plan).
 */
static List *dbbc_logged_coverage_misses = NIL;

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
	List	   *missing = NIL;
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
	 * Inheritance/partition parent (rte->inh): this baserel stands for an
	 * Append over children; a columnar scan of the parent's own heap would
	 * drop the child rows. Leave it to the normal Append. (Children are
	 * RELOPT_OTHER_MEMBER_REL, already excluded above; per-partition columnar
	 * acceleration is future work.)
	 */
	if (rte->inh)
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
			missing = lappend_int(missing, attno);
		}
	}
	dbbc_version_unpin_tracked(version);

	if (!ok)
	{
		/*
		 * Turn the otherwise-silent heap fallback into an operator-visible
		 * signal: name the unregistered columns that disqualified the scan so
		 * they can be registered. LOG (not WARNING) keeps it out of the client
		 * stream; the GUC lets operators silence it once coverage is complete.
		 * Logged at most once per relation per backend (dedup set below) so a
		 * partially-covered hot table cannot flood the log with one line/plan.
		 */
		if (dbblue_columnar_log_coverage_misses && missing != NIL &&
			!list_member_oid(dbbc_logged_coverage_misses, rte->relid))
		{
			StringInfoData buf;
			MemoryContext oldcxt;

			initStringInfo(&buf);
			foreach(lc, missing)
				appendStringInfo(&buf, "%s%s", buf.len ? ", " : "",
								 get_attname(rte->relid, lfirst_int(lc), false));
			ereport(LOG,
					(errmsg("dbblue_columnar: \"%s\" scan fell back to heap - unregistered column(s): %s",
							get_rel_name(rte->relid), buf.data),
					 errhint("Register them with dbblue_columnar_add('%s', ARRAY[...]) then repopulate.",
							 get_rel_name(rte->relid))));
			pfree(buf.data);

			/* remember it so we do not log this relation again this backend */
			oldcxt = MemoryContextSwitchTo(TopMemoryContext);
			dbbc_logged_coverage_misses =
				lappend_oid(dbbc_logged_coverage_misses, rte->relid);
			MemoryContextSwitchTo(oldcxt);
		}
		return false;
	}

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
	double		vm_frac;
	CustomPath *cpath;
	QualCost	qcost;
	bool		prefilter_unsafe = false;

	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);

	if (!dbblue_columnar_enabled || !dbblue_columnar_enable_columnar_scan)
		return;

	if (!dbbc_rel_ready(root, rel, rte, &needed))
		return;

	/*
	 * If any qual carries a non-zero security_level (RLS policy,
	 * security-barrier view), the pre-filter and zone-map skip must be
	 * disabled: they would evaluate an extractable, possibly-leaky higher-
	 * security qual on rows a lower-security qual excludes, ahead of
	 * ExecScan's security-ordered ExecQual (order_qual_clauses). The columnar
	 * scan still serves blocks (no deform); ExecScan applies ALL quals in the
	 * correct order, so results stay correct and nothing leaks.
	 */
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc);

		if (ri->security_level > 0)
			prefilter_unsafe = true;
		clauses = lappend(clauses, ri->clause);
	}

	/*
	 * Estimate zone-map skipping (CPU saving) and VM-provable validity (IO
	 * saving) against the real block metadata. The planner already holds a
	 * lock on the relation; we open it only to read its VM fork. Under a
	 * security qual the pre-filter/zone-skip are disabled (NIL clauses), but
	 * the VM IO discount still applies - it's qual-independent.
	 */
	{
		Relation	prel = table_open(rte->relid, NoLock);

		dbbc_estimate_serve_fractions(prel,
									  prefilter_unsafe ? NIL : clauses,
									  rel->relid, &skip_frac, &vm_frac);
		table_close(prel, NoLock);
	}

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
	cpath->custom_private = list_make2(needed,
									   makeInteger(prefilter_unsafe ? 1 : 0));
	cpath->methods = &dbbc_path_methods;

	/*
	 * Costing. IO: blocks the VM-fork proof validates (vm_frac) are served
	 * with NO heap page reads - only the remaining fraction pays seq IO,
	 * plus a tiny per-block VM-check term. CPU: no tuple deforming on
	 * columnar-served rows, and zone-map-skipped blocks contribute no
	 * per-tuple work at all, so the CPU terms scale by (1 - skip_frac).
	 */
	cost_qual_eval(&qcost, rel->baserestrictinfo, root);
	cpath->path.disabled_nodes = 0;
	cpath->path.startup_cost = qcost.startup;
	cpath->path.total_cost = qcost.startup +
		seq_page_cost * rel->pages * (1.0 - vm_frac) +
		cpu_operator_cost * (rel->pages / DBBC_PAGES_PER_BLOCK + 1) * 2.0 +
		(cpu_tuple_cost * 0.75 + qcost.per_tuple) * rel->tuples *
		(1.0 - skip_frac);

	add_path(rel, (Path *) cpath);

	/*
	 * Parallel variant: workers claim block-directory slots from a shared
	 * atomic cursor, each doing the same validity + zone-skip + no-deform
	 * emit (or per-slot heap fallback) over its slice, combined under Gather.
	 * This lets columnar keep its per-tuple CPU edge AND parallelize, so it
	 * competes with (and beats) a Parallel Seq Scan on scan-heavy aggregates.
	 */
	if (rel->consider_parallel && bms_is_empty(rel->lateral_relids))
	{
		int			workers = compute_parallel_worker(rel, rel->pages, -1,
													  max_parallel_workers_per_gather);

		if (workers > 0)
		{
			CustomPath *ppath = makeNode(CustomPath);
			double		divisor = (double) workers;

			ppath->path.pathtype = T_CustomScan;
			ppath->path.parent = rel;
			ppath->path.pathtarget = rel->reltarget;
			ppath->path.param_info = NULL;
			ppath->path.parallel_aware = true;
			ppath->path.parallel_safe = true;
			ppath->path.parallel_workers = workers;
			ppath->path.rows = clamp_row_est(rel->rows / divisor);
			ppath->path.pathkeys = NIL;
			ppath->flags = 0;
			ppath->custom_private =
				list_make2(needed, makeInteger(prefilter_unsafe ? 1 : 0));
			ppath->methods = &dbbc_path_methods;
			ppath->path.disabled_nodes = 0;
			ppath->path.startup_cost = qcost.startup;
			ppath->path.total_cost = qcost.startup +
				(seq_page_cost * rel->pages * (1.0 - vm_frac) +
				 cpu_operator_cost *
				 (rel->pages / DBBC_PAGES_PER_BLOCK + 1) * 2.0 +
				 (cpu_tuple_cost * 0.75 + qcost.per_tuple) * rel->tuples *
				 (1.0 - skip_frac)) / divisor;

			add_partial_path(rel, (Path *) ppath);
		}
	}
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
dbbc_scan_common(DbbcScanState *s, Relation rel, EState *estate)
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
	s->version = NULL;

	/*
	 * whole_rel_mode is ONLY for a non-heap table AM (possible via ALTER
	 * TABLE ... SET ACCESS METHOD under a cached plan): the block format and
	 * heap_setscanlimits are heap-only, so we fall back to one plain,
	 * AM-generic full scan. A heap relation with no usable version instead
	 * uses the normal per-slot loop with every slot read from the heap
	 * (dbbc_slot_block returns NULL when version is NULL), which parallelizes.
	 */
	s->whole_rel_mode = (rel->rd_rel->relam != HEAP_TABLE_AM_OID);
	s->whole_rel_done = false;

	s->heap_nblocks = RelationGetNumberOfBlocks(rel);
	s->total_slots = (s->heap_nblocks + DBBC_PAGES_PER_BLOCK - 1) /
		DBBC_PAGES_PER_BLOCK;
	s->cur_slot = 0;

	/* heap-fallback rows need the table AM's slot type, not our virtual one */
	s->heap_slot = table_slot_create(rel, &estate->es_tupleTable);
}

/*
 * Bind an already-pinned store version (or NULL) to the scan: re-validate
 * that it still covers the needed columns and, if so, resolve the directory
 * and per-column layout; otherwise unpin it and run heap-only. Separated from
 * dbbc_scan_common so a parallel worker can bind the leader's exact version
 * (attached in InitializeWorkerCustomScan) rather than the current one.
 */
static void
dbbc_scan_bind_version(DbbcScanState *s, Relation rel, List *needed_attnos,
					   List *qual_clauses, Index qual_varno,
					   DbbcRelVersion *version, bool alloc_decode)
{
	bool		usable;

	s->version = version;
	if (version == NULL)
		return;

	usable = DsaPointerIsValid(version->blockdir) && version->nblocks > 0;
	if (usable)
	{
		int16	   *reg = (int16 *) dsa_get_address(s->dsa, version->attnums);
		ListCell   *lc;

		foreach(lc, needed_attnos)
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
				usable = false;
				break;
			}
		}
	}

	if (usable)
	{
		int16	   *reg = (int16 *) dsa_get_address(s->dsa, version->attnums);
		int			natts = RelationGetDescr(rel)->natts;
		int			c;

		s->dir = (dsa_pointer *) dsa_get_address(s->dsa, version->blockdir);
		s->ndirslots = version->ndirslots;
		s->ncols = version->ncols;
		s->attnums = (int16 *) palloc(s->ncols * sizeof(int16));
		memcpy(s->attnums, reg, s->ncols * sizeof(int16));
		s->col_ptrs = (DbbcColPtrs *) palloc(s->ncols * sizeof(DbbcColPtrs));

		s->attno_to_col = (int *) palloc(natts * sizeof(int));
		for (c = 0; c < natts; c++)
			s->attno_to_col[c] = -1;
		for (c = 0; c < s->ncols; c++)
		{
			if (s->attnums[c] >= 1 && s->attnums[c] <= natts)
				s->attno_to_col[s->attnums[c] - 1] = c;
		}

		/* simple quals for zone skipping + the columnar pre-filter */
		s->nskipquals = dbbc_extract_skip_quals(qual_clauses, qual_varno,
												&s->skipquals);

		/*
		 * Per-block SoA decode buffer + selection bitmap. Only the scan-serve
		 * path (dbbc_open_columnar_block -> dbbc_decode_and_select) uses these,
		 * so alloc_decode is false for the grouped-aggregate pushdown path,
		 * whose scratch state filters via dbbc_skipqual_test and would otherwise
		 * pay several MB of dead allocation per execution. Mark the filter
		 * columns (only those are decoded), then size the buffers once to the
		 * max rows a block can hold, in this stable per-scan context (never at
		 * block-open, where CurrentMemoryContext is the per-tuple context).
		 */
		if (alloc_decode && s->nskipquals > 0)
		{
			int			qi;

			s->decode_col = (bool *) palloc0(s->ncols * sizeof(bool));
			for (qi = 0; qi < s->nskipquals; qi++)
			{
				int			qc = s->attno_to_col[s->skipquals[qi].attno - 1];

				if (qc >= 0)
					s->decode_col[qc] = true;
			}
			s->decode_cap = (uint32) MaxHeapTuplesPerPage * DBBC_PAGES_PER_BLOCK;
			s->dec_val = (Datum *)
				palloc((Size) s->ncols * s->decode_cap * sizeof(Datum));
			s->dec_null = (bool *)
				palloc((Size) s->ncols * s->decode_cap * sizeof(bool));
			s->sel = (bool *) palloc(s->decode_cap * sizeof(bool));
		}
		else
		{
			s->decode_col = NULL;
			s->decode_cap = 0;
			s->dec_val = NULL;
			s->dec_null = NULL;
			s->sel = NULL;
		}
	}
	else
	{
		dbbc_version_unpin_tracked(version);
		s->version = NULL;
	}
}

/* claim the next block-directory slot: shared atomic cursor when parallel */
static inline uint32
dbbc_claim_slot(DbbcScanState *s)
{
	if (s->pstate != NULL)
		return pg_atomic_fetch_add_u32(&s->pstate->next_slot, 1);
	return s->cur_slot++;
}

static void
dbbc_begin_scan(CustomScanState *node, EState *estate, int eflags)
{
	DbbcScanState *s = (DbbcScanState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	Relation	rel = node->ss.ss_currentRelation;

	s->needed_attnos = (List *) linitial(cscan->custom_private);
	s->prefilter_unsafe = intVal(lsecond(cscan->custom_private)) != 0;
	s->pstate = NULL;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	dbbc_scan_common(s, rel, estate);

	/*
	 * A parallel worker defers version binding to InitializeWorkerCustomScan
	 * so it pins the SAME version the leader published in DSM (a concurrent
	 * repopulate must not split participants across versions). The leader and
	 * any non-parallel execution bind the current version now.
	 */
	if (!IsParallelWorker())
	{
		DbbcRelVersion *v = (rel->rd_rel->relam == HEAP_TABLE_AM_OID)
			? dbbc_version_pin_tracked(RelationGetRelid(rel)) : NULL;

		dbbc_scan_bind_version(s, rel, s->needed_attnos,
							   s->prefilter_unsafe ? NIL : cscan->scan.plan.qual,
							   s->prefilter_unsafe ? 0 : cscan->scan.scanrelid,
							   v, true);
	}
}

/* --- parallel DSM callbacks --- */

static Size
dbbc_scan_estimate_dsm(CustomScanState *node, ParallelContext *pcxt)
{
	return sizeof(DbbcParallelState);
}

static void
dbbc_scan_initialize_dsm(CustomScanState *node, ParallelContext *pcxt,
						 void *coordinate)
{
	DbbcScanState *s = (DbbcScanState *) node;
	DbbcParallelState *p = (DbbcParallelState *) coordinate;

	pg_atomic_init_u32(&p->next_slot, 0);
	/* publish the leader's pinned version + range so workers match exactly */
	p->version_dp = (s->version != NULL) ? s->version->self : InvalidDsaPointer;
	p->heap_nblocks = s->heap_nblocks;
	p->total_slots = s->total_slots;
	s->pstate = p;
}

static void
dbbc_scan_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt,
						   void *coordinate)
{
	DbbcParallelState *p = (DbbcParallelState *) coordinate;

	/* rescan: only the shared cursor resets; version/range are stable */
	pg_atomic_write_u32(&p->next_slot, 0);
}

static void
dbbc_scan_initialize_worker(CustomScanState *node, shm_toc *toc,
							void *coordinate)
{
	DbbcScanState *s = (DbbcScanState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	Relation	rel = node->ss.ss_currentRelation;
	DbbcParallelState *p = (DbbcParallelState *) coordinate;
	DbbcRelVersion *v;

	s->pstate = p;
	/* use the leader's range so validity/extent checks agree exactly */
	s->heap_nblocks = p->heap_nblocks;
	s->total_slots = p->total_slots;

	v = dbbc_version_attach_tracked(p->version_dp);
	dbbc_scan_bind_version(s, rel, s->needed_attnos,
						   s->prefilter_unsafe ? NIL : cscan->scan.plan.qual,
						   s->prefilter_unsafe ? 0 : cscan->scan.scanrelid, v, true);
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

	/*
	 * M6 fast path: prove validity from the visibility-map fork alone - the
	 * VM is 2 bits per heap page, so this replaces up to 32 heap page reads
	 * with a couple of (hot, tiny) VM buffer reads. When the proof can't be
	 * made cheaply, fall back to the per-heap-page LSN proof below, which
	 * remains the authority.
	 */
	if (dbbc_block_vm_valid(block, rel, &s->vm_check_buf))
	{
		s->blocks_vm_validated++;
		return true;
	}

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

/*
 * Resolve chunk c's DSA addresses and hoist its hot per-value constants into
 * the small DbbcColPtrs struct, so the emit/prefilter/aggregate inner loops
 * never dereference the wide (~80 B) DbbcColumnChunk per value. Called at every
 * columnar block-open, for both the scan serve path and the aggregate paths.
 */
static inline void
dbbc_col_ptrs_set(DbbcScanState *s, int c)
{
	DbbcColumnChunk *chunk = &s->cur_chunks[c];
	DbbcColPtrs *p = &s->col_ptrs[c];

	p->values = (uint8 *) dsa_get_address(s->dsa, chunk->values);
	p->blob = DsaPointerIsValid(chunk->varblob) ?
		(uint8 *) dsa_get_address(s->dsa, chunk->varblob) : NULL;
	p->nulls = DsaPointerIsValid(chunk->nulls) ?
		(uint8 *) dsa_get_address(s->dsa, chunk->nulls) : NULL;
	p->is_dict = (chunk->encoding == DBBC_ENCODING_DICT);
	p->code_width = chunk->code_width;
	p->dict_count = chunk->dict_count;
	p->codes = p->is_dict ?
		(uint8 *) dsa_get_address(s->dsa, chunk->codes) : NULL;
	p->attlen = chunk->attlen;
	p->attbyval = chunk->attbyval;
	p->attidx = chunk->attnum - 1;
}

/* read one value of chunk c at row from the current block */
static inline Datum
dbbc_chunk_read(DbbcScanState *s, int c, uint32 row, bool *isnull)
{
	DbbcColPtrs *ptrs = &s->col_ptrs[c];
	uint32		idx = row;

	if (ptrs->nulls != NULL &&
		(ptrs->nulls[row / 8] & (1 << (row % 8))) != 0)
	{
		*isnull = true;
		return (Datum) 0;
	}

	/*
	 * Dictionary: the null check is by row (above), then the row's code selects
	 * the dictionary entry to read. `values` holds the dict; the read below is
	 * identical to PLAIN but at the code index instead of the row index.
	 */
	if (ptrs->is_dict)
		idx = (ptrs->code_width == 1) ? (uint32) ptrs->codes[row]
			: (uint32) ((uint16 *) ptrs->codes)[row];

	if (ptrs->attlen > 0)
	{
		uint8	   *ptr = ptrs->values + (Size) idx * ptrs->attlen;

		*isnull = false;
		if (ptrs->attbyval)
			return fetch_att(ptr, true, ptrs->attlen);
		return PointerGetDatum(ptr);
	}
	else
	{
		uint32		off = ((uint32 *) ptrs->values)[idx];

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

/* widen a decoded fast-int column Datum to int64 per the qual's col_ikind */
static inline int64
dbbc_widen(Datum d, int16 kind)
{
	if (kind == 4)
		return (int64) DatumGetInt32(d);
	if (kind == 8)
		return (int64) DatumGetInt64(d);
	return (int64) DatumGetInt16(d);	/* kind == 2 */
}

/* read dictionary entry `e` (a distinct value) - the PLAIN read at index e,
 * with NO row->code translation. Dict entries are never NULL. */
static inline Datum
dbbc_dict_entry(DbbcColPtrs *p, uint32 e)
{
	if (p->attlen > 0)
	{
		uint8	   *ptr = p->values + (Size) e * p->attlen;

		if (p->attbyval)
			return fetch_att(ptr, true, p->attlen);
		return PointerGetDatum(ptr);
	}
	return PointerGetDatum(p->blob + ((uint32 *) p->values)[e]);
}

/*
 * Does a single non-NULL value pass an OP or SAOP_EQ qual? Integer-family quals
 * compare inline as int64 (no fmgr); everything else uses the btree ORDER proc.
 * NULLTEST is handled by the caller (it needs the null flag, not the value).
 */
static inline bool
dbbc_qual_pass_value(DbbcSkipQual *q, Datum v)
{
	if (q->kind == DBBC_SKIP_OP)
	{
		if (q->fast_int)
		{
			int64		x = dbbc_widen(v, q->col_ikind);
			int64		rhs = q->fast_ival;

			switch (q->strategy)
			{
				case BTLessStrategyNumber:
					return x < rhs;
				case BTLessEqualStrategyNumber:
					return x <= rhs;
				case BTEqualStrategyNumber:
					return x == rhs;
				case BTGreaterEqualStrategyNumber:
					return x >= rhs;
				case BTGreaterStrategyNumber:
					return x > rhs;
			}
			return true;		/* unreachable; leave to ExecScan */
		}
		else
		{
			int32		r = dbbc_skip_cmp(q, v, q->value);

			switch (q->strategy)
			{
				case BTLessStrategyNumber:
					return r < 0;
				case BTLessEqualStrategyNumber:
					return r <= 0;
				case BTEqualStrategyNumber:
					return r == 0;
				case BTGreaterEqualStrategyNumber:
					return r >= 0;
				case BTGreaterStrategyNumber:
					return r > 0;
			}
			return true;
		}
	}
	else						/* DBBC_SKIP_SAOP_EQ */
	{
		int			e;

		if (q->fast_int)
		{
			int64		x = dbbc_widen(v, q->col_ikind);

			for (e = 0; e < q->nelems; e++)
				if (x == q->fast_ielems[e])
					return true;
		}
		else
		{
			for (e = 0; e < q->nelems; e++)
				if (dbbc_skip_cmp(q, v, q->elems[e]) == 0)
					return true;
		}
		return false;
	}
}

/*
 * Decode this block's filter columns into the SoA buffer (Step 3) and
 * precompute the row selection bitmap sel[] (Step 4). Runs once per block-open.
 *
 * Two per-column strategies:
 *  - PLAIN column: decode all rows into dec_val/dec_null, then evaluate the
 *    qual per row (dbbc_qual_pass_value: int64 inline or btree ORDER proc).
 *  - DICT column (dict-aware filtering): evaluate the qual ONCE per dictionary
 *    entry (<=256) into dict_pass[], then per row it is a 1-byte code -> 1-byte
 *    lookup. No per-row decode, no per-row fmgr - the payoff of dictionary
 *    encoding on the filter path. Such columns are NOT row-decoded above.
 *
 * Strict OP/SAOP fail NULL; NULLTEST compares the null flag. Quals stay in
 * plan.qual so ExecScan re-checks - the prefilter can only ever remove rows.
 */
static void
dbbc_decode_and_select(DbbcScanState *s, uint32 nrows)
{
	int			c;
	int			i;

	/*
	 * No prefilter (nskipquals == 0, incl. the RLS/prefilter_unsafe path): the
	 * decode buffer and selection bitmap were never allocated (decode_col is
	 * NULL), and there is nothing to decode or select. dbbc_next then emits
	 * every row (its own nskipquals>0 guard skips the sel[] lookup).
	 */
	if (s->decode_col == NULL)
		return;

	/* decode PLAIN filter columns; dict columns are evaluated via their
	 * dictionary + codes below, so they are not decoded per row. */
	for (c = 0; c < s->ncols; c++)
	{
		Datum	   *dv;
		bool	   *dn;
		uint32		row;

		if (!s->decode_col[c] || s->col_ptrs[c].is_dict)
			continue;
		dv = s->dec_val + (Size) c * s->decode_cap;
		dn = s->dec_null + (Size) c * s->decode_cap;
		for (row = 0; row < nrows; row++)
		{
			bool		isnull;

			dv[row] = dbbc_chunk_read(s, c, row, &isnull);
			dn[row] = isnull;
		}
	}

	if (s->nskipquals == 0)
		return;					/* no prefilter; sel[] is unused */

	memset(s->sel, true, nrows * sizeof(bool));

	for (i = 0; i < s->nskipquals; i++)
	{
		DbbcSkipQual *q = &s->skipquals[i];
		int			c2 = s->attno_to_col[q->attno - 1];
		DbbcColPtrs *p;
		uint32		row;

		if (c2 < 0)
			continue;			/* not served columnar; leave to ExecScan */
		p = &s->col_ptrs[c2];

		if (q->kind == DBBC_SKIP_NULLTEST)
		{
			bool	   *dn = p->is_dict ? NULL :
				(s->dec_null + (Size) c2 * s->decode_cap);

			for (row = 0; row < nrows; row++)
			{
				bool		isn;

				if (!s->sel[row])
					continue;
				isn = p->is_dict
					? (p->nulls != NULL &&
					   (p->nulls[row / 8] & (1 << (row % 8))) != 0)
					: dn[row];
				if (isn != q->nulltest_isnull)
					s->sel[row] = false;
			}
			continue;
		}

		if (p->is_dict)
		{
			/* evaluate the qual against each dictionary entry once, then map
			 * every row through its 1-byte code (dict-aware filtering). */
			bool		dict_pass[DBBC_DICT_MAX];	/* build caps dict_count here */
			uint32		e;

			Assert(p->dict_count <= DBBC_DICT_MAX);
			for (e = 0; e < p->dict_count; e++)
				dict_pass[e] = dbbc_qual_pass_value(q, dbbc_dict_entry(p, e));

			for (row = 0; row < nrows; row++)
			{
				uint32		code;

				if (!s->sel[row])
					continue;
				if (p->nulls != NULL &&
					(p->nulls[row / 8] & (1 << (row % 8))) != 0)
				{
					s->sel[row] = false;	/* strict OP/SAOP: NULL fails */
					continue;
				}
				code = (p->code_width == 1) ? (uint32) p->codes[row]
					: (uint32) ((uint16 *) p->codes)[row];
				if (!dict_pass[code])
					s->sel[row] = false;
			}
			continue;
		}

		/* PLAIN column: per-row evaluation from the decode buffer */
		{
			Datum	   *dv = s->dec_val + (Size) c2 * s->decode_cap;
			bool	   *dn = s->dec_null + (Size) c2 * s->decode_cap;

			for (row = 0; row < nrows; row++)
			{
				if (!s->sel[row])
					continue;
				if (dn[row])		/* strict OP/SAOP: NULL fails */
				{
					s->sel[row] = false;
					continue;
				}
				if (!dbbc_qual_pass_value(q, dv[row]))
					s->sel[row] = false;
			}
		}
	}
}

/*
 * Commit to serving `block` (already validated and not zone-skipped): resolve
 * its column pointers, decode its filter columns + compute the selection
 * bitmap, and establish this block's NULL background in the scan slot, so
 * dbbc_emit_row writes only the registered columns per row.
 *
 * LOAD-BEARING: the NULL background must be (re-)established here at EVERY
 * columnar block-open, not once per scan. Columnar blocks and heap-fallback
 * ranges interleave through this one scan slot, and the heap arm's
 * ExecCopySlot (in dbbc_next) overwrites all natts columns. Re-establishing
 * the background here stops a preceding heap range's values from leaking into
 * the unregistered columns of this columnar block - a silent wrong result, not
 * a crash. ExecClearTuple first frees any should-free Datums that heap copy
 * materialized before we overwrite the pointer array.
 */
static void
dbbc_open_columnar_block(DbbcScanState *s, DbbcBlock *block,
						 DbbcColumnChunk *chunks)
{
	TupleTableSlot *slot = s->css.ss.ss_ScanTupleSlot;
	int			natts = slot->tts_tupleDescriptor->natts;
	int			c;

	s->cur_block = block;
	s->cur_chunks = chunks;
	for (c = 0; c < s->ncols; c++)
		dbbc_col_ptrs_set(s, c);

	/* decode filter columns + precompute the row selection for this block */
	dbbc_decode_and_select(s, block->nrows);

	ExecClearTuple(slot);
	memset(slot->tts_isnull, true, natts * sizeof(bool));
	memset(slot->tts_values, 0, natts * sizeof(Datum));

	s->cur_row = 0;
}

/* fill the virtual scan slot from cur_block[cur_row] */
static TupleTableSlot *
dbbc_emit_row(DbbcScanState *s)
{
	TupleTableSlot *slot = s->css.ss.ss_ScanTupleSlot;
	uint32		row = s->cur_row;
	int			c;

	ExecClearTuple(slot);

	/*
	 * Write ONLY the registered columns. Every other column keeps the NULL
	 * background established once per block in dbbc_open_columnar_block(), so
	 * we avoid two natts-wide memsets per row (~531 B/row on a 59-column
	 * table = the largest single component of scan self-time). Each registered
	 * column's tts_isnull is set every row (a value present last row may be
	 * NULL this row); the Datum is written only when non-NULL (a stale Datum
	 * under tts_isnull=true is never read).
	 */
	for (c = 0; c < s->ncols; c++)
	{
		DbbcColPtrs *p = &s->col_ptrs[c];
		bool		isnull;
		Datum		value = dbbc_chunk_read(s, c, row, &isnull);

		slot->tts_isnull[p->attidx] = isnull;
		if (!isnull)
			slot->tts_values[p->attidx] = value;
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
		 * Degraded mode (non-heap AM): one plain, AM-agnostic scan of the
		 * whole relation - heap_setscanlimits can't range-limit a non-heap
		 * scan, so this can't be block-partitioned. Under parallelism only
		 * the leader scans (workers produce nothing) to avoid each
		 * participant re-scanning the whole relation. This path is a rare
		 * defensive fallback (a cached plan whose AM changed); a fresh plan
		 * over a non-heap AM never offers a columnar path.
		 */
		if (s->whole_rel_mode)
		{
			if (s->heap_scan == NULL)
			{
				if (s->whole_rel_done)
					return NULL;

				/*
				 * Non-heap AM: the range can't be block-partitioned, so
				 * exactly one participant does the single full scan. Elect it
				 * via the shared cursor (whoever claims slot 0), NOT "the
				 * leader" - with parallel_leader_participation=off the leader
				 * may never run this node, and assuming it does would lose
				 * every row. Serial execution (pstate == NULL) always scans.
				 */
				if (s->pstate != NULL &&
					pg_atomic_fetch_add_u32(&s->pstate->next_slot, 1) != 0)
				{
					s->whole_rel_done = true;
					return NULL;
				}
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
				/* sel[] was precomputed for the whole block at block-open */
				if (s->nskipquals > 0 && !s->sel[s->cur_row])
				{
					s->rows_filtered++;
					s->cur_row++;
					continue;
				}
				return dbbc_emit_row(s);
			}
			s->cur_block = NULL;
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
			continue;
		}

		/* claim the next block-directory slot (shared cursor when parallel) */
		{
			uint32		slot = dbbc_claim_slot(s);
			DbbcBlock  *block;

			if (slot >= s->total_slots)
				return NULL;
			block = dbbc_slot_block(s, slot);

			if (block != NULL && dbbc_block_valid(s, rel, block))
			{
				DbbcColumnChunk *chunks = (DbbcColumnChunk *)
					dsa_get_address(s->dsa, block->chunks);

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
					continue;
				}

				dbbc_open_columnar_block(s, block, chunks);
				s->blocks_columnar++;
			}
			else
			{
				BlockNumber start = slot * DBBC_PAGES_PER_BLOCK;
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
	if (BufferIsValid(s->vm_check_buf))
	{
		ReleaseBuffer(s->vm_check_buf);
		s->vm_check_buf = InvalidBuffer;
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
		ExplainPropertyInteger("Columnar VM-Validated Blocks", NULL,
							   (int64) s->blocks_vm_validated, es);
		ExplainPropertyInteger("Heap Fallback Ranges", NULL,
							   (int64) s->ranges_heap, es);
		ExplainPropertyInteger("Columnar Rows", NULL,
							   (int64) s->rows_columnar, es);
		ExplainPropertyInteger("Rows Removed by Columnar Filter", NULL,
							   (int64) s->rows_filtered, es);
	}
}

/*-------------------------------------------------------------------------
 * Aggregate pushdown (Milestone 2 step 4a: metadata; Milestone 5: grouped)
 *
 * At UPPERREL_GROUP_AGG this node replaces the whole Agg-over-scan stack for
 * two query shapes over a single columnar-eligible heap relation:
 *
 * METADATA mode (M2s4a):
 *     SELECT count(*) [, count(col), min(col), max(col) ...] FROM t
 * (scalar - no GROUP BY, no HAVING, no WHERE): answered entirely from block
 * metadata - a valid block contributes block->nrows to COUNT(*),
 * nrows - null_count to COUNT(col), and its zone extrema to MIN/MAX, with no
 * value reads at all.
 *
 * GROUPED mode (M5):
 *     SELECT keys..., agg(col)... FROM t [WHERE extractable-quals]
 *     [GROUP BY keys]
 * computed with PostgreSQL's own aggregate transition/final functions,
 * advanced directly over packed chunk values (no per-row slot emission, no
 * parent Agg node). Group keys are hashed in a dynahash keyed on raw Datums,
 * which is why keys are restricted to fixed-width by-value types whose bit
 * equality IS SQL equality (floats excluded: -0.0 = +0.0, NaN). Because this
 * node has NO ExecScan parent to recheck anything, the path is offered only
 * when EVERY WHERE clause is extractable by the pre-filter machinery, and a
 * block's chunks are used only if their build-time atttypid matches the
 * column's current type (same staleness rule as zone-map skipping; mismatched
 * or invalid blocks fall back to a range-limited heap scan with the query
 * snapshot). Transition-call semantics mirror nodeAgg exactly: strict-transfn
 * NULL-input skip, first-input state initialization for strict aggregates
 * with a NULL initcond, and the pass-by-ref state copy/free dance; the
 * fabricated AggState satisfies AggCheckCallContext(), which is the entire
 * documented context ABI for plain transition functions (fmgr/README).
 *
 * Anything outside these shapes adds no path, so the normal
 * Agg-over-columnar-scan plan runs unchanged.
 *-------------------------------------------------------------------------
 */

typedef enum DbbcAggKind
{
	DBBC_AGG_COUNT_STAR,
	DBBC_AGG_COUNT_COL,
	DBBC_AGG_MIN,
	DBBC_AGG_MAX,
} DbbcAggKind;

typedef struct DbbcAggItem
{
	DbbcAggKind kind;
	AttrNumber	attno;			/* COUNT_COL / MIN / MAX: the column */

	/*
	 * MIN/MAX comparison identity. A block's zone extremum may be trusted
	 * only when the chunk's build-time type and collation match these (same
	 * soundness rule as zone-map skipping); otherwise that block is read
	 * from the heap. cmpproc is the aggsortop opclass's btree ORDER proc.
	 */
	Oid			collation;
	Oid			opcintype;
	Oid			cmpproc;

	/* MIN/MAX runtime accumulator */
	FmgrInfo	cmp;
	int16		typlen;
	bool		typbyval;
	Datum		result;
	bool		result_isnull;
	bool		has_result;
} DbbcAggItem;

/* ---- grouped mode (M5) ---- */

/* group keys are hashed on raw Datum bits: bounded count, byval types only */
#define DBBC_GRP_MAX_KEYS		4
/* plan-time dNumGroups gate: the group hash has no spill path */
#define DBBC_GRP_MAX_GROUPS		100000

/* one aggregate computed by real transition functions */
typedef struct DbbcAggTrans
{
	Aggref	   *aggref;			/* the planner's Aggref (AGGSPLIT_SIMPLE) */
	AttrNumber	input_attno;	/* single Var argument, or Invalid (count(*)) */
	int			numTransInputs; /* 0 for count(*), else 1 */
	Oid			transtype;
	int16		transtypeLen;
	bool		transtypeByVal;
	int16		resulttypeLen;
	bool		resulttypeByVal;
	Datum		initValue;		/* from pg_aggregate.agginitval */
	bool		initValueIsNull;
	FmgrInfo	transfn;
	FmgrInfo	finalfn;		/* valid iff finalfn_oid is */
	Oid			finalfn_oid;
	int			numFinalArgs;
	FunctionCallInfo trans_fcinfo;	/* context = the fabricated AggState */

	/*
	 * FILTER (WHERE ...): the aggregate advances only for rows where every
	 * filter qual passes. The filter must decompose into an AND-list of simple
	 * comparisons over registered columns (dbbc_agg_filter_decompose), reusing
	 * the same skip-qual machinery as the WHERE pre-filter. nfilter == 0 means
	 * no FILTER (advance every row that reached the group).
	 */
	DbbcSkipQual *filter;
	int			nfilter;
} DbbcAggTrans;

/* per-group per-aggregate running state (mirrors AggStatePerGroupData) */
typedef struct DbbcTransState
{
	Datum		transValue;
	bool		transValueIsNull;
	bool		noTransValue;	/* strict+NULL-initcond: awaiting first input */
} DbbcTransState;

/* dynahash key: raw group-key Datums; unused slots stay zeroed (memcmp key) */
typedef struct DbbcGroupKey
{
	Datum		vals[DBBC_GRP_MAX_KEYS];
	uint32		nullmask;		/* bit k set = key k IS NULL (val zeroed) */
} DbbcGroupKey;

typedef struct DbbcGroupEntry
{
	DbbcGroupKey key;			/* hash key: must be first */
	DbbcTransState states[FLEXIBLE_ARRAY_MEMBER];	/* [ntrans] */
} DbbcGroupEntry;

/*
 * custom_private layout (all copyObject-safe Node lists):
 *   [0] Integer holding the base relation OID (absolute; an RT index
 *       would be wrong - custom_private is opaque to setrefs, so it
 *       never receives the rtoffset applied to subquery/CTE/view
 *       range tables). Bit-preserved through int; read back with (Oid).
 *   [1] Integer mode: 0 = metadata (M2s4a), 1 = grouped (M5)
 *   [2] payload:
 *       metadata: List of list_make5(kind, attno, collation, opcintype,
 *                 cmpproc) per output column
 *       grouped : list_make5(key exprs        List (bare Var or expression;
 *                                             expr keys evaluated per row),
 *                            output map       IntList (>=0: key index,
 *                                             <0: -(agg index)-1),
 *                            aggrefs          List of Aggref,
 *                            pushed quals     List of bare clause exprs,
 *                            Integer qual varno (pre-setrefs, matches the
 *                            Vars inside the quals - both live in
 *                            custom_private, so both miss the rtoffset
 *                            consistently))
 *   [3] physical output tlist (List of TargetEntry)
 */
#define DBBC_AGG_PRIV_RELOID(cp)	((Oid) intVal(linitial(cp)))
#define DBBC_AGG_PRIV_MODE(cp)		((int) intVal(lsecond(cp)))
#define DBBC_AGG_PRIV_PAYLOAD(cp)	((List *) lthird(cp))
#define DBBC_AGG_PRIV_TLIST(cp)		((List *) lfourth(cp))

typedef struct DbbcAggScanState
{
	CustomScanState css;
	bool		executed;		/* metadata: single row emitted?
								 * grouped: input consumed, hash built? */
	Relation	rel;
	bool		rel_opened;

	/* metadata mode */
	int			naggs;
	DbbcAggItem *aggs;
	int64	   *counts;			/* per-agg accumulator (COUNT kinds) */
	MemoryContext aggctx;		/* durable home for MIN/MAX result datums */

	/* grouped mode */
	bool		grouped;
	int			nkeys;
	AttrNumber	key_attnos[DBBC_GRP_MAX_KEYS];	/* bare-Var key: the column; else Invalid */

	/*
	 * Expression group keys (e.g. date_trunc('month', datecol)). key_exprstate[k]
	 * is NULL for a bare-Var key (read the column directly via key_attnos[k]),
	 * else the compiled expression, evaluated per row over key_slot (populated
	 * with the referenced columns; econtext->ecxt_scantuple = key_slot). The
	 * result type is byval + bit-equality-safe (validated at plan time), so the
	 * Datum goes straight into the raw-bits group hash. The input columns are in
	 * the used/used_types set so the per-block type-identity gate covers them.
	 */
	bool		has_key_exprs;
	ExprState **key_exprstate;	/* [nkeys], entry NULL for a bare-Var key */
	TupleTableSlot *key_slot;	/* relation-width virtual slot for the inputs */
	ExprContext *key_econtext;
	AttrNumber *key_input_attnos;	/* distinct columns the expr keys read */
	int			nkey_input;
	int			ntrans;
	DbbcAggTrans *trans;
	Datum	   *in_vals;		/* per-trans current-row input */
	bool	   *in_nulls;
	bool	   *in_filterpass;	/* per-trans: does this row pass the agg's FILTER */
	int			noutcols;
	int		   *outmap;			/* per output column: key idx or -(agg)-1 */
	List	   *qual_clauses;	/* pushed quals (this node has no recheck) */
	Index		qual_varno;
	DbbcSkipQual *skipquals;	/* extracted from qual_clauses at Begin */
	int			nskipquals;
	int			nused;			/* distinct referenced columns... */
	AttrNumber *used_attnos;
	Oid		   *used_types;		/* ...and their CURRENT catalog types */
	HTAB	   *groups;
	MemoryContext groupctx;		/* group hash + transition states */
	MemoryContext tmpctx;		/* per-row transfn call scratch */
	AggState   *fake_aggstate;	/* satisfies AggCheckCallContext */
	Size		grp_mem_limit;	/* hard runtime cap on groupctx (no spill) */
	uint64		grp_since_memcheck; /* new groups since the last memory probe */
	HASH_SEQ_STATUS seq;		/* emission cursor over the hash */
	bool		seq_active;

	/* grouped-mode instrumentation (EXPLAIN ANALYZE) */
	int64		grp_blocks_served;
	int64		grp_blocks_skipped;
	int64		grp_ranges_heap;
	int64		grp_rows;
} DbbcAggScanState;

/* pg_aggregate.aggsortop for aggfnoid, or InvalidOid (mirrors planagg.c) */
static Oid
dbbc_agg_sort_op(Oid aggfnoid)
{
	HeapTuple	tup;
	Oid			sortop;

	tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
	if (!HeapTupleIsValid(tup))
		return InvalidOid;
	sortop = ((Form_pg_aggregate) GETSTRUCT(tup))->aggsortop;
	ReleaseSysCache(tup);
	return sortop;
}

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

	memset(item, 0, sizeof(*item));
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

	/* MIN(col) / MAX(col): recognized exactly as the planner does, via
	 * aggsortop, and served from block zone maps. */
	if (list_length(agg->args) == 1)
	{
		Oid			sortop = dbbc_agg_sort_op(agg->aggfnoid);
		TargetEntry *tle = (TargetEntry *) linitial(agg->args);
		Node	   *arg = (Node *) tle->expr;

		if (!OidIsValid(sortop))
			return false;
		while (IsA(arg, RelabelType))
			arg = (Node *) ((RelabelType *) arg)->arg;
		if (IsA(arg, Var))
		{
			Var		   *var = (Var *) arg;
			Oid			opfamily;
			Oid			opcintype;
			CompareType cmptype;
			Oid			cmpproc;

			if (var->varno != relid || var->varlevelsup != 0 ||
				var->varattno <= 0)
				return false;
			if (!get_ordering_op_properties(sortop, &opfamily, &opcintype,
											&cmptype))
				return false;
			cmpproc = get_opfamily_proc(opfamily, opcintype, opcintype,
										BTORDER_PROC);
			if (!OidIsValid(cmpproc))
				return false;
			if (cmptype == COMPARE_LT)
				item->kind = DBBC_AGG_MIN;
			else if (cmptype == COMPARE_GT)
				item->kind = DBBC_AGG_MAX;
			else
				return false;
			item->attno = var->varattno;
			item->collation = agg->inputcollid;
			item->opcintype = opcintype;
			item->cmpproc = cmpproc;
			*needcols = list_append_unique_int(*needcols, var->varattno);
			return true;
		}
	}

	return false;
}

/*
 * Group keys are hashed on their raw Datum bits, so only types whose bit
 * equality IS SQL equality qualify. Floats are excluded on purpose: their
 * comparison treats -0.0 = +0.0 and groups NaNs together, neither of which
 * holds for bit equality.
 */
static bool
dbbc_grp_key_type_ok(Oid typid)
{
	switch (typid)
	{
		case BOOLOID:
		case CHAROID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case DATEOID:
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return true;
	}
	return false;
}

/*
 * Decompose an aggregate FILTER (WHERE ...) expression into an AND-list of
 * simple comparisons over relation `relid`, reusing dbbc_extract_one_qual (the
 * same Var op Const / Var = ANY / IS [NOT] NULL machinery as the WHERE
 * pre-filter). Returns true and (when out is non-NULL) fills the out/nout
 * quals; false if any conjunct is not extractable, so the pushdown is simply
 * not offered and the query falls back to a scan + core Agg. Because
 * dbbc_extract_one_qual only accepts Var/Const/array shapes, the filter is
 * inherently non-volatile. Each referenced column is added to *needcols so the
 * coverage gate requires it to be columnarized.
 */
static bool
dbbc_agg_filter_decompose(Node *filter, Index relid,
						  DbbcSkipQual **out, int *nout, List **needcols)
{
	List	   *args;
	ListCell   *lc;
	DbbcSkipQual *quals;
	int			n;
	int			i = 0;

	if (filter == NULL)
	{
		if (out)
		{
			*out = NULL;
			*nout = 0;
		}
		return true;
	}

	if (IsA(filter, BoolExpr) && ((BoolExpr *) filter)->boolop == AND_EXPR)
		args = ((BoolExpr *) filter)->args;
	else
		args = list_make1(filter);

	n = list_length(args);
	quals = (DbbcSkipQual *) palloc(n * sizeof(DbbcSkipQual));
	foreach(lc, args)
	{
		if (!dbbc_extract_one_qual((Node *) lfirst(lc), relid, &quals[i]))
		{
			pfree(quals);
			return false;
		}
		if (needcols)
			*needcols = list_append_unique_int(*needcols, quals[i].attno);
		i++;
	}

	if (out)
	{
		*out = quals;
		*nout = n;
	}
	else
		pfree(quals);
	return true;
}

/*
 * Is this Aggref computable by our transition-function machinery? Builtin
 * (trusted to use only AggCheckCallContext - the ordered-set family, the one
 * exception, is excluded by aggkind), plain, unsplit, and its argument (if
 * any) a bare Var of this relation. A FILTER (WHERE ...) is allowed when it
 * decomposes into simple comparisons over registered columns. The strict-
 * transfn/NULL-initcond binary-coercibility rule is checked here so the path
 * is simply not offered for an aggregate the executor setup would reject.
 */
static bool
dbbc_agg_trans_ok(Aggref *agg, Index relid, List **needcols)
{
	HeapTuple	aggTuple;
	Form_pg_aggregate aggform;
	bool		initValueIsNull;
	bool		ok = true;
	Var		   *var = NULL;

	if (agg->aggdistinct != NIL ||
		agg->aggorder != NIL || agg->aggkind != AGGKIND_NORMAL ||
		agg->aggsplit != AGGSPLIT_SIMPLE)
		return false;
	if (agg->aggfnoid >= FirstNormalObjectId)
		return false;			/* builtin aggregates only */
	/* FILTER (WHERE ...): only if it decomposes into simple pushable quals */
	if (agg->aggfilter != NULL &&
		!dbbc_agg_filter_decompose((Node *) agg->aggfilter, relid,
								   NULL, NULL, needcols))
		return false;
	if (!OidIsValid(agg->aggtranstype))
		return false;

	if (list_length(agg->args) == 1)
	{
		Node	   *arg = (Node *) ((TargetEntry *) linitial(agg->args))->expr;

		while (IsA(arg, RelabelType))
			arg = (Node *) ((RelabelType *) arg)->arg;
		if (!IsA(arg, Var))
			return false;
		var = (Var *) arg;
		if (var->varno != relid || var->varlevelsup != 0 || var->varattno <= 0)
			return false;
	}
	else if (!(agg->args == NIL && agg->aggstar))
		return false;			/* count(*) is the only 0-arg shape */

	aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(agg->aggfnoid));
	if (!HeapTupleIsValid(aggTuple))
		return false;
	aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
	(void) SysCacheGetAttr(AGGFNOID, aggTuple, Anum_pg_aggregate_agginitval,
						   &initValueIsNull);
	if (func_strict(aggform->aggtransfn) && initValueIsNull)
	{
		Oid			inputTypes[FUNC_MAX_ARGS];
		int			numArgs = get_aggregate_argtypes(agg, inputTypes);

		if (numArgs < 1 ||
			!IsBinaryCoercible(inputTypes[0], agg->aggtranstype))
			ok = false;
	}
	ReleaseSysCache(aggTuple);
	if (!ok)
		return false;

	if (var != NULL)
		*needcols = list_append_unique_int(*needcols, var->varattno);
	return true;
}

/*
 * Classify the whole query for grouped pushdown. Fills the path payload
 * pieces and the needed-column list; returns false (offer no path) on the
 * first unsupported construct.
 */
/*
 * Validate that every Var in a group-key expression is a live user column of
 * this relation, and record those columns in *needcols. Returns false for an
 * outer/system/other-relation Var. Used only for non-bare-Var group keys.
 */
static bool
dbbc_expr_vars_ok(Node *expr, Index relid, List **needcols)
{
	List	   *vars = pull_var_clause(expr,
									  PVC_RECURSE_AGGREGATES |
									  PVC_RECURSE_WINDOWFUNCS |
									  PVC_RECURSE_PLACEHOLDERS);
	ListCell   *lc;

	foreach(lc, vars)
	{
		Var		   *v = (Var *) lfirst(lc);

		if (!IsA(v, Var) || v->varno != relid || v->varlevelsup != 0 ||
			v->varattno <= 0)
			return false;
		*needcols = list_append_unique_int(*needcols, v->varattno);
	}
	return true;
}

static bool
dbbc_agg_grouped_classify(PlannerInfo *root, RelOptInfo *input_rel,
						  RelOptInfo *output_rel, Index relid,
						  List **keyexprs_out, List **outmap_out,
						  List **aggrefs_out, List **quals_out,
						  List **needcols, double *ngroups_out)
{
	List	   *keyexprs = NIL;
	List	   *outmap = NIL;
	List	   *aggrefs = NIL;
	List	   *quals = NIL;
	ListCell   *lc;

	/*
	 * Every WHERE clause must be evaluable by the pre-filter machinery: this
	 * node replaces the scan AND the Agg, so there is no ExecScan above it to
	 * re-check anything.
	 *
	 * We also refuse any clause with a non-zero security_level (RLS policies,
	 * security-barrier views). Core orders such quals (order_qual_clauses) so
	 * that a lower-security qual is applied before a higher-security,
	 * possibly-leaky one; this node evaluates all quals itself in list order
	 * and would break that contract - so leave those queries to the normal
	 * plan, which enforces the ordering.
	 */
	foreach(lc, input_rel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
		DbbcSkipQual probe;

		if (ri->security_level > 0 || ri->pseudoconstant)
			return false;
		if (!dbbc_extract_one_qual((Node *) ri->clause, relid, &probe))
			return false;
		quals = lappend(quals, ri->clause);
		*needcols = list_append_unique_int(*needcols, probe.attno);
	}

	/*
	 * Group keys: a bare Var, or a non-volatile expression whose result type is
	 * bit-equality-safe by-value (hashed on raw Datum bits) and whose input Vars
	 * are all live columns of this relation (e.g. date_trunc('month', datecol)).
	 * Deduplicated by equal(); the per-key expression is stored and, at Begin,
	 * either read directly (bare Var) or compiled to an ExprState.
	 */
	foreach(lc, root->processed_groupClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
		Node	   *expr = get_sortgroupclause_expr(sgc,
													root->parse->targetList);

		if (IsA(expr, Var))
		{
			Var		   *var = (Var *) expr;

			if (var->varno != relid || var->varlevelsup != 0 ||
				var->varattno <= 0)
				return false;
			if (!dbbc_grp_key_type_ok(var->vartype))
				return false;
			*needcols = list_append_unique_int(*needcols, var->varattno);
		}
		else
		{
			if (!dbbc_grp_key_type_ok(exprType(expr)))
				return false;
			if (contain_volatile_functions(expr))
				return false;
			if (!dbbc_expr_vars_ok(expr, relid, needcols))
				return false;
		}
		if (!list_member(keyexprs, expr))
		{
			if (list_length(keyexprs) >= DBBC_GRP_MAX_KEYS)
				return false;
			keyexprs = lappend(keyexprs, expr);
		}
	}

	/*
	 * Every output expr must be a group key (matched by equal(), covering both
	 * bare-Var and expression keys) or a supported bare Aggref.
	 */
	foreach(lc, output_rel->reltarget->exprs)
	{
		Node	   *expr = (Node *) lfirst(lc);

		if (IsA(expr, Aggref))
		{
			if (!dbbc_agg_trans_ok((Aggref *) expr, relid, needcols))
				return false;
			outmap = lappend_int(outmap, -(list_length(aggrefs)) - 1);
			aggrefs = lappend(aggrefs, expr);
		}
		else
		{
			int			k = 0;
			bool		found = false;
			ListCell   *lk;

			foreach(lk, keyexprs)
			{
				if (equal(lfirst(lk), expr))
				{
					found = true;
					break;
				}
				k++;
			}
			if (!found)
				return false;	/* not a group key (e.g. FD on a PK) */
			outmap = lappend_int(outmap, k);
		}
	}
	if (outmap == NIL)
		return false;

	/*
	 * Bounded group count: the group hash has no spill path (unlike stock
	 * HashAgg, which spills since v13), so a wrong low estimate would build
	 * an unbounded in-memory hash. Refuse when the estimate exceeds the cap,
	 * AND when the estimate is a DEFAULT fallback (no statistics) on a table
	 * big enough to blow the cap - that is exactly when estimate_num_groups
	 * is least trustworthy (it returns 200 for an unanalyzed column). A hard
	 * runtime backstop in dbbc_grp_lookup catches the rest.
	 */
	if (keyexprs == NIL)
		*ngroups_out = 1.0;
	else
	{
		EstimationInfo estinfo;

		memset(&estinfo, 0, sizeof(estinfo));
		*ngroups_out = estimate_num_groups(root, keyexprs,
										   input_rel->rows, NULL, &estinfo);
		if (*ngroups_out > (double) DBBC_GRP_MAX_GROUPS)
			return false;
		if ((estinfo.flags & SELFLAG_USED_DEFAULT) &&
			input_rel->rows > (double) DBBC_GRP_MAX_GROUPS)
			return false;
	}

	*keyexprs_out = keyexprs;
	*outmap_out = outmap;
	*aggrefs_out = aggrefs;
	*quals_out = quals;
	return true;
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
	bool		grouped;
	List	   *keyexprs = NIL;
	List	   *outmap = NIL;
	List	   *aggrefs = NIL;
	List	   *quals = NIL;
	double		ngroups = 1.0;

	if (prev_create_upper_paths_hook)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);

	if (stage != UPPERREL_GROUP_AGG)
		return;
	if (!dbblue_columnar_enabled || !dbblue_columnar_enable_columnar_scan)
		return;

	/* no grouping sets, no HAVING (the planner may have moved it to gextra) */
	if (parse->groupingSets != NIL || parse->havingQual != NULL ||
		(gextra != NULL && gextra->havingQual != NULL))
		return;
	if (!parse->hasAggs && parse->groupClause == NIL)
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
	/*
	 * Inheritance/partition parent: the grouping input is this one baserel
	 * but the scan is really an Append over children. Counting only the
	 * parent's own heap would miss child rows - bail (rte->inh is set only
	 * when children exist, so plain tables and FROM ONLY are unaffected).
	 */
	if (rte->inh)
		return;
	if (get_rel_relam(rte->relid) != HEAP_TABLE_AM_OID)
		return;

	/*
	 * Prefer the metadata mode when it applies (scalar, no WHERE, all
	 * COUNT/MIN/MAX: pure block metadata, no value reads); otherwise try the
	 * grouped transition-pushdown mode.
	 */
	grouped = (parse->groupClause != NIL ||
			   input_rel->baserestrictinfo != NIL);
	if (!grouped)
	{
		foreach(lc, output_rel->reltarget->exprs)
		{
			DbbcAggItem item;

			if (!dbbc_agg_classify((Node *) lfirst(lc), relid, &item,
								   &needcols))
			{
				items = NIL;
				grouped = true; /* e.g. sum()/avg(): needs transition fns */
				break;
			}
			items = lappend(items, list_make5(makeInteger((int) item.kind),
											  makeInteger((int) item.attno),
											  makeInteger((int) item.collation),
											  makeInteger((int) item.opcintype),
											  makeInteger((int) item.cmpproc)));
		}
		if (!grouped && items == NIL)
			return;
	}
	if (grouped)
	{
		needcols = NIL;			/* a failed metadata walk may have added some */
		if (!dbbc_agg_grouped_classify(root, input_rel, output_rel, relid,
									   &keyexprs, &outmap, &aggrefs, &quals,
									   &needcols, &ngroups))
			return;
	}

	/*
	 * A populated version must cover every counted column. Tracked pin:
	 * dsa_get_address just below can error on segment attach, and an untracked
	 * pin would leak the version forever (matches dbbc_rel_ready).
	 */
	version = dbbc_version_pin_tracked(rte->relid);
	if (version == NULL)
		return;
	if (!DsaPointerIsValid(version->blockdir) || version->nblocks == 0)
	{
		dbbc_version_unpin_tracked(version);
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
	dbbc_version_unpin_tracked(version);
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
	cpath->path.rows = grouped ? clamp_row_est(ngroups) : 1;
	cpath->path.pathkeys = NIL;
	cpath->flags = 0;
	cpath->custom_paths = NIL;
	/* store the absolute relation OID (not the RT index; see macro comment) */
	cpath->custom_private =
		list_make4(makeInteger((int) rte->relid),
				   makeInteger(grouped ? 1 : 0),
				   grouped ? list_make5(keyexprs, outmap, aggrefs, quals,
										makeInteger((int) relid))
				   : items,
				   tlist);
	cpath->methods = &dbbc_agg_path_methods;

	if (grouped)
	{
		/*
		 * Grouped: real per-row work. IO only for the heap fraction the
		 * VM-fork proof cannot spare (M6), a small validity-walk term, then
		 * per-surviving-row qual + transition calls, and one output tuple
		 * per group. Cheaper than Agg-over-scan by the absent per-row slot
		 * emission AND the spared heap IO.
		 */
		double		surviving = clamp_row_est(base_rel->rows);
		double		skip_frac = 0.0;
		double		vm_frac = 0.0;
		double		decoded;
		Cost		run;

		{
			Relation	prel = table_open(rte->relid, NoLock);

			dbbc_estimate_serve_fractions(prel, quals, relid,
										  &skip_frac, &vm_frac);
			table_close(prel, NoLock);
		}

		/*
		 * Decode + qual evaluation happen on EVERY row of every non-zone-
		 * skipped block (all decoded rows), not just the rows that survive the
		 * quals - charge on base_rel->tuples * (1 - skip_frac). Charging the
		 * qual term on the surviving-row estimate instead would under-price a
		 * selective qual on a scattered (non-zone-skippable) column by orders
		 * of magnitude and let columnar wrongly displace an index scan. Only
		 * the transition-function calls run on surviving rows.
		 */
		decoded = clamp_row_est(base_rel->tuples * (1.0 - skip_frac));
		run = seq_page_cost * base_rel->pages * (1.0 - vm_frac)
			+ cpu_operator_cost *
			(base_rel->pages / DBBC_PAGES_PER_BLOCK + 1) * 2.0
			/* decode every served row (per-tuple work, like the scan path's
			 * cpu_tuple_cost*0.75) + evaluate its quals; only survivors reach
			 * the transition calls */
			+ decoded * (cpu_tuple_cost * 0.75
						 + cpu_operator_cost * list_length(quals))
			+ surviving * cpu_operator_cost * (list_length(aggrefs) + 1);
		cpath->path.startup_cost = run;
		cpath->path.total_cost = run + cpu_tuple_cost * clamp_row_est(ngroups);
	}
	else
	{
		/*
		 * Metadata-only: near-zero cost so it wins for count() over a
		 * populated relation (the normal Agg-over-scan path remains as the
		 * fallback).
		 */
		cpath->path.startup_cost = 0.0;
		cpath->path.total_cost = 1.0 + cpu_operator_cost * base_rel->pages;
	}
	cpath->path.disabled_nodes = 0;

	add_path(output_rel, (Path *) cpath);
}

static Plan *
dbbc_agg_plan_custom_path(PlannerInfo *root, RelOptInfo *rel,
						  CustomPath *best_path, List *tlist,
						  List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	List	   *phys_tlist = DBBC_AGG_PRIV_TLIST(best_path->custom_private);

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

/*
 * Grouped-mode executor setup: parse the payload, fabricate the AggState,
 * and mirror ExecInitAgg's per-aggregate catalog setup (transition/final
 * FmgrInfos, initcond, ACL checks). The relation is already open (caller).
 */
static void
dbbc_grp_begin(DbbcAggScanState *as, List *payload, EState *estate, int eflags)
{
	List	   *keyexprs = (List *) linitial(payload);
	List	   *outmap = (List *) lsecond(payload);
	List	   *aggrefs = (List *) lthird(payload);
	ListCell   *lc;
	int			i;
	ExprContext *fakeecxt;
	List	   *keyinput = NIL;

	as->grouped = true;
	as->qual_clauses = (List *) lfourth(payload);
	as->qual_varno = (Index) intVal(list_nth(payload, 4));

	as->nkeys = list_length(keyexprs);
	Assert(as->nkeys <= DBBC_GRP_MAX_KEYS);
	as->key_exprstate = (ExprState **)
		palloc0(Max(as->nkeys, 1) * sizeof(ExprState *));
	as->has_key_exprs = false;
	i = 0;
	foreach(lc, keyexprs)
	{
		Node	   *kexpr = (Node *) lfirst(lc);

		if (IsA(kexpr, Var))
		{
			/* bare-Var key: read the column directly, no ExprState */
			as->key_attnos[i] = ((Var *) kexpr)->varattno;
		}
		else
		{
			/*
			 * Expression key: compiled and evaluated per row over key_slot.
			 * key_attnos[i] is Invalid so the consume loops take the expr path.
			 * Its input columns join the used/needed set (type-identity gate +
			 * version coverage) and are populated into key_slot per row.
			 */
			ListCell   *lv;

			as->key_attnos[i] = InvalidAttrNumber;
			as->has_key_exprs = true;
			foreach(lv, pull_var_clause(kexpr,
										PVC_RECURSE_AGGREGATES |
										PVC_RECURSE_WINDOWFUNCS |
										PVC_RECURSE_PLACEHOLDERS))
				keyinput = list_append_unique_int(keyinput,
												  ((Var *) lfirst(lv))->varattno);
		}
		i++;
	}

	as->nkey_input = list_length(keyinput);
	as->key_input_attnos = (AttrNumber *)
		palloc(Max(as->nkey_input, 1) * sizeof(AttrNumber));
	i = 0;
	foreach(lc, keyinput)
		as->key_input_attnos[i++] = (AttrNumber) lfirst_int(lc);

	as->noutcols = list_length(outmap);
	as->outmap = (int *) palloc(as->noutcols * sizeof(int));
	i = 0;
	foreach(lc, outmap)
		as->outmap[i++] = lfirst_int(lc);

	as->ntrans = list_length(aggrefs);
	as->trans = (DbbcAggTrans *) palloc0(Max(as->ntrans, 1) *
										 sizeof(DbbcAggTrans));
	as->in_vals = (Datum *) palloc0(Max(as->ntrans, 1) * sizeof(Datum));
	as->in_nulls = (bool *) palloc0(Max(as->ntrans, 1) * sizeof(bool));
	as->in_filterpass = (bool *) palloc0(Max(as->ntrans, 1) * sizeof(bool));

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * Expression group keys: build the per-row evaluation machinery. Placed
	 * after the EXPLAIN-only return because the scratch slot needs the relation
	 * descriptor and the relation is opened only for real execution. A
	 * relation-width virtual slot holds the input columns per row; ExecEvalExpr
	 * reads it via econtext->ecxt_scantuple (reset per row).
	 */
	if (as->has_key_exprs)
	{
		List	   *keyexprs2 = (List *) linitial(payload);

		/* EState-registered slot + econtext: freed with the plan, no leak */
		as->key_slot = ExecInitExtraTupleSlot(estate,
											  RelationGetDescr(as->rel),
											  &TTSOpsVirtual);
		as->key_econtext = CreateExprContext(estate);
		i = 0;
		foreach(lc, keyexprs2)
		{
			Node	   *kexpr = (Node *) lfirst(lc);

			if (!IsA(kexpr, Var))
				as->key_exprstate[i] = ExecInitExpr((Expr *) kexpr,
													(PlanState *) &as->css.ss.ps);
			i++;
		}
	}

	as->groupctx = AllocSetContextCreate(CurrentMemoryContext,
										 "dbblue_columnar group states",
										 ALLOCSET_DEFAULT_SIZES);
	as->tmpctx = AllocSetContextCreate(CurrentMemoryContext,
									   "dbblue_columnar agg transition",
									   ALLOCSET_SMALL_SIZES);

	/*
	 * Fabricated AggState: plain transition/final functions reach their
	 * state memory exclusively through AggCheckCallContext(), which reads
	 * only context->curaggcontext->ecxt_per_tuple_memory (the documented
	 * contract in src/backend/utils/fmgr/README). Point that at the
	 * per-group state context; everything else stays zeroed.
	 */
	as->fake_aggstate = makeNode(AggState);
	fakeecxt = makeNode(ExprContext);
	fakeecxt->ecxt_per_tuple_memory = as->groupctx;
	as->fake_aggstate->curaggcontext = fakeecxt;

	/*
	 * Runtime memory backstop: this node cannot spill, so bound the group
	 * hash by the same budget HashAgg would use (hash_mem_multiplier *
	 * work_mem). The plan-time estimate gate is the primary defense; this
	 * catches estimate errors (stale/correlated stats) before they OOM the
	 * backend - failing one query with a clear hint beats crashing the
	 * cluster into recovery.
	 */
	as->grp_mem_limit = get_hash_memory_limit();
	as->grp_since_memcheck = 0;

	/* per-aggregate catalog setup, mirroring ExecInitAgg */
	i = 0;
	foreach(lc, aggrefs)
	{
		Aggref	   *aggref = lfirst_node(Aggref, lc);
		DbbcAggTrans *t = &as->trans[i++];
		HeapTuple	aggTuple;
		HeapTuple	procTuple;
		Form_pg_aggregate aggform;
		Oid			transfn_oid;
		Oid			aggOwner;
		Datum		textInitVal;
		Oid			inputTypes[FUNC_MAX_ARGS];
		int			numArgs;
		Expr	   *transfnexpr = NULL;
		Expr	   *finalfnexpr = NULL;
		AclResult	aclresult;

		t->aggref = aggref;
		t->input_attno = InvalidAttrNumber;
		t->numTransInputs = 0;
		if (list_length(aggref->args) == 1)
		{
			Node	   *arg = (Node *)
				((TargetEntry *) linitial(aggref->args))->expr;

			while (IsA(arg, RelabelType))
				arg = (Node *) ((RelabelType *) arg)->arg;
			t->input_attno = ((Var *) arg)->varattno;
			t->numTransInputs = 1;
		}

		/*
		 * FILTER (WHERE ...): rebuild the per-agg qual list from the Aggref
		 * (which carries aggfilter through custom_private). It was validated as
		 * decomposable at plan time; here it must succeed. Lives in this
		 * (per-query) context for the scan's lifetime.
		 */
		if (!dbbc_agg_filter_decompose((Node *) aggref->aggfilter,
									   as->qual_varno, &t->filter, &t->nfilter,
									   NULL))
			elog(ERROR, "dbblue_columnar: aggregate FILTER no longer decomposes");

		/* the aggregate as the calling user, its fns as the owner */
		aclresult = object_aclcheck(ProcedureRelationId, aggref->aggfnoid,
									GetUserId(), ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_AGGREGATE,
						   get_func_name(aggref->aggfnoid));

		aggTuple = SearchSysCache1(AGGFNOID,
								   ObjectIdGetDatum(aggref->aggfnoid));
		if (!HeapTupleIsValid(aggTuple))
			elog(ERROR, "cache lookup failed for aggregate %u",
				 aggref->aggfnoid);
		aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
		transfn_oid = aggform->aggtransfn;
		t->finalfn_oid = aggform->aggfinalfn;

		procTuple = SearchSysCache1(PROCOID,
									ObjectIdGetDatum(aggref->aggfnoid));
		if (!HeapTupleIsValid(procTuple))
			elog(ERROR, "cache lookup failed for function %u",
				 aggref->aggfnoid);
		aggOwner = ((Form_pg_proc) GETSTRUCT(procTuple))->proowner;
		ReleaseSysCache(procTuple);

		aclresult = object_aclcheck(ProcedureRelationId, transfn_oid,
									aggOwner, ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_FUNCTION,
						   get_func_name(transfn_oid));
		if (OidIsValid(t->finalfn_oid))
		{
			aclresult = object_aclcheck(ProcedureRelationId, t->finalfn_oid,
										aggOwner, ACL_EXECUTE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, OBJECT_FUNCTION,
							   get_func_name(t->finalfn_oid));
		}

		numArgs = get_aggregate_argtypes(aggref, inputTypes);
		t->transtype = aggref->aggtranstype;
		get_typlenbyval(t->transtype, &t->transtypeLen, &t->transtypeByVal);
		get_typlenbyval(aggref->aggtype, &t->resulttypeLen,
						&t->resulttypeByVal);

		textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple,
									  Anum_pg_aggregate_agginitval,
									  &t->initValueIsNull);
		if (t->initValueIsNull)
			t->initValue = (Datum) 0;
		else
		{
			Oid			typinput;
			Oid			typioparam;
			char	   *strInitVal;

			getTypeInputInfo(t->transtype, &typinput, &typioparam);
			strInitVal = TextDatumGetCString(textInitVal);
			t->initValue = OidInputFunctionCall(typinput, strInitVal,
												typioparam, -1);
			pfree(strInitVal);
		}

		build_aggregate_transfn_expr(inputTypes, numArgs, 0,
									 aggref->aggvariadic,
									 t->transtype, aggref->inputcollid,
									 transfn_oid, InvalidOid,
									 &transfnexpr, NULL);
		fmgr_info(transfn_oid, &t->transfn);
		fmgr_info_set_expr((Node *) transfnexpr, &t->transfn);
		t->trans_fcinfo = (FunctionCallInfo)
			palloc0(SizeForFunctionCallInfo(t->numTransInputs + 1));
		InitFunctionCallInfoData(*t->trans_fcinfo, &t->transfn,
								 t->numTransInputs + 1, aggref->inputcollid,
								 (Node *) as->fake_aggstate, NULL);

		/* nodeAgg's strict-transfn/NULL-initcond compatibility rule */
		if (t->transfn.fn_strict && t->initValueIsNull &&
			(t->numTransInputs < 1 ||
			 !IsBinaryCoercible(inputTypes[0], t->transtype)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("aggregate %u needs to have compatible input type and transition type",
							aggref->aggfnoid)));

		if (OidIsValid(t->finalfn_oid))
		{
			t->numFinalArgs = aggform->aggfinalextra ? numArgs + 1 : 1;
			build_aggregate_finalfn_expr(inputTypes, t->numFinalArgs,
										 t->transtype, aggref->aggtype,
										 aggref->inputcollid,
										 t->finalfn_oid, &finalfnexpr);
			fmgr_info(t->finalfn_oid, &t->finalfn);
			fmgr_info_set_expr((Node *) finalfnexpr, &t->finalfn);
		}
		ReleaseSysCache(aggTuple);
	}

	/*
	 * Re-extract the pushed quals for our own per-row tests. Every clause was
	 * proven extractable at plan time; if catalog changes under a cached plan
	 * (e.g. a column type's default btree opclass was replaced - opclass
	 * membership does not invalidate the plan) make one no longer extractable,
	 * dbbc_extract_skip_quals would silently return fewer, and this node -
	 * having no parent recheck - would aggregate UNFILTERED rows. Refuse to
	 * run instead: a clear error beats a silently wrong total.
	 */
	if (as->qual_clauses != NIL)
	{
		as->nskipquals = dbbc_extract_skip_quals(as->qual_clauses,
												 as->qual_varno,
												 &as->skipquals);
		if (as->nskipquals != list_length(as->qual_clauses))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("dbblue_columnar: a pushed-down filter is no longer extractable"),
					 errhint("The plan is stale after a catalog change; re-plan the query (DISCARD PLANS) or SET dbblue_columnar.enable_columnar_scan = off.")));
	}

	/*
	 * Distinct referenced columns and their CURRENT catalog types: a block's
	 * chunk is used only if built under the same atttypid (the same
	 * staleness rule as zone-map skipping; a no-rewrite ALTER COLUMN TYPE
	 * leaves page LSNs unchanged, so the LSN proof alone cannot catch it).
	 */
	{
		List	   *used = NIL;
		int			k;
		int			a;
		int			u;

		for (k = 0; k < as->nkeys; k++)
			if (as->key_attnos[k] != InvalidAttrNumber)	/* bare-Var key */
				used = list_append_unique_int(used, as->key_attnos[k]);
		/* expression-key input columns (expr keys have key_attnos == Invalid) */
		for (k = 0; k < as->nkey_input; k++)
			used = list_append_unique_int(used, as->key_input_attnos[k]);
		for (a = 0; a < as->ntrans; a++)
		{
			int			f;

			if (as->trans[a].numTransInputs == 1)
				used = list_append_unique_int(used, as->trans[a].input_attno);
			/*
			 * FILTER columns must be in the used/used_types set too: they are
			 * read from columnar chunks (dbbc_chunk_read) and compared with a
			 * proc resolved for their CURRENT type, so the per-block
			 * type-identity gate (dbbc_grp_chunks_usable) must validate them -
			 * a no-rewrite ALTER COLUMN TYPE on a FILTER-only column would
			 * otherwise be served from a stale-typed chunk. This also puts them
			 * in `needed`, so dbbc_scan_bind_version requires them registered
			 * (else attno_to_col would be -1 and the read out of bounds).
			 */
			for (f = 0; f < as->trans[a].nfilter; f++)
				used = list_append_unique_int(used, as->trans[a].filter[f].attno);
		}
		for (a = 0; a < as->nskipquals; a++)
			used = list_append_unique_int(used, as->skipquals[a].attno);

		as->nused = list_length(used);
		as->used_attnos = (AttrNumber *)
			palloc(Max(as->nused, 1) * sizeof(AttrNumber));
		as->used_types = (Oid *) palloc(Max(as->nused, 1) * sizeof(Oid));
		u = 0;
		foreach(lc, used)
		{
			AttrNumber	attno = (AttrNumber) lfirst_int(lc);

			as->used_attnos[u] = attno;
			as->used_types[u] =
				TupleDescAttr(RelationGetDescr(as->rel), attno - 1)->atttypid;
			u++;
		}
	}
}

static void
dbbc_agg_begin(CustomScanState *node, EState *estate, int eflags)
{
	DbbcAggScanState *as = (DbbcAggScanState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	List	   *cp = cscan->custom_private;
	List	   *items = DBBC_AGG_PRIV_PAYLOAD(cp);
	Oid			reloid = DBBC_AGG_PRIV_RELOID(cp);
	ListCell   *lc;
	int			i;

	if (DBBC_AGG_PRIV_MODE(cp) == 1)
	{
		/* grouped mode; the executor holds the range-table lock already */
		if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
		{
			as->rel = table_open(reloid, NoLock);
			as->rel_opened = true;
		}
		dbbc_grp_begin(as, items, estate, eflags);
		return;
	}

	as->naggs = list_length(items);
	as->aggs = (DbbcAggItem *) palloc0(as->naggs * sizeof(DbbcAggItem));
	as->counts = (int64 *) palloc0(as->naggs * sizeof(int64));
	i = 0;
	foreach(lc, items)
	{
		List	   *it = (List *) lfirst(lc);
		DbbcAggItem *ai = &as->aggs[i++];

		ai->kind = (DbbcAggKind) intVal(linitial(it));
		ai->attno = (AttrNumber) intVal(lsecond(it));
		ai->collation = (Oid) intVal(lthird(it));
		ai->opcintype = (Oid) intVal(lfourth(it));
		ai->cmpproc = (Oid) intVal(list_nth(it, 4));
		ai->result_isnull = true;
		ai->has_result = false;
		if (ai->kind == DBBC_AGG_MIN || ai->kind == DBBC_AGG_MAX)
		{
			fmgr_info(ai->cmpproc, &ai->cmp);
			get_typlenbyval(ai->opcintype, &ai->typlen, &ai->typbyval);
		}
	}
	as->executed = false;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* durable home for MIN/MAX result datums (survives per-tuple resets) */
	as->aggctx = AllocSetContextCreate(CurrentMemoryContext,
									   "dbblue_columnar agg result",
									   ALLOCSET_SMALL_SIZES);

	/* the executor already holds the range-table lock on this relation */
	as->rel = table_open(reloid, NoLock);
	as->rel_opened = true;
}

/*
 * Fold one non-null candidate value into a MIN/MAX item (copied durably).
 *
 * On a tie (cmp == 0) the current accumulator is kept. When a type has values
 * that compare equal yet are distinguishable (numeric 4.0 vs 4.00, float -0.0
 * vs 0.0), WHICH representation MIN/MAX returns is unspecified by SQL, and
 * PostgreSQL's own answer already varies by plan (seqscan Agg vs index vs
 * parallel). So the returned value is always equal-by-ordering to the true
 * extremum; only the display form of an equal value may differ from a given
 * heap plan. This is an accepted, spec-permitted nuance, not a wrong result.
 */
static void
dbbc_minmax_combine(DbbcAggScanState *as, DbbcAggItem *item, Datum cand)
{
	if (!item->has_result)
	{
		MemoryContext old = MemoryContextSwitchTo(as->aggctx);

		item->result = datumCopy(cand, item->typbyval, item->typlen);
		MemoryContextSwitchTo(old);
		item->has_result = true;
		item->result_isnull = false;
		return;
	}
	{
		int32		c = DatumGetInt32(FunctionCall2Coll(&item->cmp,
														item->collation,
														cand, item->result));
		bool		replace = (item->kind == DBBC_AGG_MIN) ? (c < 0) : (c > 0);

		if (replace)
		{
			MemoryContext old = MemoryContextSwitchTo(as->aggctx);

			item->result = datumCopy(cand, item->typbyval, item->typlen);
			MemoryContextSwitchTo(old);
		}
	}
}

/*
 * Process a heap page range for ALL aggregates (always type-safe: current
 * catalog values via slot_getattr, combined with the current comparator).
 * With limited=false the whole relation is scanned with a plain, AM-generic
 * table scan - heap_setscanlimits is heap-only and must not be applied to a
 * non-heap scan descriptor (whole_rel_mode is entered exactly when the AM is
 * not heap, among other cases).
 */
static void
dbbc_agg_apply_heap_range(DbbcAggScanState *as, DbbcScanState *s,
						  BlockNumber start, BlockNumber nblocks, bool limited)
{
	TableScanDesc scan;

	if (limited)
	{
		scan = table_beginscan_strat(as->rel, s->snapshot, 0, NULL, true, false);
		heap_setscanlimits(scan, start, nblocks);
	}
	else
		scan = table_beginscan(as->rel, s->snapshot, 0, NULL, 0);
	while (table_scan_getnextslot(scan, ForwardScanDirection, s->heap_slot))
	{
		int			a;

		for (a = 0; a < as->naggs; a++)
		{
			DbbcAggItem *item = &as->aggs[a];
			bool		isnull;
			Datum		v;

			if (item->kind == DBBC_AGG_COUNT_STAR)
			{
				as->counts[a]++;
				continue;
			}
			v = slot_getattr(s->heap_slot, item->attno, &isnull);
			if (isnull)
				continue;
			if (item->kind == DBBC_AGG_COUNT_COL)
				as->counts[a]++;
			else
				dbbc_minmax_combine(as, item, v);
		}
	}
	table_endscan(scan);
}

/* can every MIN/MAX item's zone map be trusted for this block? (COUNT is
 * always metadata-safe; MIN/MAX need build-time type+collation to match) */
static bool
dbbc_agg_block_metadata_ok(DbbcAggScanState *as, DbbcColumnChunk *chunks,
						   int ncols)
{
	int			a;

	for (a = 0; a < as->naggs; a++)
	{
		DbbcAggItem *item = &as->aggs[a];
		DbbcColumnChunk *chunk = NULL;
		int			c;

		if (item->kind != DBBC_AGG_MIN && item->kind != DBBC_AGG_MAX)
			continue;
		for (c = 0; c < ncols; c++)
			if (chunks[c].attnum == item->attno)
			{
				chunk = &chunks[c];
				break;
			}
		if (chunk == NULL)
			return false;
		if (chunk->atttypid != item->opcintype ||
			chunk->attcollation != item->collation)
			return false;
	}
	return true;
}

/* ---------------- grouped mode (M5) executor ---------------- */

/* evaluate one extracted qual against a fetched value (both row sources) */
static bool
dbbc_skipqual_test(DbbcSkipQual *q, Datum value, bool isnull)
{
	if (q->kind == DBBC_SKIP_NULLTEST)
		return isnull == q->nulltest_isnull;
	if (isnull)
		return false;			/* strict operators never pass NULL */
	if (q->kind == DBBC_SKIP_OP)
	{
		int32		r = dbbc_skip_cmp(q, value, q->value);

		switch (q->strategy)
		{
			case BTLessStrategyNumber:
				return r < 0;
			case BTLessEqualStrategyNumber:
				return r <= 0;
			case BTEqualStrategyNumber:
				return r == 0;
			case BTGreaterEqualStrategyNumber:
				return r >= 0;
			case BTGreaterStrategyNumber:
				return r > 0;
		}
		return false;
	}
	else						/* DBBC_SKIP_SAOP_EQ */
	{
		int			e;

		for (e = 0; e < q->nelems; e++)
			if (dbbc_skip_cmp(q, value, q->elems[e]) == 0)
				return true;
		return false;
	}
}

/* find-or-create the group for the current row's key values */
static DbbcGroupEntry *
dbbc_grp_lookup(DbbcAggScanState *as, Datum *keyvals, bool *keynulls)
{
	DbbcGroupKey key;
	DbbcGroupEntry *entry;
	bool		found;
	int			k;
	int			a;

	memset(&key, 0, sizeof(key));
	for (k = 0; k < as->nkeys; k++)
	{
		if (keynulls[k])
			key.nullmask |= ((uint32) 1) << k;
		else
			key.vals[k] = keyvals[k];
	}
	entry = (DbbcGroupEntry *) hash_search(as->groups, &key,
										   HASH_ENTER, &found);
	if (!found)
	{
		/* initialize per-agg states, mirroring initialize_aggregate */
		for (a = 0; a < as->ntrans; a++)
		{
			DbbcAggTrans *t = &as->trans[a];
			DbbcTransState *st = &entry->states[a];

			if (t->initValueIsNull)
				st->transValue = (Datum) 0;
			else if (t->transtypeByVal)
				st->transValue = t->initValue;
			else
			{
				/* byref initcond: each group needs its own freeable copy */
				MemoryContext old = MemoryContextSwitchTo(as->groupctx);

				st->transValue = datumCopy(t->initValue, false,
										   t->transtypeLen);
				MemoryContextSwitchTo(old);
			}
			st->transValueIsNull = t->initValueIsNull;
			st->noTransValue = t->initValueIsNull;
		}

		/*
		 * Runtime memory backstop (no spill path). Probe the group context
		 * periodically - MemoryContextMemAllocated recurses, so amortize it -
		 * and refuse rather than grow unboundedly toward an OOM.
		 */
		if (++as->grp_since_memcheck >= 1024)
		{
			as->grp_since_memcheck = 0;
			if (MemoryContextMemAllocated(as->groupctx, true) >
				as->grp_mem_limit)
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("dbblue_columnar: grouped aggregate exceeded the hash memory limit"),
						 errhint("Too many groups for the in-node aggregate (it does not spill). ANALYZE the table, raise hash_mem_multiplier/work_mem, or SET dbblue_columnar.enable_columnar_scan = off.")));
		}
	}
	return entry;
}

/*
 * Advance every aggregate's transition state for the current row (inputs in
 * as->in_vals/in_nulls). Mirrors the EEOP_AGG_PLAIN_TRANS* interpreter steps:
 * strict transfns skip NULL inputs and NULL states; strict aggregates with a
 * NULL initcond adopt the first non-NULL input as their state (min/max); a
 * pass-by-ref transfn result that differs from the old state is copied into
 * the group context and the old state freed. internal-transtype aggregates
 * (avg/sum over numeric or bigint) report as by-value pointers here and
 * manage their own state memory via AggCheckCallContext -> groupctx.
 */
static void
dbbc_grp_advance(DbbcAggScanState *as, DbbcGroupEntry *grp)
{
	int			a;

	for (a = 0; a < as->ntrans; a++)
	{
		DbbcAggTrans *t = &as->trans[a];
		DbbcTransState *st = &grp->states[a];
		FunctionCallInfo fcinfo = t->trans_fcinfo;
		MemoryContext oldctx;
		Datum		newVal;

		/*
		 * FILTER (WHERE ...): this row does not contribute to this aggregate.
		 * The group itself was still created by dbbc_grp_lookup, so a group all
		 * of whose rows are filtered out still appears with the empty aggregate
		 * - matching SQL FILTER semantics.
		 */
		if (t->nfilter > 0 && !as->in_filterpass[a])
			continue;

		if (t->numTransInputs == 1)
		{
			if (t->transfn.fn_strict)
			{
				if (as->in_nulls[a])
					continue;	/* strict transfn skips NULL inputs */
				if (st->noTransValue)
				{
					/*
					 * First non-NULL input becomes the state, copied into
					 * the group context (ExecAggInitGroup; the input type is
					 * binary-coercible to the transtype, checked at Begin).
					 */
					oldctx = MemoryContextSwitchTo(as->groupctx);
					st->transValue = datumCopy(as->in_vals[a],
											   t->transtypeByVal,
											   t->transtypeLen);
					MemoryContextSwitchTo(oldctx);
					st->transValueIsNull = false;
					st->noTransValue = false;
					continue;
				}
			}
			fcinfo->args[1].value = as->in_vals[a];
			fcinfo->args[1].isnull = as->in_nulls[a];
		}

		/* a strict transfn is never called with a NULL state */
		if (t->transfn.fn_strict && st->transValueIsNull)
			continue;

		oldctx = MemoryContextSwitchTo(as->tmpctx);
		fcinfo->args[0].value = st->transValue;
		fcinfo->args[0].isnull = st->transValueIsNull;
		fcinfo->isnull = false; /* just in case the transfn doesn't set it */
		newVal = FunctionCallInvoke(fcinfo);

		/*
		 * Pass-by-ref state: if the transfn returned a new datum, copy it
		 * into the group context and free the old one (mirrors
		 * ExecAggPlainTransByRef / ExecAggCopyTransValue; datumCopy flattens
		 * expanded datums, and builtin plain aggregates never hand back a
		 * R/W expanded object).
		 */
		if (!t->transtypeByVal &&
			DatumGetPointer(newVal) != DatumGetPointer(st->transValue))
		{
			if (!fcinfo->isnull)
			{
				MemoryContextSwitchTo(as->groupctx);
				newVal = datumCopy(newVal, false, t->transtypeLen);
			}
			else
				newVal = (Datum) 0;
			if (!st->transValueIsNull)
				pfree(DatumGetPointer(st->transValue));
		}
		st->transValue = newVal;
		st->transValueIsNull = fcinfo->isnull;
		MemoryContextSwitchTo(oldctx);
	}
}

/* may this valid block's chunks feed keys/inputs/quals? (type identity) */
static bool
dbbc_grp_chunks_usable(DbbcAggScanState *as, DbbcScanState *s,
					   DbbcColumnChunk *chunks)
{
	int			u;

	for (u = 0; u < as->nused; u++)
	{
		int			c = s->attno_to_col[as->used_attnos[u] - 1];

		if (c < 0 || chunks[c].atttypid != as->used_types[u])
			return false;
	}
	return true;
}

/* aggregate every qualifying row of one servable columnar block */
static void
dbbc_grp_consume_block(DbbcAggScanState *as, DbbcScanState *s,
					   DbbcBlock *block, DbbcColumnChunk *chunks)
{
	uint32		row;
	int			c;

	s->cur_block = block;
	s->cur_chunks = chunks;
	for (c = 0; c < s->ncols; c++)
		dbbc_col_ptrs_set(s, c);

	for (row = 0; row < block->nrows; row++)
	{
		Datum		keyvals[DBBC_GRP_MAX_KEYS];
		bool		keynulls[DBBC_GRP_MAX_KEYS];
		MemoryContext oldctx;
		bool		pass = true;
		int			i;
		int			k;
		int			a;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Do the whole per-row evaluation in tmpctx: qual comparisons and
		 * transition-function calls can detoast/allocate (bttextcmp,
		 * numeric_*), and without this those allocations would accumulate in
		 * the query context for the entire input pass. Transition STATE is
		 * written to groupctx by the callees, so it survives the reset.
		 */
		MemoryContextReset(as->tmpctx);
		oldctx = MemoryContextSwitchTo(as->tmpctx);

		for (i = 0; i < as->nskipquals; i++)
		{
			DbbcSkipQual *q = &as->skipquals[i];
			int			qc = s->attno_to_col[q->attno - 1];
			bool		isnull;
			Datum		v;

			v = dbbc_chunk_read(s, qc, row, &isnull);
			if (!dbbc_skipqual_test(q, v, isnull))
			{
				pass = false;
				break;
			}
		}
		if (pass)
		{
			if (as->has_key_exprs)
			{
				/* load the expr-key input columns into the scratch slot */
				int			j;

				ResetExprContext(as->key_econtext);
				ExecClearTuple(as->key_slot);
				for (j = 0; j < as->nkey_input; j++)
				{
					AttrNumber	at = as->key_input_attnos[j];
					int			ic = s->attno_to_col[at - 1];

					as->key_slot->tts_values[at - 1] =
						dbbc_chunk_read(s, ic, row,
										&as->key_slot->tts_isnull[at - 1]);
				}
				ExecStoreVirtualTuple(as->key_slot);
				as->key_econtext->ecxt_scantuple = as->key_slot;
			}
			for (k = 0; k < as->nkeys; k++)
			{
				if (as->key_attnos[k] != InvalidAttrNumber)
				{
					int			kc = s->attno_to_col[as->key_attnos[k] - 1];

					keyvals[k] = dbbc_chunk_read(s, kc, row, &keynulls[k]);
				}
				else
					keyvals[k] = ExecEvalExpr(as->key_exprstate[k],
											  as->key_econtext, &keynulls[k]);
			}
			for (a = 0; a < as->ntrans; a++)
			{
				DbbcAggTrans *t = &as->trans[a];
				int			f;

				if (t->numTransInputs == 1)
				{
					int			ac = s->attno_to_col[t->input_attno - 1];

					as->in_vals[a] = dbbc_chunk_read(s, ac, row,
													 &as->in_nulls[a]);
				}
				/* FILTER (WHERE ...): advance this agg only if every qual passes */
				as->in_filterpass[a] = true;
				for (f = 0; f < t->nfilter; f++)
				{
					DbbcSkipQual *q = &t->filter[f];
					int			fc = s->attno_to_col[q->attno - 1];
					bool		fn;
					Datum		fv = dbbc_chunk_read(s, fc, row, &fn);

					if (!dbbc_skipqual_test(q, fv, fn))
					{
						as->in_filterpass[a] = false;
						break;
					}
				}
			}
			dbbc_grp_advance(as, dbbc_grp_lookup(as, keyvals, keynulls));
			as->grp_rows++;
		}
		MemoryContextSwitchTo(oldctx);
	}
}

/*
 * Aggregate a heap page range (invalid/unbuilt/grown ranges, chunks built
 * under a different column type, or - with limited=false - the whole
 * relation when no usable version exists / the AM is not heap). All quals
 * are evaluated here too: this node has no parent recheck.
 */
static void
dbbc_grp_consume_heap_range(DbbcAggScanState *as, DbbcScanState *s,
							BlockNumber start, BlockNumber nblocks,
							bool limited)
{
	TableScanDesc scan;

	if (limited)
	{
		scan = table_beginscan_strat(as->rel, s->snapshot, 0, NULL,
									 true, false);
		heap_setscanlimits(scan, start, nblocks);
	}
	else
		scan = table_beginscan(as->rel, s->snapshot, 0, NULL, 0);
	while (table_scan_getnextslot(scan, ForwardScanDirection, s->heap_slot))
	{
		Datum		keyvals[DBBC_GRP_MAX_KEYS];
		bool		keynulls[DBBC_GRP_MAX_KEYS];
		MemoryContext oldctx;
		bool		pass = true;
		int			i;
		int			k;
		int			a;

		CHECK_FOR_INTERRUPTS();

		/* per-row scratch in tmpctx; state goes to groupctx (see the block
		 * consumer for the rationale) */
		MemoryContextReset(as->tmpctx);
		oldctx = MemoryContextSwitchTo(as->tmpctx);

		for (i = 0; i < as->nskipquals; i++)
		{
			DbbcSkipQual *q = &as->skipquals[i];
			bool		isnull;
			Datum		v;

			v = slot_getattr(s->heap_slot, q->attno, &isnull);
			if (!dbbc_skipqual_test(q, v, isnull))
			{
				pass = false;
				break;
			}
		}
		if (pass)
		{
			if (as->has_key_exprs)
			{
				/* the heap slot already holds all columns; point the expr
				 * context at it directly (no separate scratch slot needed) */
				ResetExprContext(as->key_econtext);
				as->key_econtext->ecxt_scantuple = s->heap_slot;
			}
			for (k = 0; k < as->nkeys; k++)
			{
				if (as->key_attnos[k] != InvalidAttrNumber)
					keyvals[k] = slot_getattr(s->heap_slot, as->key_attnos[k],
											  &keynulls[k]);
				else
					keyvals[k] = ExecEvalExpr(as->key_exprstate[k],
											  as->key_econtext, &keynulls[k]);
			}
			for (a = 0; a < as->ntrans; a++)
			{
				DbbcAggTrans *t = &as->trans[a];
				int			f;

				if (t->numTransInputs == 1)
					as->in_vals[a] = slot_getattr(s->heap_slot,
												  t->input_attno,
												  &as->in_nulls[a]);
				/* FILTER (WHERE ...): advance this agg only if every qual passes */
				as->in_filterpass[a] = true;
				for (f = 0; f < t->nfilter; f++)
				{
					DbbcSkipQual *q = &t->filter[f];
					bool		fn;
					Datum		fv = slot_getattr(s->heap_slot, q->attno, &fn);

					if (!dbbc_skipqual_test(q, fv, fn))
					{
						as->in_filterpass[a] = false;
						break;
					}
				}
			}
			dbbc_grp_advance(as, dbbc_grp_lookup(as, keyvals, keynulls));
			as->grp_rows++;
		}
		MemoryContextSwitchTo(oldctx);
	}
	table_endscan(scan);
	as->grp_ranges_heap++;
}

/* the single input pass: build the group hash from all sources */
static void
dbbc_grp_build(DbbcAggScanState *as, EState *estate)
{
	DbbcScanState scratch;
	List	   *needed = NIL;
	uint32		cur;
	int			u;

	if (as->groups == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(DbbcGroupKey);
		ctl.entrysize = offsetof(DbbcGroupEntry, states) +
			as->ntrans * sizeof(DbbcTransState);
		ctl.hcxt = as->groupctx;
		as->groups = hash_create("dbblue_columnar groups", 256, &ctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	for (u = 0; u < as->nused; u++)
		needed = lappend_int(needed, as->used_attnos[u]);

	/* reuse the standard scan setup for version pin, dir, heap slot, etc.
	 * (this node is never parallel: pin the current version here) */
	memset(&scratch, 0, sizeof(scratch));
	scratch.css.ss.ss_currentRelation = as->rel;
	dbbc_scan_common(&scratch, as->rel, estate);
	{
		DbbcRelVersion *v = (as->rel->rd_rel->relam == HEAP_TABLE_AM_OID)
			? dbbc_version_pin_tracked(RelationGetRelid(as->rel)) : NULL;

		dbbc_scan_bind_version(&scratch, as->rel, needed, as->qual_clauses,
							   as->qual_varno, v, false);
	}

	if (scratch.whole_rel_mode)
		dbbc_grp_consume_heap_range(as, &scratch, 0, scratch.heap_nblocks,
									false);
	else
	{
		for (cur = 0; cur < scratch.total_slots; cur++)
		{
			DbbcBlock  *block = dbbc_slot_block(&scratch, cur);
			DbbcColumnChunk *chunks = NULL;

			CHECK_FOR_INTERRUPTS();

			if (block != NULL && dbbc_block_valid(&scratch, as->rel, block))
				chunks = (DbbcColumnChunk *)
					dsa_get_address(scratch.dsa, block->chunks);

			if (chunks != NULL && dbbc_grp_chunks_usable(as, &scratch, chunks))
			{
				/*
				 * Zone-map skip: a valid block provably containing no
				 * qual-matching row contributes nothing to any group.
				 */
				if (scratch.nskipquals > 0 &&
					dbbc_zone_block_skippable(scratch.skipquals,
											  scratch.nskipquals,
											  block, chunks, scratch.ncols))
				{
					as->grp_blocks_skipped++;
					continue;
				}
				dbbc_grp_consume_block(as, &scratch, block, chunks);
				as->grp_blocks_served++;
			}
			else
			{
				BlockNumber start = cur * DBBC_PAGES_PER_BLOCK;
				BlockNumber nblk = Min((BlockNumber) DBBC_PAGES_PER_BLOCK,
									   scratch.heap_nblocks - start);

				/* limited: reached only with a valid heap version */
				dbbc_grp_consume_heap_range(as, &scratch, start, nblk, true);
			}
		}
	}

	if (BufferIsValid(scratch.vm_check_buf))
		ReleaseBuffer(scratch.vm_check_buf);
	if (scratch.version != NULL)
		dbbc_version_unpin_tracked(scratch.version);

	/* scalar aggregation over zero rows still yields one output row */
	if (as->nkeys == 0 && hash_get_num_entries(as->groups) == 0)
	{
		Datum		dummyv[1];
		bool		dummyn[1];

		(void) dbbc_grp_lookup(as, dummyv, dummyn);
	}
}

/* finalize and emit one group, mirroring finalize_aggregate */
static TupleTableSlot *
dbbc_grp_emit_group(DbbcAggScanState *as, DbbcGroupEntry *grp)
{
	TupleTableSlot *slot = as->css.ss.ss_ScanTupleSlot;
	ExprContext *econtext = as->css.ss.ps.ps_ExprContext;
	MemoryContext oldctx;
	int			i;

	/* finalfns run (and byref results live) in the output-tuple context */
	ResetExprContext(econtext);
	oldctx = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	ExecClearTuple(slot);
	for (i = 0; i < as->noutcols; i++)
	{
		if (as->outmap[i] >= 0)
		{
			int			k = as->outmap[i];

			slot->tts_values[i] = grp->key.vals[k];
			slot->tts_isnull[i] =
				(grp->key.nullmask & (((uint32) 1) << k)) != 0;
		}
		else
		{
			int			a = -as->outmap[i] - 1;
			DbbcAggTrans *t = &as->trans[a];
			DbbcTransState *st = &grp->states[a];

			if (OidIsValid(t->finalfn_oid))
			{
				LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS);
				bool		anynull = st->transValueIsNull;
				int			narg;

				InitFunctionCallInfoData(*fcinfo, &t->finalfn,
										 t->numFinalArgs,
										 t->aggref->inputcollid,
										 (Node *) as->fake_aggstate, NULL);
				fcinfo->args[0].value =
					MakeExpandedObjectReadOnly(st->transValue,
											   st->transValueIsNull,
											   t->transtypeLen);
				fcinfo->args[0].isnull = st->transValueIsNull;
				for (narg = 1; narg < t->numFinalArgs; narg++)
				{
					/* aggfinalextra positions are NULL dummies */
					fcinfo->args[narg].value = (Datum) 0;
					fcinfo->args[narg].isnull = true;
					anynull = true;
				}
				if (t->finalfn.fn_strict && anynull)
				{
					slot->tts_values[i] = (Datum) 0;
					slot->tts_isnull[i] = true;
				}
				else
				{
					fcinfo->isnull = false;
					slot->tts_values[i] = FunctionCallInvoke(fcinfo);
					slot->tts_isnull[i] = fcinfo->isnull;
					slot->tts_values[i] =
						MakeExpandedObjectReadOnly(slot->tts_values[i],
												   slot->tts_isnull[i],
												   t->resulttypeLen);
				}
			}
			else
			{
				/* no finalfn: the result IS the state (may alias groupctx,
				 * which outlives every emitted tuple) */
				slot->tts_values[i] =
					MakeExpandedObjectReadOnly(st->transValue,
											   st->transValueIsNull,
											   t->transtypeLen);
				slot->tts_isnull[i] = st->transValueIsNull;
			}
		}
	}
	MemoryContextSwitchTo(oldctx);
	ExecStoreVirtualTuple(slot);
	return slot;
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

	if (as->grouped)
	{
		DbbcGroupEntry *grp;

		if (!as->executed)
		{
			dbbc_grp_build(as, node->ss.ps.state);
			hash_seq_init(&as->seq, as->groups);
			as->seq_active = true;
			as->executed = true;
		}
		if (!as->seq_active)
			return NULL;
		grp = (DbbcGroupEntry *) hash_seq_search(&as->seq);
		if (grp == NULL)
		{
			as->seq_active = false;
			return NULL;
		}
		return dbbc_grp_emit_group(as, grp);
	}

	if (as->executed)
		return NULL;
	as->executed = true;

	/*
	 * Columns feeding COUNT(col)/MIN/MAX must be covered by the current
	 * version, else their metadata is unavailable; pass them so a stale plan
	 * (version changed since planning) degrades to whole-relation heap scan.
	 */
	for (a = 0; a < as->naggs; a++)
		if (as->aggs[a].kind != DBBC_AGG_COUNT_STAR)
			needed = list_append_unique_int(needed, as->aggs[a].attno);

	/* reuse the standard scan setup for version pin, dir, heap slot, etc.
	 * (the aggregate node is never parallel: pin the current version here) */
	memset(&scratch, 0, sizeof(scratch));
	scratch.css.ss.ss_currentRelation = as->rel;
	dbbc_scan_common(&scratch, as->rel, node->ss.ps.state);
	{
		DbbcRelVersion *v = (as->rel->rd_rel->relam == HEAP_TABLE_AM_OID)
			? dbbc_version_pin_tracked(RelationGetRelid(as->rel)) : NULL;

		dbbc_scan_bind_version(&scratch, as->rel, needed, NIL, 0, v, false);
	}

	if (scratch.whole_rel_mode)
	{
		/*
		 * No usable version (stale plan, or non-heap AM): scan the whole
		 * relation with a plain AM-generic scan (no heap_setscanlimits).
		 */
		dbbc_agg_apply_heap_range(as, &scratch, 0, scratch.heap_nblocks, false);
	}
	else
	{
		for (cur = 0; cur < scratch.total_slots; cur++)
		{
			DbbcBlock  *block = dbbc_slot_block(&scratch, cur);
			DbbcColumnChunk *chunks = NULL;

			CHECK_FOR_INTERRUPTS();

			if (block != NULL && dbbc_block_valid(&scratch, as->rel, block))
				chunks = (DbbcColumnChunk *)
					dsa_get_address(scratch.dsa, block->chunks);

			/*
			 * Use block metadata only when the block is valid AND every
			 * MIN/MAX item's zone map is type/collation-consistent; else read
			 * the whole range from the heap (always type-safe).
			 */
			if (chunks != NULL &&
				dbbc_agg_block_metadata_ok(as, chunks, scratch.ncols))
			{
				for (a = 0; a < as->naggs; a++)
				{
					DbbcAggItem *item = &as->aggs[a];
					int			c;

					if (item->kind == DBBC_AGG_COUNT_STAR)
					{
						as->counts[a] += block->nrows;
						continue;
					}
					for (c = 0; c < scratch.ncols; c++)
						if (chunks[c].attnum == item->attno)
						{
							DbbcColumnChunk *ck = &chunks[c];

							if (item->kind == DBBC_AGG_COUNT_COL)
								as->counts[a] += block->nrows - ck->null_count;
							else if (ck->has_minmax)	/* MIN/MAX */
								dbbc_minmax_combine(as, item,
													dbbc_chunk_minmax_datum(ck,
																			item->kind == DBBC_AGG_MAX));
							break;
						}
				}
			}
			else
			{
				BlockNumber start = cur * DBBC_PAGES_PER_BLOCK;
				BlockNumber nblk = Min((BlockNumber) DBBC_PAGES_PER_BLOCK,
									   scratch.heap_nblocks - start);

				/* limited: reached only with a valid heap version */
				dbbc_agg_apply_heap_range(as, &scratch, start, nblk, true);
			}
		}
	}

	if (BufferIsValid(scratch.vm_check_buf))
		ReleaseBuffer(scratch.vm_check_buf);
	if (scratch.version != NULL)
		dbbc_version_unpin_tracked(scratch.version);

	/* emit the single result row, in output order */
	ExecClearTuple(slot);
	for (a = 0; a < as->naggs; a++)
	{
		DbbcAggItem *item = &as->aggs[a];

		if (item->kind == DBBC_AGG_MIN || item->kind == DBBC_AGG_MAX)
		{
			slot->tts_values[a] = item->has_result ? item->result : (Datum) 0;
			slot->tts_isnull[a] = !item->has_result;
		}
		else
		{
			slot->tts_values[a] = Int64GetDatum(as->counts[a]);
			slot->tts_isnull[a] = false;
		}
	}
	ExecStoreVirtualTuple(slot);
	return slot;
}

static void
dbbc_agg_end(CustomScanState *node)
{
	DbbcAggScanState *as = (DbbcAggScanState *) node;

	if (as->seq_active)
	{
		hash_seq_term(&as->seq);
		as->seq_active = false;
	}
	if (as->groups != NULL)
	{
		hash_destroy(as->groups);
		as->groups = NULL;
	}
	if (as->groupctx != NULL)
	{
		MemoryContextDelete(as->groupctx);
		as->groupctx = NULL;
	}
	if (as->tmpctx != NULL)
	{
		MemoryContextDelete(as->tmpctx);
		as->tmpctx = NULL;
	}
	if (as->rel_opened)
	{
		table_close(as->rel, NoLock);
		as->rel_opened = false;
	}
	if (as->aggctx != NULL)
	{
		MemoryContextDelete(as->aggctx);
		as->aggctx = NULL;
	}
}

static void
dbbc_agg_rescan(CustomScanState *node)
{
	DbbcAggScanState *as = (DbbcAggScanState *) node;
	int			a;

	if (as->grouped)
	{
		/*
		 * The grouped node's output depends on no parameter (only Const
		 * quals, plain-column keys/inputs), so a rescan yields identical
		 * groups. Keep the built hash and just rewind the emission cursor
		 * rather than rebuilding (like Material). Rebuilding would re-pin the
		 * version and register a fresh heap slot in es_tupleTable on every
		 * rescan - an unbounded leak under an unmaterialized nestloop inner.
		 */
		if (as->seq_active)
		{
			hash_seq_term(&as->seq);
			as->seq_active = false;
		}
		if (as->executed && as->groups != NULL)
		{
			hash_seq_init(&as->seq, as->groups);
			as->seq_active = true;
		}
		return;
	}

	as->executed = false;
	if (as->counts)
		memset(as->counts, 0, as->naggs * sizeof(int64));
	for (a = 0; a < as->naggs; a++)
	{
		as->aggs[a].has_result = false;
		as->aggs[a].result_isnull = true;
	}
	if (as->aggctx != NULL)
		MemoryContextReset(as->aggctx);
}

static void
dbbc_agg_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	DbbcAggScanState *as = (DbbcAggScanState *) node;

	if (!as->grouped)
	{
		ExplainPropertyText("Columnar Aggregate", "metadata (count/min/max)",
							es);
		return;
	}

	ExplainPropertyText("Columnar Aggregate", "grouped (transition pushdown)",
						es);
	if (es->analyze)
	{
		ExplainPropertyInteger("Columnar Agg Blocks Served", NULL,
							   as->grp_blocks_served, es);
		ExplainPropertyInteger("Columnar Agg Blocks Skipped", NULL,
							   as->grp_blocks_skipped, es);
		ExplainPropertyInteger("Columnar Agg Heap Ranges", NULL,
							   as->grp_ranges_heap, es);
		ExplainPropertyInteger("Columnar Agg Rows", NULL, as->grp_rows, es);
		if (as->groups != NULL)
			ExplainPropertyInteger("Columnar Agg Groups", NULL,
								   hash_get_num_entries(as->groups), es);
	}
}

static void
dbbc_agg_init(void)
{
	RegisterCustomScanMethods(&dbbc_agg_scan_methods);
	prev_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = dbbc_create_upper_paths;
}
