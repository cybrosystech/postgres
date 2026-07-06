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
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/tupmacs.h"
#include "catalog/pg_am_d.h"
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
#include "optimizer/restrictinfo.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/predicate.h"
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
	uint64		ranges_heap;
	uint64		rows_columnar;
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

void
dbbc_scan_init(void)
{
	RegisterCustomScanMethods(&dbbc_scan_methods);

	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = dbbc_set_rel_pathlist;
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
	CustomPath *cpath;
	QualCost	qcost;

	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);

	if (!dbblue_columnar_enabled || !dbblue_columnar_enable_columnar_scan)
		return;

	if (!dbbc_rel_ready(root, rel, rte, &needed))
		return;

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
	 * Milestone-2 costing: the scan still visits every heap page (validity
	 * checks pin page headers; fallback reads pages in full), so charge full
	 * I/O like a seqscan; the saving is per-tuple CPU on columnar-served
	 * rows (no tuple deforming). Real costing (zone-map skip fractions)
	 * arrives with predicate pushdown.
	 */
	cost_qual_eval(&qcost, rel->baserestrictinfo, root);
	cpath->path.disabled_nodes = 0;
	cpath->path.startup_cost = qcost.startup;
	cpath->path.total_cost = qcost.startup +
		seq_page_cost * rel->pages +
		(cpu_tuple_cost * 0.75 + qcost.per_tuple) * rel->tuples;

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
dbbc_begin_scan(CustomScanState *node, EState *estate, int eflags)
{
	DbbcScanState *s = (DbbcScanState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	Relation	rel = node->ss.ss_currentRelation;

	s->needed_attnos = (List *) linitial(cscan->custom_private);

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

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

			foreach(lc, s->needed_attnos)
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

			s->dir = (dsa_pointer *) dsa_get_address(s->dsa,
													 s->version->blockdir);
			s->ndirslots = s->version->ndirslots;
			s->ncols = s->version->ncols;
			s->attnums = (int16 *) palloc(s->ncols * sizeof(int16));
			memcpy(s->attnums, reg, s->ncols * sizeof(int16));
			s->col_ptrs = (DbbcColPtrs *)
				palloc(s->ncols * sizeof(DbbcColPtrs));
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
		DbbcColumnChunk *chunk = &s->cur_chunks[c];
		DbbcColPtrs *ptrs = &s->col_ptrs[c];
		int			attidx = chunk->attnum - 1;

		if (ptrs->nulls != NULL &&
			(ptrs->nulls[row / 8] & (1 << (row % 8))) != 0)
			continue;			/* stays NULL */

		if (chunk->attlen > 0)
		{
			uint8	   *ptr = ptrs->values + (Size) row * chunk->attlen;

			if (chunk->attbyval)
				slot->tts_values[attidx] = fetch_att(ptr, true, chunk->attlen);
			else
				slot->tts_values[attidx] = PointerGetDatum(ptr);
		}
		else
		{
			uint32		off = ((uint32 *) ptrs->values)[row];

			if (off == DBBC_VAR_NULL_OFFSET)
				continue;		/* defensive; bitmap should have caught it */
			slot->tts_values[attidx] = PointerGetDatum(ptrs->blob + off);
		}
		slot->tts_isnull[attidx] = false;
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
			if (s->cur_row < s->cur_block->nrows)
				return dbbc_emit_row(s);
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
				int			c;

				s->cur_block = block;
				s->cur_chunks = (DbbcColumnChunk *)
					dsa_get_address(s->dsa, block->chunks);
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
		ExplainPropertyInteger("Heap Fallback Ranges", NULL,
							   (int64) s->ranges_heap, es);
		ExplainPropertyInteger("Columnar Rows", NULL,
							   (int64) s->rows_columnar, es);
	}
}
