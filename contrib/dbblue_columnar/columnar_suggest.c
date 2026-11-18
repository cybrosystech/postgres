/*-------------------------------------------------------------------------
 *
 * columnar_suggest.c
 *		DBblue Columnar Engine - workload advisor.
 *
 * Answers "which columns of which table should be columnarized?" from the
 * workload the server actually runs, rather than leaving an admin to guess
 * and then discover the answer from dbblue_columnar.log_coverage_misses one
 * table at a time.
 *
 * HOW THE EVIDENCE IS GATHERED
 *
 * Two planner hooks already run on every query for other reasons, and both
 * already compute most of what the advisor needs:
 *
 *   set_rel_pathlist_hook  -> dbbc_sugg_observe_scan.  Fires for every heap
 *		base relation. dbbc_rel_shape() has, by this point, established that
 *		the relation is a shape the engine could serve and produced the exact
 *		set of columns the scan must read - target list plus every restriction
 *		clause. That set IS the bundle.
 *
 *   create_upper_paths_hook -> dbbc_sugg_observe_agg.  Fires at the grouped
 *		aggregate stage. This is what distinguishes a report that would reach
 *		grouped-aggregate pushdown (the large win) from one that would only
 *		get row-serve (a modest one), and it is recorded as a separate counter
 *		rather than inferred from "the query had an aggregate".
 *
 * Both are installed BEFORE the dbblue_columnar.enabled / enable_columnar_scan
 * gates in their respective hooks. That is deliberate and is the whole point
 * of the feature: enabled defaults to off, so an advisor that only collected
 * once the engine was already running would produce nothing for exactly the
 * admin who has not adopted it yet and is trying to decide whether to.
 *
 * WHAT IS NOT INFERRED HERE
 *
 * The collector records observations, never conclusions. In particular it
 * does not claim an acceleration "tier": whether grouped-aggregate pushdown
 * would really be reachable depends on gates this code cannot evaluate for a
 * relation that has no column store yet, and on a memory-aware gate that can
 * decline at execution time. So nagg is counted separately from nplans and the
 * interpretation is left to the view, where it can be corrected without a
 * catalog migration.
 *
 * COST DISCIPLINE
 *
 * The hot path allocates nothing, waits for nothing, and touches no catalog
 * beyond the syscache lookups the surrounding code already performed. A
 * contended stripe lock or a full slot table drops the observation and bumps
 * a counter - advice is best-effort, a user's query is not. The counters are
 * exposed so that "no suggestions" can always be told apart from "the
 * collector never got to run", which is otherwise an unfalsifiable silence.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/extension.h"
#include "common/hashfn.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "dbblue_columnar.h"

/* GUCs owned by this file but defined in dbblue_columnar.c */
bool		dbblue_columnar_suggestions = false;
int			dbblue_columnar_suggestion_slots = 4096;
int			dbblue_columnar_suggest_min_pages = 1024;	/* 8 MB */

static DbbcSuggControl *dbbc_sugg = NULL;

/* the extension's own tables, resolved lazily; see dbbc_sugg_is_self */
#define DBBC_SUGG_NSELF		3
static Oid	dbbc_sugg_self_oid[DBBC_SUGG_NSELF];
static bool dbbc_sugg_self_valid = false;

/*
 * Size of the shared area. Rounded up to a whole multiple of the stripe count
 * so every stripe owns the same number of slots and the index arithmetic in
 * the hot path needs no bounds special-case.
 */
static int
dbbc_sugg_nslots(void)
{
	int			n = dbblue_columnar_suggestion_slots;

	if (n <= 0)
		return 0;
	if (n < DBBC_SUGG_NSTRIPES)
		n = DBBC_SUGG_NSTRIPES;
	n -= n % DBBC_SUGG_NSTRIPES;
	return n;
}

static Size
dbbc_sugg_size(void)
{
	return add_size(offsetof(DbbcSuggControl, slots),
					mul_size((Size) dbbc_sugg_nslots(),
							 sizeof(DbbcSuggEntry)));
}

static void
dbbc_sugg_shmem_request(void *arg)
{
	if (dbbc_sugg_nslots() == 0)
		return;

	/*
	 * The name is only pointer-copied by the shmem machinery, so it must be a
	 * string literal with static storage duration, never a psprintf'd buffer.
	 */
	ShmemRequestStruct(.name = "dbblue_columnar suggestions",
					   .size = dbbc_sugg_size(),
					   .ptr = (void **) &dbbc_sugg);
}

static void
dbbc_sugg_shmem_init(void *arg)
{
	int			tranche;
	int			nslots = dbbc_sugg_nslots();
	int			i;

	if (nslots == 0 || dbbc_sugg == NULL)
		return;

	/* exactly one tranche id for the whole stripe array */
	tranche = LWLockNewTrancheId("dbblue_columnar_suggest");

	memset(dbbc_sugg, 0, dbbc_sugg_size());

	for (i = 0; i < DBBC_SUGG_NSTRIPES; i++)
		LWLockInitialize(&dbbc_sugg->stripe[i].lock, tranche);

	pg_atomic_init_u64(&dbbc_sugg->observations, 0);
	pg_atomic_init_u64(&dbbc_sugg->recorded, 0);
	pg_atomic_init_u64(&dbbc_sugg->contended_drops, 0);
	pg_atomic_init_u64(&dbbc_sugg->full_drops, 0);
	pg_atomic_init_u64(&dbbc_sugg->noqueryid_drops, 0);
	pg_atomic_init_u32(&dbbc_sugg->generation, 1);

	dbbc_sugg->nslots = nslots;
	dbbc_sugg->slots_per_stripe = nslots / DBBC_SUGG_NSTRIPES;
	dbbc_sugg->last_reset = GetCurrentTimestamp();
}

static const ShmemCallbacks dbbc_sugg_shmem_callbacks = {
	.request_fn = dbbc_sugg_shmem_request,
	.init_fn = dbbc_sugg_shmem_init,
};

void
dbbc_sugg_shmem_init_request(void)
{
	RegisterShmemCallbacks(&dbbc_sugg_shmem_callbacks);
}

/*
 * Is this the extension's own bookkeeping? The flush writes these tables via
 * SPI, so observing them would have the advisor recommending that it
 * columnarize its own output.
 *
 * Matched by table OID, NOT by schema. Excluding the extension's whole schema
 * looks equivalent and is not: the extension is typically installed into
 * public, which is also where an application's tables live, so a schema-wide
 * test silently discards every observation on the very tables the advisor
 * exists to advise about.
 *
 * Resolved once per backend and cached without invalidation: if the extension
 * is dropped and recreated, a stale OID costs at most one stray suggestion
 * row, never a wrong answer.
 */
static bool
dbbc_sugg_is_self(Oid relid)
{
	static const char *const self_tables[] = {
		"dbblue_columnar_relations",
		"dbblue_columnar_suggestions",
		"dbblue_columnar_suggestion_columns",
	};
	int			i;

	if (!dbbc_sugg_self_valid)
	{
		Oid			extoid = get_extension_oid("dbblue_columnar", true);
		Oid			nspoid = OidIsValid(extoid)
			? get_extension_schema(extoid) : InvalidOid;

		for (i = 0; i < DBBC_SUGG_NSELF; i++)
			dbbc_sugg_self_oid[i] = OidIsValid(nspoid)
				? get_relname_relid(self_tables[i], nspoid) : InvalidOid;
		dbbc_sugg_self_valid = true;
	}

	for (i = 0; i < DBBC_SUGG_NSELF; i++)
	{
		if (OidIsValid(dbbc_sugg_self_oid[i]) &&
			dbbc_sugg_self_oid[i] == relid)
			return true;
	}
	return false;
}

/* is the advisor able to record anything at all right now? */
static inline bool
dbbc_sugg_active(void)
{
	return (dbbc_sugg != NULL &&
			dbbc_sugg->nslots > 0 &&
			dbblue_columnar_suggestions);
}

/*
 * Accumulate one observation of (relation, query shape).
 *
 * attrs is the raw pull_varattnos bitmap (members are attno -
 * FirstLowInvalidHeapAttributeNumber); it may be NULL for the aggregate-stage
 * observation, which only bumps the tier counter of an already-known bundle.
 */
static void
dbbc_sugg_record(PlannerInfo *root, RelOptInfo *rel, Oid reloid,
				 Bitmapset *attrs, int unsupported, bool from_agg)
{
	uint64		attmap[DBBC_SUGG_ATTWORDS];
	uint64		filtmap[DBBC_SUGG_ATTWORDS];
	bool		att_overflow = false;
	bool		has_rls = false;
	int64		queryid;
	int64		width = 0;
	uint32		hash;
	uint32		stripe;
	uint32		base;
	uint32		start;
	uint32		gen;
	int			i;
	int			x;
	int			victim = -1;
	uint32		victim_gen = PG_UINT32_MAX;
	DbbcSuggEntry *slot = NULL;
	ListCell   *lc;
	TimestampTz now;

	/*
	 * Only the scan path counts as an observation. The aggregate-stage call
	 * merely annotates a bundle the scan path already established, so
	 * counting it here would make observations exceed recorded by an amount
	 * no drop counter explains - and reconciling those two numbers is the
	 * whole reason the counters exist.
	 */
	if (!from_agg)
		pg_atomic_fetch_add_u64(&dbbc_sugg->observations, 1);

	/*
	 * The query fingerprint is the shape key. A zero id means query-id
	 * computation is off, in which case every distinct report on a table
	 * would collapse into one over-wide bundle whose column set is the union
	 * of all of them - worse than useless, because applying that union costs
	 * far more memory than any single report needs. Drop instead, and count
	 * it so the reason for an empty suggestion list is visible.
	 */
	queryid = root->parse->queryId;
	if (queryid == 0)
	{
		pg_atomic_fetch_add_u64(&dbbc_sugg->noqueryid_drops, 1);
		return;
	}

	memset(attmap, 0, sizeof(attmap));
	memset(filtmap, 0, sizeof(filtmap));

	/* the columns the scan must read: this is the atomic bundle */
	x = -1;
	while (attrs != NULL && (x = bms_next_member(attrs, x)) >= 0)
	{
		AttrNumber	attno = x + FirstLowInvalidHeapAttributeNumber;

		if (attno <= 0)
		{
			/*
			 * A system column or whole-row reference. The scan gate rejects
			 * these outright, and that rejection is load-bearing for
			 * EvalPlanQual, so such a shape can never be accelerated no
			 * matter what is registered.
			 */
			unsupported = DBBC_SUGG_UNSUP_SYSCOL;
			continue;
		}
		if (attno >= DBBC_SUGG_ATTBITS)
		{
			att_overflow = true;
			continue;
		}
		attmap[attno / 64] |= UINT64CONST(1) << (attno % 64);
	}

	/*
	 * Columns read by a restriction clause are worth more than columns merely
	 * projected: they are what makes zone-map block skipping work. Tracked as
	 * a subset of the bundle rather than a separate bundle.
	 */
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		Bitmapset  *fattrs = NULL;

		if (rinfo->security_level > 0)
			has_rls = true;

		pull_varattnos((Node *) rinfo->clause, rel->relid, &fattrs);
		x = -1;
		while ((x = bms_next_member(fattrs, x)) >= 0)
		{
			AttrNumber	attno = x + FirstLowInvalidHeapAttributeNumber;

			if (attno > 0 && attno < DBBC_SUGG_ATTBITS)
				filtmap[attno / 64] |= UINT64CONST(1) << (attno % 64);
		}
		bms_free(fattrs);
	}

	/*
	 * Projected width of the bundle.
	 *
	 * rel->attr_widths cannot be used on its own: the planner fills it from
	 * the target list, so a column referenced ONLY by a restriction clause
	 * has width 0 there. Those are exactly the columns the advisor rates
	 * highest (they are what makes zone-map skipping work), so trusting
	 * attr_widths alone costs every filter column as free and understates the
	 * footprint of precisely the bundles most likely to be applied.
	 *
	 * Hence the ladder: the planner's own width when it has one, else the
	 * column's average width from pg_statistic, else the type's nominal
	 * width for a column that has never been analyzed.
	 */
	for (i = 1; i < DBBC_SUGG_ATTBITS; i++)
	{
		int32		w = 0;

		if (!(attmap[i / 64] & (UINT64CONST(1) << (i % 64))))
			continue;

		if (rel->attr_widths != NULL &&
			i >= rel->min_attr && i <= rel->max_attr)
			w = rel->attr_widths[i - rel->min_attr];

		if (w <= 0)
			w = get_attavgwidth(reloid, (AttrNumber) i);
		if (w <= 0)
		{
			Oid			atttypid;
			int32		atttypmod;
			Oid			attcoll;

			get_atttypetypmodcoll(reloid, (AttrNumber) i,
								  &atttypid, &atttypmod, &attcoll);
			w = get_typavgwidth(atttypid, atttypmod);
		}
		width += Max(w, 0);
	}

	/*
	 * Hash the bundle key. Done before taking any lock; the stripe is derived
	 * from the high half and the in-stripe start index from the low half, so
	 * the two are not correlated.
	 */
	hash = hash_combine(hash_combine(hash_uint32((uint32) MyDatabaseId),
									 hash_uint32((uint32) reloid)),
						hash_bytes((const unsigned char *) &queryid,
								   sizeof(queryid)));
	stripe = hash & (DBBC_SUGG_NSTRIPES - 1);
	base = stripe * dbbc_sugg->slots_per_stripe;
	start = (hash >> 8) % dbbc_sugg->slots_per_stripe;

	/*
	 * Never wait. The planner is on the other side of this lock; a stalled
	 * plan is a far worse outcome than a lost observation, and observations
	 * repeat by nature.
	 */
	if (!LWLockConditionalAcquire(&dbbc_sugg->stripe[stripe].lock,
								  LW_EXCLUSIVE))
	{
		pg_atomic_fetch_add_u64(&dbbc_sugg->contended_drops, 1);
		return;
	}

	gen = pg_atomic_read_u32(&dbbc_sugg->generation);

	for (i = 0; i < DBBC_SUGG_PROBE; i++)
	{
		uint32		idx = base + ((start + i) % dbbc_sugg->slots_per_stripe);
		DbbcSuggEntry *cand = &dbbc_sugg->slots[idx];

		if (!cand->inuse)
		{
			slot = cand;
			break;
		}
		if (cand->dboid == MyDatabaseId && cand->reloid == reloid &&
			cand->queryid == queryid)
		{
			slot = cand;
			break;
		}
		/* remember the stalest occupant in case we must evict */
		if (cand->gen_last_seen < victim_gen)
		{
			victim_gen = cand->gen_last_seen;
			victim = (int) idx;
		}
	}

	/*
	 * The aggregate-stage observation only attributes a tier to a bundle the
	 * scan stage already established. It carries no column set, so claiming a
	 * fresh slot here would persist a bundle with an empty column list -
	 * unapplyable advice. If the scan stage never recorded this shape (it
	 * failed a shape gate, or was evicted), drop the tier observation.
	 */
	if (from_agg && (slot == NULL || !slot->inuse))
	{
		/*
		 * Not a drop: the scan stage legitimately declines shapes the
		 * aggregate stage still sees (below the size floor, ineligible, or
		 * evicted). Nothing to annotate, so nothing to report.
		 */
		LWLockRelease(&dbbc_sugg->stripe[stripe].lock);
		return;
	}

	if (slot == NULL)
	{
		/*
		 * The probe window is full of other bundles. Evict the one least
		 * recently confirmed by a flush, but only if it predates the current
		 * generation - never evict a bundle that is also live right now, or a
		 * hot table with many shapes would displace its own evidence.
		 */
		if (victim >= 0 && victim_gen < gen)
		{
			slot = &dbbc_sugg->slots[victim];
			slot->inuse = false;
		}
		else
		{
			LWLockRelease(&dbbc_sugg->stripe[stripe].lock);
			pg_atomic_fetch_add_u64(&dbbc_sugg->full_drops, 1);
			return;
		}
	}

	now = GetCurrentTimestamp();

	if (!slot->inuse)
	{
		memset(slot, 0, sizeof(*slot));
		slot->inuse = true;
		slot->dboid = MyDatabaseId;
		slot->reloid = reloid;
		slot->queryid = queryid;
		slot->first_seen = now;
	}

	slot->gen_last_seen = gen;
	slot->last_seen = now;

	/*
	 * The aggregate stage contributes ONE fact - that this shape was read
	 * under a grouped aggregate - and nothing else. Every quantitative field
	 * belongs to the scan path, which is the only caller that carries a
	 * column set.
	 *
	 * Letting the annotation fall through into the accumulation below is a
	 * trap worth naming: it would count each aggregate plan twice in nplans,
	 * and - because this path has no columns and so contributes width 0 -
	 * would drag the bundle's mean width down, making an aggregate report
	 * look CHEAPER to columnarize the more often it ran.
	 */
	if (from_agg)
	{
		slot->nagg++;
		LWLockRelease(&dbbc_sugg->stripe[stripe].lock);
		return;
	}

	slot->nplans++;
	slot->rows_sum += (int64) Max(rel->rows, 0);
	slot->tuples_sum += (int64) Max(rel->tuples, 0);
	slot->pages_sum += (int64) rel->pages;
	slot->width_sum += width;
	slot->allvisfrac_ppm = (uint32) (Max(Min(rel->allvisfrac, 1.0), 0.0) * 1000000.0);
	slot->nquals_last = (uint16) Min(list_length(rel->baserestrictinfo),
									 PG_UINT16_MAX);

	for (i = 0; i < DBBC_SUGG_ATTWORDS; i++)
	{
		slot->attmap[i] |= attmap[i];
		slot->filtmap[i] |= filtmap[i];
	}
	if (att_overflow)
		slot->att_overflow = true;
	if (has_rls)
		slot->has_rls = true;
	if (unsupported != DBBC_SUGG_OK)
		slot->unsupported = (uint8) unsupported;

	LWLockRelease(&dbbc_sugg->stripe[stripe].lock);
	pg_atomic_fetch_add_u64(&dbbc_sugg->recorded, 1);
}

/*
 * Base-relation observation, called from dbbc_set_rel_pathlist BEFORE the
 * enabled/enable_columnar_scan gate.
 *
 * attrs is the bitmap dbbc_rel_shape already built; passing it in avoids a
 * second pull_varattnos over the same expressions. unsupported carries the
 * reason the shape can never be served, or DBBC_SUGG_OK.
 */
void
dbbc_sugg_observe_scan(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte,
					   Bitmapset *attrs, bool shape_ok, int unsupported)
{
	if (!dbbc_sugg_active())
		return;

	/*
	 * Structural non-candidates: dbbc_rel_shape rejected this without setting
	 * a reason, meaning it is not a base-table scan at all - a partition or
	 * inheritance CHILD (RELOPT_OTHER_MEMBER_REL), a subquery, a function
	 * scan. These must be dropped rather than recorded, because they would
	 * land as bundles with no columns and no explanation: querying one
	 * partitioned table would fill the admin's view with a zero-column row
	 * per partition, all marked "eligible", which is worse than saying
	 * nothing. The PARENT still gets recorded below, carrying the
	 * inheritance reason, so the admin learns why the table cannot be
	 * accelerated.
	 */
	if (!shape_ok && unsupported == DBBC_SUGG_OK)
		return;

	/*
	 * A standby can accumulate advice it can never persist - the flush is an
	 * INSERT. Collecting there would fill the table with evidence that is
	 * silently discarded at promotion.
	 */
	if (RecoveryInProgress())
		return;

	if (root->parse == NULL)
		return;

	/* provably-empty relation: rows/tuples are zeroed, so the evidence lies */
	if (IS_DUMMY_REL(rel))
		return;

	/*
	 * Size floor. Columnar acceleration is an IO and per-tuple-deform play;
	 * on a small table the heap is already fast and the store would spend
	 * budget for nothing. Measured in pages rather than rows so a wide table
	 * qualifies sooner than a narrow one with the same row count.
	 *
	 * An inheritance/partition parent is the exception: its rel->pages stays
	 * 0 (the planner leaves it zero to avoid double-counting the children in
	 * total_table_pages) while rel->tuples IS the summed child count. Testing
	 * pages alone would therefore drop every partitioned table below the
	 * floor, and the admin would get silence instead of the "partitioned,
	 * cannot be accelerated" answer they need. Fall back to a rows-equivalent
	 * floor for that case, deliberately coarse - it only decides whether the
	 * table is worth mentioning, not what it would cost.
	 */
	if (rel->pages > 0
		? rel->pages < (BlockNumber) dbblue_columnar_suggest_min_pages
		: rel->tuples < (double) dbblue_columnar_suggest_min_pages * 32.0)
		return;

	if (IsCatalogRelationOid(rte->relid))
		return;
	if (dbbc_sugg_is_self(rte->relid))
		return;

	/*
	 * populate hard-rejects anything but a permanent relation, so advising it
	 * would produce a suggestion that cannot be applied.
	 */
	if (get_rel_persistence(rte->relid) != RELPERSISTENCE_PERMANENT)
		return;

	dbbc_sugg_record(root, rel, rte->relid, attrs, unsupported, false);
}

/*
 * Grouped-aggregate observation, called from dbbc_create_upper_paths BEFORE
 * its enabled gate. Bumps the aggregate-stage counter of the bundles already
 * recorded for the input relation's base rels, which is what separates a
 * report that could reach aggregate pushdown from one that could only be
 * row-served.
 *
 * Only single-baserel inputs are attributed: with a join below the aggregate
 * there is no single relation the pushdown would belong to, and guessing one
 * would inflate whichever side happened to be probed.
 */
void
dbbc_sugg_observe_agg(PlannerInfo *root, RelOptInfo *input_rel)
{
	int			relid;
	RelOptInfo *rel;
	RangeTblEntry *rte;

	if (!dbbc_sugg_active())
		return;
	if (RecoveryInProgress())
		return;
	if (root->parse == NULL)
		return;

	if (bms_membership(input_rel->relids) != BMS_SINGLETON)
		return;
	relid = bms_singleton_member(input_rel->relids);
	if (relid <= 0 || relid >= root->simple_rel_array_size)
		return;

	rel = root->simple_rel_array[relid];
	rte = root->simple_rte_array[relid];
	if (rel == NULL || rte == NULL || rte->rtekind != RTE_RELATION)
		return;
	if (rel->reloptkind != RELOPT_BASEREL || IS_DUMMY_REL(rel))
		return;
	if (rel->pages < (BlockNumber) dbblue_columnar_suggest_min_pages)
		return;
	if (IsCatalogRelationOid(rte->relid) || dbbc_sugg_is_self(rte->relid))
		return;

	/*
	 * No attrs bitmap and no unsupported reason: the scan-stage observation
	 * owns the bundle's column set. This call only attributes the tier, and
	 * must not create a bundle with an empty column set if the scan stage
	 * never ran (which happens when the relation failed a shape gate).
	 */
	dbbc_sugg_record(root, rel, rte->relid, NULL, DBBC_SUGG_OK, true);
}

/*
 * ---------------------------------------------------------------------------
 * Flush: drain the shared accumulator into the suggestion tables.
 *
 * Deliberately a plain SQL-callable function rather than something only the
 * background worker can do. The worker connects to at most ONE database and
 * only when dbblue_columnar.autorefresh_database is set, so a worker-only
 * flush would make suggestions unavailable on a default install - and, just
 * as importantly, untestable under `make check`, which has no way to wait for
 * a background worker's next pass. The worker calls this same function when
 * it is running; the two paths cannot diverge.
 *
 * A flush is per-database by construction: it drains only the slots belonging
 * to MyDatabaseId, because the tables it writes into are this database's.
 * Slots owned by other databases are left untouched for their own flush.
 * ---------------------------------------------------------------------------
 */

/*
 * Schema-qualified name of one of the extension's tables. Returns NULL when
 * the extension is not installed here, so callers can no-op instead of
 * erroring - the collector is cluster-wide and will happily accumulate
 * evidence in a database that never created the extension.
 */
static char *
dbbc_sugg_table_name(const char *relname)
{
	Oid			extoid = get_extension_oid("dbblue_columnar", true);
	Oid			nspoid;

	if (!OidIsValid(extoid))
		return NULL;
	nspoid = get_extension_schema(extoid);
	if (!OidIsValid(nspoid))
		return NULL;

	return quote_qualified_identifier(get_namespace_name(nspoid), relname);
}

/* expand one attnum bitmap into a sorted int2vector-ish array literal */
static void
dbbc_sugg_attmap_append(StringInfo buf, const uint64 *map)
{
	bool		first = true;
	int			i;

	appendStringInfoString(buf, "{");
	for (i = 1; i < DBBC_SUGG_ATTBITS; i++)
	{
		if (!(map[i / 64] & (UINT64CONST(1) << (i % 64))))
			continue;
		appendStringInfo(buf, "%s%d", first ? "" : ",", i);
		first = false;
	}
	appendStringInfoString(buf, "}");
}

static const char *
dbbc_sugg_unsup_name(uint8 code)
{
	switch (code)
	{
		case DBBC_SUGG_OK:
			return "eligible";
		case DBBC_SUGG_UNSUP_RELKIND:
			return "relkind";
		case DBBC_SUGG_UNSUP_INH:
			return "inheritance";
		case DBBC_SUGG_UNSUP_AM:
			return "access_method";
		case DBBC_SUGG_UNSUP_SYSCOL:
			return "system_column";
		case DBBC_SUGG_UNSUP_PERSIST:
			return "persistence";
		case DBBC_SUGG_UNSUP_SAMPLE:
			return "tablesample";
		case DBBC_SUGG_UNSUP_LATERAL:
			return "lateral";
		case DBBC_SUGG_UNSUP_PARTED:
			return "partitioned";
		default:
			return "unknown";
	}
}

int64
dbbc_sugg_flush(void)
{
	char	   *bundles;
	char	   *columns;
	int64		nflushed = 0;
	uint32		gen;
	uint32		i;
	bool		own_spi = false;

	if (dbbc_sugg == NULL || dbbc_sugg->nslots == 0)
		return 0;
	if (RecoveryInProgress())
		return 0;

	bundles = dbbc_sugg_table_name("dbblue_columnar_suggestions");
	columns = dbbc_sugg_table_name("dbblue_columnar_suggestion_columns");
	if (bundles == NULL || columns == NULL)
		return 0;				/* extension not installed in this database */

	/*
	 * Bump the generation FIRST. Slots touched after this point carry the new
	 * generation and so are protected from eviction by the "never evict a
	 * bundle that is live right now" rule; slots we are about to persist get
	 * stamped with it as we go.
	 */
	gen = pg_atomic_fetch_add_u32(&dbbc_sugg->generation, 1) + 1;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "dbblue_columnar: SPI_connect failed");
	own_spi = true;

	for (i = 0; i < dbbc_sugg->nslots; i++)
	{
		DbbcSuggEntry local;
		uint32		stripe = i / dbbc_sugg->slots_per_stripe;
		StringInfoData sql;
		StringInfoData atts;
		StringInfoData filts;
		int			ret;

		/*
		 * Copy the slot out under its stripe lock and do every fallible thing
		 * afterwards. SPI can ereport, and an LWLock held across an error
		 * would be released only by the abort path - but a StringInfo palloc
		 * inside the lock would already have widened the window for no
		 * reason. Blocking (not conditional) acquisition is right here: this
		 * is a background pass, not a planner.
		 */
		LWLockAcquire(&dbbc_sugg->stripe[stripe].lock, LW_EXCLUSIVE);
		if (!dbbc_sugg->slots[i].inuse ||
			dbbc_sugg->slots[i].dboid != MyDatabaseId)
		{
			LWLockRelease(&dbbc_sugg->stripe[stripe].lock);
			continue;
		}
		local = dbbc_sugg->slots[i];
		dbbc_sugg->slots[i].gen_last_seen = gen;
		LWLockRelease(&dbbc_sugg->stripe[stripe].lock);

		/*
		 * A relation dropped since it was observed leaves a slot whose reloid
		 * no longer resolves. Skip it rather than writing a row whose regclass
		 * renders as a bare OID: unlike dbblue_columnar_relations, whose stale
		 * rows are a registration the admin may still want to see, a stale
		 * suggestion is pure noise.
		 */
		if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(local.reloid)))
			continue;

		initStringInfo(&atts);
		initStringInfo(&filts);
		dbbc_sugg_attmap_append(&atts, local.attmap);
		dbbc_sugg_attmap_append(&filts, local.filtmap);

		/*
		 * Sums are replaced, not added: the slot holds the running lifetime
		 * total, so the upsert is idempotent and a flush at any cadence gives
		 * the same answer. first_seen is kept at its earliest known value
		 * because a slot evicted and re-claimed would otherwise appear newer
		 * than the evidence actually is.
		 */
		initStringInfo(&sql);
		appendStringInfo(&sql,
						 "INSERT INTO %s AS s "
						 "(relid, queryid, nplans, nagg, rows_sum, tuples_sum, "
						 " pages_sum, width_sum, allvisfrac, nquals, att_overflow, "
						 " has_rls, eligibility, first_seen, last_seen) "
						 "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, "
						 "        $13, $14, $15) "
						 "ON CONFLICT (relid, queryid) DO UPDATE SET "
						 " nplans = EXCLUDED.nplans, "
						 " nagg = EXCLUDED.nagg, rows_sum = EXCLUDED.rows_sum, "
						 " tuples_sum = EXCLUDED.tuples_sum, pages_sum = EXCLUDED.pages_sum, "
						 " width_sum = EXCLUDED.width_sum, allvisfrac = EXCLUDED.allvisfrac, "
						 " nquals = EXCLUDED.nquals, att_overflow = EXCLUDED.att_overflow, "
						 " has_rls = EXCLUDED.has_rls, eligibility = EXCLUDED.eligibility, "
						 " first_seen = LEAST(s.first_seen, EXCLUDED.first_seen), "
						 " last_seen = EXCLUDED.last_seen",
						 bundles);

		{
			Oid			argtypes[15] = {
				OIDOID, INT8OID, INT8OID, INT4OID, INT8OID, INT8OID,
				INT8OID, INT8OID, FLOAT8OID, INT4OID, BOOLOID, BOOLOID,
				TEXTOID, TIMESTAMPTZOID, TIMESTAMPTZOID
			};
			Datum		values[15];
			char		nulls[15];

			memset(nulls, ' ', sizeof(nulls));
			values[0] = ObjectIdGetDatum(local.reloid);
			values[1] = Int64GetDatum(local.queryid);
			values[2] = Int64GetDatum(local.nplans);
			values[3] = Int32GetDatum((int32) local.nagg);
			values[4] = Int64GetDatum(local.rows_sum);
			values[5] = Int64GetDatum(local.tuples_sum);
			values[6] = Int64GetDatum(local.pages_sum);
			values[7] = Int64GetDatum(local.width_sum);
			values[8] = Float8GetDatum((double) local.allvisfrac_ppm / 1000000.0);
			values[9] = Int32GetDatum((int32) local.nquals_last);
			values[10] = BoolGetDatum(local.att_overflow);
			values[11] = BoolGetDatum(local.has_rls);
			values[12] = CStringGetTextDatum(dbbc_sugg_unsup_name(local.unsupported));
			values[13] = TimestampTzGetDatum(local.first_seen);
			values[14] = TimestampTzGetDatum(local.last_seen);

			ret = SPI_execute_with_args(sql.data, 15, argtypes, values, nulls,
										false, 0);
			if (ret != SPI_OK_INSERT)
				elog(ERROR, "dbblue_columnar: suggestion upsert failed (%d)", ret);
		}

		/*
		 * Replace the member-column rows wholesale. The bundle's column set
		 * only ever grows within a slot's life, but an evict-and-reclaim can
		 * shrink it, and a stale extra column would make the bundle look more
		 * expensive to apply than it is.
		 */
		{
			Oid			argtypes[4] = {OIDOID, INT8OID, TEXTOID, TEXTOID};
			Datum		values[4];
			char		nulls[4] = {' ', ' ', ' ', ' '};

			values[0] = ObjectIdGetDatum(local.reloid);
			values[1] = Int64GetDatum(local.queryid);
			values[2] = CStringGetTextDatum(atts.data);
			values[3] = CStringGetTextDatum(filts.data);

			/*
			 * DELETE then INSERT as two SEPARATE statements. Folding them into
			 * one "WITH del AS (DELETE ...) INSERT ..." is the obvious-looking
			 * form and is wrong: a data-modifying CTE and the enclosing query
			 * run against the same snapshot, so the INSERT cannot see the
			 * DELETE and collides with the very rows being removed. It fails
			 * only on the SECOND flush of a bundle, when there is finally
			 * something to delete.
			 */
			resetStringInfo(&sql);
			appendStringInfo(&sql,
							 "DELETE FROM %s WHERE relid = $1 AND queryid = $2",
							 columns);
			ret = SPI_execute_with_args(sql.data, 2, argtypes, values, nulls,
										false, 0);
			if (ret != SPI_OK_DELETE)
				elog(ERROR, "dbblue_columnar: suggestion column delete failed (%d)",
					 ret);

			resetStringInfo(&sql);
			appendStringInfo(&sql,
							 "INSERT INTO %s (relid, queryid, attnum, is_filter) "
							 "SELECT $1, $2, a, a = ANY($4::smallint[]) "
							 "FROM unnest($3::smallint[]) a",
							 columns);
			ret = SPI_execute_with_args(sql.data, 4, argtypes, values, nulls,
										false, 0);
			if (ret != SPI_OK_INSERT)
				elog(ERROR, "dbblue_columnar: suggestion column insert failed (%d)",
					 ret);
		}

		pfree(sql.data);
		pfree(atts.data);
		pfree(filts.data);
		nflushed++;
	}

	if (own_spi)
		SPI_finish();

	return nflushed;
}

/*
 * dbblue_columnar_suggestions_flush() -> bigint
 *
 * Drain this database's accumulated evidence into the suggestion tables and
 * return the number of bundles written.
 */
PG_FUNCTION_INFO_V1(dbblue_columnar_suggestions_flush);

Datum
dbblue_columnar_suggestions_flush(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(dbbc_sugg_flush());
}

/*
 * dbblue_columnar_suggestion_stats() -> record
 *
 * Collector health. Exists so that an empty suggestion list is always
 * diagnosable: no observations means the hooks never saw a qualifying
 * relation, noqueryid_drops means compute_query_id is off, and nonzero
 * contended/full drops mean the slot table is too small for the workload.
 */
PG_FUNCTION_INFO_V1(dbblue_columnar_suggestion_stats);

Datum
dbblue_columnar_suggestion_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[9];
	bool		nulls[9];
	int			live = 0;
	uint32		i;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	memset(nulls, false, sizeof(nulls));

	if (dbbc_sugg == NULL || dbbc_sugg->nslots == 0)
	{
		values[0] = BoolGetDatum(false);
		values[1] = Int32GetDatum(0);
		values[2] = Int32GetDatum(0);
		memset(&nulls[3], true, sizeof(bool) * 6);
		PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
	}

	for (i = 0; i < dbbc_sugg->nslots; i++)
	{
		uint32		stripe = i / dbbc_sugg->slots_per_stripe;

		LWLockAcquire(&dbbc_sugg->stripe[stripe].lock, LW_SHARED);
		if (dbbc_sugg->slots[i].inuse)
			live++;
		LWLockRelease(&dbbc_sugg->stripe[stripe].lock);
	}

	values[0] = BoolGetDatum(dbblue_columnar_suggestions);
	values[1] = Int32GetDatum((int32) dbbc_sugg->nslots);
	values[2] = Int32GetDatum(live);
	values[3] = Int64GetDatum((int64) pg_atomic_read_u64(&dbbc_sugg->observations));
	values[4] = Int64GetDatum((int64) pg_atomic_read_u64(&dbbc_sugg->recorded));
	values[5] = Int64GetDatum((int64) pg_atomic_read_u64(&dbbc_sugg->noqueryid_drops));
	values[6] = Int64GetDatum((int64) pg_atomic_read_u64(&dbbc_sugg->contended_drops));
	values[7] = Int64GetDatum((int64) pg_atomic_read_u64(&dbbc_sugg->full_drops));
	values[8] = TimestampTzGetDatum(dbbc_sugg->last_reset);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
