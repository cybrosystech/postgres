/*-------------------------------------------------------------------------
 *
 * dbblue_columnar.h
 *		DBblue Columnar Engine - shared definitions.
 *
 * Block format (Milestone 2, v1):
 *
 * A columnar block covers a fixed range of DBBC_PAGES_PER_BLOCK consecutive
 * heap pages. A block is built only when every heap page in its range is
 * all-visible (visibility map) at build time; the page LSN of every page is
 * recorded in the block header at that moment. The block is servable later
 * iff every page is still all-visible AND its current page LSN equals the
 * recorded one -- any modification (or a vacuum that changed the page after a
 * modification) advances the LSN and disqualifies the block, so a columnar
 * read can never diverge from a heap scan. Blocks that fail the check are
 * simply not served; the scan reads those pages from the heap instead.
 *
 * Within a block, each registered column has one chunk. v1 stores chunks
 * PLAIN (no compression), but every chunk carries an encoding tag so later
 * encodings (dictionary, RLE, bit-pack, delta) can be added without changing
 * the surrounding structure:
 *   - fixed-length types: a packed array with stride = attlen; only
 *     attlen in {1,2,4,8,16} is accepted (enforced at populate), so element i
 *     is naturally aligned given the MAXALIGNed base allocation. Types with
 *     other fixed widths (macaddr=6, timetz=12) are rejected: packing them at
 *     stride attlen would misalign every element after the first.
 *   - varlena types: a uint32 offset per row into a concatenated blob of
 *     fully-detoasted varlena values (self-describing, header included);
 *     NULL rows carry DBBC_VAR_NULL_OFFSET. Every value is stored at a
 *     MAXALIGN'd offset (pad bytes are zero), so blob+offset is directly
 *     usable as a varlena Datum on every platform, including for
 *     double-aligned types (int8range etc.) whose internals assume a
 *     maximally-aligned datum start.
 * NULLs are additionally tracked in a per-chunk bitmap (present only when the
 * chunk has at least one NULL). Each chunk also carries a zone map (min/max
 * for types with a btree comparator, plus a null count) used for block
 * skipping.
 *
 * All block data lives in a session-attachable DSA registered through the
 * dsm_registry ("dbblue_columnar" area); it is a cache: never WAL-logged,
 * never dumped, empty after restart.
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBBLUE_COLUMNAR_H
#define DBBLUE_COLUMNAR_H

#include "postgres.h"

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "lib/dshash.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "port/atomics.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/relcache.h"

/* heap pages per columnar block */
#define DBBC_PAGES_PER_BLOCK	32

/* offset sentinel for NULL rows in varlena chunks */
#define DBBC_VAR_NULL_OFFSET	PG_UINT32_MAX

/*
 * Ceiling on one block's detoasted varlena bytes per column. Blocks whose
 * TOASTed data would exceed this are skipped (heap-only), never errors; it
 * also keeps uint32 blob offsets far from overflow / the NULL sentinel.
 */
#define DBBC_MAX_BLOCK_BLOB		(256 * 1024 * 1024)

/* chunk encodings (v1: plain only; tag exists so more can be added) */
typedef enum DbbcEncoding
{
	DBBC_ENCODING_PLAIN = 0,

	/*
	 * Dictionary. `values` holds dict_count distinct entries in first-seen
	 * (unsorted) order - fixed-width: a stride=attlen array; varlena: uint32
	 * offsets into `varblob` (a MAXALIGN'd packed blob of the distinct values),
	 * exactly the PLAIN layout but over the dictionary. `codes` holds one
	 * code_width-byte index per row into that dictionary. NULL rows are marked
	 * in the null bitmap and their code is undefined. Zone-map min/max are
	 * unchanged - they come from the separately-tracked min/max, NOT the dict
	 * order (which is unsorted). Both builders cap distinct values at
	 * DBBC_DICT_MAX, so code_width is always 1 today (2 is reserved).
	 */
	DBBC_ENCODING_DICT = 1,
} DbbcEncoding;

/*
 * Max distinct values a dictionary chunk may hold. Shared by the builders
 * (columnar_store.c) and the dict-aware filter's stack dict_pass[] table
 * (columnar_scan.c) so they cannot drift; keeping it <= 256 keeps codes at one
 * byte. Raising it past 256 requires code_width=2 AND a heap-allocated
 * dict_pass - do not just bump this constant.
 */
#define DBBC_DICT_MAX	256

/*
 * Per-heap-page stamp taken at build time, under a share lock on the page.
 * The LSN is compared per page at serve time; a single block-level LSN would
 * be unsound (a page whose LSN was below the block maximum could be modified
 * to an LSN still below it and wrongly pass).
 */
typedef struct DbbcPageStamp
{
	XLogRecPtr	lsn;			/* page LSN when the block was built */
	uint16		nrows;			/* live rows captured from this page */
} DbbcPageStamp;

/* one column's data within one block */
typedef struct DbbcColumnChunk
{
	int16		attnum;			/* user column number in the heap relation */
	Oid			atttypid;
	Oid			attcollation;	/* collation the zone min/max were built under */
	int16		attlen;			/* pg_attribute.attlen (>0 fixed, -1 varlena) */
	bool		attbyval;
	uint8		encoding;		/* DbbcEncoding */

	/* zone map */
	bool		has_minmax;		/* false: no btree cmp proc, or all NULL */
	uint32		null_count;
	dsa_pointer min_value;		/* copy of min: attlen bytes, or varlena */
	dsa_pointer max_value;		/* copy of max */

	/* data */
	dsa_pointer nulls;			/* null bitmap (nrows bits); Invalid if none */
	dsa_pointer values;			/* PLAIN: nrows values (fixed: stride=attlen;
								 * varlena: uint32 offsets into varblob). DICT:
								 * dict_count distinct entries (stride=attlen). */
	dsa_pointer varblob;		/* varlena only: concatenated detoasted values */
	Size		varblob_len;

	/* dictionary encoding (DBBC_ENCODING_DICT): else Invalid / 0 */
	dsa_pointer codes;			/* nrows codes, code_width bytes each */
	uint32		dict_count;		/* number of distinct entries in `values` */
	uint8		code_width;		/* 1 (dict_count<=256) or 2 (<=65536) */

	Size		total_bytes;	/* accounting: all DSA bytes of this chunk */
} DbbcColumnChunk;

/* one columnar block: DBBC_PAGES_PER_BLOCK heap pages x ncols chunks */
typedef struct DbbcBlock
{
	BlockNumber first_page;		/* heap range [first_page, first_page+npages) */
	uint16		npages;
	uint32		nrows;			/* total live rows captured */

	/*
	 * VM-fork validity stamp (M6): the LSN of the visibility-map page
	 * covering this block's heap range, captured at build BEFORE any heap
	 * page was copied. Serve-time fast path: if every covered VM bit is
	 * still ALL_VISIBLE and the VM page's current LSN equals this stamp, the
	 * block is valid with NO heap page reads. Soundness: modifying an
	 * all-visible heap page must first clear its VM bit (caught by the bit
	 * check - the clear itself is the evidence), and every later re-set goes
	 * through visibilitymap_set, whose callers PageSetLSN the VM page
	 * (pruneheap.c log_heap_prune_and_freeze; heapam_xlog.c redo), moving it
	 * past the stamp (caught by the LSN check). Capturing the stamp before
	 * the copies makes any set during the build move the LSN past it -
	 * conservative fallback, never false validity. CRITICAL: the bit and the
	 * LSN must be read together under a SHARE lock on the VM buffer, because
	 * a setter holds the buffer EXCLUSIVE across [set bit ... PageSetLSN] and
	 * a lock-free reader could otherwise catch the middle - a re-set bit with
	 * the not-yet-bumped LSN still equal to the stamp - and serve a stale
	 * block (see dbbc_block_vm_valid). InvalidXLogRecPtr means the fast path
	 * must not be used for this block (range crosses a VM page boundary, or
	 * the VM page had no LSN at build); the per-heap-page LSN proof below
	 * remains the fallback and the authority.
	 *
	 * Stored as pg_atomic_uint64 because the serve path re-stamps it in place
	 * (dbbc_block_valid) once a block is proven byte-identical, while other
	 * backends read it lock-free w.r.t. the block - a plain load/store would be
	 * a torn read on 32-bit platforms and a data race everywhere. The value is
	 * still an LSN (0 == InvalidXLogRecPtr); read/write via pg_atomic_read_u64 /
	 * pg_atomic_write_u64, initialized once at build with pg_atomic_init_u64.
	 */
	pg_atomic_uint64 vm_lsn;
	/*
	 * Refcount: how many published (or in-flight) versions reference this
	 * block. A block is shared when an incremental refresh reuses it from the
	 * prior version, so it is freed only when the LAST referencing version is
	 * freed - not with any single version. dbbc_block_unref decrements and
	 * frees (releasing block_bytes to the memory budget) at zero.
	 */
	pg_atomic_uint32 refs;
	Size		block_bytes;	/* this block's total DSA bytes, released once
								 * at the free that drops refs to zero */
	DbbcPageStamp stamps[DBBC_PAGES_PER_BLOCK];
	dsa_pointer chunks;			/* DbbcColumnChunk[ncols] (rel entry order) */
} DbbcBlock;

/* dshash key: a relation in a database */
typedef struct DbbcRelKey
{
	Oid			dboid;
	Oid			reloid;
} DbbcRelKey;

/*
 * One immutable population of a relation: the block directory (an array of
 * dsa_pointer to DbbcBlock, one slot per DBBC_PAGES_PER_BLOCK-page range as
 * of its populate; InvalidDsaPointer marks ranges that were not built) plus
 * the column set it was built for.
 *
 * Lifetime is refcounted: `pins` is 1 while the version is the entry's
 * current one, +1 for every active reader (scan, introspection, planner
 * check). Publish swaps the entry to a new version and unpins the old;
 * whoever drops the count to zero frees the version and its blocks. dshash
 * entry locks are therefore only ever held momentarily (dshash forbids
 * holding one across another lookup), and a reader can never observe a
 * freed block. Readers must register their pin with a ResourceOwner (see
 * columnar_scan.c) so aborted queries unpin too.
 */
#define DBBC_VERSION_MAGIC		0xDB10C0DEu
#define DBBC_VERSION_POISON		0xDEADBEEFu

typedef struct DbbcRelVersion
{
	uint32		magic;			/* DBBC_VERSION_MAGIC live / POISON freed */
	pg_atomic_uint32 pins;		/* 1 while current + 1 per active reader */
	dsa_pointer self;			/* this struct's own dsa_pointer */
	int			ncols;
	dsa_pointer attnums;		/* int16[ncols], ascending */
	uint32		ndirslots;		/* directory length */
	uint32		nblocks;		/* directory slots actually built */
	uint32		av_at_build;	/* visibilitymap all-visible page count when
								 * this version was built - the auto-refresh
								 * staleness baseline (compared to the current
								 * count, same page granularity, so a relation
								 * that cannot be fully columnarized still
								 * reaches a stable no-refresh state) */
	dsa_pointer blockdir;		/* dsa_pointer[ndirslots] -> DbbcBlock */
	Size		total_bytes;	/* accounting: all DSA bytes of this version */
} DbbcRelVersion;

/* per-relation entry in the shared hash: just a pointer to the version */
typedef struct DbbcRelEntry
{
	DbbcRelKey	key;			/* hash key: must be first */
	dsa_pointer version;		/* DbbcRelVersion, Invalid if none */
} DbbcRelEntry;

/*
 * ---------------------------------------------------------------------------
 * Workload advisor (columnar_suggest.c)
 *
 * The advisor answers "which columns of which table should be columnarized?"
 * from the actual workload, instead of making the admin guess. Two planner
 * hooks record evidence into the fixed-size shared table below; a flush drains
 * it into the dbblue_columnar_suggestions tables, where an admin reviews it.
 *
 * The unit of advice is a BUNDLE: one (relation, query shape) pair together
 * with the complete set of columns that shape touches. It has to be atomic,
 * because dbbc_rel_ready() disqualifies a columnar scan when ANY referenced
 * column is unregistered - so registering 8 of a shape's 9 columns buys
 * exactly zero speedup. A per-column suggestion list would invite precisely
 * that mistake.
 *
 * Two properties of this shared table are load-bearing:
 *
 * 1. It is in MAIN shared memory (RegisterShmemCallbacks), reserved at
 *    postmaster start, NOT in the column store's DSA. Collector bytes must
 *    never be drawn from the dbblue_columnar.memory_mb budget: that budget is
 *    the store's, dbblue_columnar_memory_status divides by it, and charging
 *    advice against it would make pct_of_budget lie. Main shmem also cannot
 *    fail to allocate at runtime, whereas a lazily-created DSM segment could
 *    ereport inside a user's planner.
 *
 * 2. It NEVER allocates and NEVER waits. Entries are fixed-size slots claimed
 *    by linear probing under a stripe lock taken with
 *    LWLockConditionalAcquire: a contended or full table drops the
 *    observation and bumps a counter rather than delaying a query. Advice is
 *    best-effort by definition; a query is not.
 * ---------------------------------------------------------------------------
 */

/*
 * Width of the per-bundle column bitmap. Attnums are never reused (a dropped
 * column keeps its attnum), so this is a ceiling on a table's lifetime column
 * count, not its live one - hence 512 rather than something snug around the
 * ~100-200 columns of a wide Odoo table. Bundles that reference an attnum
 * past this set att_overflow and are reported but never auto-applied.
 */
#define DBBC_SUGG_ATTBITS	512
#define DBBC_SUGG_ATTWORDS	(DBBC_SUGG_ATTBITS / 64)

/* stripe count (power of two) and how far a claim may probe within a stripe */
#define DBBC_SUGG_NSTRIPES	64
#define DBBC_SUGG_PROBE		8

/* why a shape can never be accelerated (0 = eligible); mirrors dbbc_rel_shape */
#define DBBC_SUGG_OK			0
#define DBBC_SUGG_UNSUP_RELKIND	1	/* not a plain table or matview */
#define DBBC_SUGG_UNSUP_INH		2	/* inheritance/partition parent */
#define DBBC_SUGG_UNSUP_AM		3	/* not the heap AM */
#define DBBC_SUGG_UNSUP_SYSCOL	4	/* system column or whole-row ref */
#define DBBC_SUGG_UNSUP_PERSIST	5	/* unlogged or temporary */
#define DBBC_SUGG_UNSUP_SAMPLE	6	/* TABLESAMPLE */
#define DBBC_SUGG_UNSUP_LATERAL	7	/* laterally-dependent */
#define DBBC_SUGG_UNSUP_PARTED	8	/* partitioned table (the parent) */

/*
 * One bundle's accumulated evidence. Sums (not averages) are stored so the
 * flush can divide by nplans at any cadence without losing precision, and so
 * two observation points can add into the same slot without coordinating.
 *
 * Deliberately absent: any score. Scoring weights are still being calibrated
 * against real Odoo workloads, so the score is computed in the
 * dbblue_columnar_suggestion_status view from these raw columns - which means
 * retuning it is a view replacement, not a catalog migration.
 */
typedef struct DbbcSuggEntry
{
	/* 8-byte members first, so the struct needs no internal padding */
	int64		queryid;		/* root->parse->queryId; 0 is never stored */
	int64		nplans;			/* how many plans referenced this bundle */
	int64		rows_sum;		/* Sum of rel->rows: post-filter estimate */
	int64		tuples_sum;		/* Sum of rel->tuples: table cardinality */
	int64		pages_sum;		/* Sum of rel->pages */
	int64		width_sum;		/* Sum of the bundle's projected byte width */
	uint64		attmap[DBBC_SUGG_ATTWORDS];		/* referenced attnums, 1-based */
	uint64		filtmap[DBBC_SUGG_ATTWORDS];	/* subset read by a restriction */
	TimestampTz first_seen;
	TimestampTz last_seen;

	Oid			dboid;
	Oid			reloid;
	uint32		allvisfrac_ppm;	/* last rel->allvisfrac, parts per million */
	uint32		gen_last_seen;	/* control->generation when last touched */
	uint32		nagg;			/* observations via create_upper_paths_hook */
	uint16		nquals_last;	/* list_length(rel->baserestrictinfo) */

	bool		inuse;
	bool		att_overflow;	/* referenced an attnum >= DBBC_SUGG_ATTBITS */
	bool		has_rls;		/* a qual carried a non-zero security level */
	uint8		unsupported;	/* DBBC_SUGG_UNSUP_*, or DBBC_SUGG_OK */
	uint8		pad[1];
} DbbcSuggEntry;

/*
 * Shared control for the advisor. The counters are atomics rather than
 * lock-protected fields so the hot path can report a drop without having
 * acquired anything - the drop paths are exactly the ones that failed to get
 * a lock.
 */
typedef struct DbbcSuggControl
{
	pg_atomic_uint64 observations;	/* offered to the collector */
	pg_atomic_uint64 recorded;		/* successfully accumulated */
	pg_atomic_uint64 contended_drops;	/* stripe lock was held */
	pg_atomic_uint64 full_drops;		/* no claimable slot in the window */
	pg_atomic_uint64 noqueryid_drops;	/* compute_query_id produced no id */
	pg_atomic_uint32 generation;		/* bumped by each flush */

	uint32		nslots;			/* 0 = advisor unavailable this startup */
	uint32		slots_per_stripe;
	TimestampTz last_reset;

	LWLockPadded stripe[DBBC_SUGG_NSTRIPES];
	DbbcSuggEntry slots[FLEXIBLE_ARRAY_MEMBER];
} DbbcSuggControl;

/* GUCs (defined in dbblue_columnar.c) */
extern bool dbblue_columnar_enabled;
extern bool dbblue_columnar_enable_columnar_scan;
extern int	dbblue_columnar_memory_mb;
extern bool dbblue_columnar_log_coverage_misses;
extern bool dbblue_columnar_enable_restamp;
extern bool dbblue_columnar_enable_dimjoin_agg;
extern bool dbblue_columnar_enable_int128_sum;
extern int	dbblue_columnar_dimjoin_max_dim_rows;
extern bool dbblue_columnar_suggestions;
extern int	dbblue_columnar_suggestion_slots;
extern int	dbblue_columnar_suggest_min_pages;

/* columnar_store.c */
extern void dbbc_store_attach(void);
extern dsa_area *dbbc_store_dsa(void);
extern dshash_table *dbbc_store_hash(void);
extern int64 dbbc_store_bytes_used(void);
extern char *dbbc_registry_table_name(void);
extern DbbcRelVersion *dbbc_version_pin(Oid reloid);
extern void dbbc_version_unpin(DbbcRelVersion *version);
extern DbbcRelVersion *dbbc_version_pin_tracked(Oid reloid);
extern DbbcRelVersion *dbbc_version_attach_tracked(dsa_pointer vp);
extern void dbbc_version_unpin_tracked(DbbcRelVersion *version);

/* populate / auto-refresh (columnar_store.c) */
extern int	dbbc_populate_relation(Oid relid);
extern List *dbbc_registered_relids(void);
extern bool dbbc_relation_needs_refresh(Oid relid, int threshold_pct);
extern Datum dbbc_chunk_minmax_datum(DbbcColumnChunk *chunk, bool want_max);
extern bool dbbc_block_vm_valid(DbbcBlock *block, Relation rel,
								Buffer *vmbuf);
extern XLogRecPtr dbbc_block_vm_capture(DbbcBlock *block, Relation rel,
										Buffer *vmbuf);
extern bool dbbc_restamp_block(DbbcBlock *block, XLogRecPtr fresh);

/* columnar_scan.c */
extern void dbbc_scan_init(void);

/* workload advisor (columnar_suggest.c) */
extern void dbbc_sugg_shmem_init_request(void);
extern void dbbc_sugg_observe_scan(PlannerInfo *root, RelOptInfo *rel,
								   RangeTblEntry *rte, Bitmapset *attrs,
								   bool shape_ok, int unsupported);
extern void dbbc_sugg_observe_agg(PlannerInfo *root, RelOptInfo *input_rel);
extern int64 dbbc_sugg_flush(void);

#endif							/* DBBLUE_COLUMNAR_H */
