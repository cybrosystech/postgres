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
#include "lib/dshash.h"
#include "nodes/pg_list.h"
#include "port/atomics.h"
#include "storage/block.h"
#include "utils/dsa.h"

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
} DbbcEncoding;

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
	dsa_pointer values;			/* fixed: stride=attlen array; varlena: uint32
								 * offsets into varblob */
	dsa_pointer varblob;		/* varlena only: concatenated detoasted values */
	Size		varblob_len;
	Size		total_bytes;	/* accounting: all DSA bytes of this chunk */
} DbbcColumnChunk;

/* one columnar block: DBBC_PAGES_PER_BLOCK heap pages x ncols chunks */
typedef struct DbbcBlock
{
	BlockNumber first_page;		/* heap range [first_page, first_page+npages) */
	uint16		npages;
	uint32		nrows;			/* total live rows captured */
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

/* GUCs (defined in dbblue_columnar.c) */
extern bool dbblue_columnar_enabled;
extern bool dbblue_columnar_enable_columnar_scan;
extern int	dbblue_columnar_memory_mb;

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

/* columnar_scan.c */
extern void dbbc_scan_init(void);

#endif							/* DBBLUE_COLUMNAR_H */
