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
 *     NULL rows carry DBBC_VAR_NULL_OFFSET. The blob packs values with NO
 *     alignment padding: decoders must memcpy values out (or use
 *     unaligned-safe access), never cast blob+offset to an aligned struct.
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
 * Per-relation entry in the shared hash. The block directory is an array of
 * dsa_pointer to DbbcBlock, one slot per DBBC_PAGES_PER_BLOCK-page range of
 * the heap as of the last populate; InvalidDsaPointer marks ranges that were
 * not built (some page was not all-visible, or the memory budget was hit).
 *
 * Concurrency (Milestone 2): the dshash entry lock protects the whole entry
 * including the directory and all blocks under it. Writers (populate) hold it
 * exclusively; readers (introspection) hold it shared for the duration of
 * their read. This is acceptable while readers are short-lived; the scan
 * executor milestone must replace it with refcounted/epoch reclamation before
 * long-running readers appear.
 */
typedef struct DbbcRelEntry
{
	DbbcRelKey	key;			/* hash key: must be first */
	int			ncols;
	dsa_pointer attnums;		/* int16[ncols], ascending */
	uint32		ndirslots;		/* directory length */
	dsa_pointer blockdir;		/* dsa_pointer[ndirslots] -> DbbcBlock */
	Size		total_bytes;	/* accounting: all DSA bytes of this entry */
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

#endif							/* DBBLUE_COLUMNAR_H */
