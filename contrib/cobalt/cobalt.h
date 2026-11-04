/*-------------------------------------------------------------------------
 *
 * cobalt.h
 *	  Cobalt: a bitmap-posting trigram index access method.
 *
 * Cobalt indexes the set of trigrams (three consecutive characters, after
 * case folding) of a text value.  A LIKE / ILIKE pattern is answered by
 * intersecting the posting bitmaps of the trigrams its literal parts require.
 * The result is lossy, so the heap executor always rechecks the qual.
 *
 * Document identifiers are heap TIDs.  A posting list is stored as a sequence
 * of "groups"; one group covers COBALT_GROUP_PAGES consecutive heap blocks.
 * Inside a group a term is stored either as
 *   - a sparse array (3 bytes per posting), or
 *   - a two-level bitmap: a page bitmap (one bit per heap block of the group)
 *     followed by one offset bitmap per set page.
 * whichever is smaller.  Intersections are done on the bitmaps, a word at a
 * time.
 *
 * Index layout
 *   block 0           meta page
 *   segment streams   dictionary stream + postings stream (immutable, written
 *                     by CREATE INDEX)
 *   pending pages     chain of slotted pages with rows inserted since the
 *                     build (scanned by brute force)
 *
 * Copyright (c) 2026, Dbblue contributors
 *
 * IDENTIFICATION
 *	  contrib/cobalt/cobalt.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COBALT_H
#define COBALT_H

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/htup_details.h"
#include "access/itup.h"
#include "access/xlog.h"
#include "fmgr.h"
#include "nodes/pathnodes.h"
#include "storage/bufmgr.h"
#include "mb/pg_wchar.h"
#include "nodes/tidbitmap.h"
#include "utils/rel.h"

/* Strategy numbers (operator class cobalt_text_ops) */
#define COBALT_LIKE_STRATEGY	1	/* ~~  */
#define COBALT_ILIKE_STRATEGY	2	/* ~~* */
#define COBALT_NSTRATEGIES		2
#define COBALT_NPROC			0

/* Page types */
#define COBALT_PAGE_META		1
#define COBALT_PAGE_DATA		2
#define COBALT_PAGE_PENDING		3
#define COBALT_PAGE_ID			0xC0BA

#define COBALT_META_BLKNO		0
#define COBALT_MAGIC			0xC0BA1701
#define COBALT_VERSION			1
#define COBALT_MAX_SEGMENTS		32

/* Reserved term key that is present in every indexed row ("match all"). */
#define COBALT_KEY_ALL			0

/* Postings geometry */
#define COBALT_GROUP_PAGES		256		/* heap blocks per group */
#define COBALT_PAGEBITS_BYTES	(COBALT_GROUP_PAGES / 8)
#define COBALT_PAGEBITS_WORDS	(COBALT_GROUP_PAGES / 64)
#define COBALT_OFFBYTES			((MaxHeapTuplesPerPage + 7) / 8)
#define COBALT_OFFSLOT			MAXALIGN(COBALT_OFFBYTES)
#define COBALT_OFFSLOT_WORDS	(COBALT_OFFSLOT / 8)

#define COBALT_ENC_SPARSE		1
#define COBALT_ENC_BITMAP		2

/* Row limits for the pending list */
#define COBALT_PENDING_OVERFLOW	0xFFFF	/* nkeys marker: row matches anything */

typedef struct CobaltPageOpaqueData
{
	BlockNumber next;			/* pending chain link */
	uint16		page_type;
	uint16		page_id;
} CobaltPageOpaqueData;

typedef CobaltPageOpaqueData *CobaltPageOpaque;

#define CobaltPageGetOpaque(page) ((CobaltPageOpaque) PageGetSpecialPointer(page))

/* Usable bytes in a DATA (stream) page */
#define COBALT_DATA_BYTES \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(CobaltPageOpaqueData)))

/* One immutable segment (dictionary stream + postings stream) */
typedef struct CobaltSegmentDesc
{
	BlockNumber dict_start;		/* first block of dictionary stream */
	BlockNumber post_start;		/* first block of postings stream */
	uint32		dict_nentries;
	uint32		pad;
	uint64		post_bytes;
	uint64		ndocs;
} CobaltSegmentDesc;

typedef struct CobaltMetaPageData
{
	uint32		magic;
	uint32		version;
	uint32		nsegments;
	BlockNumber pending_head;
	BlockNumber pending_tail;
	uint32		pad;
	uint64		pending_ntuples;
	uint64		ntuples;		/* rows indexed, build + pending */
	CobaltSegmentDesc segs[COBALT_MAX_SEGMENTS];
} CobaltMetaPageData;

#define CobaltPageGetMeta(page) ((CobaltMetaPageData *) PageGetContents(page))

/* Dictionary entry, fixed size, sorted by key */
typedef struct CobaltDictEntry
{
	uint32		key;
	uint32		ndocs;
	uint64		post_off;		/* byte offset in postings stream */
	uint32		post_len;
	uint32		pad;
} CobaltDictEntry;

/* Group header in a postings stream */
typedef struct CobaltGroupHdr
{
	uint32		group_id;		/* heap block / COBALT_GROUP_PAGES */
	uint8		enc;
	uint8		pad;
	uint16		npages;			/* set pages (bitmap encoding) */
	uint32		nitems;			/* postings in the group */
} CobaltGroupHdr;

/* Decoded group: what the intersection code works on */
typedef struct CobaltGroupBits
{
	uint64		pagebits[COBALT_PAGEBITS_WORDS];
	uint64	   *off;			/* COBALT_GROUP_PAGES * COBALT_OFFSLOT_WORDS */
} CobaltGroupBits;

/* Pending list item: heap TID followed by sorted trigram keys */
typedef struct CobaltPendingItem
{
	BlockNumber blkno;
	OffsetNumber offnum;
	uint16		nkeys;
	uint32		keys[FLEXIBLE_ARRAY_MEMBER];
} CobaltPendingItem;

/* Sequential reader over a stream of DATA pages */
typedef struct CobaltStream
{
	Relation	index;
	BlockNumber start;			/* first block of the stream */
	Buffer		buf;			/* cached pin, or InvalidBuffer */
	BlockNumber bufblk;
} CobaltStream;

/* cobalt_util.c */
extern void CobaltInitPage(Page page, uint16 page_type);
extern void CobaltFillMetapage(Relation index, Page page);
extern void CobaltInitMetapage(Relation index, ForkNumber forknum);
extern Buffer CobaltNewBuffer(Relation index);
extern uint32 cobalt_trgm_key(pg_wchar a, pg_wchar b, pg_wchar c);
extern uint32 *cobalt_doc_keys(text *doc, Oid collid, int *nkeys);
extern uint32 *cobalt_pattern_keys(text *pattern, Oid collid, int *nkeys);
extern void cobalt_stream_init(CobaltStream *s, Relation index, BlockNumber start);
extern void cobalt_stream_read(CobaltStream *s, uint64 addr, void *dst, uint32 len);
extern void cobalt_stream_end(CobaltStream *s);
extern void cobalt_read_meta(Relation index, CobaltMetaPageData *meta);

/* cobalt_build.c */
extern IndexBuildResult *cobaltbuild(Relation heap, Relation index, IndexInfo *indexInfo);
extern void cobaltbuildempty(Relation index);

/* cobalt_insert.c */
extern bool cobaltinsert(Relation index, Datum *values, bool *isnull,
						 ItemPointer ht_ctid, Relation heapRel,
						 IndexUniqueCheck checkUnique, bool indexUnchanged,
						 IndexInfo *indexInfo);

/* cobalt_scan.c */
extern IndexScanDesc cobaltbeginscan(Relation r, int nkeys, int norderbys);
extern void cobaltrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
						 ScanKey orderbys, int norderbys);
extern int64 cobaltgetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern void cobaltendscan(IndexScanDesc scan);

/* cobalt_vacuum.c */
extern IndexBulkDeleteResult *cobaltbulkdelete(IndexVacuumInfo *info,
											   IndexBulkDeleteResult *stats,
											   IndexBulkDeleteCallback callback,
											   void *callback_state);
extern IndexBulkDeleteResult *cobaltvacuumcleanup(IndexVacuumInfo *info,
												  IndexBulkDeleteResult *stats);

/* cobalt_util.c (AM glue) */
extern bytea *cobaltoptions(Datum reloptions, bool validate);
extern bool cobaltvalidate(Oid opclassoid);
extern void cobaltcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
							   Cost *indexStartupCost, Cost *indexTotalCost,
							   Selectivity *indexSelectivity,
							   double *indexCorrelation, double *indexPages);

#endif							/* COBALT_H */
