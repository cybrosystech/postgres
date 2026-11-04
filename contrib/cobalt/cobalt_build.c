/*-------------------------------------------------------------------------
 *
 * cobalt_build.c
 *	  CREATE INDEX for cobalt.
 *
 * The heap is scanned in physical order, so every term sees its TIDs in
 * ascending order.  Each term keeps a small buffer for the group (256 heap
 * blocks) currently being filled; when the scan moves to the next group the
 * buffer is encoded (sparse array or two-level bitmap, whichever is smaller)
 * and appended to the term's compressed posting bytes.  Memory use is
 * therefore close to the size of the finished index.
 *
 * IDENTIFICATION
 *	  contrib/cobalt/cobalt_build.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/tableam.h"
#include "cobalt.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#define NO_GROUP	PG_UINT32_MAX

typedef struct TermBuilder
{
	uint32		key;			/* hash key, must be first */
	uint32		ndocs;
	uint32		cur_group;
	uint32		nitems;
	uint32		itemcap;
	uint32	   *items;			/* (page_in_group << 16) | offset */
	uint64		outlen;
	uint64		outcap;
	uint8	   *out;
} TermBuilder;

typedef struct CobaltBuildState
{
	HTAB	   *terms;
	MemoryContext buildCtx;		/* long lived buffers */
	MemoryContext tmpCtx;		/* per-row scratch */
	Oid			collid;
	uint64		indtuples;
} CobaltBuildState;

typedef struct StreamWriter
{
	Relation	index;
	PGAlignedBlock page;
	uint32		used;
	BlockNumber first;
	BlockNumber last;
	uint64		total;
} StreamWriter;

static int
cmp_item(const void *a, const void *b)
{
	uint32		x = *(const uint32 *) a;
	uint32		y = *(const uint32 *) b;

	return (x > y) - (x < y);
}

static void
out_append(CobaltBuildState *bs, TermBuilder *tb, const void *data, uint32 len)
{
	if (tb->outlen + len > tb->outcap)
	{
		uint64		newcap = Max(tb->outcap * 2, Max(tb->outlen + len, 64));

		if (tb->out == NULL)
			tb->out = MemoryContextAlloc(bs->buildCtx, newcap);
		else
			tb->out = repalloc_huge(tb->out, newcap);
		tb->outcap = newcap;
	}
	memcpy(tb->out + tb->outlen, data, len);
	tb->outlen += len;
}

/* Encode the buffered group of a term and append it to the term's bytes. */
static void
finalize_group(CobaltBuildState *bs, TermBuilder *tb)
{
	CobaltGroupHdr hdr;
	uint32		n = 0;
	uint32		npages = 0;
	uint32		i;
	uint32		lastpage = PG_UINT32_MAX;
	uint64		sparse_bytes;
	uint64		bitmap_bytes;

	if (tb->nitems == 0)
		return;

	qsort(tb->items, tb->nitems, sizeof(uint32), cmp_item);

	/* de-duplicate (a HOT chain root can be reported more than once) */
	for (i = 0; i < tb->nitems; i++)
	{
		if (n > 0 && tb->items[n - 1] == tb->items[i])
			continue;
		tb->items[n++] = tb->items[i];
	}
	for (i = 0; i < n; i++)
	{
		uint32		page = tb->items[i] >> 16;

		if (page != lastpage)
		{
			npages++;
			lastpage = page;
		}
	}

	sparse_bytes = (uint64) n * 3;
	bitmap_bytes = COBALT_PAGEBITS_BYTES + (uint64) npages * COBALT_OFFBYTES;

	memset(&hdr, 0, sizeof(hdr));
	hdr.group_id = tb->cur_group;
	hdr.npages = (uint16) npages;
	hdr.nitems = n;

	if (sparse_bytes <= bitmap_bytes)
	{
		hdr.enc = COBALT_ENC_SPARSE;
		out_append(bs, tb, &hdr, sizeof(hdr));
		for (i = 0; i < n; i++)
		{
			uint8		b[3];
			uint32		off = tb->items[i] & 0xFFFF;

			b[0] = (uint8) (tb->items[i] >> 16);
			b[1] = (uint8) (off & 0xFF);
			b[2] = (uint8) (off >> 8);
			out_append(bs, tb, b, 3);
		}
	}
	else
	{
		uint8		pagebits[COBALT_PAGEBITS_BYTES];
		uint8		offbits[COBALT_OFFBYTES];

		hdr.enc = COBALT_ENC_BITMAP;
		memset(pagebits, 0, sizeof(pagebits));
		for (i = 0; i < n; i++)
		{
			uint32		page = tb->items[i] >> 16;

			pagebits[page / 8] |= (uint8) (1 << (page % 8));
		}
		out_append(bs, tb, &hdr, sizeof(hdr));
		out_append(bs, tb, pagebits, sizeof(pagebits));

		i = 0;
		while (i < n)
		{
			uint32		page = tb->items[i] >> 16;

			memset(offbits, 0, sizeof(offbits));
			while (i < n && (tb->items[i] >> 16) == page)
			{
				uint32		bit = (tb->items[i] & 0xFFFF) - 1;

				offbits[bit / 8] |= (uint8) (1 << (bit % 8));
				i++;
			}
			out_append(bs, tb, offbits, sizeof(offbits));
		}
	}

	tb->nitems = 0;
}

static void
add_posting(CobaltBuildState *bs, uint32 key, BlockNumber blk, OffsetNumber off)
{
	TermBuilder *tb;
	bool		found;
	uint32		group = blk / COBALT_GROUP_PAGES;

	tb = (TermBuilder *) hash_search(bs->terms, &key, HASH_ENTER, &found);
	if (!found)
	{
		tb->ndocs = 0;
		tb->cur_group = NO_GROUP;
		tb->nitems = 0;
		tb->itemcap = 0;
		tb->items = NULL;
		tb->outlen = 0;
		tb->outcap = 0;
		tb->out = NULL;
	}

	if (tb->cur_group != group)
	{
		if (tb->cur_group != NO_GROUP)
		{
			if (group < tb->cur_group)
				elog(ERROR, "cobalt: heap scan returned TIDs out of order");
			finalize_group(bs, tb);
		}
		tb->cur_group = group;
	}

	if (tb->nitems == tb->itemcap)
	{
		uint32		newcap = tb->itemcap ? tb->itemcap * 2 : 8;

		if (tb->items == NULL)
			tb->items = MemoryContextAlloc(bs->buildCtx, newcap * sizeof(uint32));
		else
			tb->items = repalloc(tb->items, newcap * sizeof(uint32));
		tb->itemcap = newcap;
	}
	tb->items[tb->nitems++] = ((blk % COBALT_GROUP_PAGES) << 16) | off;
	tb->ndocs++;
}

static void
cobalt_build_callback(Relation index, ItemPointer tid, Datum *values,
					  bool *isnull, bool tupleIsAlive, void *state)
{
	CobaltBuildState *bs = (CobaltBuildState *) state;
	MemoryContext oldCtx;
	BlockNumber blk = ItemPointerGetBlockNumber(tid);
	OffsetNumber off = ItemPointerGetOffsetNumber(tid);
	uint32	   *keys;
	int			nkeys;
	int			i;

	if (isnull[0])
		return;

	if (off > MaxHeapTuplesPerPage)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cobalt supports only heap-style tuple identifiers"),
				 errdetail("Offset %u exceeds the maximum of %d.",
						   off, MaxHeapTuplesPerPage)));

	oldCtx = MemoryContextSwitchTo(bs->tmpCtx);
	keys = cobalt_doc_keys(DatumGetTextPP(values[0]), bs->collid, &nkeys);
	MemoryContextSwitchTo(oldCtx);

	add_posting(bs, COBALT_KEY_ALL, blk, off);
	for (i = 0; i < nkeys; i++)
		add_posting(bs, keys[i], blk, off);

	bs->indtuples++;
	MemoryContextReset(bs->tmpCtx);
	CHECK_FOR_INTERRUPTS();
}

/* ------------------------------------------------------------------------
 * Stream writer
 * ------------------------------------------------------------------------
 */

static void
sw_init_page(StreamWriter *sw)
{
	CobaltInitPage((Page) sw->page.data, COBALT_PAGE_DATA);
	sw->used = 0;
}

static void
sw_init(StreamWriter *sw, Relation index)
{
	sw->index = index;
	sw->first = InvalidBlockNumber;
	sw->last = InvalidBlockNumber;
	sw->total = 0;
	sw_init_page(sw);
}

static void
sw_flush(StreamWriter *sw)
{
	Buffer		buffer = CobaltNewBuffer(sw->index);
	BlockNumber blkno = BufferGetBlockNumber(buffer);
	GenericXLogState *state;
	Page		page;

	if (sw->first == InvalidBlockNumber)
		sw->first = blkno;
	else if (blkno != sw->last + 1)
		elog(ERROR, "cobalt: non-contiguous stream block %u after %u",
			 blkno, sw->last);
	sw->last = blkno;

	state = GenericXLogStart(sw->index);
	page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
	memcpy(page, sw->page.data, BLCKSZ);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buffer);

	sw_init_page(sw);
}

static void
sw_write(StreamWriter *sw, const void *data, uint64 len)
{
	const char *p = (const char *) data;

	while (len > 0)
	{
		uint32		n = (uint32) Min(len, (uint64) (COBALT_DATA_BYTES - sw->used));

		memcpy((char *) PageGetContents((Page) sw->page.data) + sw->used, p, n);
		sw->used += n;
		sw->total += n;
		p += n;
		len -= n;
		if (sw->used == COBALT_DATA_BYTES)
		{
			sw_flush(sw);
			CHECK_FOR_INTERRUPTS();
		}
	}
}

static void
sw_finish(StreamWriter *sw)
{
	if (sw->used > 0)
		sw_flush(sw);
}

static int
cmp_term(const void *a, const void *b)
{
	uint32		x = (*(TermBuilder *const *) a)->key;
	uint32		y = (*(TermBuilder *const *) b)->key;

	return (x > y) - (x < y);
}

/* ------------------------------------------------------------------------
 * ambuild
 * ------------------------------------------------------------------------
 */

IndexBuildResult *
cobaltbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	CobaltBuildState bs;
	HASHCTL		ctl;
	double		reltuples;
	TermBuilder **terms;
	CobaltDictEntry *dict;
	HASH_SEQ_STATUS seq;
	TermBuilder *tb;
	StreamWriter sw;
	uint64		nterms;
	uint64		i;
	uint64		post_off = 0;
	CobaltSegmentDesc seg;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	if (indexInfo->ii_NumIndexKeyAttrs != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cobalt indexes support exactly one column")));

	CobaltInitMetapage(index, MAIN_FORKNUM);

	memset(&bs, 0, sizeof(bs));
	bs.collid = index->rd_indcollation[0];
	bs.buildCtx = AllocSetContextCreate(CurrentMemoryContext,
										"cobalt build", ALLOCSET_DEFAULT_SIZES);
	bs.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "cobalt build row", ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(TermBuilder);
	ctl.hcxt = bs.buildCtx;
	bs.terms = hash_create("cobalt terms", 4096, &ctl,
						   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* allow_sync = false: TIDs must arrive in ascending order */
	reltuples = table_index_build_scan(heap, index, indexInfo, false, true,
									   cobalt_build_callback, &bs, NULL);

	nterms = hash_get_num_entries(bs.terms);
	if (bs.indtuples > 0)
	{
		/* Finish every term and order the dictionary by key. */
		terms = (TermBuilder **) palloc(nterms * sizeof(TermBuilder *));
		i = 0;
		hash_seq_init(&seq, bs.terms);
		while ((tb = (TermBuilder *) hash_seq_search(&seq)) != NULL)
		{
			finalize_group(&bs, tb);
			terms[i++] = tb;
		}
		qsort(terms, nterms, sizeof(TermBuilder *), cmp_term);

		dict = (CobaltDictEntry *) palloc0(nterms * sizeof(CobaltDictEntry));
		for (i = 0; i < nterms; i++)
		{
			dict[i].key = terms[i]->key;
			dict[i].ndocs = terms[i]->ndocs;
			dict[i].post_off = post_off;
			dict[i].post_len = (uint32) terms[i]->outlen;
			post_off += terms[i]->outlen;
		}

		memset(&seg, 0, sizeof(seg));

		sw_init(&sw, index);
		sw_write(&sw, dict, nterms * sizeof(CobaltDictEntry));
		sw_finish(&sw);
		seg.dict_start = sw.first;
		seg.dict_nentries = (uint32) nterms;

		sw_init(&sw, index);
		for (i = 0; i < nterms; i++)
			sw_write(&sw, terms[i]->out, terms[i]->outlen);
		sw_finish(&sw);
		seg.post_start = sw.first;
		seg.post_bytes = post_off;
		seg.ndocs = bs.indtuples;

		/* Publish the segment in the meta page. */
		{
			Buffer		mb = ReadBuffer(index, COBALT_META_BLKNO);
			GenericXLogState *state;
			Page		mpage;
			CobaltMetaPageData *meta;

			LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE);
			state = GenericXLogStart(index);
			mpage = GenericXLogRegisterBuffer(state, mb, 0);
			meta = CobaltPageGetMeta(mpage);
			meta->segs[0] = seg;
			meta->nsegments = 1;
			meta->ntuples = bs.indtuples;
			GenericXLogFinish(state);
			UnlockReleaseBuffer(mb);
		}
	}

	MemoryContextDelete(bs.tmpCtx);
	MemoryContextDelete(bs.buildCtx);

	result = palloc_object(IndexBuildResult);
	result->heap_tuples = reltuples;
	result->index_tuples = (double) bs.indtuples;
	return result;
}

void
cobaltbuildempty(Relation index)
{
	CobaltInitMetapage(index, INIT_FORKNUM);
}
