/*-------------------------------------------------------------------------
 *
 * cobalt_scan.c
 *	  Bitmap scans for cobalt.
 *
 * A LIKE / ILIKE pattern is reduced to the set of trigram keys that every
 * match must contain.  For each immutable segment the posting streams of
 * those keys are intersected group by group, starting from the rarest key:
 * groups that are missing from any key are skipped by header arithmetic
 * alone, without reading their payload.  Surviving groups are decoded into
 * two-level bitmaps (page bitmap + per-page offset bitmaps), ANDed word by
 * word, and the remaining bits are emitted into the executor's TIDBitmap.
 * Rows still in the pending list are tested one by one.
 *
 * The result is always lossy (recheck = true).
 *
 * IDENTIFICATION
 *	  contrib/cobalt/cobalt_scan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "cobalt.h"
#include "executor/instrument_node.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/pg_bitutils.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"

typedef struct CobaltScanOpaqueData
{
	MemoryContext scanCtx;
} CobaltScanOpaqueData;

typedef CobaltScanOpaqueData *CobaltScanOpaque;

typedef struct TermCursor
{
	CobaltDictEntry ent;
	uint64		addr;			/* where the next group header starts */
	uint64		end;
	bool		valid;
	CobaltGroupHdr hdr;			/* current group */
	uint64		payload_addr;
} TermCursor;

typedef struct ScanScratch
{
	CobaltGroupBits a;
	CobaltGroupBits b;
	uint8	   *buf;
	uint64		bufcap;
	ItemPointerData tids[MaxHeapTuplesPerPage];
} ScanScratch;

IndexScanDesc
cobaltbeginscan(Relation r, int nkeys, int norderbys)
{
	IndexScanDesc scan = RelationGetIndexScan(r, nkeys, norderbys);
	CobaltScanOpaque so = palloc0_object(CobaltScanOpaqueData);

	so->scanCtx = AllocSetContextCreate(CurrentMemoryContext, "cobalt scan",
										ALLOCSET_DEFAULT_SIZES);
	scan->opaque = so;
	return scan;
}

void
cobaltrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
			 ScanKey orderbys, int norderbys)
{
	if (scankey && scan->numberOfKeys > 0)
		memcpy(scan->keyData, scankey, scan->numberOfKeys * sizeof(ScanKeyData));
}

void
cobaltendscan(IndexScanDesc scan)
{
	CobaltScanOpaque so = (CobaltScanOpaque) scan->opaque;

	MemoryContextDelete(so->scanCtx);
	pfree(so);
	scan->opaque = NULL;
}

/* ------------------------------------------------------------------------
 * Dictionary lookup and posting cursors
 * ------------------------------------------------------------------------
 */

static bool
dict_find(CobaltStream *dict, uint32 nentries, uint32 key, CobaltDictEntry *ent)
{
	uint32		lo = 0,
				hi = nentries;

	while (lo < hi)
	{
		uint32		mid = lo + (hi - lo) / 2;

		cobalt_stream_read(dict, (uint64) mid * sizeof(CobaltDictEntry),
						   ent, sizeof(CobaltDictEntry));
		if (ent->key == key)
			return true;
		if (ent->key < key)
			lo = mid + 1;
		else
			hi = mid;
	}
	return false;
}

static inline uint64
group_payload_len(const CobaltGroupHdr *h)
{
	if (h->enc == COBALT_ENC_SPARSE)
		return (uint64) h->nitems * 3;
	return COBALT_PAGEBITS_BYTES + (uint64) h->npages * COBALT_OFFBYTES;
}

/* Move to the next group of the term; the payload is not read. */
static bool
cursor_next(CobaltStream *post, TermCursor *c)
{
	if (c->addr >= c->end)
	{
		c->valid = false;
		return false;
	}
	cobalt_stream_read(post, c->addr, &c->hdr, sizeof(CobaltGroupHdr));
	if (c->hdr.enc != COBALT_ENC_SPARSE && c->hdr.enc != COBALT_ENC_BITMAP)
		elog(ERROR, "cobalt: corrupt posting group header");
	c->payload_addr = c->addr + sizeof(CobaltGroupHdr);
	c->addr = c->payload_addr + group_payload_len(&c->hdr);
	c->valid = true;
	return true;
}

static void
scratch_reserve(ScanScratch *sc, uint64 need)
{
	if (need > sc->bufcap)
	{
		sc->bufcap = Max(need, sc->bufcap * 2);
		if (sc->buf)
			pfree(sc->buf);
		sc->buf = MemoryContextAlloc(CurrentMemoryContext, sc->bufcap);
	}
}

/* Decode the cursor's current group into two-level bitmaps. */
static void
decode_group(CobaltStream *post, TermCursor *c, CobaltGroupBits *g, ScanScratch *sc)
{
	uint8	   *pb = (uint8 *) g->pagebits;
	uint8	   *offbase = (uint8 *) g->off;

	if (c->hdr.enc == COBALT_ENC_SPARSE)
	{
		uint64		len = (uint64) c->hdr.nitems * 3;
		uint32		i;

		scratch_reserve(sc, len);
		cobalt_stream_read(post, c->payload_addr, sc->buf, (uint32) len);
		memset(g->pagebits, 0, sizeof(g->pagebits));

		for (i = 0; i < c->hdr.nitems; i++)
		{
			uint32		page = sc->buf[i * 3];
			uint32		off = sc->buf[i * 3 + 1] | ((uint32) sc->buf[i * 3 + 2] << 8);
			uint32		bit = off - 1;
			uint8	   *slot = offbase + (size_t) page * COBALT_OFFSLOT;

			if (!(pb[page / 8] & (1 << (page % 8))))
			{
				pb[page / 8] |= (uint8) (1 << (page % 8));
				memset(slot, 0, COBALT_OFFSLOT);
			}
			slot[bit / 8] |= (uint8) (1 << (bit % 8));
		}
	}
	else
	{
		uint64		len = (uint64) c->hdr.npages * COBALT_OFFBYTES;
		uint32		bi;
		uint32		k = 0;

		cobalt_stream_read(post, c->payload_addr, pb, COBALT_PAGEBITS_BYTES);
		scratch_reserve(sc, len);
		cobalt_stream_read(post, c->payload_addr + COBALT_PAGEBITS_BYTES,
						   sc->buf, (uint32) len);

		for (bi = 0; bi < COBALT_PAGEBITS_BYTES; bi++)
		{
			uint8		v = pb[bi];

			while (v)
			{
				uint32		page = bi * 8 + pg_rightmost_one_pos32(v);
				uint8	   *slot = offbase + (size_t) page * COBALT_OFFSLOT;

				if (k >= c->hdr.npages)
					elog(ERROR, "cobalt: corrupt bitmap group");
				memcpy(slot, sc->buf + (size_t) k * COBALT_OFFBYTES, COBALT_OFFBYTES);
				memset(slot + COBALT_OFFBYTES, 0, COBALT_OFFSLOT - COBALT_OFFBYTES);
				k++;
				v &= v - 1;
			}
		}
		if (k != c->hdr.npages)
			elog(ERROR, "cobalt: corrupt bitmap group");
	}
}

/* acc &= other; pages that become empty are cleared. */
static bool
and_groups(CobaltGroupBits *acc, const CobaltGroupBits *other)
{
	uint8	   *pb;
	uint32		bi;
	int			w;
	bool		any = false;

	for (w = 0; w < COBALT_PAGEBITS_WORDS; w++)
		acc->pagebits[w] &= other->pagebits[w];

	pb = (uint8 *) acc->pagebits;
	for (bi = 0; bi < COBALT_PAGEBITS_BYTES; bi++)
	{
		uint8		v = pb[bi];

		while (v)
		{
			uint32		bit = pg_rightmost_one_pos32(v);
			uint32		page = bi * 8 + bit;
			uint64	   *a = acc->off + (size_t) page * COBALT_OFFSLOT_WORDS;
			const uint64 *o = other->off + (size_t) page * COBALT_OFFSLOT_WORDS;
			uint64		nz = 0;
			int			k;

			for (k = 0; k < COBALT_OFFSLOT_WORDS; k++)
			{
				a[k] &= o[k];
				nz |= a[k];
			}
			if (nz == 0)
				pb[bi] &= (uint8) ~(1 << bit);
			else
				any = true;
			v &= v - 1;
		}
	}
	return any;
}

static int64
emit_group(TIDBitmap *tbm, const CobaltGroupBits *g, uint32 group_id,
		   ScanScratch *sc)
{
	const uint8 *pb = (const uint8 *) g->pagebits;
	const uint8 *offbase = (const uint8 *) g->off;
	int64		total = 0;
	uint32		bi;

	for (bi = 0; bi < COBALT_PAGEBITS_BYTES; bi++)
	{
		uint8		v = pb[bi];

		while (v)
		{
			uint32		page = bi * 8 + pg_rightmost_one_pos32(v);
			BlockNumber blk = (BlockNumber) group_id * COBALT_GROUP_PAGES + page;
			const uint8 *slot = offbase + (size_t) page * COBALT_OFFSLOT;
			int			n = 0;
			uint32		ob;

			for (ob = 0; ob < COBALT_OFFBYTES; ob++)
			{
				uint8		x = slot[ob];

				while (x)
				{
					uint32		off = ob * 8 + pg_rightmost_one_pos32(x) + 1;

					if (off <= MaxHeapTuplesPerPage)
						ItemPointerSet(&sc->tids[n++], blk, (OffsetNumber) off);
					x &= x - 1;
				}
			}
			if (n > 0)
			{
				tbm_add_tuples(tbm, sc->tids, n, true);
				total += n;
			}
			v &= v - 1;
		}
	}
	return total;
}

static int
cmp_cursor_ndocs(const void *a, const void *b)
{
	const TermCursor *x = (const TermCursor *) a;
	const TermCursor *y = (const TermCursor *) b;

	return (x->ent.ndocs > y->ent.ndocs) - (x->ent.ndocs < y->ent.ndocs);
}

/* Intersect all required keys inside one immutable segment. */
static int64
scan_segment(Relation index, const CobaltSegmentDesc *seg, const uint32 *keys,
			 int nkeys, TIDBitmap *tbm, ScanScratch *sc)
{
	CobaltStream dict,
				post;
	TermCursor *cur;
	int64		total = 0;
	int			i,
				t;
	bool		done = false;
	bool		missing = false;

	cobalt_stream_init(&dict, index, seg->dict_start);
	cobalt_stream_init(&post, index, seg->post_start);
	cur = (TermCursor *) palloc0(nkeys * sizeof(TermCursor));

	for (i = 0; i < nkeys; i++)
	{
		if (!dict_find(&dict, seg->dict_nentries, keys[i], &cur[i].ent))
		{
			missing = true;
			break;
		}
		cur[i].addr = cur[i].ent.post_off;
		cur[i].end = cur[i].ent.post_off + cur[i].ent.post_len;
	}

	if (!missing)
	{
		qsort(cur, nkeys, sizeof(TermCursor), cmp_cursor_ndocs);

		while (!done && cursor_next(&post, &cur[0]))
		{
			uint32		g = cur[0].hdr.group_id;
			bool		match = true;

			for (t = 1; t < nkeys; t++)
			{
				while (!cur[t].valid || cur[t].hdr.group_id < g)
				{
					if (!cursor_next(&post, &cur[t]))
					{
						done = true;
						break;
					}
				}
				if (done)
					break;
				if (cur[t].hdr.group_id != g)
				{
					match = false;
					break;
				}
			}
			if (done)
				break;

			if (match)
			{
				bool		any = true;

				decode_group(&post, &cur[0], &sc->a, sc);
				for (t = 1; t < nkeys && any; t++)
				{
					decode_group(&post, &cur[t], &sc->b, sc);
					any = and_groups(&sc->a, &sc->b);
				}
				if (any)
					total += emit_group(tbm, &sc->a, g, sc);
			}
			CHECK_FOR_INTERRUPTS();
		}
	}

	cobalt_stream_end(&dict);
	cobalt_stream_end(&post);
	pfree(cur);
	return total;
}

/* Brute-force the pending list. */
static int64
scan_pending(Relation index, BlockNumber head, const uint32 *keys, int nkeys,
			 bool all_mode, TIDBitmap *tbm)
{
	int64		total = 0;
	BlockNumber blk = head;

	while (blk != InvalidBlockNumber)
	{
		Buffer		buf = ReadBuffer(index, blk);
		Page		page;
		OffsetNumber off,
					maxoff;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		if (PageIsNew(page) ||
			CobaltPageGetOpaque(page)->page_type != COBALT_PAGE_PENDING)
			elog(ERROR, "cobalt: unexpected page %u in pending list", blk);

		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			CobaltPendingItem *item =
				(CobaltPendingItem *) PageGetItem(page, PageGetItemId(page, off));
			bool		match = true;

			if (!all_mode && item->nkeys != COBALT_PENDING_OVERFLOW)
			{
				int			i;

				for (i = 0; i < nkeys && match; i++)
				{
					int			lo = 0,
								hi = item->nkeys;
					bool		found = false;

					while (lo < hi)
					{
						int			mid = lo + (hi - lo) / 2;

						if (item->keys[mid] == keys[i])
						{
							found = true;
							break;
						}
						if (item->keys[mid] < keys[i])
							lo = mid + 1;
						else
							hi = mid;
					}
					match = found;
				}
			}
			if (match)
			{
				ItemPointerData tid;

				ItemPointerSet(&tid, item->blkno, item->offnum);
				tbm_add_tuples(tbm, &tid, 1, true);
				total++;
			}
		}
		blk = CobaltPageGetOpaque(page)->next;
		UnlockReleaseBuffer(buf);
		CHECK_FOR_INTERRUPTS();
	}
	return total;
}

static int
cmp_key(const void *a, const void *b)
{
	uint32		x = *(const uint32 *) a;
	uint32		y = *(const uint32 *) b;

	return (x > y) - (x < y);
}

int64
cobaltgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	CobaltScanOpaque so = (CobaltScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	Oid			collid = index->rd_indcollation[0];
	MemoryContext oldCtx;
	CobaltMetaPageData *meta;
	uint32	   *keys = NULL;
	int			nkeys = 0;
	int			cap = 0;
	bool		all_mode;
	ScanScratch *sc;
	int64		ntids = 0;
	uint32		s;
	int			i;

	oldCtx = MemoryContextSwitchTo(so->scanCtx);
	MemoryContextReset(so->scanCtx);

	/* Collect the keys every match must contain, over all scan keys. */
	for (i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		skey = &scan->keyData[i];
		uint32	   *k;
		int			nk;

		if (skey->sk_flags & SK_ISNULL)
		{
			MemoryContextSwitchTo(oldCtx);
			return 0;			/* operators are strict */
		}
		if (skey->sk_strategy != COBALT_LIKE_STRATEGY &&
			skey->sk_strategy != COBALT_ILIKE_STRATEGY)
			elog(ERROR, "cobalt: unsupported strategy %d", skey->sk_strategy);

		k = cobalt_pattern_keys(DatumGetTextPP(skey->sk_argument), collid, &nk);
		if (nk == 0)
			continue;
		if (nkeys + nk > cap)
		{
			cap = Max(cap * 2, nkeys + nk);
			keys = keys ? repalloc(keys, cap * sizeof(uint32))
				: palloc(cap * sizeof(uint32));
		}
		memcpy(keys + nkeys, k, nk * sizeof(uint32));
		nkeys += nk;
	}

	if (nkeys > 1)
	{
		int			j = 1;

		qsort(keys, nkeys, sizeof(uint32), cmp_key);
		for (i = 1; i < nkeys; i++)
			if (keys[i] != keys[j - 1])
				keys[j++] = keys[i];
		nkeys = j;
	}

	all_mode = (nkeys == 0);
	if (all_mode)
	{
		keys = (uint32 *) palloc(sizeof(uint32));
		keys[0] = COBALT_KEY_ALL;
		nkeys = 1;
	}

	pgstat_count_index_scan(index);
	if (scan->instrument)
		scan->instrument->nsearches++;

	meta = (CobaltMetaPageData *) palloc(sizeof(CobaltMetaPageData));
	cobalt_read_meta(index, meta);

	sc = (ScanScratch *) palloc0(sizeof(ScanScratch));
	sc->a.off = (uint64 *) palloc((size_t) COBALT_GROUP_PAGES * COBALT_OFFSLOT);
	sc->b.off = (uint64 *) palloc((size_t) COBALT_GROUP_PAGES * COBALT_OFFSLOT);

	for (s = 0; s < meta->nsegments; s++)
		ntids += scan_segment(index, &meta->segs[s], keys, nkeys, tbm, sc);

	if (meta->pending_head != InvalidBlockNumber)
		ntids += scan_pending(index, meta->pending_head, keys, nkeys, all_mode, tbm);

	MemoryContextSwitchTo(oldCtx);
	return ntids;
}
