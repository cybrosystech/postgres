/*-------------------------------------------------------------------------
 *
 * cobalt_insert.c
 *	  Row insertion for cobalt.
 *
 * Rows added after CREATE INDEX are appended to the pending list: a chain of
 * slotted pages, each item holding the heap TID and the sorted trigram keys
 * of one row.  Scans test pending items by brute force, so the list is
 * correct but gets slower as it grows; folding it into an immutable segment
 * is not implemented yet.  Rows with more distinct trigrams than fit in an
 * item are stored with a marker that makes them a candidate for every query
 * (the heap recheck then decides).
 *
 * Inserters are serialized by the exclusive lock on the meta page.
 *
 * IDENTIFICATION
 *	  contrib/cobalt/cobalt_insert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "cobalt.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"

#define COBALT_PENDING_MAXITEM \
	MAXALIGN_DOWN(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - \
				  MAXALIGN(sizeof(CobaltPageOpaqueData)) - sizeof(ItemIdData))

#define COBALT_PENDING_MAXKEYS \
	((COBALT_PENDING_MAXITEM - offsetof(CobaltPendingItem, keys)) / sizeof(uint32))

static void
cobalt_append_pending(Relation index, CobaltPendingItem *item, Size size)
{
	Buffer		mbuf = ReadBuffer(index, COBALT_META_BLKNO);
	Buffer		tbuf = InvalidBuffer;
	Buffer		nbuf;
	GenericXLogState *state;
	Page		mpage,
				tpage = NULL,
				npage;
	CobaltMetaPageData *meta;

	LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	mpage = GenericXLogRegisterBuffer(state, mbuf, 0);
	meta = CobaltPageGetMeta(mpage);

	if (meta->pending_tail != InvalidBlockNumber)
	{
		tbuf = ReadBuffer(index, meta->pending_tail);
		LockBuffer(tbuf, BUFFER_LOCK_EXCLUSIVE);
		tpage = GenericXLogRegisterBuffer(state, tbuf, 0);

		if (PageGetFreeSpace(tpage) >= MAXALIGN(size))
		{
			if (PageAddItem(tpage, item, size, InvalidOffsetNumber,
							false, false) == InvalidOffsetNumber)
				elog(ERROR, "cobalt: failed to add pending item");
			meta->pending_ntuples++;
			meta->ntuples++;
			GenericXLogFinish(state);
			UnlockReleaseBuffer(tbuf);
			UnlockReleaseBuffer(mbuf);
			return;
		}
	}

	/* No room: start a new pending page and link it in. */
	nbuf = CobaltNewBuffer(index);
	npage = GenericXLogRegisterBuffer(state, nbuf, GENERIC_XLOG_FULL_IMAGE);
	CobaltInitPage(npage, COBALT_PAGE_PENDING);
	if (PageAddItem(npage, item, size, InvalidOffsetNumber,
					false, false) == InvalidOffsetNumber)
		elog(ERROR, "cobalt: failed to add pending item to empty page");

	if (tpage != NULL)
		CobaltPageGetOpaque(tpage)->next = BufferGetBlockNumber(nbuf);
	else
		meta->pending_head = BufferGetBlockNumber(nbuf);
	meta->pending_tail = BufferGetBlockNumber(nbuf);
	meta->pending_ntuples++;
	meta->ntuples++;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(nbuf);
	if (tbuf != InvalidBuffer)
		UnlockReleaseBuffer(tbuf);
	UnlockReleaseBuffer(mbuf);
}

bool
cobaltinsert(Relation index, Datum *values, bool *isnull, ItemPointer ht_ctid,
			 Relation heapRel, IndexUniqueCheck checkUnique,
			 bool indexUnchanged, IndexInfo *indexInfo)
{
	MemoryContext ctx;
	MemoryContext oldCtx;
	uint32	   *keys;
	int			nkeys;
	CobaltPendingItem *item;
	Size		size;

	if (isnull[0])
		return false;

	if (ItemPointerGetOffsetNumber(ht_ctid) > MaxHeapTuplesPerPage)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cobalt supports only heap-style tuple identifiers")));

	ctx = AllocSetContextCreate(CurrentMemoryContext, "cobalt insert",
								ALLOCSET_DEFAULT_SIZES);
	oldCtx = MemoryContextSwitchTo(ctx);

	keys = cobalt_doc_keys(DatumGetTextPP(values[0]), index->rd_indcollation[0],
						   &nkeys);

	if ((Size) nkeys > COBALT_PENDING_MAXKEYS)
	{
		/* Too many trigrams for one item: candidate for every query. */
		size = offsetof(CobaltPendingItem, keys);
		item = (CobaltPendingItem *) palloc0(size);
		item->nkeys = COBALT_PENDING_OVERFLOW;
	}
	else
	{
		size = offsetof(CobaltPendingItem, keys) + nkeys * sizeof(uint32);
		item = (CobaltPendingItem *) palloc0(size);
		item->nkeys = (uint16) nkeys;
		if (nkeys > 0)
			memcpy(item->keys, keys, nkeys * sizeof(uint32));
	}
	item->blkno = ItemPointerGetBlockNumber(ht_ctid);
	item->offnum = ItemPointerGetOffsetNumber(ht_ctid);

	cobalt_append_pending(index, item, size);

	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(ctx);

	return false;
}
