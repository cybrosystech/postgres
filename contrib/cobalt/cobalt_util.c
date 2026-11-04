/*-------------------------------------------------------------------------
 *
 * cobalt_util.c
 *	  Cobalt index: handler, page helpers, trigram extraction, stream reader,
 *	  options, operator class validation and cost estimation.
 *
 * IDENTIFICATION
 *	  contrib/cobalt/cobalt_util.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amvalidate.h"
#include "access/generic_xlog.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "cobalt.h"
#include "commands/vacuum.h"
#include "nodes/nodeFuncs.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "varatt.h"

PG_MODULE_MAGIC_EXT(
					.name = "cobalt",
					.version = PG_VERSION
);

PG_FUNCTION_INFO_V1(cobalt_handler);

static relopt_kind cobalt_relopt_kind;

void
_PG_init(void)
{
	/* No tunables yet; the kind exists so unknown options are rejected. */
	cobalt_relopt_kind = add_reloption_kind();
}

Datum
cobalt_handler(PG_FUNCTION_ARGS)
{
	static const IndexAmRoutine amroutine = {
		.type = T_IndexAmRoutine,
		.amstrategies = COBALT_NSTRATEGIES,
		.amsupport = COBALT_NPROC,
		.amoptsprocnum = 0,
		.amcanorder = false,
		.amcanorderbyop = false,
		.amcanhash = false,
		.amconsistentequality = false,
		.amconsistentordering = false,
		.amcanbackward = false,
		.amcanunique = false,
		.amcanmulticol = false,
		.amoptionalkey = false,
		.amsearcharray = false,
		.amsearchnulls = false,
		.amstorage = false,
		.amclusterable = false,
		.ampredlocks = false,
		.amcanparallel = false,
		.amcanbuildparallel = false,
		.amcaninclude = false,
		.amusemaintenanceworkmem = false,
		.amsummarizing = false,
		.amparallelvacuumoptions =
		VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_CLEANUP,
		.amkeytype = InvalidOid,

		.ambuild = cobaltbuild,
		.ambuildempty = cobaltbuildempty,
		.aminsert = cobaltinsert,
		.aminsertcleanup = NULL,
		.ambulkdelete = cobaltbulkdelete,
		.amvacuumcleanup = cobaltvacuumcleanup,
		.amcanreturn = NULL,
		.amcostestimate = cobaltcostestimate,
		.amgettreeheight = NULL,
		.amoptions = cobaltoptions,
		.amproperty = NULL,
		.ambuildphasename = NULL,
		.amvalidate = cobaltvalidate,
		.amadjustmembers = NULL,
		.ambeginscan = cobaltbeginscan,
		.amrescan = cobaltrescan,
		.amgettuple = NULL,
		.amgetbitmap = cobaltgetbitmap,
		.amendscan = cobaltendscan,
		.ammarkpos = NULL,
		.amrestrpos = NULL,
		.amestimateparallelscan = NULL,
		.aminitparallelscan = NULL,
		.amparallelrescan = NULL,
		.amtranslatestrategy = NULL,
		.amtranslatecmptype = NULL,
	};

	PG_RETURN_POINTER(&amroutine);
}

bytea *
cobaltoptions(Datum reloptions, bool validate)
{
	return (bytea *) build_reloptions(reloptions, validate,
									  cobalt_relopt_kind, 0, NULL, 0);
}

/* ------------------------------------------------------------------------
 * Pages
 * ------------------------------------------------------------------------
 */

void
CobaltInitPage(Page page, uint16 page_type)
{
	CobaltPageOpaque opaque;

	PageInit(page, BLCKSZ, sizeof(CobaltPageOpaqueData));
	opaque = CobaltPageGetOpaque(page);
	opaque->next = InvalidBlockNumber;
	opaque->page_type = page_type;
	opaque->page_id = COBALT_PAGE_ID;

	/*
	 * Stream pages are raw byte arrays: mark the whole body as used so that
	 * full-page images do not try to elide a "hole" in the middle of it.
	 */
	if (page_type == COBALT_PAGE_DATA)
		((PageHeader) page)->pd_lower = ((PageHeader) page)->pd_upper;
}

void
CobaltFillMetapage(Relation index, Page page)
{
	CobaltMetaPageData *meta;

	CobaltInitPage(page, COBALT_PAGE_META);
	meta = CobaltPageGetMeta(page);
	memset(meta, 0, sizeof(CobaltMetaPageData));
	meta->magic = COBALT_MAGIC;
	meta->version = COBALT_VERSION;
	meta->pending_head = InvalidBlockNumber;
	meta->pending_tail = InvalidBlockNumber;

	((PageHeader) page)->pd_lower =
		((char *) meta - (char *) page) + sizeof(CobaltMetaPageData);
}

void
CobaltInitMetapage(Relation index, ForkNumber forknum)
{
	Buffer		metaBuffer;
	Page		metaPage;
	GenericXLogState *state;

	metaBuffer = ReadBufferExtended(index, forknum, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);
	Assert(BufferGetBlockNumber(metaBuffer) == COBALT_META_BLKNO);

	state = GenericXLogStart(index);
	metaPage = GenericXLogRegisterBuffer(state, metaBuffer,
										 GENERIC_XLOG_FULL_IMAGE);
	CobaltFillMetapage(index, metaPage);
	GenericXLogFinish(state);

	UnlockReleaseBuffer(metaBuffer);
}

/* Allocate a new, exclusively locked page at the end of the index. */
Buffer
CobaltNewBuffer(Relation index)
{
	return ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL,
							 EB_LOCK_FIRST);
}

void
cobalt_read_meta(Relation index, CobaltMetaPageData *meta)
{
	Buffer		buf = ReadBuffer(index, COBALT_META_BLKNO);
	Page		page;

	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	if (PageIsNew(page) || CobaltPageGetOpaque(page)->page_id != COBALT_PAGE_ID ||
		CobaltPageGetMeta(page)->magic != COBALT_MAGIC)
		elog(ERROR, "index \"%s\" is not a cobalt index",
			 RelationGetRelationName(index));
	memcpy(meta, CobaltPageGetMeta(page), sizeof(CobaltMetaPageData));
	UnlockReleaseBuffer(buf);
}

/* ------------------------------------------------------------------------
 * Stream reader.  Streams are immutable runs of DATA pages, addressed by a
 * logical byte offset.  One page stays pinned between reads.
 * ------------------------------------------------------------------------
 */

void
cobalt_stream_init(CobaltStream *s, Relation index, BlockNumber start)
{
	s->index = index;
	s->start = start;
	s->buf = InvalidBuffer;
	s->bufblk = InvalidBlockNumber;
}

void
cobalt_stream_read(CobaltStream *s, uint64 addr, void *dst, uint32 len)
{
	char	   *out = (char *) dst;

	while (len > 0)
	{
		BlockNumber blk = s->start + (BlockNumber) (addr / COBALT_DATA_BYTES);
		uint32		poff = (uint32) (addr % COBALT_DATA_BYTES);
		uint32		n = Min(len, COBALT_DATA_BYTES - poff);
		Page		page;

		if (s->buf == InvalidBuffer || s->bufblk != blk)
		{
			if (s->buf != InvalidBuffer)
				ReleaseBuffer(s->buf);
			s->buf = ReadBuffer(s->index, blk);
			s->bufblk = blk;
		}

		LockBuffer(s->buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(s->buf);
		if (PageIsNew(page) ||
			CobaltPageGetOpaque(page)->page_id != COBALT_PAGE_ID ||
			CobaltPageGetOpaque(page)->page_type != COBALT_PAGE_DATA)
			elog(ERROR, "index \"%s\" contains unexpected page at block %u",
				 RelationGetRelationName(s->index), blk);
		memcpy(out, (char *) PageGetContents(page) + poff, n);
		LockBuffer(s->buf, BUFFER_LOCK_UNLOCK);

		out += n;
		addr += n;
		len -= n;
	}
}

void
cobalt_stream_end(CobaltStream *s)
{
	if (s->buf != InvalidBuffer)
		ReleaseBuffer(s->buf);
	s->buf = InvalidBuffer;
	s->bufblk = InvalidBlockNumber;
}

/* ------------------------------------------------------------------------
 * Trigram extraction
 * ------------------------------------------------------------------------
 */

/*
 * Hash three characters to a 32-bit term key.  Collisions only produce false
 * positives, which the heap recheck removes.  Key 0 is reserved.
 */
uint32
cobalt_trgm_key(pg_wchar a, pg_wchar b, pg_wchar c)
{
	uint64		x = ((uint64) a << 32) ^ ((uint64) b * UINT64CONST(0x9E3779B97F4A7C15));

	x ^= (uint64) c * UINT64CONST(0xC2B2AE3D27D4EB4F);
	x ^= x >> 29;
	x *= UINT64CONST(0xBF58476D1CE4E5B9);
	x ^= x >> 32;
	return ((uint32) x) ? (uint32) x : 1;
}

static int
cmp_uint32(const void *a, const void *b)
{
	uint32		x = *(const uint32 *) a;
	uint32		y = *(const uint32 *) b;

	return (x > y) - (x < y);
}

static int
sort_unique_keys(uint32 *keys, int n)
{
	int			i,
				j;

	if (n <= 1)
		return n;
	qsort(keys, n, sizeof(uint32), cmp_uint32);
	for (i = 1, j = 1; i < n; i++)
		if (keys[i] != keys[j - 1])
			keys[j++] = keys[i];
	return j;
}

/* Case-fold a text value and widen it to pg_wchar. */
static pg_wchar *
cobalt_fold(text *t, Oid collid, int *nchars)
{
	char	   *lowered;
	int			len;
	pg_wchar   *w;

	lowered = str_tolower(VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t), collid);
	len = strlen(lowered);
	w = (pg_wchar *) palloc((len + 1) * sizeof(pg_wchar));
	*nchars = pg_mb2wchar_with_len(lowered, w, len);
	pfree(lowered);
	return w;
}

/*
 * All distinct trigram keys of a document, sorted.  Windows slide over the
 * whole folded string, so "a b" yields (a, ,b).
 */
uint32 *
cobalt_doc_keys(text *doc, Oid collid, int *nkeys)
{
	int			nchars;
	pg_wchar   *w = cobalt_fold(doc, collid, &nchars);
	uint32	   *keys;
	int			n = 0;
	int			i;

	if (nchars < 3)
	{
		pfree(w);
		*nkeys = 0;
		return NULL;
	}

	keys = (uint32 *) palloc((nchars - 2) * sizeof(uint32));
	for (i = 0; i + 2 < nchars; i++)
		keys[n++] = cobalt_trgm_key(w[i], w[i + 1], w[i + 2]);
	pfree(w);

	*nkeys = sort_unique_keys(keys, n);
	return keys;
}

/*
 * Keys that every match of a LIKE/ILIKE pattern must contain.  Only literal
 * runs of three or more characters contribute; '%' and '_' end a run and a
 * backslash escapes the next character.  Returns NULL with *nkeys == 0 when
 * the pattern gives no usable trigram.
 */
uint32 *
cobalt_pattern_keys(text *pattern, Oid collid, int *nkeys)
{
	int			nchars;
	pg_wchar   *w = cobalt_fold(pattern, collid, &nchars);
	pg_wchar   *frag = (pg_wchar *) palloc((nchars + 1) * sizeof(pg_wchar));
	uint32	   *keys = (uint32 *) palloc((nchars + 1) * sizeof(uint32));
	int			flen = 0;
	int			n = 0;
	int			i = 0;
	int			j;

#define FLUSH_FRAGMENT() \
	do { \
		for (j = 0; j + 2 < flen; j++) \
			keys[n++] = cobalt_trgm_key(frag[j], frag[j + 1], frag[j + 2]); \
		flen = 0; \
	} while (0)

	while (i < nchars)
	{
		pg_wchar	c = w[i];

		if (c == '\\')
		{
			if (i + 1 < nchars)
				frag[flen++] = w[i + 1];
			i += 2;
		}
		else if (c == '%' || c == '_')
		{
			FLUSH_FRAGMENT();
			i++;
		}
		else
		{
			frag[flen++] = c;
			i++;
		}
	}
	FLUSH_FRAGMENT();

	pfree(w);
	pfree(frag);

	if (n == 0)
	{
		pfree(keys);
		*nkeys = 0;
		return NULL;
	}
	*nkeys = sort_unique_keys(keys, n);
	return keys;
}

/* ------------------------------------------------------------------------
 * Cost estimation
 * ------------------------------------------------------------------------
 */

void
cobaltcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				   Cost *indexStartupCost, Cost *indexTotalCost,
				   Selectivity *indexSelectivity, double *indexCorrelation,
				   double *indexPages)
{
	IndexOptInfo *index = path->indexinfo;
	GenericCosts costs = {0};
	ListCell   *lc;

	/*
	 * A pattern with no literal run of three characters cannot be narrowed by
	 * the index.  Make such paths prohibitively expensive.
	 */
	foreach(lc, path->indexclauses)
	{
		IndexClause *iclause = lfirst_node(IndexClause, lc);
		ListCell   *lc2;

		foreach(lc2, iclause->indexquals)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc2);

			if (IsA(rinfo->clause, OpExpr))
			{
				OpExpr	   *op = (OpExpr *) rinfo->clause;
				Node	   *right = (Node *) lsecond(op->args);

				if (IsA(right, Const) && !((Const *) right)->constisnull)
				{
					int			nk;

					(void) cobalt_pattern_keys(DatumGetTextPP(((Const *) right)->constvalue),
											   index->indexcollations[0], &nk);
					if (nk == 0)
					{
						*indexStartupCost = *indexTotalCost = 1.0e10;
						*indexSelectivity = 1.0;
						*indexCorrelation = 0;
						*indexPages = index->pages;
						return;
					}
				}
			}
		}
	}

	costs.numNonLeafPages = 1;
	genericcostestimate(root, path, loop_count, &costs);

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

/* ------------------------------------------------------------------------
 * Operator class validation
 * ------------------------------------------------------------------------
 */

bool
cobaltvalidate(Oid opclassoid)
{
	bool		result = true;
	HeapTuple	classtup;
	Form_pg_opclass classform;
	Oid			opfamilyoid;
	char	   *opclassname;
	char	   *opfamilyname;
	CatCList   *proclist,
			   *oprlist;
	int			i;

	classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
	if (!HeapTupleIsValid(classtup))
		elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
	classform = (Form_pg_opclass) GETSTRUCT(classtup);

	opfamilyoid = classform->opcfamily;
	opclassname = NameStr(classform->opcname);
	opfamilyname = get_opfamily_name(opfamilyoid, false);

	oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
	proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));

	/* Cobalt has no support functions. */
	for (i = 0; i < proclist->n_members; i++)
	{
		HeapTuple	proctup = &proclist->members[i]->tuple;
		Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(proctup);

		ereport(INFO,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("cobalt opfamily %s contains unexpected support function %s",
						opfamilyname,
						format_procedure(procform->amproc))));
		result = false;
	}

	for (i = 0; i < oprlist->n_members; i++)
	{
		HeapTuple	oprtup = &oprlist->members[i]->tuple;
		Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(oprtup);

		if (oprform->amopstrategy < 1 ||
			oprform->amopstrategy > COBALT_NSTRATEGIES)
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("cobalt opfamily %s contains operator %s with invalid strategy number %d",
							opfamilyname,
							format_operator(oprform->amopopr),
							oprform->amopstrategy)));
			result = false;
		}

		if (oprform->amoppurpose != AMOP_SEARCH ||
			OidIsValid(oprform->amopsortfamily))
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("cobalt opfamily %s contains invalid ORDER BY specification for operator %s",
							opfamilyname,
							format_operator(oprform->amopopr))));
			result = false;
		}

		if (!check_amop_signature(oprform->amopopr, BOOLOID,
								  oprform->amoplefttype,
								  oprform->amoprighttype))
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("cobalt opfamily %s contains operator %s with wrong signature",
							opfamilyname,
							format_operator(oprform->amopopr))));
			result = false;
		}
	}

	if (oprlist->n_members == 0)
	{
		ereport(INFO,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("cobalt opclass %s has no operators", opclassname)));
		result = false;
	}

	ReleaseCatCacheList(proclist);
	ReleaseCatCacheList(oprlist);
	ReleaseSysCache(classtup);

	return result;
}
