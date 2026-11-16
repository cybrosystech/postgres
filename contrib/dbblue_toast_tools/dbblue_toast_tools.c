/*-------------------------------------------------------------------------
 *
 * dbblue_toast_tools.c
 *	  Helpers for finding zstd-compressed data and rewriting it to a
 *	  different compression method.
 *
 * Changing a column's compression method does not touch data already
 * stored, and the obvious ways of forcing a rewrite do not work: an
 * UPDATE that assigns a column to itself carries the existing TOAST
 * pointer across untouched, VACUUM FULL and CLUSTER copy tuples verbatim,
 * and ALTER COLUMN TYPE with an identity USING clause likewise preserves
 * the datum.  All three silently leave the data exactly as it was.
 *
 * To actually recompress, the value has to be materialized as a fresh
 * plain datum, so that re-toasting has to run from scratch.  That is what
 * dbblue_force_detoast() does, for any varlena type, without going through
 * a text representation (which would risk reformatting values of types such
 * as jsonb).
 *
 * Compressed data is not only found at the top level of a column value:
 *
 *	- A composite value keeps its fields in whatever form they had when the
 *	  row was built, so a field copied from a zstd-compressed column stays
 *	  zstd-compressed inside the composite, while pg_column_compression()
 *	  on the column only describes the outer value.  Arrays of composites
 *	  carry the same problem one level down.
 *
 *	- Index entries are compressed independently of the table, using the
 *	  index column's compression method or default_toast_compression at the
 *	  time the entry was made.  An index can hold zstd keys while every
 *	  value in its table is pglz.
 *
 * dbblue_datum_has_zstd() and dbblue_force_detoast() look inside composite
 * and array values; dbblue_index_zstd_entries() reads index pages.
 *
 * IDENTIFICATION
 *	  contrib/dbblue_toast_tools/dbblue_toast_tools.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/brin_internal.h"
#include "access/brin_page.h"
#include "access/brin_tuple.h"
#include "access/detoast.h"
#include "access/genam.h"
#include "access/gin_private.h"
#include "access/gist_private.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/table.h"
#include "access/toast_compression.h"
#include "catalog/index.h"
#include "catalog/pg_am_d.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/typcache.h"
#include "varatt.h"

PG_MODULE_MAGIC_EXT(
					.name = "dbblue_toast_tools",
					.version = PG_VERSION
);

/*
 * Can a value of this type contain other varlena values that keep their own
 * compression?  True for composite types and for arrays whose elements can
 * (domains are looked through).  Arrays of plain varlenas, ranges and
 * multiranges detoast their members when they are built, so they cannot.
 */
static bool
type_can_nest(Oid typid)
{
	Oid			elemtype;

	typid = getBaseType(typid);

	if (type_is_rowtype(typid))
		return true;

	elemtype = get_element_type(typid);
	if (OidIsValid(elemtype))
		return type_can_nest(elemtype);

	return false;
}

/*
 * Deform a composite datum.  Values that point into the tuple are returned
 * as stored, so nested compressed fields stay compressed.
 */
static TupleDesc
deform_composite(HeapTupleHeader th, Datum **values, bool **nulls)
{
	TupleDesc	tupdesc;
	HeapTupleData tmptup;

	tupdesc = lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(th),
									 HeapTupleHeaderGetTypMod(th));

	tmptup.t_len = HeapTupleHeaderGetDatumLength(th);
	ItemPointerSetInvalid(&(tmptup.t_self));
	tmptup.t_tableOid = InvalidOid;
	tmptup.t_data = th;

	*values = palloc_array(Datum, tupdesc->natts);
	*nulls = palloc_array(bool, tupdesc->natts);
	heap_deform_tuple(&tmptup, tupdesc, *values, *nulls);

	return tupdesc;
}

/*
 * Does this datum, of type typid, contain zstd-compressed data at any level?
 */
static bool
datum_has_zstd(Datum value, Oid typid, bool can_nest)
{
	varlena    *attr = (varlena *) DatumGetPointer(value);
	varlena    *plain;
	bool		found = false;

	check_stack_depth();

	if (toast_get_compression_id(attr) == TOAST_ZSTD_COMPRESSION_ID)
		return true;

	if (!can_nest)
		return false;

	typid = getBaseType(typid);
	plain = pg_detoast_datum(attr);

	if (type_is_rowtype(typid))
	{
		Datum	   *values;
		bool	   *nulls;
		TupleDesc	tupdesc;

		tupdesc = deform_composite((HeapTupleHeader) plain, &values, &nulls);

		for (int i = 0; i < tupdesc->natts && !found; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);

			if (att->attisdropped || nulls[i] || att->attlen != -1)
				continue;
			found = datum_has_zstd(values[i], att->atttypid,
								   type_can_nest(att->atttypid));
		}

		ReleaseTupleDesc(tupdesc);
	}
	else
	{
		ArrayType  *arr = (ArrayType *) plain;
		Oid			elemtype = ARR_ELEMTYPE(arr);
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;
		Datum	   *elems;
		bool	   *elemnulls;
		int			nelems;
		bool		elem_can_nest = type_can_nest(elemtype);

		get_typlenbyvalalign(elemtype, &elmlen, &elmbyval, &elmalign);
		if (elmlen != -1)
			return false;

		deconstruct_array(arr, elemtype, elmlen, elmbyval, elmalign,
						  &elems, &elemnulls, &nelems);

		for (int i = 0; i < nelems && !found; i++)
		{
			if (!elemnulls[i])
				found = datum_has_zstd(elems[i], elemtype, elem_can_nest);
		}
	}

	return found;
}

/*
 * Return a copy of a varlena datum with no compression or external storage
 * at any level: composite fields and array elements are flattened too.
 */
static Datum
deep_flatten(Datum value, Oid typid)
{
	varlena    *plain;

	check_stack_depth();

	plain = pg_detoast_datum_copy((varlena *) DatumGetPointer(value));

	if (!type_can_nest(typid))
		return PointerGetDatum(plain);

	typid = getBaseType(typid);

	if (type_is_rowtype(typid))
	{
		Datum	   *values;
		bool	   *nulls;
		TupleDesc	tupdesc;
		HeapTuple	newtup;
		Datum		result;

		tupdesc = deform_composite((HeapTupleHeader) plain, &values, &nulls);

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);

			if (att->attisdropped || nulls[i] || att->attlen != -1)
				continue;
			values[i] = deep_flatten(values[i], att->atttypid);
		}

		newtup = heap_form_tuple(tupdesc, values, nulls);
		result = heap_copy_tuple_as_datum(newtup, tupdesc);
		ReleaseTupleDesc(tupdesc);
		return result;
	}
	else
	{
		ArrayType  *arr = (ArrayType *) plain;
		Oid			elemtype = ARR_ELEMTYPE(arr);
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;
		Datum	   *elems;
		bool	   *elemnulls;
		int			nelems;
		ArrayType  *result;

		get_typlenbyvalalign(elemtype, &elmlen, &elmbyval, &elmalign);
		if (elmlen != -1)
			return PointerGetDatum(plain);

		deconstruct_array(arr, elemtype, elmlen, elmbyval, elmalign,
						  &elems, &elemnulls, &nelems);
		for (int i = 0; i < nelems; i++)
		{
			if (!elemnulls[i])
				elems[i] = deep_flatten(elems[i], elemtype);
		}

		result = construct_md_array(elems, elemnulls,
									ARR_NDIM(arr), ARR_DIMS(arr), ARR_LBOUND(arr),
									elemtype, elmlen, elmbyval, elmalign);
		return PointerGetDatum(result);
	}
}

/* Per-call-site cache of the argument type, for the row-by-row functions */
typedef struct ArgTypeInfo
{
	Oid			typid;
	int16		typlen;
	bool		can_nest;
} ArgTypeInfo;

static ArgTypeInfo *
get_arg_type_info(FunctionCallInfo fcinfo)
{
	ArgTypeInfo *info = (ArgTypeInfo *) fcinfo->flinfo->fn_extra;
	Oid			typid = get_fn_expr_argtype(fcinfo->flinfo, 0);

	if (!OidIsValid(typid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine argument data type")));

	if (info == NULL || info->typid != typid)
	{
		if (info == NULL)
			info = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
									  sizeof(ArgTypeInfo));
		info->typid = typid;
		info->typlen = get_typlen(typid);
		info->can_nest = info->typlen == -1 && type_can_nest(typid);
		fcinfo->flinfo->fn_extra = info;
	}

	return info;
}

/*
 * Return the argument as a fully detoasted, decompressed copy, including
 * any fields of composite values and elements of arrays of composites.
 *
 * Non-varlena types cannot be toasted, so they are passed through.
 */
PG_FUNCTION_INFO_V1(dbblue_force_detoast);
Datum
dbblue_force_detoast(PG_FUNCTION_ARGS)
{
	ArgTypeInfo *info = get_arg_type_info(fcinfo);

	if (info->typlen != -1)
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));

	PG_RETURN_DATUM(deep_flatten(PG_GETARG_DATUM(0), info->typid));
}

/*
 * Does the argument contain zstd-compressed data, either as the value
 * itself or anywhere inside it?
 */
PG_FUNCTION_INFO_V1(dbblue_datum_has_zstd);
Datum
dbblue_datum_has_zstd(PG_FUNCTION_ARGS)
{
	ArgTypeInfo *info = get_arg_type_info(fcinfo);

	if (info->typlen != -1)
		PG_RETURN_BOOL(false);

	PG_RETURN_BOOL(datum_has_zstd(PG_GETARG_DATUM(0), info->typid,
								  info->can_nest));
}

/*
 * Check the attributes of one index tuple; natts may be less than the
 * descriptor's when the tuple is a truncated pivot or a non-leaf tuple.
 */
static bool
index_tuple_has_zstd(IndexTuple itup, TupleDesc tupdesc, int natts)
{
	for (int attno = 1; attno <= natts; attno++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, attno - 1);
		Datum		value;
		bool		isnull;

		if (att->attlen != -1)
			continue;

		value = index_getattr(itup, attno, tupdesc, &isnull);
		if (!isnull &&
			datum_has_zstd(value, att->atttypid, type_can_nest(att->atttypid)))
			return true;
	}
	return false;
}

/*
 * Count the zstd-compressed entries on one index page.
 */
static int64
scan_index_page(Relation index, Page page, GinState *ginstate,
				BrinDesc *bdesc)
{
	TupleDesc	tupdesc = RelationGetDescr(index);
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	Oid			am = index->rd_rel->relam;
	int			natts = IndexRelationGetNumberOfAttributes(index);
	int64		count = 0;

	/* Skip pages that hold no entry tuples */
	switch (am)
	{
		case BTREE_AM_OID:
			{
				BTPageOpaque opaque = BTPageGetOpaque(page);

				if (P_ISMETA(opaque) || P_IGNORE(opaque))
					return 0;
				break;
			}
		case GIST_AM_OID:
			if (GistPageIsDeleted(page))
				return 0;
			/* non-leaf GiST tuples carry key columns only */
			if (!GistPageIsLeaf(page))
				natts = IndexRelationGetNumberOfKeyAttributes(index);
			break;
		case GIN_AM_OID:
			/* posting tree pages hold item pointers, not keys */
			if (GinPageIsData(page) || GinPageIsDeleted(page))
				return 0;
			break;
		case BRIN_AM_OID:
			if (!BRIN_IS_REGULAR_PAGE(page))
				return 0;
			break;
	}

	for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
	{
		ItemId		iid = PageGetItemId(page, off);
		bool		found = false;

		/* dead btree entries still hold their key; count them too */
		if (!ItemIdHasStorage(iid))
			continue;

		switch (am)
		{
			case BTREE_AM_OID:
				{
					IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);

					found = index_tuple_has_zstd(itup, tupdesc,
												 BTreeTupleGetNAtts(itup, index));
					break;
				}
			case GIST_AM_OID:
				{
					IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);

					if (!GistTupleIsInvalid(itup))
						found = index_tuple_has_zstd(itup, tupdesc, natts);
					break;
				}
			case GIN_AM_OID:
				{
					IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);
					OffsetNumber attnum = gintuple_get_attrnum(ginstate, itup);
					GinNullCategory category;
					Datum		key = gintuple_get_key(ginstate, itup, &category);
					Form_pg_attribute att =
						TupleDescAttr(ginstate->origTupdesc, attnum - 1);

					if (category == GIN_CAT_NORM_KEY && att->attlen == -1)
						found = datum_has_zstd(key, att->atttypid,
											   type_can_nest(att->atttypid));
					break;
				}
			case BRIN_AM_OID:
				{
					BrinTuple  *btup = (BrinTuple *) PageGetItem(page, iid);
					BrinMemTuple *dtup = brin_deform_tuple(bdesc, btup, NULL);

					for (int keyno = 0;
						 keyno < bdesc->bd_tupdesc->natts && !found;
						 keyno++)
					{
						BrinValues *bv = &dtup->bt_columns[keyno];
						BrinOpcInfo *opcinfo = bdesc->bd_info[keyno];

						if (bv->bv_allnulls)
							continue;
						for (int i = 0; i < opcinfo->oi_nstored && !found; i++)
						{
							TypeCacheEntry *tc = opcinfo->oi_typcache[i];

							if (tc->typlen == -1)
								found = datum_has_zstd(bv->bv_values[i],
													   tc->type_id,
													   type_can_nest(tc->type_id));
						}
					}
					break;
				}
		}

		if (found)
			count++;
	}

	return count;
}

/*
 * Count index entries that contain zstd-compressed data.
 *
 * Supports btree, GiST, GIN and BRIN, the built-in access methods that
 * compress keys.  Hash indexes store only hash codes and SP-GiST detoasts
 * its input, so both always return 0.  For any other access method the
 * page layout is unknown and NULL is returned: the caller cannot tell, and
 * should rebuild the index to be safe.
 */
PG_FUNCTION_INFO_V1(dbblue_index_zstd_entries);
Datum
dbblue_index_zstd_entries(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	Oid			heapoid;
	Relation	heaprel;
	Relation	index;
	Oid			am;
	int64		count = 0;
	GinState	ginstate;
	BrinDesc   *bdesc = NULL;
	BlockNumber nblocks;
	BufferAccessStrategy strategy;
	MemoryContext pagecxt;
	MemoryContext oldcxt;

	heapoid = IndexGetRelation(indexoid, true);
	if (!OidIsValid(heapoid))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index", get_rel_name(indexoid))));

	if (pg_class_aclcheck(heapoid, GetUserId(), ACL_SELECT) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for table %s", get_rel_name(heapoid))));

	/* lock the table before the index, as everything else does */
	heaprel = table_open(heapoid, AccessShareLock);
	index = index_open(indexoid, AccessShareLock);
	am = index->rd_rel->relam;

	/* partitioned indexes have no storage; their partitions are checked */
	if (index->rd_rel->relkind != RELKIND_INDEX)
		goto done;

	if (RELATION_IS_OTHER_TEMP(index))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot inspect temporary indexes of other sessions")));

	switch (am)
	{
		case BTREE_AM_OID:
		case GIST_AM_OID:
			break;
		case GIN_AM_OID:
			initGinState(&ginstate, index);
			break;
		case BRIN_AM_OID:
			bdesc = brin_build_desc(index);
			break;
		case HASH_AM_OID:
		case SPGIST_AM_OID:
			goto done;
		default:
			index_close(index, AccessShareLock);
			table_close(heaprel, AccessShareLock);
			PG_RETURN_NULL();
	}

	nblocks = RelationGetNumberOfBlocks(index);
	strategy = GetAccessStrategy(BAS_BULKREAD);
	pagecxt = AllocSetContextCreate(CurrentMemoryContext,
									"dbblue_index_zstd_entries",
									ALLOCSET_DEFAULT_SIZES);

	for (BlockNumber blkno = 0; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;

		CHECK_FOR_INTERRUPTS();

		/* the GIN metapage has no special-space flags to test */
		if (am == GIN_AM_OID && blkno == GIN_METAPAGE_BLKNO)
			continue;

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL,
								 strategy);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		oldcxt = MemoryContextSwitchTo(pagecxt);
		if (!PageIsNew(page))
			count += scan_index_page(index, page,
									 am == GIN_AM_OID ? &ginstate : NULL,
									 bdesc);
		MemoryContextSwitchTo(oldcxt);
		MemoryContextReset(pagecxt);

		UnlockReleaseBuffer(buf);
	}

	MemoryContextDelete(pagecxt);
	FreeAccessStrategy(strategy);
	if (bdesc)
		brin_free_desc(bdesc);

done:
	index_close(index, AccessShareLock);
	table_close(heaprel, AccessShareLock);

	PG_RETURN_INT64(count);
}
