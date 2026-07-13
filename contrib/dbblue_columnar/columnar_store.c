/*-------------------------------------------------------------------------
 *
 * columnar_store.c
 *		DBblue Columnar Engine - in-memory column store (Milestone 2).
 *
 * Implements the shared column store described in dbblue_columnar.h: a DSA
 * area (via the dsm_registry) holding per-relation block directories, plus
 * the populate path that builds columnar blocks from the heap and the
 * introspection SRF used to inspect them.
 *
 * Correctness contract of a built block: every heap page in its range had
 * PD_ALL_VISIBLE set while we held a share lock on it, and the page's LSN at
 * that instant is recorded in the block header. A later reader may serve the
 * block only if every page is still all-visible in the visibility map AND its
 * current page LSN equals the recorded one. Any modification clears the VM
 * bit and (on a logged relation) advances the LSN, so a block can never be
 * served after its heap range changed - including after a vacuum re-sets the
 * VM bit, which is exactly the case the LSN comparison exists for. Unlogged
 * and temporary relations are rejected: their heap pages do not carry
 * reliable LSNs, which would make that comparison unsound.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tupmacs.h"
#include "access/visibilitymap.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/dshash.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/dsm_registry.h"
#include "storage/itemid.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/tuplestore.h"
#include "utils/typcache.h"

#include "dbblue_columnar.h"

/* shared control: memory accounting for the whole column store */
typedef struct DbbcControl
{
	LWLock		lck;
	int64		bytes_used;
} DbbcControl;

static DbbcControl *dbbc_control = NULL;
static dsa_area *dbbc_dsa = NULL;
static dshash_table *dbbc_hash = NULL;

static const dshash_parameters dbbc_hash_params = {
	sizeof(DbbcRelKey),
	sizeof(DbbcRelEntry),
	dshash_memcmp,
	dshash_memhash,
	dshash_memcpy
};

/* per-column build-time metadata */
typedef struct DbbcColMeta
{
	int16		attnum;
	Oid			atttypid;
	int16		attlen;
	bool		attbyval;
	Oid			attcollation;
	bool		has_cmp;
	FmgrInfo	cmpfn;
} DbbcColMeta;

/* per-column local accumulator while building one block */
typedef struct DbbcColBuild
{
	uint8	   *fixed;			/* fixed-length: stride = attlen */
	uint32	   *offsets;		/* varlena: per-row offset or NULL sentinel */
	uint8	   *blob;			/* varlena bytes */
	Size		blob_len;
	Size		blob_cap;
	uint8	   *nullbits;		/* lazily allocated null bitmap */
	uint32		null_count;
	bool		has_minmax;
	Datum		min_val;		/* point into build memory */
	Datum		max_val;
} DbbcColBuild;

static void dbbc_init_control(void *ptr, void *arg);
static bool dbbc_try_reserve(Size nbytes);
static void dbbc_release_bytes(int64 nbytes);
static int	dbbc_registered_attnums(Oid relid, int16 **attnums_out);
static void dbbc_version_free(DbbcRelVersion *version);
static dsa_pointer dbbc_copy_to_dsa(const void *src, Size len, Size *acct);
static Datum dbbc_chunk_fetch_minmax(DbbcColumnChunk *chunk, dsa_pointer ptr);
static void dbbc_pin_release_cb(Datum arg);

/*
 * Version pins held by this backend are registered with the current
 * ResourceOwner so that an aborted query still unpins (EndCustomScan and
 * normal cleanup paths are not reached on ERROR).
 */
static const ResourceOwnerDesc dbbc_pin_resowner_desc = {
	.name = "dbblue_columnar version pin",
	.release_phase = RESOURCE_RELEASE_BEFORE_LOCKS,
	.release_priority = RELEASE_PRIO_FIRST,
	.ReleaseResource = dbbc_pin_release_cb,
	.DebugPrint = NULL,
};

/*
 * Attach this backend to the shared column store, creating it on first use
 * anywhere in the cluster.
 */
void
dbbc_store_attach(void)
{
	bool		found;

	if (dbbc_control == NULL)
		dbbc_control = GetNamedDSMSegment("dbblue_columnar_control",
										  sizeof(DbbcControl),
										  dbbc_init_control, &found, NULL);
	if (dbbc_dsa == NULL)
		dbbc_dsa = GetNamedDSA("dbblue_columnar", &found);
	if (dbbc_hash == NULL)
		dbbc_hash = GetNamedDSHash("dbblue_columnar_hash",
								   &dbbc_hash_params, &found);
}

static void
dbbc_init_control(void *ptr, void *arg)
{
	DbbcControl *control = (DbbcControl *) ptr;

	LWLockInitialize(&control->lck, LWLockNewTrancheId("dbblue_columnar"));
	control->bytes_used = 0;
}

dsa_area *
dbbc_store_dsa(void)
{
	dbbc_store_attach();
	return dbbc_dsa;
}

dshash_table *
dbbc_store_hash(void)
{
	dbbc_store_attach();
	return dbbc_hash;
}

int64
dbbc_store_bytes_used(void)
{
	int64		result;

	dbbc_store_attach();
	LWLockAcquire(&dbbc_control->lck, LW_SHARED);
	result = dbbc_control->bytes_used;
	LWLockRelease(&dbbc_control->lck);
	return result;
}

/*
 * Reserve nbytes against the dbblue_columnar.memory_mb budget; false if the
 * budget would be exceeded (caller stops building, never errors).
 */
static bool
dbbc_try_reserve(Size nbytes)
{
	int64		budget = (int64) dbblue_columnar_memory_mb * 1024 * 1024;
	bool		ok;

	LWLockAcquire(&dbbc_control->lck, LW_EXCLUSIVE);
	ok = (dbbc_control->bytes_used + (int64) nbytes <= budget);
	if (ok)
		dbbc_control->bytes_used += (int64) nbytes;
	LWLockRelease(&dbbc_control->lck);
	return ok;
}

static void
dbbc_release_bytes(int64 nbytes)
{
	LWLockAcquire(&dbbc_control->lck, LW_EXCLUSIVE);
	dbbc_control->bytes_used -= nbytes;
	Assert(dbbc_control->bytes_used >= 0);
	LWLockRelease(&dbbc_control->lck);
}

/*
 * Schema-qualified, quoted name of the registration table, resolved through
 * the extension's namespace so that neither reads nor writes depend on (or
 * can be captured by) the caller's search_path.
 */
char *
dbbc_registry_table_name(void)
{
	Oid			extoid;
	Oid			nspoid;

	extoid = get_extension_oid("dbblue_columnar", true);
	if (!OidIsValid(extoid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("extension \"dbblue_columnar\" is not installed"),
				 errhint("Run CREATE EXTENSION dbblue_columnar first.")));
	nspoid = get_extension_schema(extoid);

	return psprintf("%s.dbblue_columnar_relations",
					quote_identifier(get_namespace_name(nspoid)));
}

/*
 * Read the registered attnums for relid from the registration table.
 * Returns the count; *attnums_out is palloc'd in the caller's context,
 * ascending.
 */
static int
dbbc_registered_attnums(Oid relid, int16 **attnums_out)
{
	char	   *sql;
	Oid			argtypes[1] = {OIDOID};
	Datum		values[1];
	int			ret;
	int			ncols;
	int16	   *attnums;
	int			i;
	MemoryContext callercxt = CurrentMemoryContext;

	sql = psprintf("SELECT attnum FROM %s WHERE relid = $1 ORDER BY attnum",
				   dbbc_registry_table_name());

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "dbblue_columnar: SPI_connect failed");

	values[0] = ObjectIdGetDatum(relid);
	ret = SPI_execute_with_args(sql, 1, argtypes, values, NULL, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "dbblue_columnar: registration lookup failed (%d)", ret);

	ncols = (int) SPI_processed;

	/*
	 * Allocate in the caller's context, not the SPI procedure context that
	 * SPI_finish is about to destroy.
	 */
	attnums = (int16 *) MemoryContextAlloc(callercxt,
										   Max(ncols, 1) * sizeof(int16));

	/* copy out before SPI_finish frees SPI memory */
	for (i = 0; i < ncols; i++)
	{
		bool		isnull;
		Datum		d;

		d = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc,
						  1, &isnull);
		Assert(!isnull);
		attnums[i] = DatumGetInt16(d);
	}

	SPI_finish();

	*attnums_out = attnums;
	return ncols;
}

/*
 * Append len bytes to a column's varlena blob at the next MAXALIGN'd offset
 * (zero pad bytes), growing as needed. Returns the offset the value was
 * stored at, so blob+offset is directly usable as a varlena Datum even for
 * double-aligned types (e.g. int8range) whose internals assume the datum
 * starts at a maximally-aligned address.
 */
static uint32
dbbc_blob_append(DbbcColBuild *cb, const void *data, Size len)
{
	Size		aligned = MAXALIGN(cb->blob_len);

	if (aligned + len > cb->blob_cap)
	{
		Size		newcap = Max(cb->blob_cap * 2, (Size) 65536);

		while (newcap < aligned + len)
			newcap *= 2;
		if (cb->blob == NULL)
			cb->blob = (uint8 *) palloc(newcap);
		else
			cb->blob = (uint8 *) repalloc(cb->blob, newcap);
		cb->blob_cap = newcap;
	}
	if (aligned > cb->blob_len)
		memset(cb->blob + cb->blob_len, 0, aligned - cb->blob_len);
	memcpy(cb->blob + aligned, data, len);
	cb->blob_len = aligned + len;

	return (uint32) aligned;
}

/* track zone-map min/max for one non-null value */
static void
dbbc_track_minmax(DbbcColMeta *meta, DbbcColBuild *cb, Datum value)
{
	int32		cmp;

	if (!meta->has_cmp)
		return;
	if (!cb->has_minmax)
	{
		cb->min_val = value;
		cb->max_val = value;
		cb->has_minmax = true;
		return;
	}
	cmp = DatumGetInt32(FunctionCall2Coll(&meta->cmpfn, meta->attcollation,
										  value, cb->min_val));
	if (cmp < 0)
		cb->min_val = value;
	cmp = DatumGetInt32(FunctionCall2Coll(&meta->cmpfn, meta->attcollation,
										  value, cb->max_val));
	if (cmp > 0)
		cb->max_val = value;
}

/* copy len bytes into the DSA, adding to *acct */
static dsa_pointer
dbbc_copy_to_dsa(const void *src, Size len, Size *acct)
{
	dsa_pointer p = dsa_allocate(dbbc_dsa, len);

	memcpy(dsa_get_address(dbbc_dsa, p), src, len);
	*acct += len;
	return p;
}

/* store a min/max datum copy (fixed: attlen bytes; varlena: whole value) */
static dsa_pointer
dbbc_store_minmax(DbbcColMeta *meta, Datum value, Size *acct)
{
	if (meta->attlen > 0)
	{
		char		buf[16];

		Assert(meta->attlen <= (int16) sizeof(buf));
		if (meta->attbyval)
			store_att_byval(buf, value, meta->attlen);
		else
			memcpy(buf, DatumGetPointer(value), meta->attlen);
		return dbbc_copy_to_dsa(buf, meta->attlen, acct);
	}
	else
	{
		struct varlena *v = (struct varlena *) DatumGetPointer(value);

		Assert(!VARATT_IS_EXTENDED(v));
		return dbbc_copy_to_dsa(v, VARSIZE(v), acct);
	}
}

/* rebuild a chunk's zone-map min or max as a Datum (chunk must have_minmax) */
Datum
dbbc_chunk_minmax_datum(DbbcColumnChunk *chunk, bool want_max)
{
	dbbc_store_attach();
	return dbbc_chunk_fetch_minmax(chunk,
								   want_max ? chunk->max_value : chunk->min_value);
}

/* rebuild a Datum from a stored min/max copy */
static Datum
dbbc_chunk_fetch_minmax(DbbcColumnChunk *chunk, dsa_pointer ptr)
{
	char	   *addr = (char *) dsa_get_address(dbbc_dsa, ptr);

	if (chunk->attlen > 0)
	{
		if (chunk->attbyval)
			return fetch_att(addr, true, chunk->attlen);
		return PointerGetDatum(addr);
	}
	return PointerGetDatum(addr);
}

/*
 * Drop one reference to a block. If this was the last reference, free all of
 * its DSA allocations and release its bytes to the memory budget; otherwise
 * (the block is still reachable through another version, e.g. an incremental
 * refresh that reused it) leave it untouched. Every version referencing a
 * block holds exactly one ref, taken at build (new block) or reuse
 * (dbbc_block_ref), so the block outlives every version that points at it.
 */
static void
dbbc_block_unref(dsa_pointer blockptr, int ncols)
{
	DbbcBlock  *block = (DbbcBlock *) dsa_get_address(dbbc_dsa, blockptr);
	DbbcColumnChunk *chunks;
	int			c;

	if (pg_atomic_sub_fetch_u32(&block->refs, 1) != 0)
		return;					/* still referenced by another version */

	/* a block abandoned before its chunk array was allocated */
	if (DsaPointerIsValid(block->chunks))
	{
		chunks = (DbbcColumnChunk *) dsa_get_address(dbbc_dsa, block->chunks);
		for (c = 0; c < ncols; c++)
		{
			DbbcColumnChunk *chunk = &chunks[c];

			if (DsaPointerIsValid(chunk->values))
				dsa_free(dbbc_dsa, chunk->values);
			if (DsaPointerIsValid(chunk->nulls))
				dsa_free(dbbc_dsa, chunk->nulls);
			if (DsaPointerIsValid(chunk->varblob))
				dsa_free(dbbc_dsa, chunk->varblob);
			if (DsaPointerIsValid(chunk->min_value))
				dsa_free(dbbc_dsa, chunk->min_value);
			if (DsaPointerIsValid(chunk->max_value))
				dsa_free(dbbc_dsa, chunk->max_value);
		}
		dsa_free(dbbc_dsa, block->chunks);
	}

	/* release this block's whole reservation exactly once, at physical free */
	dbbc_release_bytes((int64) block->block_bytes);
	dsa_free(dbbc_dsa, blockptr);
}

/* take a reference to an existing block (incremental refresh block reuse) */
static inline void
dbbc_block_ref(dsa_pointer blockptr)
{
	DbbcBlock  *block = (DbbcBlock *) dsa_get_address(dbbc_dsa, blockptr);

	pg_atomic_add_fetch_u32(&block->refs, 1);
}

/*
 * Is a prior version's block still a byte-for-byte accurate columnarization of
 * its heap range - so an incremental refresh can reuse it instead of rebuilding
 * it? True iff the block covers exactly this slot's current range AND every
 * page is still all-visible with the same LSN recorded when the block was built
 * (both checked under a share lock). This is the same proof the serve path uses
 * (dbbc_block_valid): identical VM+LSN means the page bytes are unchanged, so
 * the columnar data still matches the heap.
 */
static bool
dbbc_block_still_valid(DbbcBlock *block, Relation rel,
					   BlockNumber first_page, uint16 npages)
{
	uint16		p;

	if (block->first_page != first_page || block->npages != npages)
		return false;

	for (p = 0; p < npages; p++)
	{
		Buffer		buf = ReadBuffer(rel, first_page + p);
		Page		page;
		bool		ok;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		ok = PageIsAllVisible(page) &&
			BufferGetLSNAtomic(buf) == block->stamps[p].lsn;
		UnlockReleaseBuffer(buf);
		if (!ok)
			return false;
	}
	return true;
}

/* free a retired version: its blocks, directory, attnums, and itself */
static void
dbbc_version_free(DbbcRelVersion *version)
{
	Size		meta = version->total_bytes;

	if (DsaPointerIsValid(version->blockdir))
	{
		dsa_pointer *dir = (dsa_pointer *)
			dsa_get_address(dbbc_dsa, version->blockdir);
		uint32		i;

		/*
		 * Drop this version's reference to each block; a block shared with a
		 * live version (incremental reuse) survives, and only the block whose
		 * last reference this is gets freed - releasing its own bytes.
		 */
		for (i = 0; i < version->ndirslots; i++)
		{
			if (DsaPointerIsValid(dir[i]))
				dbbc_block_unref(dir[i], version->ncols);
		}
		dsa_free(dbbc_dsa, version->blockdir);
	}
	if (DsaPointerIsValid(version->attnums))
		dsa_free(dbbc_dsa, version->attnums);

	version->magic = DBBC_VERSION_POISON;
	dsa_free(dbbc_dsa, version->self);

	/*
	 * Release only this version's own metadata bytes (directory + attnums +
	 * the version struct). Block bytes are released per block in
	 * dbbc_block_unref, so shared blocks are accounted exactly once.
	 */
	dbbc_release_bytes((int64) meta);
}

/*
 * Pin the current version of a relation's column store, or NULL if none.
 * The pin keeps the version (and every block under it) alive even across a
 * concurrent repopulate/drop; the entry lock itself is held only momentarily
 * (dshash forbids holding one across another lookup).
 */
DbbcRelVersion *
dbbc_version_pin(Oid reloid)
{
	DbbcRelKey	key;
	DbbcRelEntry *entry;
	DbbcRelVersion *version = NULL;

	dbbc_store_attach();

	key.dboid = MyDatabaseId;
	key.reloid = reloid;
	entry = (DbbcRelEntry *) dshash_find(dbbc_hash, &key, false);
	if (entry == NULL)
		return NULL;
	if (DsaPointerIsValid(entry->version))
	{
		version = (DbbcRelVersion *)
			dsa_get_address(dbbc_dsa, entry->version);
		/*
		 * Liveness guard: a live version carries DBBC_VERSION_MAGIC (set at
		 * publish, cleared to POISON at free). Reading + bumping happen under
		 * the entry's dshash lock, and publish swaps entry->version under the
		 * same lock's exclusive mode, so entry->version always references a
		 * live version - this should never trip. It is kept as cheap
		 * defense-in-depth for a shared, refcounted, droppable cache: if it
		 * ever did, degrade to a heap scan rather than dereference freed DSA.
		 */
		if (unlikely(version->magic != DBBC_VERSION_MAGIC))
		{
			elog(DEBUG1, "dbblue_columnar: skipping non-live version (magic=0x%08x)",
				 version->magic);
			version = NULL;
		}
		else
			pg_atomic_add_fetch_u32(&version->pins, 1);
	}
	dshash_release_lock(dbbc_hash, entry);

	return version;
}

/* drop a pin; whoever reaches zero frees the version */
void
dbbc_version_unpin(DbbcRelVersion *version)
{
	if (pg_atomic_sub_fetch_u32(&version->pins, 1) == 0)
		dbbc_version_free(version);
}

static void
dbbc_pin_release_cb(Datum arg)
{
	dbbc_version_unpin((DbbcRelVersion *) DatumGetPointer(arg));
}

/* pin/unpin registered with the current ResourceOwner (abort-safe) */
DbbcRelVersion *
dbbc_version_pin_tracked(Oid reloid)
{
	DbbcRelVersion *version;

	ResourceOwnerEnlarge(CurrentResourceOwner);
	version = dbbc_version_pin(reloid);
	if (version != NULL)
		ResourceOwnerRemember(CurrentResourceOwner,
							  PointerGetDatum(version),
							  &dbbc_pin_resowner_desc);
	return version;
}

/*
 * Attach to a SPECIFIC version by its dsa_pointer and pin it (tracked). Used
 * by parallel workers: the leader publishes its pinned version's dsa_pointer
 * into the scan's DSM so every participant pins the exact same immutable
 * version, even if a concurrent repopulate swapped the relation's current
 * version in between. The pin keeps it alive regardless.
 */
DbbcRelVersion *
dbbc_version_attach_tracked(dsa_pointer vp)
{
	DbbcRelVersion *version;

	if (!DsaPointerIsValid(vp))
		return NULL;
	dbbc_store_attach();
	version = (DbbcRelVersion *) dsa_get_address(dbbc_dsa, vp);

	ResourceOwnerEnlarge(CurrentResourceOwner);
	pg_atomic_add_fetch_u32(&version->pins, 1);
	ResourceOwnerRemember(CurrentResourceOwner,
						  PointerGetDatum(version),
						  &dbbc_pin_resowner_desc);
	return version;
}

void
dbbc_version_unpin_tracked(DbbcRelVersion *version)
{
	ResourceOwnerForget(CurrentResourceOwner,
						PointerGetDatum(version),
						&dbbc_pin_resowner_desc);
	dbbc_version_unpin(version);
}

/*
 * dbblue_columnar_populate(regclass) -> integer
 *
 * Build (or rebuild) the column store for a registered relation. Returns the
 * number of columnar blocks built. Ranges whose pages are not all all-visible
 * are skipped (their data stays heap-only); building stops early if the
 * memory budget is reached.
 */
PG_FUNCTION_INFO_V1(dbblue_columnar_populate);

Datum
dbblue_columnar_populate(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(dbbc_populate_relation(PG_GETARG_OID(0)));
}

/*
 * Build (or rebuild) the in-memory column store for one relation; returns the
 * number of columnar blocks built. Callable from the SQL function above and
 * from the background auto-refresh worker.
 */
int
dbbc_populate_relation(Oid relid)
{
	Relation	rel;
	TupleDesc	tupdesc;
	int16	   *attnums;
	int			ncols;
	DbbcColMeta *colmeta;
	DbbcColBuild *colbuild;
	BlockNumber nheappages;
	uint32		ndirslots;
	dsa_pointer *newdir;

	/*
	 * State shared with the PG_CATCH cleanup: what has been reserved and
	 * allocated but not yet published (volatile per setjmp rules).
	 */
	volatile Size inflight_bytes = 0;	/* reservation of the block being built */
	volatile Size meta_resv = 0;	/* version+directory+attnums reservation */
	volatile dsa_pointer attnums_dsa = InvalidDsaPointer;
	volatile dsa_pointer blockdir_dsa = InvalidDsaPointer;
	volatile dsa_pointer version_dsa = InvalidDsaPointer;
	volatile bool published = false;
	volatile int blocks_built = 0;
	volatile int blocks_reused = 0; /* of blocks_built, reused from prior ver */
	bool		budget_hit = false;
	Buffer		vmbuf = InvalidBuffer;
	MemoryContext blkcxt;
	MemoryContext oldcxt;
	DbbcRelKey	key;
	DbbcRelEntry *entry;
	bool		found;
	/*
	 * Incremental refresh: the prior published version, whose still-valid
	 * blocks this build reuses instead of rebuilding. Pinned across the build
	 * (tracked, so an abort unpins), unpinned after publish. NULL if there is
	 * no prior version.
	 */
	DbbcRelVersion *reuse_src;
	dsa_pointer *olddir = NULL;
	bool		reuse_cols_ok = false;
	uint32		slot;
	int			c;

	dbbc_store_attach();

	ncols = dbbc_registered_attnums(relid, &attnums);
	if (ncols == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("no columns of relation \"%s\" are registered for columnarization",
						get_rel_name(relid)),
				 errhint("Register columns with dbblue_columnar_add first.")));

	/*
	 * ShareUpdateExclusiveLock, not AccessShareLock: it is self-conflicting,
	 * so two populates of the same relation (e.g. the auto-refresh worker and
	 * a manual call, or two manual calls) serialize instead of racing to
	 * build+publish+free versions concurrently. It does not conflict with
	 * readers (AccessShareLock), so queries are unaffected; it does serialize
	 * with VACUUM, which is desirable - populate then sees a stable heap.
	 */
	rel = table_open(relid, ShareUpdateExclusiveLock);

	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a plain table or materialized view",
						RelationGetRelationName(rel))));

	/*
	 * Unlogged and temporary relations do not carry reliable page LSNs, so
	 * the LSN half of the serve-time validity check would be unsound (a
	 * modified-then-revacuumed page could present an unchanged LSN). Reject
	 * them loudly rather than ever risking a stale block.
	 */
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot columnarize unlogged or temporary relation \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("The columnar validity check relies on page LSNs, which only permanent relations maintain.")));

	/*
	 * The raw-page walk below (line pointers, PD_ALL_VISIBLE, heap tuples)
	 * and the scan's range-limited fallback are heap-AM-only.
	 */
	if (rel->rd_rel->relam != HEAP_TABLE_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot columnarize relation \"%s\": unsupported table access method",
						RelationGetRelationName(rel))));

	tupdesc = RelationGetDescr(rel);

	/* resolve and validate per-column metadata */
	colmeta = (DbbcColMeta *) palloc0(ncols * sizeof(DbbcColMeta));
	for (c = 0; c < ncols; c++)
	{
		Form_pg_attribute att;
		TypeCacheEntry *typentry;

		if (attnums[c] <= 0 || attnums[c] > tupdesc->natts)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("registered column %d of relation \"%s\" does not exist",
							attnums[c], RelationGetRelationName(rel))));
		att = TupleDescAttr(tupdesc, attnums[c] - 1);
		if (att->attisdropped)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("registered column %d of relation \"%s\" has been dropped",
							attnums[c], RelationGetRelationName(rel))));
		if (att->attlen == -2)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("column \"%s\" has an unsupported cstring-like type",
							NameStr(att->attname))));

		/*
		 * The stride=attlen packed format is only naturally aligned for
		 * widths 1/2/4/8/16; odd fixed widths (macaddr=6, timetz=12) would
		 * misalign every element after the first, blowing up on
		 * alignment-strict platforms when the serve path fetches them.
		 */
		if (att->attlen > 0 &&
			att->attlen != 1 && att->attlen != 2 && att->attlen != 4 &&
			att->attlen != 8 && att->attlen != 16)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("column \"%s\" has an unsupported fixed width (%d)",
							NameStr(att->attname), att->attlen)));

		colmeta[c].attnum = attnums[c];
		colmeta[c].atttypid = att->atttypid;
		colmeta[c].attlen = att->attlen;
		colmeta[c].attbyval = att->attbyval;
		colmeta[c].attcollation = att->attcollation;

		typentry = lookup_type_cache(att->atttypid, TYPECACHE_CMP_PROC_FINFO);
		colmeta[c].has_cmp = OidIsValid(typentry->cmp_proc_finfo.fn_oid);
		if (colmeta[c].has_cmp)
			fmgr_info_copy(&colmeta[c].cmpfn, &typentry->cmp_proc_finfo,
						   CurrentMemoryContext);
	}

	nheappages = RelationGetNumberOfBlocks(rel);
	ndirslots = (nheappages + DBBC_PAGES_PER_BLOCK - 1) / DBBC_PAGES_PER_BLOCK;

	/* local (backend-memory) directory; published to DSA at the end */
	newdir = (dsa_pointer *) palloc0(Max(ndirslots, 1) * sizeof(dsa_pointer));
	colbuild = (DbbcColBuild *) palloc0(ncols * sizeof(DbbcColBuild));

	blkcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "dbblue_columnar block build",
								   ALLOCSET_DEFAULT_SIZES);

	/*
	 * Pin the prior version (if any) so its blocks stay alive while we decide,
	 * range by range, which are still valid to reuse. Tracked: an abort during
	 * the build unpins it. Pinned before PG_TRY so its value is stable across
	 * the setjmp; the fallible reads of its directory/attnums happen inside.
	 * ShareUpdateExclusiveLock already bars a concurrent populate, so the entry
	 * cannot swap under us; the pin additionally guards against a concurrent
	 * drop.
	 */
	reuse_src = dbbc_version_pin_tracked(relid);

	/*
	 * From here to the end of the publish, an ERROR (cancel, timeout, OOM,
	 * detoast failure) must not strand anything: blocks are reachable only
	 * through the backend-local newdir until the entry is published, and the
	 * DSA is pinned for the postmaster's lifetime, so an unhandled longjmp
	 * would leak them and their budget reservation permanently. The catch
	 * block frees every unpublished allocation, releases every unreconciled
	 * reservation, and rethrows.
	 */
	PG_TRY();
	{
		/*
		 * Reuse is possible only when the prior version covers the SAME column
		 * set in the same order (else its blocks' chunks do not match what this
		 * build must produce). Resolve its directory and compare attnums once.
		 */
		if (reuse_src != NULL && DsaPointerIsValid(reuse_src->blockdir) &&
			reuse_src->ncols == ncols)
		{
			int16	   *oldatt = (int16 *) dsa_get_address(dbbc_dsa,
														   reuse_src->attnums);

			if (memcmp(oldatt, attnums, ncols * sizeof(int16)) == 0)
			{
				olddir = (dsa_pointer *) dsa_get_address(dbbc_dsa,
														 reuse_src->blockdir);
				reuse_cols_ok = true;
			}
		}

		for (slot = 0; slot < ndirslots && !budget_hit; slot++)
		{
			BlockNumber first_page = slot * DBBC_PAGES_PER_BLOCK;
			uint16		npages = (uint16) Min((BlockNumber) DBBC_PAGES_PER_BLOCK,
											  nheappages - first_page);
			DbbcPageStamp stamps[DBBC_PAGES_PER_BLOCK];
			HeapTuple  *tuples;
			int			ntuples = 0;
			int			tupcap;
			bool		usable = true;
			bool		oversize = false;
			uint16		p;
			int			i;
			Size		block_total = 0;
			dsa_pointer blockptr;
			DbbcBlock  *block;
			DbbcColumnChunk *chunks;

			CHECK_FOR_INTERRUPTS();

			/* cheap pre-check: every page all-visible in the VM? */
			for (p = 0; p < npages; p++)
			{
				if (!(visibilitymap_get_status(rel, first_page + p, &vmbuf) &
					  VISIBILITYMAP_ALL_VISIBLE))
				{
					usable = false;
					break;
				}
			}
			if (!usable)
				continue;

			/*
			 * Incremental refresh: if the prior version has a block for this
			 * range that is still byte-for-byte valid (same range, every page
			 * all-visible with an unchanged LSN), reuse it - take a reference
			 * and skip the rebuild entirely. This is the whole point of the
			 * refresh being incremental: an append-mostly table re-scans only
			 * its changed tail, not every cold block. The reused block's bytes
			 * are already reserved, so no budget accounting happens here.
			 */
			if (reuse_cols_ok && slot < reuse_src->ndirslots &&
				DsaPointerIsValid(olddir[slot]))
			{
				DbbcBlock  *oldblk = (DbbcBlock *)
					dsa_get_address(dbbc_dsa, olddir[slot]);

				if (dbbc_block_still_valid(oldblk, rel, first_page, npages))
				{
					dbbc_block_ref(olddir[slot]);
					newdir[slot] = olddir[slot];
					blocks_built++;
					blocks_reused++;
					continue;
				}
			}

			MemoryContextReset(blkcxt);
			oldcxt = MemoryContextSwitchTo(blkcxt);

			tupcap = npages * (int) MaxHeapTuplesPerPage;
			tuples = (HeapTuple *) palloc(tupcap * sizeof(HeapTuple));

			/*
			 * Copy every live tuple of every page in the range, recording
			 * each page's LSN under the same share lock that certifies its
			 * PD_ALL_VISIBLE flag. If any page lost all-visibility between
			 * the VM pre-check and here, give up on this block (its range
			 * stays heap-only until a later populate/refresh).
			 */
			for (p = 0; p < npages && usable; p++)
			{
				Buffer		buf;
				Page		page;
				OffsetNumber maxoff;
				OffsetNumber off;
				uint16		page_rows = 0;

				buf = ReadBuffer(rel, first_page + p);
				LockBuffer(buf, BUFFER_LOCK_SHARE);
				page = BufferGetPage(buf);

				if (!PageIsAllVisible(page))
				{
					UnlockReleaseBuffer(buf);
					usable = false;
					break;
				}

				stamps[p].lsn = BufferGetLSNAtomic(buf);

				/*
				 * A page with no valid LSN (possible for all-visible pages
				 * written by WAL-skipping paths, e.g. COPY ... FREEZE under
				 * wal_level=minimal) can never participate in an
				 * LSN-equality proof: a later rewrite by the same path would
				 * also leave LSN 0 and the serve-time check would certify
				 * nothing. Treat it like a not-all-visible page.
				 */
				if (XLogRecPtrIsInvalid(stamps[p].lsn))
				{
					UnlockReleaseBuffer(buf);
					usable = false;
					break;
				}

				maxoff = PageGetMaxOffsetNumber(page);
				for (off = FirstOffsetNumber; off <= maxoff; off++)
				{
					ItemId		iid = PageGetItemId(page, off);
					HeapTupleData tup;

					if (!ItemIdIsNormal(iid))
						continue;

					tup.t_data = (HeapTupleHeader) PageGetItem(page, iid);
					tup.t_len = ItemIdGetLength(iid);
					ItemPointerSet(&tup.t_self, first_page + p, off);
					tup.t_tableOid = RelationGetRelid(rel);

					tuples[ntuples++] = heap_copytuple(&tup);
					page_rows++;
				}
				stamps[p].nrows = page_rows;

				UnlockReleaseBuffer(buf);
			}

			if (!usable || ntuples == 0)
			{
				MemoryContextSwitchTo(oldcxt);
				continue;
			}

			/* accumulate per-column arrays */
			for (c = 0; c < ncols; c++)
			{
				DbbcColBuild *cb = &colbuild[c];

				memset(cb, 0, sizeof(DbbcColBuild));
				if (colmeta[c].attlen > 0)
					cb->fixed = (uint8 *) palloc0((Size) ntuples * colmeta[c].attlen);
				else
					cb->offsets = (uint32 *) palloc((Size) ntuples * sizeof(uint32));
			}

			for (i = 0; i < ntuples && !oversize; i++)
			{
				Datum	   *values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
				bool	   *isnull = (bool *) palloc(tupdesc->natts * sizeof(bool));

				heap_deform_tuple(tuples[i], tupdesc, values, isnull);

				for (c = 0; c < ncols; c++)
				{
					DbbcColMeta *meta = &colmeta[c];
					DbbcColBuild *cb = &colbuild[c];
					int			attidx = meta->attnum - 1;

					if (isnull[attidx])
					{
						cb->null_count++;
						if (cb->nullbits == NULL)
							cb->nullbits = (uint8 *)
								palloc0((ntuples + 7) / 8);
						cb->nullbits[i / 8] |= (uint8) (1 << (i % 8));
						if (meta->attlen < 0)
							cb->offsets[i] = DBBC_VAR_NULL_OFFSET;
						/* fixed slot stays zeroed */
						continue;
					}

					if (meta->attlen > 0)
					{
						uint8	   *dst = cb->fixed + (Size) i * meta->attlen;

						if (meta->attbyval)
							store_att_byval(dst, values[attidx], meta->attlen);
						else
							memcpy(dst, DatumGetPointer(values[attidx]),
								   meta->attlen);
						dbbc_track_minmax(meta, cb, values[attidx]);
					}
					else
					{
						struct varlena *v;

						v = pg_detoast_datum((struct varlena *)
											 DatumGetPointer(values[attidx]));

						/*
						 * Degrade, never error: a block whose detoasted data
						 * won't fit the per-block ceiling stays heap-only.
						 */
						if (MAXALIGN(cb->blob_len) + VARSIZE(v) > DBBC_MAX_BLOCK_BLOB)
						{
							oversize = true;
							break;
						}
						cb->offsets[i] = dbbc_blob_append(cb, v, VARSIZE(v));
						dbbc_track_minmax(meta, cb, PointerGetDatum(v));
					}
				}
			}

			if (oversize)
			{
				MemoryContextSwitchTo(oldcxt);
				ereport(DEBUG1,
						(errmsg("dbblue_columnar: skipping block at heap pages %u..%u of \"%s\": detoasted data exceeds %d MB",
								first_page, first_page + npages - 1,
								RelationGetRelationName(rel),
								DBBC_MAX_BLOCK_BLOB / (1024 * 1024))));
				continue;
			}

			/* size the block; reserve against the budget before publishing */
			block_total = sizeof(DbbcBlock) + ncols * sizeof(DbbcColumnChunk);
			for (c = 0; c < ncols; c++)
			{
				DbbcColMeta *meta = &colmeta[c];
				DbbcColBuild *cb = &colbuild[c];

				if (meta->attlen > 0)
					block_total += (Size) ntuples * meta->attlen;
				else
					block_total += (Size) ntuples * sizeof(uint32) + cb->blob_len;
				if (cb->nullbits)
					block_total += (ntuples + 7) / 8;
				if (cb->has_minmax)
					block_total += 2 * (meta->attlen > 0 ? meta->attlen : 64);
			}

			if (!dbbc_try_reserve(block_total))
			{
				budget_hit = true;
				MemoryContextSwitchTo(oldcxt);
				ereport(NOTICE,
						(errmsg("dbblue_columnar: memory budget (%d MB) reached after %d blocks",
								dbblue_columnar_memory_mb, blocks_built)));
				break;
			}
			inflight_bytes = block_total;

			/*
			 * Copy into the DSA. The block becomes reachable through
			 * newdir[slot] as soon as its header is consistent, so the catch
			 * path can free a partially-filled block (its chunk array is
			 * zero-initialized: unfilled chunk pointers are Invalid).
			 * Sizing note: the reservation slightly over-estimates varlena
			 * min/max copies at a flat 64 bytes; the exact per-chunk
			 * accounting below is reconciled against it.
			 */
			{
				Size		acct = 0;

				blockptr = dsa_allocate(dbbc_dsa, sizeof(DbbcBlock));
				block = (DbbcBlock *) dsa_get_address(dbbc_dsa, blockptr);
				block->first_page = first_page;
				block->npages = npages;
				block->nrows = (uint32) ntuples;
				pg_atomic_init_u32(&block->refs, 1);	/* this version's ref */
				memset(block->stamps, 0, sizeof(block->stamps));
				memcpy(block->stamps, stamps, npages * sizeof(DbbcPageStamp));
				block->chunks = InvalidDsaPointer;
				/*
				 * The block now owns the coarse reservation and is reachable
				 * through newdir[slot], so it self-accounts: hand block_total
				 * to block_bytes and clear inflight_bytes. The CATCH path frees
				 * reachable blocks via dbbc_block_unref (which releases
				 * block_bytes), so counting it in inflight too would
				 * double-release. block_bytes is refined to the exact acct once
				 * the block is fully built (below).
				 */
				block->block_bytes = block_total;
				newdir[slot] = blockptr;
				inflight_bytes = 0;

				block->chunks = dsa_allocate0(dbbc_dsa,
											  ncols * sizeof(DbbcColumnChunk));
				chunks = (DbbcColumnChunk *) dsa_get_address(dbbc_dsa,
															 block->chunks);
				acct += sizeof(DbbcBlock) + ncols * sizeof(DbbcColumnChunk);

				for (c = 0; c < ncols; c++)
				{
					DbbcColMeta *meta = &colmeta[c];
					DbbcColBuild *cb = &colbuild[c];
					DbbcColumnChunk *chunk = &chunks[c];
					Size		chunk_acct = 0;

					chunk->attnum = meta->attnum;
					chunk->atttypid = meta->atttypid;
					chunk->attcollation = meta->attcollation;
					chunk->attlen = meta->attlen;
					chunk->attbyval = meta->attbyval;
					chunk->encoding = DBBC_ENCODING_PLAIN;
					chunk->null_count = cb->null_count;
					chunk->has_minmax = cb->has_minmax;
					chunk->min_value = InvalidDsaPointer;
					chunk->max_value = InvalidDsaPointer;
					chunk->nulls = InvalidDsaPointer;
					chunk->varblob = InvalidDsaPointer;
					chunk->varblob_len = 0;

					if (meta->attlen > 0)
						chunk->values = dbbc_copy_to_dsa(cb->fixed,
														 (Size) ntuples * meta->attlen,
														 &chunk_acct);
					else
					{
						chunk->values = dbbc_copy_to_dsa(cb->offsets,
														 (Size) ntuples * sizeof(uint32),
														 &chunk_acct);
						if (cb->blob_len > 0)
							chunk->varblob = dbbc_copy_to_dsa(cb->blob,
															  cb->blob_len,
															  &chunk_acct);
						chunk->varblob_len = cb->blob_len;
					}

					if (cb->nullbits)
						chunk->nulls = dbbc_copy_to_dsa(cb->nullbits,
														(ntuples + 7) / 8,
														&chunk_acct);

					if (cb->has_minmax)
					{
						chunk->min_value = dbbc_store_minmax(meta, cb->min_val,
															 &chunk_acct);
						chunk->max_value = dbbc_store_minmax(meta, cb->max_val,
															 &chunk_acct);
					}

					chunk->total_bytes = chunk_acct;
					acct += chunk_acct;
				}

				/*
				 * Reconcile the coarse reservation with the exact allocation
				 * so the shared counter tracks reality. The block already holds
				 * block_total via block_bytes (and is out of inflight); adjust
				 * the reservation and block_bytes to the exact acct.
				 */
				if (acct > block_total)
				{
					if (!dbbc_try_reserve(acct - block_total))
					{
						/*
						 * Over the line after exact sizing: undo the block. It
						 * still carries block_bytes == block_total, so unref
						 * releases exactly the reservation we hold.
						 */
						dbbc_block_unref(blockptr, ncols);
						newdir[slot] = InvalidDsaPointer;
						budget_hit = true;
						MemoryContextSwitchTo(oldcxt);
						ereport(NOTICE,
								(errmsg("dbblue_columnar: memory budget (%d MB) reached after %d blocks",
										dbblue_columnar_memory_mb, blocks_built)));
						break;
					}
				}
				else if (acct < block_total)
					dbbc_release_bytes((int64) (block_total - acct));

				block->block_bytes = acct;	/* exact; released once at unref */
				blocks_built++;
			}

			MemoryContextSwitchTo(oldcxt);
		}

		if (BufferIsValid(vmbuf))
			ReleaseBuffer(vmbuf);
		vmbuf = InvalidBuffer;
		MemoryContextDelete(blkcxt);

		/*
		 * Publish, replacing any previous version. All fallible work (the
		 * DSA copies, the version allocation, the hash insert) happens
		 * before any entry field is assigned, so a half-initialized entry
		 * can never be observed: once assignment starts, every remaining
		 * step is a plain store. The old version is unpinned, not freed:
		 * concurrent readers that pinned it keep it alive until their last
		 * unpin.
		 */
		{
			Size		meta_bytes = 0;
			Size		dirsize = Max(ndirslots, 1) * sizeof(dsa_pointer);
			Size		attsize = ncols * sizeof(int16);
			DbbcRelVersion *version;
			dsa_pointer oldv = InvalidDsaPointer;
			DbbcRelVersion *oldver = NULL;

			/*
			 * Version + directory + attnum metadata is tiny; reserve it
			 * unconditionally (going a whisker over budget beats publishing
			 * a broken entry).
			 */
			LWLockAcquire(&dbbc_control->lck, LW_EXCLUSIVE);
			dbbc_control->bytes_used +=
				(int64) (dirsize + attsize + sizeof(DbbcRelVersion));
			LWLockRelease(&dbbc_control->lck);
			meta_resv = dirsize + attsize + sizeof(DbbcRelVersion);

			attnums_dsa = dbbc_copy_to_dsa(attnums, attsize, &meta_bytes);
			blockdir_dsa = dbbc_copy_to_dsa(newdir, dirsize, &meta_bytes);
			version_dsa = dsa_allocate(dbbc_dsa, sizeof(DbbcRelVersion));
			meta_bytes += sizeof(DbbcRelVersion);

			version = (DbbcRelVersion *) dsa_get_address(dbbc_dsa,
														 version_dsa);
			version->magic = DBBC_VERSION_MAGIC;
			pg_atomic_init_u32(&version->pins, 1);	/* the entry's own pin */
			version->self = version_dsa;
			version->ncols = ncols;
			version->attnums = attnums_dsa;
			version->ndirslots = ndirslots;
			version->nblocks = (uint32) blocks_built;
			{
				BlockNumber av = 0;
				BlockNumber af = 0;

				/* all-visible page count now = the staleness baseline */
				visibilitymap_count(rel, &av, &af);
				version->av_at_build = av;
			}
			version->blockdir = blockdir_dsa;
			/*
			 * Only the version's own metadata bytes; each block's bytes are
			 * owned by the block and released in dbbc_block_unref, so a block
			 * shared with another version (incremental reuse) is never
			 * double-counted or freed early.
			 */
			version->total_bytes = meta_bytes;

			key.dboid = MyDatabaseId;
			key.reloid = relid;
			entry = (DbbcRelEntry *) dshash_find_or_insert(dbbc_hash, &key,
														   &found);
			oldv = found ? entry->version : InvalidDsaPointer;
			/*
			 * Resolve the old version's address BEFORE the swap:
			 * dsa_get_address can attach a segment and error, and after we
			 * flip published=true the PG_CATCH must do nothing, so no
			 * fallible call may run past this point. On error here we are
			 * still !published (entry untouched, still points to the old
			 * version) and the CATCH frees the new version.
			 */
			oldver = DsaPointerIsValid(oldv)
				? (DbbcRelVersion *) dsa_get_address(dbbc_dsa, oldv) : NULL;

			/* no fallible calls below this point */
			entry->version = version_dsa;
			published = true;
			dshash_release_lock(dbbc_hash, entry);

			if (oldver != NULL)
				dbbc_version_unpin(oldver);
		}
	}
	PG_CATCH();
	{
		if (!published)
		{
			uint32		ci;

			/*
			 * Unref every block reachable through newdir. A freshly built
			 * block (refs==1) is freed and its block_bytes released; a block
			 * reused from the prior version (incremental refresh, refs>1) is
			 * merely un-referenced, undoing the ref we took - never freed and
			 * its bytes never released (they belong to the surviving version).
			 * So only the not-yet-tracked inflight reservation (a block
			 * reserved but not yet reachable through newdir) and the version
			 * metadata reservation remain to release here.
			 */
			for (ci = 0; ci < ndirslots; ci++)
			{
				if (DsaPointerIsValid(newdir[ci]))
					dbbc_block_unref(newdir[ci], ncols);
			}
			if (DsaPointerIsValid(attnums_dsa))
				dsa_free(dbbc_dsa, attnums_dsa);
			if (DsaPointerIsValid(blockdir_dsa))
				dsa_free(dbbc_dsa, blockdir_dsa);
			if (DsaPointerIsValid(version_dsa))
				dsa_free(dbbc_dsa, version_dsa);
			dbbc_release_bytes((int64) (inflight_bytes + meta_resv));
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Drop our build-time pin on the prior version. Its own blocks that we did
	 * NOT reuse are freed when its last reference goes (here, or a lingering
	 * reader's unpin); blocks we reused survive because the just-published
	 * version now holds a reference to each. (Only on the success path - an
	 * abort released this tracked pin already.)
	 */
	if (reuse_src != NULL)
		dbbc_version_unpin_tracked(reuse_src);

	elog(DEBUG1, "dbblue_columnar: populated \"%s\": %d blocks (%d reused, %d rebuilt)",
		 RelationGetRelationName(rel), blocks_built, blocks_reused,
		 blocks_built - blocks_reused);

	table_close(rel, ShareUpdateExclusiveLock);

	return (int) blocks_built;
}

/*
 * Distinct relation OIDs currently registered for columnarization in this
 * database. Returns NIL (not an error) if the extension isn't installed here.
 * Used by the background auto-refresh worker.
 */
List *
dbbc_registered_relids(void)
{
	List	   *result = NIL;
	MemoryContext caller = CurrentMemoryContext;
	char	   *sql;
	int			ret;
	uint64		i;

	if (!OidIsValid(get_extension_oid("dbblue_columnar", true)))
		return NIL;

	sql = psprintf("SELECT DISTINCT relid FROM %s", dbbc_registry_table_name());

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "dbblue_columnar: SPI_connect failed");
	ret = SPI_execute(sql, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "dbblue_columnar: registered-relid query failed (%d)", ret);

	for (i = 0; i < SPI_processed; i++)
	{
		bool		isnull;
		Datum		d = SPI_getbinval(SPI_tuptable->vals[i],
									  SPI_tuptable->tupdesc, 1, &isnull);

		if (!isnull)
		{
			MemoryContext old = MemoryContextSwitchTo(caller);

			result = lappend_oid(result, DatumGetObjectId(d));
			MemoryContextSwitchTo(old);
		}
	}

	SPI_finish();
	return result;
}

/*
 * Cheap staleness test for the auto-refresh worker: should this relation's
 * column store be (re)built?
 *
 * The signal is the gap between the pages that are all-visible NOW
 * (visibilitymap_count - VM fork reads only, no heap page reads) and the
 * pages the current version actually covers (built_pages). A large gap means
 * either un-columnarized all-visible data has appeared (growth, or pages that
 * were not yet all-visible when the version was built - including a version
 * built when NOTHING was all-visible, nblocks == 0) or columnarized pages
 * have been modified (their VM bit cleared). Either way a rebuild helps.
 *
 * Refresh when the gap reaches max(one block, threshold_pct of coverage): the
 * one-block floor bootstraps a zero-coverage version and absorbs a single
 * partial trailing range without thrashing. Approximation is fine because
 * staleness only affects performance - the serve path always falls back to
 * the heap for any block failing the exact VM+LSN check at query time.
 * Non-heap / non-permanent / wrong-relkind relations never need refresh
 * (populate would reject them).
 */
bool
dbbc_relation_needs_refresh(Oid relid, int threshold_pct)
{
	DbbcRelVersion *version;
	Relation	rel;
	BlockNumber all_visible = 0;
	BlockNumber all_frozen = 0;
	int64		baseline;
	int64		gap;
	int64		floor_pages;
	bool		needs;

	/*
	 * Guard FIRST, before touching the version: an unbuildable-but-registered
	 * relation (unlogged/temp, non-heap AM, wrong relkind) never has and never
	 * will have a version, so it must return false here - otherwise the
	 * version==NULL path below would say "needs build" every cycle and the
	 * worker would call populate (which rejects it) forever.
	 */
	rel = table_open(relid, AccessShareLock);
	if (rel->rd_rel->relam != HEAP_TABLE_AM_OID ||
		rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
		(rel->rd_rel->relkind != RELKIND_RELATION &&
		 rel->rd_rel->relkind != RELKIND_MATVIEW))
	{
		table_close(rel, AccessShareLock);
		return false;
	}

	version = dbbc_version_pin_tracked(relid);
	if (version == NULL)
	{
		table_close(rel, AccessShareLock);
		return true;			/* buildable but not built yet */
	}

	/*
	 * Compare current all-visible page count to the count when the version
	 * was built (same page granularity - NOT built_pages, which is
	 * block-granular and would make a relation with a hot page in every
	 * 32-page range refresh forever). After a (re)populate the baseline is
	 * reset to the then-current count, so any relation converges to a stable
	 * no-refresh state until its all-visibility actually moves.
	 */
	visibilitymap_count(rel, &all_visible, &all_frozen);
	baseline = (int64) version->av_at_build;
	gap = (int64) all_visible - baseline;
	if (gap < 0)
		gap = -gap;

	floor_pages = baseline * threshold_pct / 100;
	if (floor_pages < DBBC_PAGES_PER_BLOCK)
		floor_pages = DBBC_PAGES_PER_BLOCK;

	needs = (gap >= floor_pages);

	table_close(rel, AccessShareLock);
	dbbc_version_unpin_tracked(version);
	return needs;
}

/*
 * dbblue_columnar_blocks(regclass) -> setof record
 *
 * Introspection: one row per (block, column chunk) with zone-map contents.
 */
PG_FUNCTION_INFO_V1(dbblue_columnar_blocks);

Datum
dbblue_columnar_blocks(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	DbbcRelVersion *version;
	dsa_pointer *dir;
	uint32		slot;

	InitMaterializedSRF(fcinfo, 0);

	dbbc_store_attach();

	/* pin (abort-safe): no dshash lock is held while we format output */
	version = dbbc_version_pin_tracked(relid);
	if (version == NULL)
		return (Datum) 0;
	if (!DsaPointerIsValid(version->blockdir))
	{
		dbbc_version_unpin_tracked(version);
		return (Datum) 0;
	}

	dir = (dsa_pointer *) dsa_get_address(dbbc_dsa, version->blockdir);

	for (slot = 0; slot < version->ndirslots; slot++)
	{
		DbbcBlock  *block;
		DbbcColumnChunk *chunks;
		int			c;

		if (!DsaPointerIsValid(dir[slot]))
			continue;

		block = (DbbcBlock *) dsa_get_address(dbbc_dsa, dir[slot]);
		chunks = (DbbcColumnChunk *) dsa_get_address(dbbc_dsa, block->chunks);

		for (c = 0; c < version->ncols; c++)
		{
			DbbcColumnChunk *chunk = &chunks[c];
			Datum		values[10];
			bool		nulls[10];
			Oid			outfunc;
			bool		varlenatype;

			memset(nulls, 0, sizeof(nulls));
			values[0] = Int32GetDatum((int32) slot);
			values[1] = Int64GetDatum((int64) block->first_page);
			values[2] = Int32GetDatum((int32) block->npages);
			values[3] = Int64GetDatum((int64) block->nrows);
			values[4] = Int16GetDatum(chunk->attnum);
			values[5] = CStringGetTextDatum("plain");
			values[6] = Int64GetDatum((int64) chunk->null_count);

			if (chunk->has_minmax)
			{
				getTypeOutputInfo(chunk->atttypid, &outfunc, &varlenatype);
				values[7] = CStringGetTextDatum(
												OidOutputFunctionCall(outfunc,
																	  dbbc_chunk_fetch_minmax(chunk, chunk->min_value)));
				values[8] = CStringGetTextDatum(
												OidOutputFunctionCall(outfunc,
																	  dbbc_chunk_fetch_minmax(chunk, chunk->max_value)));
			}
			else
			{
				nulls[7] = true;
				nulls[8] = true;
			}
			values[9] = Int64GetDatum((int64) chunk->total_bytes);

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								 values, nulls);
		}
	}

	dbbc_version_unpin_tracked(version);

	return (Datum) 0;
}

/*
 * dbblue_columnar_drop(regclass) -> boolean
 *
 * Free the in-memory column store for a relation (by OID, so it also works
 * after the relation itself was dropped). Returns whether an entry existed.
 * Registrations in the catalog table are not touched: this drops the cache,
 * not the configuration.
 */
PG_FUNCTION_INFO_V1(dbblue_columnar_drop);

Datum
dbblue_columnar_drop(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	DbbcRelKey	key;
	DbbcRelEntry *entry;
	dsa_pointer oldv;

	dbbc_store_attach();

	key.dboid = MyDatabaseId;
	key.reloid = relid;
	entry = (DbbcRelEntry *) dshash_find(dbbc_hash, &key, true);
	if (entry == NULL)
		PG_RETURN_BOOL(false);

	oldv = entry->version;
	dshash_delete_entry(dbbc_hash, entry);	/* releases the lock */

	/* concurrent readers that pinned the version keep it alive */
	if (DsaPointerIsValid(oldv))
		dbbc_version_unpin((DbbcRelVersion *)
						   dsa_get_address(dbbc_dsa, oldv));

	PG_RETURN_BOOL(true);
}

/*
 * dbblue_columnar_memory()
 *		-> (budget_mb integer, used_bytes bigint, dsa_total_bytes bigint)
 *
 * used_bytes is the engine's logical accounting; dsa_total_bytes is the real
 * size of the shared area (size-class and segment overhead included), so the
 * gap between the two is observable.
 */
PG_FUNCTION_INFO_V1(dbblue_columnar_memory);

Datum
dbblue_columnar_memory(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[3];
	bool		nulls[3] = {false, false, false};

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	dbbc_store_attach();

	values[0] = Int32GetDatum(dbblue_columnar_memory_mb);
	values[1] = Int64GetDatum(dbbc_store_bytes_used());
	values[2] = Int64GetDatum((int64) dsa_get_total_size(dbbc_dsa));

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
