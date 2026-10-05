/*-------------------------------------------------------------------------
 *
 * toast_compression.c
 *	  Functions for toast compression.
 *
 * Copyright (c) 2021-2026, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/common/toast_compression.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif

#ifdef USE_ZSTD
#include <zstd.h>
#endif

#include "access/detoast.h"
#include "access/toast_compression.h"
#include "catalog/catalog.h"
#include "common/pg_lzcompress.h"
#include "miscadmin.h"
#include "utils/guc_hooks.h"
#include "utils/rel.h"
#include "varatt.h"

/*
 * ZSTD compression level for TOAST.  Level 3 is the libzstd default and the
 * sweet spot for Odoo's HTML/XML/JSON workloads: ~5x compression at >700 MB/s
 * compression speed and >1 GB/s decompress.  Higher levels add CPU without
 * proportional ratio gains; lower levels lose ratio without gaining speed.
 */
#define ZSTD_TOAST_COMPRESS_LEVEL	3

/* GUC */
int			default_toast_compression = DEFAULT_TOAST_COMPRESSION;
bool		dbblue_allow_zstd = true;

/* set by contrib/zstd_toast_compat on a build without USE_ZSTD */
zstd_compress_datum_hook_type zstd_compress_datum_hook = NULL;
zstd_decompress_datum_hook_type zstd_decompress_datum_hook = NULL;

#define NO_COMPRESSION_SUPPORT(method) \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("compression method %s not supported", method), \
			 errdetail("This functionality requires the server to be built with %s support.", method)))

#define NO_ZSTD_SUPPORT() \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("compression method zstd is not available"), \
			 errdetail("This server was not built with --with-zstd, and no module supplying zstd support is loaded."), \
			 errhint("Load the zstd_toast_compat module (for example via session_preload_libraries), or rebuild the server with --with-zstd.")))

#define ZSTD_DISABLED() \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("compression method zstd is disabled on this cluster"), \
			 errdetail("dbblue_allow_zstd is off, so no new data may be compressed with zstd."), \
			 errhint("Set dbblue_allow_zstd = on in postgresql.conf and restart to re-enable it.")))

/*
 * Can this process actually compress with zstd?  Either the server was built
 * --with-zstd, or a loadable module (contrib/zstd_toast_compat) has supplied
 * the compression hook.
 */
static bool
zstd_compression_available(void)
{
#ifdef USE_ZSTD
	return true;
#else
	return zstd_compress_datum_hook != NULL;
#endif
}

/*
 * Decide which compression method a column's values will actually use.
 *
 * Resolves the "use the default" case, then applies the one rule that isn't
 * the user's to choose: system catalogs are never compressed with zstd.
 * Catalog contents are written by initdb and by ordinary DDL, and a catalog
 * full of zstd is what makes a data directory unreadable by a server without
 * zstd -- the cluster won't even answer catalog queries.  Keeping catalogs on
 * pglz costs nothing (these values are small and rarely read hot) and keeps
 * the cluster openable by a plain build, so only user data is ever at stake.
 */
char
toast_resolve_compression(Relation rel, char cmethod)
{
	if (!CompressionMethodIsValid(cmethod))
		cmethod = default_toast_compression;

	if (cmethod == TOAST_ZSTD_COMPRESSION && IsCatalogRelation(rel))
		cmethod = TOAST_PGLZ_COMPRESSION;

	return cmethod;
}

/*
 * GUC check hook for default_toast_compression.
 *
 * Reject zstd up front when this process can't actually compress with it,
 * rather than letting the setting be accepted and then failing on the first
 * INSERT large enough to be toasted.
 *
 * Startup is exempt: libraries named in session_preload_libraries (and
 * friends) are loaded only once GUC processing is complete, so a value
 * arriving from postgresql.conf, a per-database/per-user setting, or the
 * connection request may well become valid moments later.  Rejecting it here
 * would break that ordering, so those sources are accepted and the check in
 * zstd_compress_datum() remains the backstop.  What we do catch is the
 * interactive cases -- SET, and ALTER SYSTEM SET run from a live backend --
 * where the module, if it were coming at all, would already be loaded.
 */
bool
check_default_toast_compression(int *newval, void **extra, GucSource source)
{
	if (*newval != TOAST_ZSTD_COMPRESSION)
		return true;

	/*
	 * A cluster-wide "off" is a deliberate stance, not an ordering artifact,
	 * so reject it from every source including startup -- unlike the
	 * availability check below.
	 */
	if (!dbblue_allow_zstd)
	{
		GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
		GUC_check_errmsg("compression method zstd is disabled on this cluster");
		GUC_check_errdetail("dbblue_allow_zstd is off, so no new data may be compressed with zstd.");
		GUC_check_errhint("Set dbblue_allow_zstd = on in postgresql.conf and restart to re-enable it.");
		return false;
	}

	if (zstd_compression_available())
		return true;

	if (source >= PGC_S_INTERACTIVE ||
		(source == PGC_S_FILE && IsUnderPostmaster && OidIsValid(MyDatabaseId)))
	{
		GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
		GUC_check_errmsg("compression method zstd is not available");
		GUC_check_errdetail("This server was not built with --with-zstd, and no module supplying zstd support is loaded.");
		GUC_check_errhint("Load the zstd_toast_compat module (for example via session_preload_libraries), or rebuild the server with --with-zstd.");
		return false;
	}

	return true;
}

/*
 * Compress a varlena using PGLZ.
 *
 * Returns the compressed varlena, or NULL if compression fails.
 */
varlena *
pglz_compress_datum(const varlena *value)
{
	int32		valsize,
				len;
	varlena    *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(value);

	/*
	 * No point in wasting a palloc cycle if value size is outside the allowed
	 * range for compression.
	 */
	if (valsize < PGLZ_strategy_default->min_input_size ||
		valsize > PGLZ_strategy_default->max_input_size)
		return NULL;

	/*
	 * Figure out the maximum possible size of the pglz output, add the bytes
	 * that will be needed for varlena overhead, and allocate that amount.
	 */
	tmp = (varlena *) palloc(PGLZ_MAX_OUTPUT(valsize) +
							 VARHDRSZ_COMPRESSED);

	len = pglz_compress(VARDATA_ANY(value),
						valsize,
						(char *) tmp + VARHDRSZ_COMPRESSED,
						NULL);
	if (len < 0)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED);

	return tmp;
}

/*
 * Decompress a varlena that was compressed using PGLZ.
 */
varlena *
pglz_decompress_datum(const varlena *value)
{
	varlena    *result;
	int32		rawsize;

	/* allocate memory for the uncompressed data */
	result = (varlena *) palloc(VARDATA_COMPRESSED_GET_EXTSIZE(value) + VARHDRSZ);

	/* decompress the data */
	rawsize = pglz_decompress((const char *) value + VARHDRSZ_COMPRESSED,
							  VARSIZE(value) - VARHDRSZ_COMPRESSED,
							  VARDATA(result),
							  VARDATA_COMPRESSED_GET_EXTSIZE(value), true);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed pglz data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

/*
 * Decompress part of a varlena that was compressed using PGLZ.
 */
varlena *
pglz_decompress_datum_slice(const varlena *value,
							int32 slicelength)
{
	varlena    *result;
	int32		rawsize;

	/* allocate memory for the uncompressed data */
	result = (varlena *) palloc(slicelength + VARHDRSZ);

	/* decompress the data */
	rawsize = pglz_decompress((const char *) value + VARHDRSZ_COMPRESSED,
							  VARSIZE(value) - VARHDRSZ_COMPRESSED,
							  VARDATA(result),
							  slicelength, false);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed pglz data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

/*
 * Compress a varlena using LZ4.
 *
 * Returns the compressed varlena, or NULL if compression fails.
 */
varlena *
lz4_compress_datum(const varlena *value)
{
#ifndef USE_LZ4
	NO_COMPRESSION_SUPPORT("lz4");
	return NULL;				/* keep compiler quiet */
#else
	int32		valsize;
	int32		len;
	int32		max_size;
	varlena    *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(value);

	/*
	 * Figure out the maximum possible size of the LZ4 output, add the bytes
	 * that will be needed for varlena overhead, and allocate that amount.
	 */
	max_size = LZ4_compressBound(valsize);
	tmp = (varlena *) palloc(max_size + VARHDRSZ_COMPRESSED);

	len = LZ4_compress_default(VARDATA_ANY(value),
							   (char *) tmp + VARHDRSZ_COMPRESSED,
							   valsize, max_size);
	if (len <= 0)
		elog(ERROR, "lz4 compression failed");

	/* data is incompressible so just free the memory and return NULL */
	if (len > valsize)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED);

	return tmp;
#endif
}

/*
 * Decompress a varlena that was compressed using LZ4.
 */
varlena *
lz4_decompress_datum(const varlena *value)
{
#ifndef USE_LZ4
	NO_COMPRESSION_SUPPORT("lz4");
	return NULL;				/* keep compiler quiet */
#else
	int32		rawsize;
	varlena    *result;

	/* allocate memory for the uncompressed data */
	result = (varlena *) palloc(VARDATA_COMPRESSED_GET_EXTSIZE(value) + VARHDRSZ);

	/* decompress the data */
	rawsize = LZ4_decompress_safe((const char *) value + VARHDRSZ_COMPRESSED,
								  VARDATA(result),
								  VARSIZE(value) - VARHDRSZ_COMPRESSED,
								  VARDATA_COMPRESSED_GET_EXTSIZE(value));
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed lz4 data is corrupt")));


	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

/*
 * Decompress part of a varlena that was compressed using LZ4.
 */
varlena *
lz4_decompress_datum_slice(const varlena *value, int32 slicelength)
{
#ifndef USE_LZ4
	NO_COMPRESSION_SUPPORT("lz4");
	return NULL;				/* keep compiler quiet */
#else
	int32		rawsize;
	varlena    *result;

	/* slice decompression not supported prior to 1.8.3 */
	if (LZ4_versionNumber() < 10803)
		return lz4_decompress_datum(value);

	/* allocate memory for the uncompressed data */
	result = (varlena *) palloc(slicelength + VARHDRSZ);

	/* decompress the data */
	rawsize = LZ4_decompress_safe_partial((const char *) value + VARHDRSZ_COMPRESSED,
										  VARDATA(result),
										  VARSIZE(value) - VARHDRSZ_COMPRESSED,
										  slicelength,
										  slicelength);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed lz4 data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

/*
 * Compress a varlena using ZSTD.
 *
 * Returns the compressed varlena, or NULL if compression fails or does not
 * shrink the input.
 */
varlena *
zstd_compress_datum(const varlena *value)
{
#ifndef USE_ZSTD
	if (zstd_compress_datum_hook)
		return zstd_compress_datum_hook(value);
	NO_ZSTD_SUPPORT();
	return NULL;				/* keep compiler quiet */
#else
	int32		valsize;
	size_t		max_size;
	size_t		len;
	varlena    *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(value);

	/*
	 * Figure out the maximum possible size of the ZSTD output, add the bytes
	 * that will be needed for varlena overhead, and allocate that amount.
	 */
	max_size = ZSTD_compressBound(valsize);
	tmp = (varlena *) palloc(max_size + VARHDRSZ_COMPRESSED);

	len = ZSTD_compress((char *) tmp + VARHDRSZ_COMPRESSED, max_size,
						VARDATA_ANY(value), valsize,
						ZSTD_TOAST_COMPRESS_LEVEL);
	if (ZSTD_isError(len))
	{
		pfree(tmp);
		elog(ERROR, "zstd compression failed: %s", ZSTD_getErrorName(len));
	}

	/* data is incompressible so just free the memory and return NULL */
	if ((int32) len > valsize)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED);

	return tmp;
#endif
}

/*
 * Decompress a varlena that was compressed using ZSTD.
 */
varlena *
zstd_decompress_datum(const varlena *value)
{
#ifndef USE_ZSTD
	if (zstd_decompress_datum_hook)
		return zstd_decompress_datum_hook(value);
	NO_ZSTD_SUPPORT();
	return NULL;				/* keep compiler quiet */
#else
	int32		rawsize;
	size_t		decompressed;
	varlena    *result;

	/* allocate memory for the uncompressed data */
	rawsize = VARDATA_COMPRESSED_GET_EXTSIZE(value);
	result = (varlena *) palloc(rawsize + VARHDRSZ);

	/* decompress the data */
	decompressed = ZSTD_decompress(VARDATA(result), rawsize,
								   (const char *) value + VARHDRSZ_COMPRESSED,
								   VARSIZE(value) - VARHDRSZ_COMPRESSED);
	if (ZSTD_isError(decompressed))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed zstd data is corrupt: %s",
								 ZSTD_getErrorName(decompressed))));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

/*
 * Decompress part of a varlena that was compressed using ZSTD.
 *
 * ZSTD does not natively support partial decompression of arbitrary slices,
 * so we decompress the whole value and then copy out the requested prefix.
 * This matches the behaviour Postgres uses for lz4 versions that lack
 * partial-decode support.
 *
 * This delegates to zstd_decompress_datum() for the actual decompression,
 * so it works unmodified whether that came from a compiled-in USE_ZSTD or
 * from zstd_decompress_datum_hook.
 */
varlena *
zstd_decompress_datum_slice(const varlena *value, int32 slicelength)
{
	varlena    *full;
	varlena    *result;
	int32		rawsize;
	int32		copylen;

	full = zstd_decompress_datum(value);
	rawsize = VARSIZE(full) - VARHDRSZ;
	copylen = Min(rawsize, slicelength);

	result = (varlena *) palloc(copylen + VARHDRSZ);
	memcpy(VARDATA(result), VARDATA(full), copylen);
	SET_VARSIZE(result, copylen + VARHDRSZ);

	pfree(full);
	return result;
}

/*
 * Extract compression ID from a varlena.
 *
 * Returns TOAST_INVALID_COMPRESSION_ID if the varlena is not compressed.
 */
ToastCompressionId
toast_get_compression_id(varlena *attr)
{
	ToastCompressionId cmid = TOAST_INVALID_COMPRESSION_ID;

	/*
	 * If it is stored externally then fetch the compression method id from
	 * the external toast pointer.  If compressed inline, fetch it from the
	 * toast compression header.
	 */
	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		varatt_external toast_pointer;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

		if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
			cmid = VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer);
	}
	else if (VARATT_IS_COMPRESSED(attr))
		cmid = VARDATA_COMPRESSED_GET_COMPRESS_METHOD(attr);

	return cmid;
}

/*
 * CompressionNameToMethod - Get compression method from compression name
 *
 * Search in the available built-in methods.  If the compression not found
 * in the built-in methods then return InvalidCompressionMethod.
 */
char
CompressionNameToMethod(const char *compression)
{
	if (strcmp(compression, "pglz") == 0)
		return TOAST_PGLZ_COMPRESSION;
	else if (strcmp(compression, "lz4") == 0)
	{
#ifndef USE_LZ4
		NO_COMPRESSION_SUPPORT("lz4");
#endif
		return TOAST_LZ4_COMPRESSION;
	}
	else if (strcmp(compression, "zstd") == 0)
	{
		/*
		 * Unlike lz4 above, this isn't a purely compile-time decision: a
		 * loadable module may supply zstd support on a build without
		 * USE_ZSTD.  Reject only when neither is the case, so the failure
		 * lands here at DDL time rather than on a later INSERT.
		 */
		if (!dbblue_allow_zstd)
			ZSTD_DISABLED();
		if (!zstd_compression_available())
			NO_ZSTD_SUPPORT();
		return TOAST_ZSTD_COMPRESSION;
	}

	return InvalidCompressionMethod;
}

/*
 * GetCompressionMethodName - Get compression method name
 */
const char *
GetCompressionMethodName(char method)
{
	switch (method)
	{
		case TOAST_PGLZ_COMPRESSION:
			return "pglz";
		case TOAST_LZ4_COMPRESSION:
			return "lz4";
		case TOAST_ZSTD_COMPRESSION:
			return "zstd";
		default:
			elog(ERROR, "invalid compression method %c", method);
			return NULL;		/* keep compiler quiet */
	}
}
