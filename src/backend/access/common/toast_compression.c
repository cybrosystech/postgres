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
#include <zstd_errors.h>
#endif

#include "access/detoast.h"
#include "access/toast_compression.h"
#include "catalog/catalog.h"
#include "common/pg_lzcompress.h"
#include "miscadmin.h"
#include "utils/guc_hooks.h"
#include "utils/guc_tables.h"
#include "utils/memutils.h"
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
zstd_decompress_datum_slice_hook_type zstd_decompress_datum_slice_hook = NULL;
bool		zstd_hooks_cluster_wide = false;

#ifdef USE_ZSTD
/*
 * Compression and decompression contexts, created on first use and kept for
 * the life of the backend.  ZSTD_compress() and ZSTD_decompress() would
 * create and free one for every value; a level-3 compression context alone
 * is over a megabyte.  Every use starts a new frame, so one left mid-frame
 * by an error is harmless.
 */
static ZSTD_CCtx *zstd_cctx = NULL;
static ZSTD_DCtx *zstd_dctx = NULL;

static ZSTD_CCtx *
zstd_get_cctx(void)
{
	if (zstd_cctx == NULL)
	{
		zstd_cctx = ZSTD_createCCtx();
		if (zstd_cctx == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail("Failed to create a zstd compression context.")));
	}
	return zstd_cctx;
}

static ZSTD_DCtx *
zstd_get_dctx(void)
{
	if (zstd_dctx == NULL)
	{
		zstd_dctx = ZSTD_createDCtx();
		if (zstd_dctx == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail("Failed to create a zstd decompression context.")));
	}
	else
		ZSTD_DCtx_reset(zstd_dctx, ZSTD_reset_session_only);
	return zstd_dctx;
}
#endif

#define NO_COMPRESSION_SUPPORT(method) \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("compression method %s not supported", method), \
			 errdetail("This functionality requires the server to be built with %s support.", method)))

/*
 * Only shared_preload_libraries reaches every process.  Autovacuum workers,
 * logical replication workers and other background workers never process
 * session_preload_libraries, so a module loaded that way leaves them unable
 * to read or write zstd values.
 */
#define ZSTD_COMPAT_HINT \
	"Add zstd_toast_compat to shared_preload_libraries and restart the server, or rebuild the server with --with-zstd."

#define NO_ZSTD_SUPPORT() \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("compression method zstd is not available"), \
			 errdetail("This server was not built with --with-zstd, and no module supplying zstd support is loaded through shared_preload_libraries."), \
			 errhint(ZSTD_COMPAT_HINT)))

#define ZSTD_DISABLED() \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("compression method zstd is disabled on this cluster"), \
			 errdetail("dbblue_allow_zstd is off, so no new data may be compressed with zstd."), \
			 errhint("Set dbblue_allow_zstd = on in postgresql.conf and restart to re-enable it.")))

/*
 * Can the whole cluster compress with zstd?  Either the server was built
 * --with-zstd, or a loadable module (contrib/zstd_toast_compat) has supplied
 * the compression hook from shared_preload_libraries.
 *
 * It is not enough for this process to have the hook.  A module loaded into
 * one session (LOAD, CREATE EXTENSION, session_preload_libraries) leaves
 * every other session, autovacuum and logical replication workers without
 * it, yet a zstd column created or a zstd value written by that session is
 * visible to all of them -- and unreadable by them.  So DDL choosing zstd,
 * default_toast_compression = zstd and the write path all ask this, not
 * merely whether the hook is set here.
 */
bool
zstd_compression_available(void)
{
#ifdef USE_ZSTD
	return true;
#else
	return zstd_compress_datum_hook != NULL && zstd_hooks_cluster_wide;
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
 * Set by toast_compression_check_preload() once the configuration files and
 * shared_preload_libraries have been processed.  Child processes inherit it
 * through fork(); under EXEC_BACKEND they are covered by IsUnderPostmaster.
 */
static bool toast_config_loaded = false;

/*
 * GUC check hook for default_toast_compression.
 *
 * Reject zstd up front when this process can't actually compress with it,
 * rather than letting the setting be accepted and then failing on the first
 * INSERT large enough to be toasted.
 *
 * Initial startup is exempt: libraries named in shared_preload_libraries are
 * loaded only once GUC processing is complete, so a value arriving from
 * postgresql.conf may well become valid moments later.  Rejecting it here
 * would break that ordering, so it is accepted, and
 * toast_compression_check_preload() settles it once the preload libraries
 * have been loaded.  After that the answer is final: a reload of
 * postgresql.conf, SET, and ALTER SYSTEM SET are all refused.  The check in
 * zstd_compress_datum() remains the backstop.
 */
bool
check_default_toast_compression(int *newval, void **extra, GucSource source)
{
	if (*newval != TOAST_ZSTD_COMPRESSION)
		return true;

	/*
	 * Reject zstd while it is disabled cluster-wide -- but only once
	 * dbblue_allow_zstd has its final value.  While the configuration files
	 * are first being read, settings are applied in file order, so here we
	 * may still be looking at its boot value, or already at its new one,
	 * depending only on which line comes first.  Judging the combination
	 * then would make the server start or refuse to start on line order
	 * alone; toast_compression_check_preload() resolves it instead, once
	 * everything is loaded.  In child processes, and in the postmaster after
	 * startup, the value can no longer change (it is PGC_POSTMASTER).
	 */
	if (!dbblue_allow_zstd && (IsUnderPostmaster || toast_config_loaded))
	{
		/*
		 * The compiled-in default (zstd on a --with-zstd build) is not
		 * something anyone asked for; it comes back whenever a setting is
		 * removed from postgresql.conf and the configuration is reloaded.
		 * Refusing it would leave the previous value in place and log an
		 * error about a zstd nobody wrote, so resolve it to pglz instead,
		 * as toast_compression_check_preload() does at startup.
		 */
		if (source == PGC_S_DEFAULT)
		{
			*newval = TOAST_PGLZ_COMPRESSION;
			return true;
		}

		GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
		GUC_check_errmsg("compression method zstd is disabled on this cluster");
		GUC_check_errdetail("dbblue_allow_zstd is off, so no new data may be compressed with zstd.");
		GUC_check_errhint("Set dbblue_allow_zstd = on in postgresql.conf and restart to re-enable it.");
		return false;
	}

	if (zstd_compression_available())
		return true;

	/*
	 * Configuration file: refuse on reload, in the postmaster as well as in
	 * backends.  The postmaster is what matters most -- every process forked
	 * after the reload inherits its value, and would otherwise start with a
	 * default it cannot write.  Backends without a database (and per-database
	 * or per-role settings, which are applied before session_preload_libraries
	 * are loaded) are still let through.
	 */
	if (source >= PGC_S_INTERACTIVE ||
		(source == PGC_S_FILE &&
		 (IsUnderPostmaster ? OidIsValid(MyDatabaseId) : toast_config_loaded)))
	{
		GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
		GUC_check_errmsg("compression method zstd is not available");
		GUC_check_errdetail("This server was not built with --with-zstd, and no module supplying zstd support is loaded through shared_preload_libraries.");
		GUC_check_errhint(ZSTD_COMPAT_HINT);
		return false;
	}

	return true;
}

/*
 * Called by the postmaster (and by a single-user backend) right after
 * shared_preload_libraries has been processed, i.e. once the configuration
 * is complete.
 *
 * First it settles default_toast_compression = zstd combined with
 * dbblue_allow_zstd = off, which the check hook cannot judge reliably while
 * the configuration files are still being read.
 *
 * check_default_toast_compression() also has to accept zstd from postgresql.conf
 * at startup, because the module that supplies it is loaded afterwards.
 * Now that every preload library has had its chance to install the hooks,
 * if zstd is the default and still nothing can compress with it, warn and
 * fall back to pglz: every process forked from here inherits that state, so
 * otherwise every write of a large value to a column using the default would
 * fail.
 *
 * Catalog contents (columns explicitly marked COMPRESSION zstd) cannot be
 * checked here, since the postmaster has no database access; the module
 * itself warns when it is loaded outside shared_preload_libraries.
 */
void
toast_compression_check_preload(void)
{
	/*
	 * zstd as the default while zstd is disabled.  The check hook lets this
	 * through while the configuration is being read (see there), so settle
	 * it now that both values are final: fall back to pglz, as the write
	 * path would anyway, so that SHOW reports what is actually used and the
	 * server starts regardless of the order of the lines.
	 *
	 * Warn only when someone asked for zstd.  On a build whose compiled-in
	 * default is zstd, turning zstd off without touching
	 * default_toast_compression is the normal way to disable it.
	 */
	if (default_toast_compression == TOAST_ZSTD_COMPRESSION &&
		!dbblue_allow_zstd)
	{
		struct config_generic *rec = find_option("default_toast_compression",
												 false, false, ERROR);
		GucSource	src = rec->source;

		if (src != PGC_S_DEFAULT)
			ereport(WARNING,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("\"default_toast_compression\" is set to zstd, but \"dbblue_allow_zstd\" is off; using pglz"),
					 errdetail("The setting comes from %s.",
							   rec->sourcefile ?
							   psprintf("\"%s\" line %d", rec->sourcefile, rec->sourceline) :
							   GucSource_Names[src]),
					 errhint("Remove the zstd setting of \"default_toast_compression\", or set it to pglz or lz4.")));

		/*
		 * Same priority as the configuration file, so that per-database and
		 * per-role settings still override it.  A value the server merely
		 * defaulted to stays a default, so that a later reload adding a real
		 * setting still takes effect.
		 */
		SetConfigOption("default_toast_compression", "pglz", PGC_POSTMASTER,
						src == PGC_S_DEFAULT ? PGC_S_DYNAMIC_DEFAULT : src);
	}

	/*
	 * zstd as the default on a build without zstd, with no module supplying
	 * it.  Left alone, every write of a large value to a column using the
	 * default would fail.  Fall back to pglz the same way as above.
	 */
	if (default_toast_compression == TOAST_ZSTD_COMPRESSION &&
		!zstd_compression_available())
	{
		struct config_generic *rec = find_option("default_toast_compression",
												 false, false, ERROR);
		GucSource	src = rec->source;

		ereport(WARNING,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"default_toast_compression\" is set to zstd, but no module supplying zstd support is loaded through \"shared_preload_libraries\"; using pglz"),
				 rec->sourcefile ?
				 errdetail("This server was not built with --with-zstd.  The setting comes from \"%s\" line %d.",
						   rec->sourcefile, rec->sourceline) :
				 errdetail("This server was not built with --with-zstd."),
				 errhint(ZSTD_COMPAT_HINT)));

		SetConfigOption("default_toast_compression", "pglz", PGC_POSTMASTER,
						src == PGC_S_DEFAULT ? PGC_S_DYNAMIC_DEFAULT : src);
	}

	toast_config_loaded = true;
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
	 * Give zstd room for its worst case, ZSTD_compressBound(), which lets it
	 * skip bounds checks.  For values near the 1GB varlena limit that would
	 * exceed MaxAllocSize, though.  Only a result smaller than the input is
	 * of any use (see toast_compress_datum()), so then offer just the
	 * input's size, and treat running out of room as incompressible.
	 */
	max_size = ZSTD_compressBound(valsize);
	if (max_size > MaxAllocSize - VARHDRSZ_COMPRESSED)
		max_size = Min((size_t) valsize, MaxAllocSize - VARHDRSZ_COMPRESSED);
	tmp = (varlena *) palloc(max_size + VARHDRSZ_COMPRESSED);

	len = ZSTD_compressCCtx(zstd_get_cctx(),
							(char *) tmp + VARHDRSZ_COMPRESSED, max_size,
							VARDATA_ANY(value), valsize,
							ZSTD_TOAST_COMPRESS_LEVEL);
	if (ZSTD_isError(len))
	{
		pfree(tmp);
		if (ZSTD_getErrorCode(len) == ZSTD_error_dstSize_tooSmall)
			return NULL;		/* would not have been smaller */
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
	decompressed = ZSTD_decompressDCtx(zstd_get_dctx(),
									   VARDATA(result), rawsize,
									   (const char *) value + VARHDRSZ_COMPRESSED,
									   VARSIZE(value) - VARHDRSZ_COMPRESSED);
	if (ZSTD_isError(decompressed))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("----compressed zstd data is corrupt: %s",
								 ZSTD_getErrorName(decompressed))));
	if (decompressed != (size_t) rawsize)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed zstd data is corrupt"),
				 errdetail_internal("Decompressed %zu bytes, expected %d.",
									decompressed, rawsize)));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

/*
 * Decompress part of a varlena that was compressed using ZSTD.
 *
 * Only a prefix is ever requested.  With USE_ZSTD the frame is streamed and
 * decompression stops once slicelength bytes have been produced.  Otherwise
 * a module's zstd_decompress_datum_slice_hook is used if it offers one, and
 * failing that the whole value is decompressed and the prefix copied out.
 */
varlena *
zstd_decompress_datum_slice(const varlena *value, int32 slicelength)
{
#ifdef USE_ZSTD
	int32		rawsize;
	int32		want;
	varlena    *result;
	ZSTD_DCtx  *dctx;
	ZSTD_inBuffer in;
	ZSTD_outBuffer out;
	size_t		ret = 1;

	/*
	 * Stream the frame, stopping as soon as the requested prefix has been
	 * produced, rather than decompressing the whole value just to copy out
	 * its beginning.
	 */
	rawsize = VARDATA_COMPRESSED_GET_EXTSIZE(value);
	want = Min(slicelength, rawsize);
	result = (varlena *) palloc(want + VARHDRSZ);

	dctx = zstd_get_dctx();
	in.src = (const char *) value + VARHDRSZ_COMPRESSED;
	in.size = VARSIZE(value) - VARHDRSZ_COMPRESSED;
	in.pos = 0;
	out.dst = VARDATA(result);
	out.size = want;
	out.pos = 0;

	while (out.pos < out.size)
	{
		ret = ZSTD_decompressStream(dctx, &out, &in);
		if (ZSTD_isError(ret))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg_internal("compressed zstd data is corrupt: %s",
									 ZSTD_getErrorName(ret))));
		if (ret == 0)
			break;				/* frame complete */
		if (in.pos == in.size && out.pos < out.size)
			break;				/* input exhausted, frame unfinished */
	}

	if (out.pos != (size_t) want)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed zstd data is corrupt"),
				 errdetail_internal("Decompressed %zu bytes, expected %d.",
									out.pos, want)));

	SET_VARSIZE(result, want + VARHDRSZ);
	return result;
#else
	varlena    *full;
	varlena    *result;
	int32		rawsize;
	int32		copylen;

	/* a module may offer partial decompression; otherwise do it all */
	if (zstd_decompress_datum_slice_hook)
		return zstd_decompress_datum_slice_hook(value, slicelength);

	full = zstd_decompress_datum(value);
	rawsize = VARSIZE(full) - VARHDRSZ;
	copylen = Min(rawsize, slicelength);

	result = (varlena *) palloc(copylen + VARHDRSZ);
	memcpy(VARDATA(result), VARDATA(full), copylen);
	SET_VARSIZE(result, copylen + VARHDRSZ);

	pfree(full);
	return result;
#endif
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

/*
 * GetCopiedCompressionMethodName - name of the compression method a new
 * column copies from an existing one
 *
 * Partitions, INHERITS children and LIKE ... INCLUDING COMPRESSION copy a
 * column's compression by turning it back into a name, which is then
 * resolved through CompressionNameToMethod() exactly as if the user had
 * typed it.  For zstd that would refuse the copy whenever zstd is disabled
 * or unavailable -- which is precisely when tables marked zstd are waiting
 * to be normalized, and must still be able to get new partitions.  The user
 * did not ask for zstd here, so copy pglz instead; that is also what the
 * write path falls back to for such columns.  An explicit COMPRESSION zstd
 * still goes through CompressionNameToMethod() and is still rejected.
 */
const char *
GetCopiedCompressionMethodName(char method, const char *attname)
{
	if (method == TOAST_ZSTD_COMPRESSION &&
		(!dbblue_allow_zstd || !zstd_compression_available()))
	{
		ereport(NOTICE,
				(errmsg("column \"%s\" will use compression method pglz instead of zstd",
						attname),
				 errdetail(!dbblue_allow_zstd
						   ? "Compression method zstd is disabled on this cluster."
						   : "Compression method zstd is not available on this server.")));
		return "pglz";
	}

	return GetCompressionMethodName(method);
}
