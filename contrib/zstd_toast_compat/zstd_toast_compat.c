/*-------------------------------------------------------------------------
 *
 * zstd_toast_compat.c
 *	  Supply zstd TOAST compression and/or decompression on a server that
 *	  was not built --with-zstd, by registering zstd_compress_datum_hook
 *	  and zstd_decompress_datum_hook.
 *
 * Two independent use cases:
 *
 *  - Read compatibility: a server that never intends to write new
 *    zstd-compressed values, but needs to read some it encounters --
 *    e.g. a physical backup or a streaming replica of a --with-zstd
 *    server.  Only the decompress hook matters here; a read-only standby
 *    never originates new TOAST compression, so this alone is a complete
 *    fix for that case.
 *
 *  - Full read/write compatibility: a server that wants to actually start
 *    using COMPRESSION zstd / default_toast_compression = zstd itself,
 *    without a full rebuild --with-zstd.  Both hooks are needed for that.
 *
 * This module always registers both; there's no reason to only offer one
 * once libzstd is linked in here regardless.  A build that was already
 * compiled --with-zstd doesn't need this module at all (core's compiled-in
 * path is used and these hooks are simply never consulted).
 *
 * The hooks are per-process function pointers, not catalog state, so they
 * only take effect in processes that actually load this library.  Add it to
 * shared_preload_libraries: that is the only setting that reaches every
 * process, because each one is forked from the postmaster after the library
 * is loaded.  session_preload_libraries is not enough -- autovacuum workers,
 * logical replication workers and other background workers never process
 * it, so autoanalyze and subscriptions fail on zstd values.  CREATE
 * EXTENSION alone only loads the library into the session that runs the
 * install script.
 *
 * Loaded any way other than through shared_preload_libraries, the library
 * gives read-only support: _PG_init() sets zstd_hooks_cluster_wide only when
 * preloaded, and without it core refuses COMPRESSION zstd and
 * default_toast_compression = zstd, and writes pglz instead of zstd, so that
 * one session cannot create data the rest of the cluster cannot read.
 * _PG_init() warns in that case.
 *
 * IDENTIFICATION
 *	  contrib/zstd_toast_compat/zstd_toast_compat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <zstd.h>

#include "access/parallel.h"
#include "access/toast_compression.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "varatt.h"

PG_MODULE_MAGIC_EXT(
					.name = "zstd_toast_compat",
					.version = PG_VERSION
);

/*
 * Matches core's ZSTD_TOAST_COMPRESS_LEVEL (toast_compression.c).  Not a
 * correctness requirement -- zstd decompression is level-agnostic -- just
 * kept in step so compression behavior is consistent with a native
 * --with-zstd build.
 */
#define ZSTD_TOAST_COMPRESS_LEVEL	3

#ifndef USE_ZSTD
/* true when _PG_init ran while processing shared_preload_libraries */
static bool loaded_via_shared_preload = false;
#endif

static varlena *zstd_compat_compress_datum(const varlena *value);
static varlena *zstd_compat_decompress_datum(const varlena *value);

void		_PG_init(void);
void		_PG_fini(void);

void
_PG_init(void)
{
#ifdef USE_ZSTD
	/* core already provides real zstd support; nothing for us to add */
	return;
#else
	zstd_compress_datum_hook = zstd_compat_compress_datum;
	zstd_decompress_datum_hook = zstd_compat_decompress_datum;

	loaded_via_shared_preload = process_shared_preload_libraries_in_progress;

	/*
	 * Tell core whether every process has the hooks.  Only then does it
	 * allow zstd in DDL and write zstd values; see zstd_hooks_cluster_wide.
	 * Under EXEC_BACKEND each child reloads shared_preload_libraries, with
	 * process_shared_preload_libraries_in_progress set, so it is set there
	 * too.
	 */
	if (loaded_via_shared_preload)
		zstd_hooks_cluster_wide = true;

	/*
	 * Loaded by session_preload_libraries, LOAD, CREATE EXTENSION or a
	 * function call: only this process gets the hooks, so core will let it
	 * read zstd values but not create zstd columns or write zstd values.
	 * Say so.  Parallel workers reload whatever their leader had loaded, so
	 * the leader has already been warned.
	 */
	if (!loaded_via_shared_preload && !IsParallelWorker())
		ereport(WARNING,
				(errmsg("zstd_toast_compat is not loaded via \"shared_preload_libraries\""),
				 errdetail("This session can read zstd-compressed values, but cannot create zstd columns or write zstd values; values are written with pglz instead.  Other sessions, autovacuum workers and logical replication workers cannot read zstd-compressed values at all."),
				 errhint("Add zstd_toast_compat to \"shared_preload_libraries\" and restart the server.")));
#endif
}

void
_PG_fini(void)
{
	if (zstd_compress_datum_hook == zstd_compat_compress_datum)
		zstd_compress_datum_hook = NULL;
	if (zstd_decompress_datum_hook == zstd_compat_decompress_datum)
		zstd_decompress_datum_hook = NULL;
}

/*
 * Same logic as the compiled-in USE_ZSTD path of core's zstd_compress_datum
 * (src/backend/access/common/toast_compression.c), duplicated here because
 * that path is unreachable in a build without USE_ZSTD.
 */
static varlena *
zstd_compat_compress_datum(const varlena *value)
{
	int32		valsize;
	size_t		max_size;
	size_t		len;
	varlena    *tmp;

	valsize = VARSIZE_ANY_EXHDR(value);

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

	/* data is incompressible, so just free the memory and return NULL */
	if ((int32) len > valsize)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED);

	return tmp;
}

/*
 * Same logic as the compiled-in USE_ZSTD path of core's
 * zstd_decompress_datum.
 */
static varlena *
zstd_compat_decompress_datum(const varlena *value)
{
	int32		rawsize;
	size_t		decompressed;
	varlena    *result;

	rawsize = VARDATA_COMPRESSED_GET_EXTSIZE(value);
	result = (varlena *) palloc(rawsize + VARHDRSZ);

	decompressed = ZSTD_decompress(VARDATA(result), rawsize,
								   (const char *) value + VARHDRSZ_COMPRESSED,
								   VARSIZE(value) - VARHDRSZ_COMPRESSED);
	if (ZSTD_isError(decompressed))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed zstd data is corrupt: %s",
								 ZSTD_getErrorName(decompressed))));
	if (decompressed != (size_t) rawsize)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed zstd data is corrupt"),
				 errdetail_internal("Decompressed %zu bytes, expected %d.",
									decompressed, rawsize)));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

PG_FUNCTION_INFO_V1(zstd_toast_compat_enable);
Datum
zstd_toast_compat_enable(PG_FUNCTION_ARGS)
{
	/*
	 * The library is already loaded (and _PG_init() already ran) by the
	 * time this C function can be called at all.  This function exists
	 * only so the extension's install script can force that load to
	 * happen during CREATE EXTENSION, rather than lazily on first use.
	 */
	PG_RETURN_BOOL(zstd_compress_datum_hook == zstd_compat_compress_datum &&
				   zstd_decompress_datum_hook == zstd_compat_decompress_datum);
}

PG_FUNCTION_INFO_V1(zstd_toast_compat_status);
Datum
zstd_toast_compat_status(PG_FUNCTION_ARGS)
{
#ifdef USE_ZSTD
	PG_RETURN_TEXT_P(cstring_to_text("not needed: server built with --with-zstd"));
#else
	if (zstd_compress_datum_hook == zstd_compat_compress_datum &&
		zstd_decompress_datum_hook == zstd_compat_decompress_datum)
	{
		if (loaded_via_shared_preload)
			PG_RETURN_TEXT_P(cstring_to_text("active in all processes (read/write, loaded via shared_preload_libraries)"));
		else
			PG_RETURN_TEXT_P(cstring_to_text("read-only, in this session only -- add zstd_toast_compat to "
											 "shared_preload_libraries and restart; until then no zstd columns "
											 "or values can be created, and other sessions, autovacuum and "
											 "logical replication workers cannot read zstd values"));
	}
	else
		PG_RETURN_TEXT_P(cstring_to_text("NOT active in this session -- add zstd_toast_compat to "
										 "shared_preload_libraries and restart"));
#endif
}
