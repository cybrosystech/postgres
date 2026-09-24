/*-------------------------------------------------------------------------
 *
 * zstd_toast_reader.c
 *	  Supply zstd TOAST decompression on a server that was not built
 *	  --with-zstd, by registering zstd_decompress_datum_hook.
 *
 * This lets a plain build read zstd-compressed TOAST values it encounters
 * in a physical backup, a streaming replica, or a raw file copy taken from
 * a server that was built --with-zstd, without recompiling the main
 * "postgres" binary.  It covers reads only: a server without USE_ZSTD still
 * cannot create new zstd-compressed values (CREATE TABLE ... COMPRESSION
 * zstd remains unavailable, as does default_toast_compression = zstd).
 *
 * The hook is a per-process function pointer, not catalog state, so it
 * only takes effect in backends that actually load this library.  Add it
 * to shared_preload_libraries or session_preload_libraries so that every
 * new connection picks it up automatically; CREATE EXTENSION alone only
 * loads it into the session that runs the install script.
 *
 * IDENTIFICATION
 *	  contrib/zstd_toast_reader/zstd_toast_reader.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <zstd.h>

#include "access/toast_compression.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "varatt.h"

PG_MODULE_MAGIC_EXT(
					.name = "zstd_toast_reader",
					.version = PG_VERSION
);

static varlena *zstd_reader_decompress_datum(const varlena *value);

void		_PG_init(void);
void		_PG_fini(void);

void
_PG_init(void)
{
#ifdef USE_ZSTD
	/* core already provides real zstd support; nothing for us to add */
	return;
#else
	zstd_decompress_datum_hook = zstd_reader_decompress_datum;
#endif
}

void
_PG_fini(void)
{
	if (zstd_decompress_datum_hook == zstd_reader_decompress_datum)
		zstd_decompress_datum_hook = NULL;
}

/*
 * Same logic as the compiled-in USE_ZSTD path of core's zstd_decompress_datum
 * (src/backend/access/common/toast_compression.c), duplicated here because
 * that path is unreachable in a build without USE_ZSTD.
 */
static varlena *
zstd_reader_decompress_datum(const varlena *value)
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

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

PG_FUNCTION_INFO_V1(zstd_toast_reader_enable);
Datum
zstd_toast_reader_enable(PG_FUNCTION_ARGS)
{
	/*
	 * The library is already loaded (and _PG_init() already ran) by the
	 * time this C function can be called at all.  This function exists
	 * only so the extension's install script can force that load to
	 * happen during CREATE EXTENSION, rather than lazily on first use.
	 */
	PG_RETURN_BOOL(zstd_decompress_datum_hook == zstd_reader_decompress_datum);
}

PG_FUNCTION_INFO_V1(zstd_toast_reader_status);
Datum
zstd_toast_reader_status(PG_FUNCTION_ARGS)
{
#ifdef USE_ZSTD
	PG_RETURN_TEXT_P(cstring_to_text("not needed: server built with --with-zstd"));
#else
	if (zstd_decompress_datum_hook == zstd_reader_decompress_datum)
		PG_RETURN_TEXT_P(cstring_to_text("active in this session"));
	else
		PG_RETURN_TEXT_P(cstring_to_text("NOT active in this session -- add zstd_toast_reader to "
										 "shared_preload_libraries or session_preload_libraries"));
#endif
}
