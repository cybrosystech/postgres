/*-------------------------------------------------------------------------
 *
 * pg_tde_utils.c
 *		Utility functions for transparent data encryption
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_tde_utils/pg_tde_utils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/cipher.h"
#include "crypto/kmgr.h"
#include "fmgr.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_tde_is_enabled);
PG_FUNCTION_INFO_V1(pg_tde_cipher);
PG_FUNCTION_INFO_V1(pg_tde_check_kmgr_file);
PG_FUNCTION_INFO_V1(pg_tde_rotate_cluster_key);

/*
 * Report whether transparent data encryption is active in this cluster.
 */
Datum
pg_tde_is_enabled(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(DataEncryptionEnabled());
}

/*
 * Report the cipher used for data encryption.
 */
Datum
pg_tde_cipher(PG_FUNCTION_ARGS)
{
	const char *name;

	switch (GetDataEncryptionCipherInUse())
	{
		case PG_CIPHER_AES256_XTS:
			name = "aes-256-xts";
			break;
		default:
			name = "none";
			break;
	}

	PG_RETURN_TEXT_P(cstring_to_text(name));
}

/*
 * Validate the on-disk key manager file against the keys in shared memory.
 */
Datum
pg_tde_check_kmgr_file(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(KmgrCheckFile());
}

/*
 * Rotate the cluster key: re-run cluster_key_command and re-wrap the data
 * encryption keys with the key it returns.
 */
Datum
pg_tde_rotate_cluster_key(PG_FUNCTION_ARGS)
{
	KmgrRotateClusterKey();
	PG_RETURN_BOOL(true);
}
