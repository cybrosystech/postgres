/*-------------------------------------------------------------------------
 *
 * cipher.c
 *		Fallback block cipher implementation
 *
 * This file is compiled when the server is built without OpenSSL.  All
 * operations fail, since PostgreSQL has no built-in block cipher; data
 * encryption requires building --with-ssl=openssl.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/cipher.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/cipher.h"

PgCipherCtx *
pg_cipher_ctx_create(void)
{
	return NULL;
}

void
pg_cipher_ctx_free(PgCipherCtx *ctx)
{
}

bool
pg_cipher_xts_crypt(PgCipherCtx *ctx, bool encrypt,
					const unsigned char *key,
					const unsigned char *tweak,
					const unsigned char *in,
					unsigned char *out,
					int len)
{
	return false;
}

bool
pg_cipher_ctr_crypt(PgCipherCtx *ctx,
					const unsigned char *key,
					const unsigned char *iv,
					const unsigned char *in,
					unsigned char *out,
					int len)
{
	return false;
}

bool
pg_cipher_gcm_wrap(const unsigned char *key,
				   const unsigned char *iv,
				   const unsigned char *in, int inlen,
				   unsigned char *out,
				   unsigned char *tag)
{
	return false;
}

bool
pg_cipher_gcm_unwrap(const unsigned char *key,
					 const unsigned char *iv,
					 const unsigned char *in, int inlen,
					 unsigned char *out,
					 const unsigned char *tag)
{
	return false;
}
