/*-------------------------------------------------------------------------
 *
 * cipher_openssl.c
 *		Block cipher implementation using OpenSSL
 *
 * This file is compiled when the server is built with OpenSSL.  See
 * src/include/common/cipher.h for an overview.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/cipher_openssl.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <openssl/evp.h>

#include "common/cipher.h"

struct PgCipherCtx
{
	EVP_CIPHER_CTX *evpctx;
};

PgCipherCtx *
pg_cipher_ctx_create(void)
{
	PgCipherCtx *ctx;

	ctx = (PgCipherCtx *) malloc(sizeof(PgCipherCtx));
	if (ctx == NULL)
		return NULL;

	ctx->evpctx = EVP_CIPHER_CTX_new();
	if (ctx->evpctx == NULL)
	{
		free(ctx);
		return NULL;
	}

	return ctx;
}

void
pg_cipher_ctx_free(PgCipherCtx *ctx)
{
	if (ctx == NULL)
		return;
	EVP_CIPHER_CTX_free(ctx->evpctx);
	free(ctx);
}

/*
 * Encrypt or decrypt 'len' bytes from 'in' into 'out' with AES-256-XTS.
 * 'key' is PG_AES256_XTS_KEY_LEN bytes, 'tweak' is PG_XTS_TWEAK_LEN bytes.
 * In-place operation (in == out) is allowed.  Returns false on failure.
 */
bool
pg_cipher_xts_crypt(PgCipherCtx *ctx, bool encrypt,
					const unsigned char *key,
					const unsigned char *tweak,
					const unsigned char *in,
					unsigned char *out,
					int len)
{
	int			outlen,
				finallen;

	if (ctx == NULL)
		return false;

	if (EVP_CipherInit_ex(ctx->evpctx, EVP_aes_256_xts(), NULL,
						  key, tweak, encrypt ? 1 : 0) != 1)
		return false;

	if (EVP_CIPHER_CTX_set_padding(ctx->evpctx, 0) != 1)
		return false;

	if (EVP_CipherUpdate(ctx->evpctx, out, &outlen, in, len) != 1)
		return false;

	if (EVP_CipherFinal_ex(ctx->evpctx, out + outlen, &finallen) != 1)
		return false;

	return (outlen + finallen == len);
}

/*
 * Encrypt or decrypt 'len' bytes from 'in' into 'out' with AES-256-CTR.
 * CTR is a stream cipher (XOR with a keystream), so the same call both
 * encrypts and decrypts.  'key' is PG_AES256_KEY_LEN bytes and 'iv' is the
 * 16-byte initial counter block.  In-place operation (in == out) is allowed.
 * Used for WAL encryption, where the data is a byte stream rather than
 * fixed-size blocks.  Returns false on failure.
 */
bool
pg_cipher_ctr_crypt(PgCipherCtx *ctx,
					const unsigned char *key,
					const unsigned char *iv,
					const unsigned char *in,
					unsigned char *out,
					int len)
{
	int			outlen,
				finallen;

	if (ctx == NULL)
		return false;

	/* The direction argument is ignored for CTR, but must be provided. */
	if (EVP_CipherInit_ex(ctx->evpctx, EVP_aes_256_ctr(), NULL,
						  key, iv, 1) != 1)
		return false;

	if (EVP_CipherUpdate(ctx->evpctx, out, &outlen, in, len) != 1)
		return false;

	if (EVP_CipherFinal_ex(ctx->evpctx, out + outlen, &finallen) != 1)
		return false;

	return (outlen + finallen == len);
}

/*
 * Encrypt 'inlen' bytes of key material with AES-256-GCM, producing
 * ciphertext of the same length in 'out' and a PG_GCM_TAG_LEN-byte
 * authentication tag in 'tag'.  'key' is PG_AES256_KEY_LEN bytes and
 * 'iv' is PG_GCM_IV_LEN bytes.
 */
bool
pg_cipher_gcm_wrap(const unsigned char *key,
				   const unsigned char *iv,
				   const unsigned char *in, int inlen,
				   unsigned char *out,
				   unsigned char *tag)
{
	EVP_CIPHER_CTX *evpctx;
	int			outlen,
				finallen;
	bool		ok = false;

	evpctx = EVP_CIPHER_CTX_new();
	if (evpctx == NULL)
		return false;

	if (EVP_EncryptInit_ex(evpctx, EVP_aes_256_gcm(), NULL, NULL, NULL) != 1)
		goto out;
	if (EVP_CIPHER_CTX_ctrl(evpctx, EVP_CTRL_GCM_SET_IVLEN,
							PG_GCM_IV_LEN, NULL) != 1)
		goto out;
	if (EVP_EncryptInit_ex(evpctx, NULL, NULL, key, iv) != 1)
		goto out;
	if (EVP_EncryptUpdate(evpctx, out, &outlen, in, inlen) != 1)
		goto out;
	if (EVP_EncryptFinal_ex(evpctx, out + outlen, &finallen) != 1)
		goto out;
	if (EVP_CIPHER_CTX_ctrl(evpctx, EVP_CTRL_GCM_GET_TAG,
							PG_GCM_TAG_LEN, tag) != 1)
		goto out;

	ok = (outlen + finallen == inlen);

out:
	EVP_CIPHER_CTX_free(evpctx);
	return ok;
}

/*
 * Reverse of pg_cipher_gcm_wrap().  Returns false if decryption fails,
 * notably when the authentication tag does not verify, which is the
 * expected failure mode when the wrong KEK is supplied.
 */
bool
pg_cipher_gcm_unwrap(const unsigned char *key,
					 const unsigned char *iv,
					 const unsigned char *in, int inlen,
					 unsigned char *out,
					 const unsigned char *tag)
{
	EVP_CIPHER_CTX *evpctx;
	int			outlen,
				finallen;
	bool		ok = false;

	evpctx = EVP_CIPHER_CTX_new();
	if (evpctx == NULL)
		return false;

	if (EVP_DecryptInit_ex(evpctx, EVP_aes_256_gcm(), NULL, NULL, NULL) != 1)
		goto out;
	if (EVP_CIPHER_CTX_ctrl(evpctx, EVP_CTRL_GCM_SET_IVLEN,
							PG_GCM_IV_LEN, NULL) != 1)
		goto out;
	if (EVP_DecryptInit_ex(evpctx, NULL, NULL, key, iv) != 1)
		goto out;
	if (EVP_DecryptUpdate(evpctx, out, &outlen, in, inlen) != 1)
		goto out;
	if (EVP_CIPHER_CTX_ctrl(evpctx, EVP_CTRL_GCM_SET_TAG,
							PG_GCM_TAG_LEN, unconstify(unsigned char *, tag)) != 1)
		goto out;
	if (EVP_DecryptFinal_ex(evpctx, out + outlen, &finallen) != 1)
		goto out;

	ok = (outlen + finallen == inlen);

out:
	EVP_CIPHER_CTX_free(evpctx);
	return ok;
}
