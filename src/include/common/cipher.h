/*-------------------------------------------------------------------------
 *
 * cipher.h
 *		Block cipher support for transparent data encryption (TDE)
 *
 * This provides a thin wrapper around the OpenSSL EVP block cipher
 * routines, usable from both frontend and backend code.  Two cipher
 * uses are supported:
 *
 * - AES-256-XTS for data page encryption.  XTS is a length-preserving,
 *	 tweakable cipher designed for storage encryption; the tweak is
 *	 derived from the block's location so that identical plaintext at
 *	 different locations encrypts differently.
 *
 * - AES-256-GCM for key wrapping.  GCM is authenticated, so unwrapping
 *	 with the wrong key encryption key (KEK) is reliably detected via
 *	 the authentication tag.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/common/cipher.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CIPHER_H
#define PG_CIPHER_H

/*
 * Supported data encryption ciphers.  These values are stored in
 * pg_control and in the key manager file, so they must not change.
 *
 * AES256_XTS is used for relation data pages; AES256_CTR is used for the
 * WAL, which is a byte stream rather than fixed-size blocks.
 */
#define PG_CIPHER_NONE			0
#define PG_CIPHER_AES256_XTS	1
#define PG_CIPHER_AES256_CTR	2

#define PG_AES256_KEY_LEN		32
/* XTS mode uses a double-length key (two AES-256 keys) */
#define PG_AES256_XTS_KEY_LEN	64
#define PG_XTS_TWEAK_LEN		16
#define PG_GCM_IV_LEN			12
#define PG_GCM_TAG_LEN			16

/* Opaque cipher context, reusable across many block operations */
typedef struct PgCipherCtx PgCipherCtx;

extern PgCipherCtx *pg_cipher_ctx_create(void);
extern void pg_cipher_ctx_free(PgCipherCtx *ctx);

extern bool pg_cipher_xts_crypt(PgCipherCtx *ctx, bool encrypt,
								const unsigned char *key,
								const unsigned char *tweak,
								const unsigned char *in,
								unsigned char *out,
								int len);

extern bool pg_cipher_ctr_crypt(PgCipherCtx *ctx,
								const unsigned char *key,
								const unsigned char *iv,
								const unsigned char *in,
								unsigned char *out,
								int len);

extern bool pg_cipher_gcm_wrap(const unsigned char *key,
							   const unsigned char *iv,
							   const unsigned char *in, int inlen,
							   unsigned char *out,
							   unsigned char *tag);
extern bool pg_cipher_gcm_unwrap(const unsigned char *key,
								 const unsigned char *iv,
								 const unsigned char *in, int inlen,
								 unsigned char *out,
								 const unsigned char *tag);

#endif							/* PG_CIPHER_H */
