/*-------------------------------------------------------------------------
 *
 * xlogencrypt.c
 *		Transparent encryption of the write-ahead log
 *
 * When WAL encryption is enabled, the body of each WAL page (everything
 * after the page header) is encrypted with AES-256-CTR on its way to disk
 * and decrypted on its way back in.  The page header is left in plaintext:
 * it carries no user data, and readers need xlp_pageaddr and xlp_tli to
 * derive the encryption position before they can decrypt.
 *
 * WAL is a byte stream that is appended to and rewritten as pages fill, so
 * the block-oriented XTS cipher used for relation data does not fit; CTR is
 * a stream cipher keyed by position instead.  The 128-bit CTR counter block
 * ("IV") for a page is
 *
 *		bytes 0..7   : xlp_pageaddr (the page's start LSN), big-endian
 *		bytes 8..11  : xlp_tli, big-endian
 *		bytes 12..15 : block counter, starts at zero
 *
 * A page is 8 kB, so at most 512 counter blocks are consumed per page and
 * the counter never carries out of its four low bytes into the timeline or
 * pageaddr fields.  Because (pageaddr, tli) uniquely identifies a page's
 * content within the WAL stream, and WAL is append-only within a timeline,
 * no keystream position is ever reused for differing data that could be
 * observed in a single on-disk image.
 *
 * The XLP_ENCRYPTED flag in xlp_info marks a page whose body is encrypted,
 * so plaintext pages written before encryption was enabled, and the zeroed
 * tails of partially-filled segments, are handled transparently.
 *
 * Most of this file is usable from frontend code as well (e.g. pg_waldump
 * decrypting WAL when given the cluster key): the core takes the WAL key as
 * an explicit argument.  The convenience wrappers that fetch the key from
 * shared memory are backend-only.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/xlogencrypt.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#include "access/xlog.h"
#include "crypto/kmgr.h"
#else
#include "postgres_fe.h"
#include "common/logging.h"
#endif

#include "access/xlog_internal.h"
#include "access/xlogencrypt.h"
#include "common/cipher.h"

#ifndef FRONTEND
#define WAL_CRYPT_ERROR(...) elog(ERROR, __VA_ARGS__)
#else
#define WAL_CRYPT_ERROR(...) pg_fatal(__VA_ARGS__)
#endif

/* Reusable cipher context, created lazily per process. */
static PgCipherCtx *wal_cipher_ctx = NULL;

/*
 * Build the 16-byte AES-CTR initial counter block for a WAL page.  See the
 * file header comment for the layout.
 */
static void
wal_build_iv(XLogRecPtr pageaddr, TimeLineID tli, unsigned char *iv)
{
	iv[0] = (unsigned char) (pageaddr >> 56);
	iv[1] = (unsigned char) (pageaddr >> 48);
	iv[2] = (unsigned char) (pageaddr >> 40);
	iv[3] = (unsigned char) (pageaddr >> 32);
	iv[4] = (unsigned char) (pageaddr >> 24);
	iv[5] = (unsigned char) (pageaddr >> 16);
	iv[6] = (unsigned char) (pageaddr >> 8);
	iv[7] = (unsigned char) (pageaddr);
	iv[8] = (unsigned char) (tli >> 24);
	iv[9] = (unsigned char) (tli >> 16);
	iv[10] = (unsigned char) (tli >> 8);
	iv[11] = (unsigned char) (tli);
	iv[12] = 0;
	iv[13] = 0;
	iv[14] = 0;
	iv[15] = 0;
}

static void
wal_ensure_ctx(void)
{
	if (wal_cipher_ctx == NULL)
	{
		wal_cipher_ctx = pg_cipher_ctx_create();
		if (wal_cipher_ctx == NULL)
			WAL_CRYPT_ERROR("could not create WAL cipher context");
	}
}

/*
 * Encrypt or decrypt the body of one WAL page in place with the given key.
 * CTR is symmetric, so this serves both directions; the caller manages the
 * XLP_ENCRYPTED flag.
 */
static void
wal_crypt_body(char *page, const unsigned char *walkey)
{
	XLogPageHeader hdr = (XLogPageHeader) page;
	uint32		hs = XLogPageHeaderSize(hdr);
	unsigned char iv[16];

	wal_build_iv(hdr->xlp_pageaddr, hdr->xlp_tli, iv);
	wal_ensure_ctx();

	if (!pg_cipher_ctr_crypt(wal_cipher_ctx, walkey, iv,
							 (const unsigned char *) page + hs,
							 (unsigned char *) page + hs,
							 XLOG_BLCKSZ - hs))
		WAL_CRYPT_ERROR("could not process WAL page at %X/%08X",
						LSN_FORMAT_ARGS(hdr->xlp_pageaddr));
}

/*
 * Decrypt one BLCKSZ WAL page in place using an explicit key.  Pages without
 * the XLP_ENCRYPTED flag (plaintext pages, zeroed tails) are left untouched.
 * On return the buffer is an ordinary plaintext WAL page with the flag
 * cleared.
 */
void
XLogDecryptPageWithKey(char *page, const unsigned char *walkey)
{
	XLogPageHeader hdr = (XLogPageHeader) page;

	if ((hdr->xlp_info & XLP_ENCRYPTED) == 0)
		return;

	wal_crypt_body(page, walkey);
	hdr->xlp_info &= ~XLP_ENCRYPTED;
}

#ifndef FRONTEND

/*
 * Is the WAL encrypted in this cluster?  Determined by the cipher recorded
 * in the control file at initdb time.
 */
bool
WALEncryptionEnabled(void)
{
	return GetWALEncryptionCipher() != PG_CIPHER_NONE;
}

/*
 * Encrypt one BLCKSZ WAL page from 'src' into 'dst'.  The header is copied
 * verbatim and marked with XLP_ENCRYPTED; the body is encrypted.  'src' and
 * 'dst' must not overlap.  A page whose header is not a valid WAL page (for
 * example an all-zero, never-written page) is copied through unchanged.
 */
void
XLogEncryptPage(const char *src, char *dst)
{
	XLogPageHeader hdr = (XLogPageHeader) dst;

	memcpy(dst, src, XLOG_BLCKSZ);

	/* Never-written / non-WAL pages carry no data to protect. */
	if (hdr->xlp_magic != XLOG_PAGE_MAGIC)
		return;

	hdr->xlp_info |= XLP_ENCRYPTED;
	wal_crypt_body(dst, KmgrGetWALKey());
}

/*
 * Decrypt one BLCKSZ WAL page in place, sourcing the key from shared memory.
 */
void
XLogDecryptPage(char *page)
{
	XLogDecryptPageWithKey(page, KmgrGetWALKey());
}

#endif							/* !FRONTEND */
