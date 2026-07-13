/*-------------------------------------------------------------------------
 *
 * xlogencrypt.h
 *		Transparent encryption of the write-ahead log
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/access/xlogencrypt.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef XLOGENCRYPT_H
#define XLOGENCRYPT_H

#include "access/xlogdefs.h"

/*
 * Decrypt one WAL page in place using an explicitly supplied WAL key.  Usable
 * from both frontend (e.g. pg_waldump given the cluster key) and backend
 * code.  Pages without the XLP_ENCRYPTED flag are left untouched.
 */
extern void XLogDecryptPageWithKey(char *page, const unsigned char *walkey);

#ifndef FRONTEND
/* Backend-only helpers that source the WAL key from shared memory. */
extern bool WALEncryptionEnabled(void);
extern void XLogEncryptPage(const char *src, char *dst);
extern void XLogDecryptPage(char *page);
#endif

#endif							/* XLOGENCRYPT_H */
