/*-------------------------------------------------------------------------
 *
 * kmgr_utils.h
 *		Declarations for key management, shared between frontend and backend
 *
 * The key manager uses a two-tier key architecture.  The key encryption
 * key (KEK, or "cluster key") is never stored on disk; it is obtained by
 * running cluster_key_command, which must print the key as 64 hexadecimal
 * characters on standard output.  The data encryption keys (DEKs) are
 * generated at initdb time and stored in KMGR_FILE_NAME, wrapped
 * (encrypted and authenticated) with the KEK using AES-256-GCM.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/common/kmgr_utils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KMGR_UTILS_H
#define KMGR_UTILS_H

#include "common/cipher.h"
#include "port/pg_crc32c.h"

/* Key encryption key (cluster key) is AES-256 */
#define KMGR_CLUSTER_KEY_LEN	PG_AES256_KEY_LEN
/* Data encryption keys are AES-256-XTS double-length keys */
#define KMGR_DATA_KEY_LEN		PG_AES256_XTS_KEY_LEN

#define KMGR_FILE_NAME			"global/pg_kmgr"
#define KMGR_FILE_NAME_TMP		KMGR_FILE_NAME ".tmp"
#define KMGR_FILE_MAGIC			0x504b4d47
#define KMGR_FILE_VERSION		1

/*
 * A data encryption key wrapped with the KEK using AES-256-GCM.
 */
typedef struct KmgrWrappedKey
{
	unsigned char iv[PG_GCM_IV_LEN];
	unsigned char key[KMGR_DATA_KEY_LEN];	/* encrypted key material */
	unsigned char tag[PG_GCM_TAG_LEN];
} KmgrWrappedKey;

/*
 * Contents of KMGR_FILE_NAME.  Changing this struct requires bumping
 * KMGR_FILE_VERSION.
 */
typedef struct KmgrFileData
{
	uint32		magic;			/* KMGR_FILE_MAGIC */
	uint32		version;		/* KMGR_FILE_VERSION */
	uint32		cipher;			/* PG_CIPHER_*, must match pg_control */
	KmgrWrappedKey relkey;		/* key for relation data files */
	KmgrWrappedKey walkey;		/* reserved for future WAL encryption */
	pg_crc32c	crc;			/* CRC of all above ... MUST BE LAST! */
} KmgrFileData;

extern bool kmgr_run_cluster_key_command(const char *command,
										 unsigned char *kek,
										 char *errstr, size_t errsize);
extern bool kmgr_wrap_key(const unsigned char *kek,
						  const unsigned char *plainkey,
						  KmgrWrappedKey *wrapped);
extern bool kmgr_unwrap_key(const unsigned char *kek,
							const KmgrWrappedKey *wrapped,
							unsigned char *plainkey);
extern void kmgr_compute_file_crc(KmgrFileData *filedata);
extern bool kmgr_verify_file_crc(const KmgrFileData *filedata);

#endif							/* KMGR_UTILS_H */
