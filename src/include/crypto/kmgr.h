/*-------------------------------------------------------------------------
 *
 * kmgr.h
 *		Backend key manager for transparent data encryption
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/crypto/kmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KMGR_H
#define KMGR_H

#include "common/kmgr_utils.h"
#include "storage/shmem.h"

/* GUC */
extern PGDLLIMPORT char *cluster_key_command;

extern PGDLLIMPORT const ShmemCallbacks KmgrShmemCallbacks;

extern void InitializeKmgr(uint32 bootstrap_cipher);
extern bool DataEncryptionEnabled(void);
extern uint32 GetDataEncryptionCipherInUse(void);
extern const unsigned char *KmgrGetRelationKey(void);
extern const unsigned char *KmgrGetWALKey(void);
extern bool KmgrCheckFile(void);
extern void KmgrRotateClusterKey(void);

#endif							/* KMGR_H */
