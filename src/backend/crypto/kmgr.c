/*-------------------------------------------------------------------------
 *
 * kmgr.c
 *		Backend key manager for transparent data encryption
 *
 * The key manager keeps the cluster's data encryption keys in shared
 * memory, unwrapped.  At postmaster startup (and in bootstrap and
 * single-user mode), InitializeKmgr() runs cluster_key_command to obtain
 * the key encryption key (KEK), reads the wrapped data keys from
 * KMGR_FILE_NAME, and unwraps them into shared memory where all backends
 * and IO workers can use them.  Unwrapping is authenticated (AES-256-GCM),
 * so starting with the wrong cluster key fails cleanly.
 *
 * The KEK is also kept in shared memory to support online cluster key
 * rotation: KmgrRotateClusterKey() re-runs cluster_key_command and
 * re-wraps the (unchanged) data keys with the new KEK.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/crypto/kmgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/xlog.h"
#include "common/cipher.h"
#include "common/kmgr_utils.h"
#include "crypto/kmgr.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

/* GUC */
char	   *cluster_key_command = NULL;

typedef struct KmgrShmemData
{
	bool		enabled;
	uint32		cipher;			/* PG_CIPHER_* */
	unsigned char kek[KMGR_CLUSTER_KEY_LEN];
	unsigned char relkey[KMGR_DATA_KEY_LEN];
	unsigned char walkey[KMGR_DATA_KEY_LEN];
} KmgrShmemData;

static KmgrShmemData *KmgrShmem = NULL;

static void KmgrShmemRequest(void *arg);
static void KmgrShmemInit(void *arg);
static void KmgrReadFile(KmgrFileData *filedata);

const ShmemCallbacks KmgrShmemCallbacks = {
	.request_fn = KmgrShmemRequest,
	.init_fn = KmgrShmemInit,
};

static void
KmgrShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "Key manager",
					   .size = sizeof(KmgrShmemData),
					   .ptr = (void **) &KmgrShmem,
		);
}

static void
KmgrShmemInit(void *arg)
{
	memset(KmgrShmem, 0, sizeof(KmgrShmemData));
}

/*
 * Read and validate the key manager file.  Errors are FATAL since an
 * encrypted cluster cannot operate without its keys.
 */
static void
KmgrReadFile(KmgrFileData *filedata)
{
	int			fd;
	int			r;

	fd = OpenTransientFile(KMGR_FILE_NAME, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open key manager file \"%s\": %m",
						KMGR_FILE_NAME)));

	r = read(fd, filedata, sizeof(KmgrFileData));
	if (r != sizeof(KmgrFileData))
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not read key manager file \"%s\": read %d of %zu bytes",
						KMGR_FILE_NAME, r, sizeof(KmgrFileData))));

	if (CloseTransientFile(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", KMGR_FILE_NAME)));

	if (filedata->magic != KMGR_FILE_MAGIC ||
		filedata->version != KMGR_FILE_VERSION ||
		!kmgr_verify_file_crc(filedata))
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("key manager file \"%s\" is corrupted",
						KMGR_FILE_NAME)));
}

/*
 * Write the key manager file durably (via a temporary file and rename).
 * Caller must hold KmgrFileLock.
 */
static void
KmgrWriteFile(KmgrFileData *filedata)
{
	int			fd;

	kmgr_compute_file_crc(filedata);

	fd = OpenTransientFile(KMGR_FILE_NAME_TMP,
						   O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m",
						KMGR_FILE_NAME_TMP)));

	if (write(fd, filedata, sizeof(KmgrFileData)) != sizeof(KmgrFileData))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						KMGR_FILE_NAME_TMP)));

	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m",
						KMGR_FILE_NAME_TMP)));

	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m",
						KMGR_FILE_NAME_TMP)));

	durable_rename(KMGR_FILE_NAME_TMP, KMGR_FILE_NAME, ERROR);
}

/*
 * Initialize the key manager.  Called once at postmaster startup (after
 * shared memory is created, before any process performs relation IO), at
 * postmaster crash-restart reinitialization, and in bootstrap and
 * single-user modes.
 *
 * In bootstrap mode the control file does not exist yet, so the cipher is
 * passed in from the -K command line flag; otherwise pass 0 and the value
 * stored in the control file is used.
 */
void
InitializeKmgr(uint32 bootstrap_cipher)
{
	uint32		cipher;
	KmgrFileData filedata;
	unsigned char kek[KMGR_CLUSTER_KEY_LEN];
	char		errstr[512];

	cipher = (bootstrap_cipher != PG_CIPHER_NONE) ?
		bootstrap_cipher : GetDataEncryptionCipher();

	if (cipher == PG_CIPHER_NONE)
		return;

#ifndef USE_OPENSSL
	ereport(FATAL,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cluster is encrypted, but the server was not built with OpenSSL support")));
#endif

	if (cluster_key_command == NULL || cluster_key_command[0] == '\0')
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("data encryption is enabled, but \"cluster_key_command\" is not set")));

	if (!kmgr_run_cluster_key_command(cluster_key_command, kek,
									  errstr, sizeof(errstr)))
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not obtain cluster key: %s", errstr)));

	KmgrReadFile(&filedata);

	if (filedata.cipher != cipher)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cipher in key manager file \"%s\" does not match server configuration",
						KMGR_FILE_NAME)));

	if (!kmgr_unwrap_key(kek, &filedata.relkey, KmgrShmem->relkey) ||
		!kmgr_unwrap_key(kek, &filedata.walkey, KmgrShmem->walkey))
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster key verification failed"),
				 errdetail("The key returned by \"cluster_key_command\" does not match the key this cluster was initialized with.")));

	memcpy(KmgrShmem->kek, kek, KMGR_CLUSTER_KEY_LEN);
	explicit_bzero(kek, sizeof(kek));

	KmgrShmem->cipher = cipher;
	KmgrShmem->enabled = true;

	ereport(LOG,
			(errmsg("transparent data encryption is enabled (cipher: aes-256-xts)")));
}

bool
DataEncryptionEnabled(void)
{
	return KmgrShmem != NULL && KmgrShmem->enabled;
}

uint32
GetDataEncryptionCipherInUse(void)
{
	return DataEncryptionEnabled() ? KmgrShmem->cipher : PG_CIPHER_NONE;
}

const unsigned char *
KmgrGetRelationKey(void)
{
	Assert(DataEncryptionEnabled());
	return KmgrShmem->relkey;
}

/*
 * Return the key used for WAL encryption.  WAL uses AES-256-CTR, which needs
 * a single AES-256 key, so we use the first PG_AES256_KEY_LEN bytes of the
 * (double-length) WAL data key.
 */
const unsigned char *
KmgrGetWALKey(void)
{
	Assert(DataEncryptionEnabled());
	return KmgrShmem->walkey;
}

/*
 * Re-read and validate the on-disk key manager file against the keys
 * currently in shared memory.  Returns true if the file is intact and
 * its keys unwrap correctly with the current KEK.
 */
bool
KmgrCheckFile(void)
{
	KmgrFileData filedata;
	unsigned char relkey[KMGR_DATA_KEY_LEN];
	unsigned char walkey[KMGR_DATA_KEY_LEN];
	bool		ok;

	if (!DataEncryptionEnabled())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("data encryption is not enabled")));

	LWLockAcquire(KmgrFileLock, LW_SHARED);

	KmgrReadFile(&filedata);

	ok = (filedata.cipher == KmgrShmem->cipher &&
		  kmgr_unwrap_key(KmgrShmem->kek, &filedata.relkey, relkey) &&
		  kmgr_unwrap_key(KmgrShmem->kek, &filedata.walkey, walkey) &&
		  memcmp(relkey, KmgrShmem->relkey, KMGR_DATA_KEY_LEN) == 0 &&
		  memcmp(walkey, KmgrShmem->walkey, KMGR_DATA_KEY_LEN) == 0);

	LWLockRelease(KmgrFileLock);

	explicit_bzero(relkey, sizeof(relkey));
	explicit_bzero(walkey, sizeof(walkey));

	return ok;
}

/*
 * Rotate the cluster key (KEK).  Runs cluster_key_command to obtain the
 * new KEK and re-wraps the existing data encryption keys with it.  The
 * data keys themselves do not change, so no data is re-encrypted.
 *
 * The caller is expected to have updated the secret that
 * cluster_key_command returns (or changed the command via SIGHUP) before
 * calling this.
 */
void
KmgrRotateClusterKey(void)
{
	KmgrFileData filedata;
	unsigned char newkek[KMGR_CLUSTER_KEY_LEN];
	char		errstr[512];

	if (!DataEncryptionEnabled())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("data encryption is not enabled")));

	if (cluster_key_command == NULL || cluster_key_command[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"cluster_key_command\" is not set")));

	if (!kmgr_run_cluster_key_command(cluster_key_command, newkek,
									  errstr, sizeof(errstr)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not obtain cluster key: %s", errstr)));

	LWLockAcquire(KmgrFileLock, LW_EXCLUSIVE);

	memset(&filedata, 0, sizeof(filedata));
	filedata.magic = KMGR_FILE_MAGIC;
	filedata.version = KMGR_FILE_VERSION;
	filedata.cipher = KmgrShmem->cipher;

	if (!kmgr_wrap_key(newkek, KmgrShmem->relkey, &filedata.relkey) ||
		!kmgr_wrap_key(newkek, KmgrShmem->walkey, &filedata.walkey))
	{
		LWLockRelease(KmgrFileLock);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not wrap data encryption keys")));
	}

	KmgrWriteFile(&filedata);

	memcpy(KmgrShmem->kek, newkek, KMGR_CLUSTER_KEY_LEN);

	LWLockRelease(KmgrFileLock);

	explicit_bzero(newkek, sizeof(newkek));

	ereport(LOG,
			(errmsg("cluster key was rotated")));
}
