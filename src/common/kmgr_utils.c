/*-------------------------------------------------------------------------
 *
 * kmgr_utils.c
 *		Key management routines shared between frontend and backend
 *
 * See src/include/common/kmgr_utils.h for an overview.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/kmgr_utils.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <ctype.h>

#include "common/kmgr_utils.h"

static int
hex_digit(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	return -1;
}

/*
 * Run 'command' with popen() and parse its standard output as the cluster
 * key: exactly KMGR_CLUSTER_KEY_LEN * 2 hexadecimal characters, optionally
 * followed by whitespace.  On success the decoded key is stored in 'kek'
 * and true is returned.  On failure, false is returned and an explanation
 * is written to 'errstr'.
 */
bool
kmgr_run_cluster_key_command(const char *command, unsigned char *kek,
							 char *errstr, size_t errsize)
{
	FILE	   *fh;
	char		buf[256];
	size_t		len;
	int			rc;

	fflush(NULL);
	errno = 0;
	fh = popen(command, "r");
	if (fh == NULL)
	{
		snprintf(errstr, errsize, "could not execute command \"%s\": %m",
				 command);
		return false;
	}

	len = fread(buf, 1, sizeof(buf) - 1, fh);
	buf[len] = '\0';

	rc = pclose(fh);
	if (rc != 0)
	{
		snprintf(errstr, errsize, "command \"%s\" failed with exit status %d",
				 command, rc);
		return false;
	}

	/* strip trailing whitespace (typically a newline) */
	while (len > 0 && isspace((unsigned char) buf[len - 1]))
		buf[--len] = '\0';

	if (len != KMGR_CLUSTER_KEY_LEN * 2)
	{
		snprintf(errstr, errsize,
				 "cluster key command returned %zu characters, expected %d hexadecimal characters",
				 len, KMGR_CLUSTER_KEY_LEN * 2);
		return false;
	}

	for (int i = 0; i < KMGR_CLUSTER_KEY_LEN; i++)
	{
		int			hi = hex_digit(buf[i * 2]);
		int			lo = hex_digit(buf[i * 2 + 1]);

		if (hi < 0 || lo < 0)
		{
			snprintf(errstr, errsize,
					 "cluster key command output is not valid hexadecimal");
			return false;
		}
		kek[i] = (unsigned char) ((hi << 4) | lo);
	}

	explicit_bzero(buf, sizeof(buf));

	return true;
}

/*
 * Wrap a KMGR_DATA_KEY_LEN-byte data key with the KEK, using a freshly
 * generated random IV.
 */
bool
kmgr_wrap_key(const unsigned char *kek, const unsigned char *plainkey,
			  KmgrWrappedKey *wrapped)
{
	if (!pg_strong_random(wrapped->iv, PG_GCM_IV_LEN))
		return false;

	return pg_cipher_gcm_wrap(kek, wrapped->iv,
							  plainkey, KMGR_DATA_KEY_LEN,
							  wrapped->key, wrapped->tag);
}

/*
 * Unwrap a data key.  Returns false if the KEK is wrong (authentication
 * tag mismatch).
 */
bool
kmgr_unwrap_key(const unsigned char *kek, const KmgrWrappedKey *wrapped,
				unsigned char *plainkey)
{
	return pg_cipher_gcm_unwrap(kek, wrapped->iv,
								wrapped->key, KMGR_DATA_KEY_LEN,
								plainkey, wrapped->tag);
}

void
kmgr_compute_file_crc(KmgrFileData *filedata)
{
	INIT_CRC32C(filedata->crc);
	COMP_CRC32C(filedata->crc, filedata,
				offsetof(KmgrFileData, crc));
	FIN_CRC32C(filedata->crc);
}

bool
kmgr_verify_file_crc(const KmgrFileData *filedata)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, filedata, offsetof(KmgrFileData, crc));
	FIN_CRC32C(crc);

	return EQ_CRC32C(crc, filedata->crc);
}
