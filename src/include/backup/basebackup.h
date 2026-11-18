/*-------------------------------------------------------------------------
 *
 * basebackup.h
 *	  Exports from replication/basebackup.c.
 *
 * Portions Copyright (c) 2010-2026, PostgreSQL Global Development Group
 *
 * src/include/backup/basebackup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _BASEBACKUP_H
#define _BASEBACKUP_H

#include "nodes/replnodes.h"
#include "utils/guc.h"

/*
 * Minimum and maximum values of MAX_RATE option in BASE_BACKUP command.
 */
#define MAX_RATE_LOWER	32
#define MAX_RATE_UPPER	1048576

/*
 * How a base backup reads data files (GUC basebackup_read_mode).
 */
typedef enum BaseBackupReadMode
{
	BASEBACKUP_READ_BUFFERED,	/* plain reads through the OS page cache */
	BASEBACKUP_READ_DIRECT,		/* O_DIRECT, bypassing the page cache */
	BASEBACKUP_READ_NOREUSE,	/* POSIX_FADV_NOREUSE on each file */
	BASEBACKUP_READ_DONTNEED,	/* POSIX_FADV_DONTNEED after each read */
} BaseBackupReadMode;

extern PGDLLIMPORT int basebackup_read_mode;
extern PGDLLIMPORT int basebackup_direct_read_size;
extern PGDLLIMPORT const struct config_enum_entry basebackup_read_mode_options[];

/*
 * Information about a tablespace
 *
 * In some usages, "path" can be NULL to denote the PGDATA directory itself.
 */
typedef struct
{
	Oid			oid;			/* tablespace's OID */
	char	   *path;			/* full path to tablespace's directory */
	char	   *rpath;			/* relative path if it's within PGDATA, else
								 * NULL */
	int64		size;			/* total size as sent; -1 if not known */
} tablespaceinfo;

struct IncrementalBackupInfo;

extern void SendBaseBackup(BaseBackupCmd *cmd,
						   struct IncrementalBackupInfo *ib);

#endif							/* _BASEBACKUP_H */
