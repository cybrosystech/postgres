/*-------------------------------------------------------------------------
 *
 * xlog_smgr.c
 *	  The active WAL storage manager.
 *
 * WAL segment reads and writes are dispatched through a small vtable so that
 * an extension can substitute an implementation of its own -- pg_tde uses
 * this to encrypt and decrypt WAL in place.  The default simply performs
 * unbuffered positioned I/O, so an unpatched server behaves exactly as it did
 * before the indirection was introduced.
 *
 * This lives in src/common rather than beside the WAL code because both the
 * backend and the frontend utilities that touch WAL (pg_waldump, pg_rewind,
 * pg_resetwal) need it, and pg_basebackup needs it without compiling the WAL
 * reader at all.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/xlog_smgr.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "access/xlog_smgr.h"

/*
 * Points at the standard implementation until an extension installs its own.
 * Statically initialised so it is valid from process start, before any
 * initialisation code has had a chance to run -- WAL is read very early, in
 * bootstrap and at the start of crash recovery.
 */
const XLogSmgr *xlog_smgr = &xlog_smgr_standard;

void
SetXLogSmgr(const XLogSmgr *xlsmgr)
{
	xlog_smgr = xlsmgr;
}
