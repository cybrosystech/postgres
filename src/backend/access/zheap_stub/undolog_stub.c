/*-------------------------------------------------------------------------
 *
 * undolog_stub.c
 *	  Stub implementation of the undo-log rmgr redo callback.
 *
 * The real implementation lives in src/backend/access/undo/undolog.c
 * (staged from the PG12 zheap tree) but is not yet ported to PG19.
 * Until that port lands, undolog_redo must exist as a symbol so that
 * src/include/access/rmgrlist.h links successfully.  This stub PANICs
 * if it ever fires — that would mean someone wired in an undo writer
 * before porting recovery, which is a development error.
 *
 * The companion undolog_desc / undolog_identify functions live in
 * src/backend/access/rmgrdesc/undologdesc.c because that directory is
 * linked into pg_waldump and other frontend tools that inspect WAL.
 *
 * Portions Copyright (c) 2026, dbblue / Cybrosys Technologies.
 *
 * IDENTIFICATION
 *	  src/backend/access/zheap_stub/undolog_stub.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/undolog_rmgr.h"
#include "access/xlogreader.h"

void
undolog_redo(XLogReaderState *record)
{
	elog(PANIC, "undolog_redo: undo log subsystem is not yet ported to PG19; "
		 "a WAL record for RM_UNDOLOG_ID should not exist in this build");
}
