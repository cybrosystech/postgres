/*-------------------------------------------------------------------------
 *
 * undolog_stub.c
 *	  Stub implementations for undo-log helpers that have not yet been
 *	  ported from PG12 to PG19.
 *
 * Until src/backend/storage/smgr/undofile.c is ported, the undo log
 * code in src/backend/access/undo/undolog.c calls a small number of
 * undofile helpers that do not yet exist as compiled symbols.  We
 * provide weak stubs here so the binary links.  Because the stub zheap
 * AM never actually writes undo segments, these stubs are never reached
 * in practice; if they do fire it indicates someone wired in undo I/O
 * before porting undofile.c, and PANIC is the correct response.
 *
 * Note: the rmgr redo / desc / identify callbacks for RM_UNDOLOG_ID are
 * now defined by the real undolog.c (port phase 1.2) and by
 * rmgrdesc/undologdesc.c, so they are no longer stubbed here.
 *
 * Portions Copyright (c) 2026, dbblue / Cybrosys Technologies.
 *
 * IDENTIFICATION
 *	  src/backend/access/zheap_stub/undolog_stub.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/block.h"
#include "storage/undofile.h"

void
undofile_forget_sync(UndoLogNumber logno, BlockNumber segno, Oid tablespace)
{
	elog(PANIC, "undofile_forget_sync: undofile.c not yet ported to PG19; "
		 "this should be unreachable until the undo SMGR is wired in");
}
