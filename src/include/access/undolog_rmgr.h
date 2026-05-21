/*-------------------------------------------------------------------------
 *
 * undolog_rmgr.h
 *	  Minimal extern declarations for the undo-log resource manager.
 *
 * This header exists solely to satisfy the rmgr table expansion in
 * src/backend/access/transam/rmgr.c (via src/include/access/rmgrlist.h)
 * without dragging in the full PG12 src/include/access/undolog.h, which
 * references unported PG12 types (UndoLogNumber, UndoPersistence, etc.).
 *
 * The three functions declared here are currently stubbed out in
 * src/backend/access/zheap_stub/undolog_stub.c.  When the real undo log
 * subsystem is ported (src/backend/access/undo/undolog.c), the
 * definitions move there and this header may be folded back into
 * access/undolog.h.
 *
 * Portions Copyright (c) 2026, dbblue / Cybrosys Technologies.
 *
 * IDENTIFICATION
 *	  src/include/access/undolog_rmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDOLOG_RMGR_H
#define UNDOLOG_RMGR_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

extern void undolog_redo(XLogReaderState *record);
extern void undolog_desc(StringInfo buf, XLogReaderState *record);
extern const char *undolog_identify(uint8 info);

#endif							/* UNDOLOG_RMGR_H */
