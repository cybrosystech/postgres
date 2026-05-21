/*-------------------------------------------------------------------------
 *
 * undologdesc.c
 *	  rmgr descriptor routines for the undo log resource manager.
 *
 * Lives in rmgrdesc/ (not in the undo subsystem proper) so that
 * pg_waldump and other frontend tools that link the rmgrdesc objects
 * can resolve undolog_desc / undolog_identify even when the full undo
 * log backend is not yet built (in-progress port from PG12 — see
 * ZHEAP_PORT_NOTES.md).
 *
 * Until src/backend/access/undo/ is ported and emitting WAL,
 * undolog_desc will never be called with a real record, but the symbol
 * still has to exist for the rmgr table to link.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, dbblue / Cybrosys Technologies.
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/undologdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/undolog_rmgr.h"
#include "access/undolog_xlog.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"

void
undolog_desc(StringInfo buf, XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	appendStringInfo(buf, "info: 0x%02X (undo log subsystem not yet ported)",
					 info);
}

const char *
undolog_identify(uint8 info)
{
	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_UNDOLOG_CREATE:
			return "CREATE";
		case XLOG_UNDOLOG_EXTEND:
			return "EXTEND";
		case XLOG_UNDOLOG_ATTACH:
			return "ATTACH";
		case XLOG_UNDOLOG_DISCARD:
			return "DISCARD";
		case XLOG_UNDOLOG_REWIND:
			return "REWIND";
		case XLOG_UNDOLOG_META:
			return "META";
		case XLOG_UNDOLOG_SWITCH:
			return "SWITCH";
		default:
			return NULL;
	}
}
