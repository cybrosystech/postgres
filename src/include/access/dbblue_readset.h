/*-------------------------------------------------------------------------
 *
 * dbblue_readset.h
 *	  Per-transaction read-set tracking for read-set-gated merging of
 *	  concurrent updates (DBblue).
 *
 * See src/backend/access/heap/dbblue_readset.c for an overview of what this
 * is for and why it is shaped the way it is.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/access/dbblue_readset.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBBLUE_READSET_H
#define DBBLUE_READSET_H

#include "access/htup.h"
#include "nodes/bitmapset.h"
#include "storage/itemptr.h"
#include "utils/relcache.h"

/* GUCs, all in the db_blue.* namespace */
extern PGDLLIMPORT bool db_blue_rr_merge_track;
extern PGDLLIMPORT bool db_blue_rr_merge_log;
extern PGDLLIMPORT int db_blue_rr_merge_work_mem;
extern PGDLLIMPORT int db_blue_rr_merge_max_rows;
extern PGDLLIMPORT char *db_blue_rr_merge_ignore_cols;

typedef struct DBBlueReadSet DBBlueReadSet;

/*
 * The current transaction's read set, or NULL when tracking is not active.
 * This is tested inline on every tuple-returning path, so it stays a plain
 * global rather than hiding behind a function call.
 */
extern PGDLLIMPORT DBBlueReadSet *DBBlueReadSetCurrent;

/*
 * Nonzero while we are ourselves inspecting rows in order to answer a merge
 * question.  Those reads must not be recorded, or the read set would answer
 * its own question with "yes, you read it".
 */
extern PGDLLIMPORT int DBBlueTrackingSuppressed;

/* setup / teardown */
extern void DBBlueReadSetBegin(void);
extern bool DBBlueReadSetUsable(void);
extern const char *DBBlueReadSetUnusableReason(void);
extern void DBBlueTaintReadSet(const char *reason);

/* recording */
extern void DBBlueNoteRowReadImpl(Oid relid, ItemPointer tid);
extern void DBBlueNoteRelationRead(Oid relid, const Bitmapset *selectedCols);
extern void DBBlueNoteAllColsRead(Oid relid);

/* interrogation */
extern bool DBBlueRowWasRead(Oid relid, ItemPointer tid);
extern Bitmapset *DBBlueGetReadCols(Oid relid, bool *allCols);

/* conflict-time analysis (heap-specific) */
struct TupleTableSlot;
extern bool DBBlueAnalyzeUpdateConflict(Relation rel, ItemPointer snaptid,
										HeapTuple snaptup,
										const Bitmapset *writecols);
extern void DBBlueLogSkippedConflict(Relation rel, ItemPointer tid,
									 const char *reason);

/*
 * Record that the current statement read the row at `tid` of `relid`.
 *
 * Inlined because it sits on the per-tuple path of every scan; when the
 * feature is off this is one test of a global pointer.
 */
static inline void
DBBlueNoteRowRead(Oid relid, ItemPointer tid)
{
	if (DBBlueReadSetCurrent != NULL)
		DBBlueNoteRowReadImpl(relid, tid);
}

#endif							/* DBBLUE_READSET_H */
