/*-------------------------------------------------------------------------
 *
 * dbblue_readset.c
 *	  Per-transaction read-set tracking for read-set-gated merging of
 *	  concurrent updates (DBblue).
 *
 * BACKGROUND
 *
 * Odoo runs one REPEATABLE READ transaction per RPC request.  When such a
 * transaction UPDATEs a row that another transaction updated and committed
 * after our snapshot was taken, ExecUpdate() aborts with 40001 (see the
 * TM_Updated branch in nodeModifyTable.c).  Many of those aborts are false
 * conflicts: the other transaction changed columns that we neither read nor
 * write, so the literal value we are about to store cannot have been computed
 * from anything it changed.
 *
 * Deciding that requires answering exactly one question at conflict time:
 *
 *		"did this transaction read any of the columns that the other
 *		 transaction changed, on this row?"
 *
 * with no false negatives (a missed read would let us merge a write that
 * really did depend on stale data) and with false positives tolerated (they
 * only cost us an abort we would have taken anyway).
 *
 * WHAT IS TRACKED
 *
 * Rows: a fixed-size Bloom filter of (relid, block, offset).  Memory is
 * bounded regardless of how many rows the transaction reads; the price is a
 * tunable false-positive rate.  A ctid identifies a tuple *version*, which is
 * what makes this work: any read of a row under our snapshot returns the
 * snapshot version's ctid, and that is exactly the ctid the conflict arrives
 * with, so both sides agree on the key by construction.
 *
 * Columns: per relation, for the whole transaction, taken from the query's
 * RTEPermissionInfo.selectedCols.  That is deliberately coarser than
 * per-(row, column) tracking -- a column read from any row of a relation
 * counts as read for every row of it -- because for Odoo's access pattern
 * (the same column list fetched across many rows of a table) the extra
 * precision buys very little for a much higher per-tuple cost.  Coarser only
 * ever means more aborts, never fewer.
 *
 * Both sets are over-approximations, on purpose.  Anything we cannot attribute
 * taints the transaction, and a tainted transaction is never merged.
 *
 * WHAT THIS FILE DOES *NOT* DO
 *
 * Nothing here changes behavior.  This is the instrumentation half of the
 * feature: it measures how many 40001 aborts are false conflicts, which is
 * the go/no-go gate for building the merge itself.  ExecUpdate() still aborts
 * unconditionally.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/dbblue_readset.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/dbblue_readset.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_am_d.h"
#include "executor/tuptable.h"
#include "lib/bloomfilter.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/varlena.h"

/* GUCs */
bool		db_blue_rr_merge_track = false;
bool		db_blue_rr_merge_log = false;
int			db_blue_rr_merge_work_mem = 2048;	/* KB */
int			db_blue_rr_merge_max_rows = 1000000;
char	   *db_blue_rr_merge_ignore_cols = NULL;

/* Give up on following an update chain after this many hops */
#define DBBLUE_MAX_CHAIN_HOPS	1000

/*
 * Key identifying one tuple version.  Zeroed before use, because it is hashed
 * as a byte string and padding bytes would otherwise be indeterminate.
 */
typedef struct DBBlueRowKey
{
	Oid			relid;
	BlockNumber blk;
	uint16		off;
} DBBlueRowKey;

/* Per-relation record of which columns the transaction has read */
typedef struct DBBlueColEntry
{
	Oid			relid;			/* hash key */
	Bitmapset  *cols;			/* attnums, offset as in selectedCols */
	bool		all;			/* true: assume every column was read */
} DBBlueColEntry;

struct DBBlueReadSet
{
	bloom_filter *rows;
	HTAB	   *cols;
	int64		nrows;			/* rows recorded, for the cap */
	bool		tainted;		/* if set, this transaction can never merge */
	const char *taint_reason;
};

DBBlueReadSet *DBBlueReadSetCurrent = NULL;
int			DBBlueTrackingSuppressed = 0;

static void dbblue_readset_reset_cb(void *arg);
static bool dbblue_fetch_latest_version(Relation rel, ItemPointer tid,
										TupleTableSlot *slot);
static Bitmapset *dbblue_changed_cols(Relation rel, HeapTuple snaptup,
									  TupleTableSlot *newslot,
									  List *ignorecols,
									  Bitmapset **ignored);
static List *dbblue_parse_ignore_cols(void);
static bool dbblue_name_in_list(const char *name, List *namelist);
static void dbblue_append_cols(StringInfo buf, Relation rel,
							   const Bitmapset *cols, bool all);

static inline void
dbblue_row_key(DBBlueRowKey *key, Oid relid, ItemPointer tid)
{
	memset(key, 0, sizeof(*key));
	key->relid = relid;
	key->blk = ItemPointerGetBlockNumber(tid);
	key->off = ItemPointerGetOffsetNumber(tid);
}


/*
 * DBBlueReadSetBegin
 *
 * Start tracking, if we aren't already.  Called from InitPlan(), i.e. at the
 * start of every statement, because a transaction's reads precede the write
 * that conflicts -- there is no way to switch tracking on lazily at the first
 * write and still know what was read before it.
 *
 * Only REPEATABLE READ transactions are tracked: READ COMMITTED never raises
 * the error we are trying to remove, and SERIALIZABLE has guarantees of its
 * own that a merge would break.
 */
void
DBBlueReadSetBegin(void)
{
	MemoryContext oldcxt;
	MemoryContextCallback *cb;
	DBBlueReadSet *rs;
	HASHCTL		ctl;

	if (DBBlueReadSetCurrent != NULL)
		return;
	if (!db_blue_rr_merge_track)
		return;
	if (XactIsoLevel != XACT_REPEATABLE_READ)
		return;
	if (TopTransactionContext == NULL)
		return;

	oldcxt = MemoryContextSwitchTo(TopTransactionContext);

	rs = palloc0(sizeof(DBBlueReadSet));
	rs->rows = bloom_create(db_blue_rr_merge_max_rows,
							db_blue_rr_merge_work_mem, 0);

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DBBlueColEntry);
	ctl.hcxt = TopTransactionContext;
	rs->cols = hash_create("DBblue read columns", 32, &ctl,
						   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/*
	 * Everything above lives in TopTransactionContext, so it goes away by
	 * itself at commit or abort.  Clear the global from a reset callback
	 * rather than from xact.c, so that every transaction-end path -- commit,
	 * abort, prepare, error recovery -- is covered without having to find
	 * them all.
	 */
	cb = palloc0(sizeof(MemoryContextCallback));
	cb->func = dbblue_readset_reset_cb;
	MemoryContextRegisterResetCallback(TopTransactionContext, cb);

	DBBlueReadSetCurrent = rs;

	MemoryContextSwitchTo(oldcxt);
}

static void
dbblue_readset_reset_cb(void *arg)
{
	DBBlueReadSetCurrent = NULL;
	DBBlueTrackingSuppressed = 0;
}

/*
 * Is there a read set we are allowed to draw conclusions from?
 */
bool
DBBlueReadSetUsable(void)
{
	return DBBlueReadSetCurrent != NULL && !DBBlueReadSetCurrent->tainted;
}

/*
 * DBBlueReadSetUnusableReason
 *
 * NULL when the read set can be trusted, otherwise why it cannot be.  Callers
 * put this in the log, so the measurement can tell a conflict that was fenced
 * on its merits apart from one that went unclassified because the feature was
 * not switched on properly.
 */
const char *
DBBlueReadSetUnusableReason(void)
{
	DBBlueReadSet *rs = DBBlueReadSetCurrent;

	if (rs == NULL)
	{
		if (!db_blue_rr_merge_track)
			return "tracking-disabled";
		if (XactIsoLevel != XACT_REPEATABLE_READ)
			return "not-repeatable-read";
		return "no-read-set";
	}

	if (rs->tainted)
		return rs->taint_reason != NULL ? rs->taint_reason : "tainted";

	return NULL;
}

/*
 * DBBlueTaintReadSet
 *
 * Declare the transaction un-mergeable.  Called whenever a read cannot be
 * attributed or a cap is exceeded.  Losing the optimization for a transaction
 * is fine; losing soundness is not.
 */
void
DBBlueTaintReadSet(const char *reason)
{
	DBBlueReadSet *rs = DBBlueReadSetCurrent;

	if (rs == NULL || rs->tainted)
		return;

	rs->tainted = true;
	rs->taint_reason = reason;

	if (db_blue_rr_merge_log)
		ereport(LOG,
				(errmsg("dbblue_rr_merge: read set tainted: %s", reason)));
}

/*
 * DBBlueNoteRowReadImpl
 *
 * Record one row read.  Out of line; callers go through the inline
 * DBBlueNoteRowRead().
 */
void
DBBlueNoteRowReadImpl(Oid relid, ItemPointer tid)
{
	DBBlueReadSet *rs = DBBlueReadSetCurrent;
	DBBlueRowKey key;

	Assert(rs != NULL);

	if (rs->tainted || DBBlueTrackingSuppressed > 0)
		return;

	/*
	 * Catalog reads cannot be an input to a user-visible computed value in
	 * the sense we care about, and recording them would only add noise (and
	 * false positives) to the filter.
	 */
	if (relid < FirstNormalObjectId)
		return;

	if (!ItemPointerIsValid(tid))
		return;

	if (rs->nrows >= db_blue_rr_merge_max_rows)
	{
		DBBlueTaintReadSet("row-cap-exceeded");
		return;
	}

	dbblue_row_key(&key, relid, tid);
	bloom_add_element(rs->rows, (unsigned char *) &key, sizeof(key));
	rs->nrows++;
}

/*
 * DBBlueNoteRelationRead
 *
 * Record the columns one statement reads of one relation.  `selectedCols` is
 * the query's RTEPermissionInfo.selectedCols, i.e. every column referenced
 * anywhere in the rewritten query, after view expansion and RLS.
 *
 * Note that an empty set is a perfectly ordinary answer, not a missing one:
 * "UPDATE t SET c = 'lit'" with no WHERE references no column of t at all, and
 * PostgreSQL spells the empty Bitmapset as a NULL pointer.  Callers that
 * genuinely cannot attribute a read must say so with DBBlueNoteAllColsRead().
 *
 * Bit numbering follows selectedCols: offset by
 * FirstLowInvalidHeapAttributeNumber, which is also what ExecGetUpdatedCols()
 * returns, so read and write sets can be compared without conversion.
 */
void
DBBlueNoteRelationRead(Oid relid, const Bitmapset *selectedCols)
{
	DBBlueReadSet *rs = DBBlueReadSetCurrent;
	DBBlueColEntry *entry;
	MemoryContext oldcxt;
	bool		found;

	if (rs == NULL || rs->tainted)
		return;
	if (relid < FirstNormalObjectId)
		return;

	entry = (DBBlueColEntry *) hash_search(rs->cols, &relid,
										   HASH_ENTER, &found);
	if (!found)
	{
		entry->cols = NULL;
		entry->all = false;
	}
	if (entry->all)
		return;

	/*
	 * A whole-row reference reads everything, present and future, so don't
	 * try to enumerate it.
	 */
	if (bms_is_member(InvalidAttrNumber - FirstLowInvalidHeapAttributeNumber,
					  selectedCols))
	{
		entry->all = true;
		entry->cols = NULL;
		return;
	}

	if (bms_is_empty(selectedCols))
		return;					/* nothing read of this relation */

	oldcxt = MemoryContextSwitchTo(TopTransactionContext);
	entry->cols = bms_add_members(entry->cols, selectedCols);
	MemoryContextSwitchTo(oldcxt);
}

/*
 * DBBlueNoteAllColsRead
 *
 * Record that we must assume every column of `relid` was read, because the
 * read could not be attributed to specific columns.  Sticky for the rest of
 * the transaction; the only effect is extra aborts.
 */
void
DBBlueNoteAllColsRead(Oid relid)
{
	DBBlueReadSet *rs = DBBlueReadSetCurrent;
	DBBlueColEntry *entry;
	bool		found;

	if (rs == NULL || rs->tainted)
		return;
	if (relid < FirstNormalObjectId)
		return;

	entry = (DBBlueColEntry *) hash_search(rs->cols, &relid,
										   HASH_ENTER, &found);
	if (!found)
		entry->cols = NULL;
	entry->all = true;
}

/*
 * DBBlueRowWasRead
 *
 * "Possibly read" counts as read.  Returns true when we have no usable read
 * set, so callers fall back to today's behavior.
 */
bool
DBBlueRowWasRead(Oid relid, ItemPointer tid)
{
	DBBlueReadSet *rs = DBBlueReadSetCurrent;
	DBBlueRowKey key;

	if (rs == NULL || rs->tainted)
		return true;

	dbblue_row_key(&key, relid, tid);
	return !bloom_lacks_element(rs->rows, (unsigned char *) &key,
								sizeof(key));
}

/*
 * DBBlueGetReadCols
 *
 * Columns of `relid` read by this transaction.  Sets *allCols if we must
 * assume every column was read; the returned set is then meaningless.
 */
Bitmapset *
DBBlueGetReadCols(Oid relid, bool *allCols)
{
	DBBlueReadSet *rs = DBBlueReadSetCurrent;
	DBBlueColEntry *entry;

	*allCols = false;

	if (rs == NULL || rs->tainted)
	{
		*allCols = true;
		return NULL;
	}

	entry = (DBBlueColEntry *) hash_search(rs->cols, &relid, HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	*allCols = entry->all;
	return entry->cols;
}


/*
 * dbblue_fetch_latest_version
 *
 * Walk the update chain from `tid` to the newest committed version and fetch
 * it into `slot`.
 *
 * Deliberately lock-free and write-free.  table_tuple_lock() with
 * TUPLE_LOCK_FLAG_FIND_LAST_VERSION would be the natural way to do this --
 * and is what the merge itself should use, since it handles HOT chains,
 * aborted intermediates and key-share lockers authoritatively -- but it
 * stamps xmax onto the newest version and writes WAL.  On this path we are
 * about to abort, so doing that would dirty pages and possibly block other
 * backends purely for the sake of measurement.
 *
 * Returns false if the row was deleted, moved to another partition, or the
 * chain could not be followed; the caller then treats the conflict as
 * un-mergeable.
 */
static bool
dbblue_fetch_latest_version(Relation rel, ItemPointer tid,
							TupleTableSlot *slot)
{
	ItemPointerData ctid;
	int			hops;

	ItemPointerCopy(tid, &ctid);

	for (hops = 0; hops < DBBLUE_MAX_CHAIN_HOPS; hops++)
	{
		HeapTuple	tup;
		TransactionId xmax;

		if (!table_tuple_fetch_row_version(rel, &ctid, SnapshotAny, slot))
			return false;

		tup = ExecFetchSlotHeapTuple(slot, false, NULL);

		/* A row that left the partition has no version to merge onto */
		if (HeapTupleHeaderIndicatesMovedPartitions(tup->t_data))
			return false;

		/* No updater at all, or only lockers: this is the newest version */
		if ((tup->t_data->t_infomask & HEAP_XMAX_INVALID) != 0 ||
			HeapTupleHeaderIsOnlyLocked(tup->t_data) ||
			ItemPointerEquals(&ctid, &tup->t_data->t_ctid))
			return true;

		/*
		 * Only follow the chain into a version whose creator committed; an
		 * in-progress or aborted successor is not the newest *committed*
		 * version.
		 */
		xmax = HeapTupleHeaderGetUpdateXid(tup->t_data);
		if (!TransactionIdIsValid(xmax) || !TransactionIdDidCommit(xmax))
			return true;

		ItemPointerCopy(&tup->t_data->t_ctid, &ctid);
	}

	return false;
}

/*
 * dbblue_changed_cols
 *
 * Columns whose value differs between our snapshot's version of the row and
 * the newest version.  This is the "C" of the merge decision, and it must be
 * computed against the *snapshot* version rather than an intermediate
 * successor, so that with several concurrent updaters C is the union of every
 * change since our snapshot.
 *
 * datumIsEqual() compares the stored representation without detoasting, so a
 * value that was rewritten into a different physical form (recompressed,
 * pushed out of line) reports as changed even when it is logically identical.
 * That is the safe direction: it can only produce an unnecessary abort, never
 * a missed change.
 *
 * Columns named in `ignorecols` are collected into *ignored instead of into the
 * result, so the caller can report what was suppressed.  See
 * db_blue.rr_merge_ignore_cols.
 */
static Bitmapset *
dbblue_changed_cols(Relation rel, HeapTuple snaptup, TupleTableSlot *newslot,
					List *ignorecols, Bitmapset **ignored)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Bitmapset  *changed = NULL;
	int			attnum;

	*ignored = NULL;
	slot_getallattrs(newslot);

	for (attnum = 1; attnum <= desc->natts; attnum++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, attnum - 1);
		Datum		oldval;
		Datum		newval;
		bool		oldnull;
		bool		newnull;
		bool		differs;
		int			bit = attnum - FirstLowInvalidHeapAttributeNumber;

		if (att->attisdropped)
			continue;

		oldval = heap_getattr(snaptup, attnum, desc, &oldnull);
		newval = newslot->tts_values[attnum - 1];
		newnull = newslot->tts_isnull[attnum - 1];

		if (oldnull != newnull)
			differs = true;
		else if (oldnull)
			differs = false;
		else
			differs = !datumIsEqual(oldval, newval, att->attbyval, att->attlen);

		if (!differs)
			continue;

		if (dbblue_name_in_list(NameStr(att->attname), ignorecols))
			*ignored = bms_add_member(*ignored, bit);
		else
			changed = bms_add_member(changed, bit);
	}

	return changed;
}

/*
 * Parse db_blue.rr_merge_ignore_cols into a list of lowercased names.
 *
 * Done per conflict rather than in an assign hook: conflicts are rare, and the
 * result must live no longer than the caller.  A malformed setting ignores
 * nothing, which is the safe direction.
 */
static List *
dbblue_parse_ignore_cols(void)
{
	char	   *rawstring;
	List	   *namelist = NIL;

	if (db_blue_rr_merge_ignore_cols == NULL ||
		db_blue_rr_merge_ignore_cols[0] == '\0')
		return NIL;

	/* SplitIdentifierString scribbles on its input and points into it */
	rawstring = pstrdup(db_blue_rr_merge_ignore_cols);
	if (!SplitIdentifierString(rawstring, ',', &namelist))
		return NIL;

	return namelist;
}

/*
 * Case-insensitive membership test against a SplitIdentifierString() result,
 * whose entries are already downcased.
 */
static bool
dbblue_name_in_list(const char *name, List *namelist)
{
	ListCell   *lc;

	foreach(lc, namelist)
	{
		if (pg_strcasecmp(name, (const char *) lfirst(lc)) == 0)
			return true;
	}

	return false;
}

/*
 * DBBlueAnalyzeUpdateConflict
 *
 * Decide whether a TM_Updated conflict on `snaptid` is a false conflict that
 * could have been merged, and log the decision.  `snaptup` is our snapshot's
 * version of the row and `writecols` the columns this UPDATE assigns.
 *
 * Returns true if the conflict is mergeable.  The caller does not act on the
 * answer yet -- ExecUpdate() aborts either way -- but the return value is
 * what the merge gate will branch on once it exists.
 */
bool
DBBlueAnalyzeUpdateConflict(Relation rel, ItemPointer snaptid,
							HeapTuple snaptup, const Bitmapset *writecols)
{
	TupleTableSlot *latestslot;
	Bitmapset  *changed = NULL;
	Bitmapset  *ignored = NULL;
	Bitmapset  *readcols;
	List	   *ignorecols;
	bool		allread;
	bool		rowWasRead;
	bool		gotlatest;
	bool		mergeable;
	bool		wcolsOnly;
	StringInfoData buf;

	/* Only heap has an update chain we know how to walk */
	if (rel->rd_rel->relam != HEAP_TABLE_AM_OID)
	{
		DBBlueLogSkippedConflict(rel, snaptid, "not-heap");
		return false;
	}

	/*
	 * Ask the read set first: the fetches below go through the same table AM
	 * entry points that feed it, and would otherwise record this very row.
	 * The suppression counter covers the rest.
	 */
	rowWasRead = DBBlueRowWasRead(RelationGetRelid(rel), snaptid);
	readcols = DBBlueGetReadCols(RelationGetRelid(rel), &allread);

	latestslot = table_slot_create(rel, NULL);
	ignorecols = dbblue_parse_ignore_cols();

	DBBlueTrackingSuppressed++;
	PG_TRY();
	{
		gotlatest = dbblue_fetch_latest_version(rel, snaptid, latestslot);
		if (gotlatest)
			changed = dbblue_changed_cols(rel, snaptup, latestslot,
										  ignorecols, &ignored);
	}
	PG_FINALLY();
	{
		DBBlueTrackingSuppressed--;
	}
	PG_END_TRY();

	if (!gotlatest)
	{
		ExecDropSingleTupleTableSlot(latestslot);
		DBBlueLogSkippedConflict(rel, snaptid, "no-newest-version");
		return false;
	}

	/*
	 * The decision.  A row we never read cannot have contributed to what we
	 * are writing, whatever changed on it.
	 */
	if (!rowWasRead)
		mergeable = true;
	else if (allread)
		mergeable = false;
	else
		mergeable = !bms_overlap(changed, readcols) &&
			!bms_overlap(changed, writecols);

	/*
	 * Would the write-columns-only variant have merged this?  That variant is
	 * UNSOUND and must never gate anything -- it is the counterexample in the
	 * design doc, where a write predicated on a column the other transaction
	 * changed slips through.  It is reported only to size the ceiling: it is
	 * what a perfectly precise notion of "which reads actually fed this value"
	 * could at best achieve, so if this is rarely true the whole approach is
	 * not worth pursuing.
	 */
	wcolsOnly = !bms_overlap(changed, writecols);

	if (db_blue_rr_merge_log)
	{
		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "dbblue_rr_merge: decision=%s wcols_only=%c rel=\"%s\" ctid=(%u,%u) row_read=%c",
						 mergeable ? "mergeable" : "abort",
						 wcolsOnly ? 't' : 'f',
						 RelationGetRelationName(rel),
						 ItemPointerGetBlockNumber(snaptid),
						 ItemPointerGetOffsetNumber(snaptid),
						 rowWasRead ? 't' : 'f');

		appendStringInfoString(&buf, " changed={");
		dbblue_append_cols(&buf, rel, changed, false);
		appendStringInfoString(&buf, "} read={");
		dbblue_append_cols(&buf, rel, readcols, allread);
		appendStringInfoString(&buf, "} written={");
		dbblue_append_cols(&buf, rel, writecols, false);
		appendStringInfoChar(&buf, '}');

		/* only mention suppression when it actually suppressed something */
		if (!bms_is_empty(ignored))
		{
			appendStringInfoString(&buf, " ignored={");
			dbblue_append_cols(&buf, rel, ignored, false);
			appendStringInfoChar(&buf, '}');
		}

		ereport(LOG, (errmsg_internal("%s", buf.data)));
		pfree(buf.data);
	}

	ExecDropSingleTupleTableSlot(latestslot);

	return mergeable;
}

/*
 * Log a conflict we declined to analyze, so that the measurement can account
 * for every 40001 rather than only the ones it understood.
 */
void
DBBlueLogSkippedConflict(Relation rel, ItemPointer tid, const char *reason)
{
	if (!db_blue_rr_merge_log)
		return;

	ereport(LOG,
			(errmsg_internal("dbblue_rr_merge: decision=skipped reason=%s rel=\"%s\" ctid=(%u,%u)",
							 reason, RelationGetRelationName(rel),
							 ItemPointerGetBlockNumber(tid),
							 ItemPointerGetOffsetNumber(tid))));
}

/*
 * Render a set of attnums as a comma-separated list of column names.
 */
static void
dbblue_append_cols(StringInfo buf, Relation rel, const Bitmapset *cols,
				   bool all)
{
	TupleDesc	desc = RelationGetDescr(rel);
	bool		first = true;
	int			bit = -1;

	if (all)
	{
		appendStringInfoString(buf, "*");
		return;
	}

	while ((bit = bms_next_member(cols, bit)) >= 0)
	{
		AttrNumber	attnum = bit + FirstLowInvalidHeapAttributeNumber;

		if (!first)
			appendStringInfoChar(buf, ',');
		first = false;

		if (attnum > 0 && attnum <= desc->natts)
			appendStringInfoString(buf,
								   NameStr(TupleDescAttr(desc, attnum - 1)->attname));
		else
			appendStringInfo(buf, "attnum%d", attnum);
	}
}
