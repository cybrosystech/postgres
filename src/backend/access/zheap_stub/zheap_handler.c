/*-------------------------------------------------------------------------
 *
 * zheap_handler.c
 *	  Stub table access method "zheap" for the in-progress zheap port.
 *
 * The real zheap source (~37 kLOC of access-method + undo-log code,
 * originally written for PostgreSQL 12) is staged unmodified under
 *   src/backend/access/zheap/
 *   src/backend/access/undo/
 *   src/backend/storage/smgr/undofile.c
 * but is NOT compiled — the API surface it depends on has shifted
 * across seven PG releases (RelFileNode → RelFileNumber, ShmemVariableCache
 * → TransamVariables, TableAmRoutine struct growth, etc.) and several
 * core hooks it adds (LWTRANCHE_UNDOLOG, RM_UNDOLOG_ID,
 * ONCOMMIT_TEMP_DISCARD, PROC_HDR.oldestXidWithEpochHavingUndo) were
 * never merged upstream.  See ZHEAP_PORT_NOTES.md at the repo root for
 * the full port plan.
 *
 * This stub exists so that the access method NAME is visible in pg_am
 * and so that CREATE TABLE ... USING zheap succeeds (using heap's
 * physical storage callbacks, which are semantically neutral
 * file-creation / size / vacuum operations).  Any DATA-modifying
 * callback — insert, update, delete, multi-insert, speculative insert,
 * lock — raises a clear "not yet implemented" error so users cannot
 * mistake the stub for working zheap.
 *
 * Once the real zheap modules are ported (Phase 1 through Phase N in
 * ZHEAP_PORT_NOTES.md §11), this stub file is replaced by the ported
 * src/backend/access/zheap/zheapam_handler.c.
 *
 * Portions Copyright (c) 2026, dbblue / Cybrosys Technologies.
 *
 * IDENTIFICATION
 *	  src/backend/access/zheap_stub/zheap_handler.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tableam.h"
#include "fmgr.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(zheap_tableam_handler);

#define ZHEAP_STUB_HINT \
	"zheap is a staged-but-not-yet-ported table access method. " \
	"Storage-layer callbacks (file creation, size estimation, vacuum) " \
	"are wired to heap, but row-modifying operations are not yet " \
	"implemented.  See ZHEAP_PORT_NOTES.md."

/*
 * Stub row-modifying callbacks.  Signatures must match TableAmRoutine
 * exactly (see src/include/access/tableam.h) so the static initializer
 * type-checks.  All raise ERRCODE_FEATURE_NOT_SUPPORTED.
 */

static void
zheap_stub_tuple_insert(Relation rel, TupleTableSlot *slot,
						CommandId cid, uint32 options,
						BulkInsertStateData *bistate)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("zheap_tuple_insert is not yet implemented"),
			 errhint("%s", ZHEAP_STUB_HINT)));
}

static void
zheap_stub_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
									CommandId cid, uint32 options,
									BulkInsertStateData *bistate,
									uint32 specToken)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("zheap_tuple_insert_speculative is not yet implemented"),
			 errhint("%s", ZHEAP_STUB_HINT)));
}

static void
zheap_stub_tuple_complete_speculative(Relation rel, TupleTableSlot *slot,
									  uint32 specToken, bool succeeded)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("zheap_tuple_complete_speculative is not yet implemented"),
			 errhint("%s", ZHEAP_STUB_HINT)));
}

static void
zheap_stub_multi_insert(Relation rel, TupleTableSlot **slots, int nslots,
						CommandId cid, uint32 options,
						BulkInsertStateData *bistate)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("zheap multi_insert is not yet implemented"),
			 errhint("%s", ZHEAP_STUB_HINT)));
}

static TM_Result
zheap_stub_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
						uint32 options, Snapshot snapshot, Snapshot crosscheck,
						bool wait, TM_FailureData *tmfd)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("zheap_tuple_delete is not yet implemented"),
			 errhint("%s", ZHEAP_STUB_HINT)));
	return TM_Ok;				/* unreachable */
}

static TM_Result
zheap_stub_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
						CommandId cid, uint32 options, Snapshot snapshot,
						Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
						LockTupleMode *lockmode,
						TU_UpdateIndexes *update_indexes)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("zheap_tuple_update is not yet implemented"),
			 errhint("%s", ZHEAP_STUB_HINT)));
	return TM_Ok;				/* unreachable */
}

static TM_Result
zheap_stub_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
					  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					  LockWaitPolicy wait_policy, uint8 flags,
					  TM_FailureData *tmfd)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("zheap_tuple_lock is not yet implemented"),
			 errhint("%s", ZHEAP_STUB_HINT)));
	return TM_Ok;				/* unreachable */
}

static TransactionId
zheap_stub_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("zheap index_delete_tuples is not yet implemented"),
			 errhint("%s", ZHEAP_STUB_HINT)));
	return InvalidTransactionId;	/* unreachable */
}

static void
zheap_stub_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
									 Relation OldIndex, bool use_sort,
									 TransactionId OldestXmin,
									 Snapshot snapshot,
									 TransactionId *xid_cutoff,
									 MultiXactId *multi_cutoff,
									 double *num_tuples,
									 double *tups_vacuumed,
									 double *tups_recently_dead)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("CLUSTER on a zheap table is not yet implemented"),
			 errhint("%s", ZHEAP_STUB_HINT)));
}

static double
zheap_stub_index_build_range_scan(Relation table_rel, Relation index_rel,
								  IndexInfo *index_info, bool allow_sync,
								  bool anyvisible, bool progress,
								  BlockNumber start_blockno,
								  BlockNumber numblocks,
								  IndexBuildCallback callback,
								  void *callback_state,
								  TableScanDesc scan)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("building an index on a zheap table is not yet implemented"),
			 errhint("%s", ZHEAP_STUB_HINT)));
	return 0;					/* unreachable */
}

static void
zheap_stub_index_validate_scan(Relation table_rel, Relation index_rel,
							   IndexInfo *index_info, Snapshot snapshot,
							   ValidateIndexState *state)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("validating an index on a zheap table is not yet implemented"),
			 errhint("%s", ZHEAP_STUB_HINT)));
}

/*
 * zheap_tableam_handler
 *
 * Returns a TableAmRoutine that is a copy of heap's, with every
 * row-modifying / index-build callback redirected to a stub that
 * raises ERRCODE_FEATURE_NOT_SUPPORTED.
 *
 * Read-side callbacks (scan_begin, slot_callbacks, parallel scan, bitmap
 * scan, analyze, relation_size, relation_needs_toast_table, etc.) keep
 * heap's implementations because they are physical-storage operations
 * with no zheap-specific semantics.  This makes CREATE TABLE ... USING
 * zheap and SELECT-on-empty-table behave correctly so the AM is
 * discoverable; any attempt to write to a zheap table fails fast with
 * a clear message.
 */
Datum
zheap_tableam_handler(PG_FUNCTION_ARGS)
{
	static TableAmRoutine zheap_methods;
	static bool initialized = false;

	if (!initialized)
	{
		const TableAmRoutine *heap = GetHeapamTableAmRoutine();

		memcpy(&zheap_methods, heap, sizeof(TableAmRoutine));

		zheap_methods.tuple_insert = zheap_stub_tuple_insert;
		zheap_methods.tuple_insert_speculative = zheap_stub_tuple_insert_speculative;
		zheap_methods.tuple_complete_speculative = zheap_stub_tuple_complete_speculative;
		zheap_methods.multi_insert = zheap_stub_multi_insert;
		zheap_methods.tuple_delete = zheap_stub_tuple_delete;
		zheap_methods.tuple_update = zheap_stub_tuple_update;
		zheap_methods.tuple_lock = zheap_stub_tuple_lock;
		zheap_methods.index_delete_tuples = zheap_stub_index_delete_tuples;
		zheap_methods.relation_copy_for_cluster = zheap_stub_relation_copy_for_cluster;
		zheap_methods.index_build_range_scan = zheap_stub_index_build_range_scan;
		zheap_methods.index_validate_scan = zheap_stub_index_validate_scan;

		initialized = true;
	}

	PG_RETURN_POINTER(&zheap_methods);
}
