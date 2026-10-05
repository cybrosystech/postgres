/*-------------------------------------------------------------------------
 *
 * execIndexing.c
 *	  routines for inserting index tuples and enforcing unique and
 *	  exclusion constraints.
 *
 * ExecInsertIndexTuples() is the main entry point.  It's called after
 * inserting a tuple to the heap, and it inserts corresponding index tuples
 * into all indexes.  At the same time, it enforces any unique and
 * exclusion constraints:
 *
 * Unique Indexes
 * --------------
 *
 * Enforcing a unique constraint is straightforward.  When the index AM
 * inserts the tuple to the index, it also checks that there are no
 * conflicting tuples in the index already.  It does so atomically, so that
 * even if two backends try to insert the same key concurrently, only one
 * of them will succeed.  All the logic to ensure atomicity, and to wait
 * for in-progress transactions to finish, is handled by the index AM.
 *
 * If a unique constraint is deferred, we request the index AM to not
 * throw an error if a conflict is found.  Instead, we make note that there
 * was a conflict and return the list of indexes with conflicts to the
 * caller.  The caller must re-check them later, by calling index_insert()
 * with the UNIQUE_CHECK_EXISTING option.
 *
 * Exclusion Constraints
 * ---------------------
 *
 * Exclusion constraints are different from unique indexes in that when the
 * tuple is inserted to the index, the index AM does not check for
 * duplicate keys at the same time.  After the insertion, we perform a
 * separate scan on the index to check for conflicting tuples, and if one
 * is found, we throw an error and the transaction is aborted.  If the
 * conflicting tuple's inserter or deleter is in-progress, we wait for it
 * to finish first.
 *
 * There is a chance of deadlock, if two backends insert a tuple at the
 * same time, and then perform the scan to check for conflicts.  They will
 * find each other's tuple, and both try to wait for each other.  The
 * deadlock detector will detect that, and abort one of the transactions.
 * That's fairly harmless, as one of them was bound to abort with a
 * "duplicate key error" anyway, although you get a different error
 * message.
 *
 * If an exclusion constraint is deferred, we still perform the conflict
 * checking scan immediately after inserting the index tuple.  But instead
 * of throwing an error if a conflict is found, we return that information
 * to the caller.  The caller must re-check them later by calling
 * check_exclusion_constraint().
 *
 * Speculative insertion
 * ---------------------
 *
 * Speculative insertion is a two-phase mechanism used to implement
 * INSERT ... ON CONFLICT.  The tuple is first inserted into the heap
 * and the indexes are updated as usual, but if a constraint is violated,
 * we can still back out of the insertion without aborting the whole
 * transaction.  In an INSERT ... ON CONFLICT statement, if a conflict is
 * detected, the inserted tuple is backed out and the ON CONFLICT action is
 * executed instead.
 *
 * Insertion to a unique index works as usual: the index AM checks for
 * duplicate keys atomically with the insertion.  But instead of throwing
 * an error on a conflict, the speculatively inserted heap tuple is backed
 * out.
 *
 * Exclusion constraints are slightly more complicated.  As mentioned
 * earlier, there is a risk of deadlock when two backends insert the same
 * key concurrently.  That was not a problem for regular insertions, when
 * one of the transactions has to be aborted anyway, but with a speculative
 * insertion we cannot let a deadlock happen, because we only want to back
 * out the speculatively inserted tuple on conflict, not abort the whole
 * transaction.
 *
 * When a backend detects that the speculative insertion conflicts with
 * another in-progress tuple, it has two options:
 *
 * 1. back out the speculatively inserted tuple, then wait for the other
 *	  transaction, and retry. Or,
 * 2. wait for the other transaction, with the speculatively inserted tuple
 *	  still in place.
 *
 * If two backends insert at the same time, and both try to wait for each
 * other, they will deadlock.  So option 2 is not acceptable.  Option 1
 * avoids the deadlock, but it is prone to a livelock instead.  Both
 * transactions will wake up immediately as the other transaction backs
 * out.  Then they both retry, and conflict with each other again, lather,
 * rinse, repeat.
 *
 * To avoid the livelock, one of the backends must back out first, and then
 * wait, while the other one waits without backing out.  It doesn't matter
 * which one backs out, so we employ an arbitrary rule that the transaction
 * with the higher XID backs out.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execIndexing.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/tupconvert.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/partition.h"
#include "catalog/pg_index.h"
#include "catalog/pg_index_d.h"
#include "utils/fmgroids.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "partitioning/partbounds.h"
#include "partitioning/partdesc.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/injection_point.h"
#include "utils/lsyscache.h"
#include "utils/multirangetypes.h"
#include "utils/partcache.h"
#include "utils/rangetypes.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"

/* waitMode argument to check_exclusion_or_unique_constraint() */
typedef enum
{
	CEOUC_WAIT,
	CEOUC_NOWAIT,
	CEOUC_LIVELOCK_PREVENTING_WAIT,
} CEOUC_WAIT_MODE;

static bool check_exclusion_or_unique_constraint(Relation heap, Relation index,
												 IndexInfo *indexInfo,
												 const ItemPointerData *tupleid,
												 const Datum *values, const bool *isnull,
												 EState *estate, bool newIndex,
												 CEOUC_WAIT_MODE waitMode,
												 bool violationOK,
												 ItemPointer conflictTid);

static bool index_recheck_constraint(Relation index, const Oid *constr_procs,
									 const Datum *existing_values, const bool *existing_isnull,
									 const Datum *new_values);
static bool index_unchanged_by_update(ResultRelInfo *resultRelInfo,
									  EState *estate, IndexInfo *indexInfo,
									  Relation indexRelation);
static bool index_expression_changed_walker(Node *node,
											Bitmapset *allUpdatedCols);
static void ExecWithoutOverlapsNotEmpty(Relation rel, NameData attname, Datum attval,
										char typtype, Oid atttypid);
static bool global_index_keys_equal(Relation gidx, IndexInfo *indexInfo,
									int nkeys, const Datum *values1, const bool *isnull1,
									const Datum *values2, const bool *isnull2);

/* ----------------------------------------------------------------
 *		ExecGlobalIndexRoutePartition
 *
 *		Map an entry of a global partition index to the OID of the leaf
 *		partition that owns its heap TID, using the partition key value(s)
 *		stored in the index tuple (a global index always contains the
 *		partition key columns).  Mirrors get_partition_for_tuple().  Returns
 *		InvalidOid if no partition accepts the value.  With
 *		'include_detached', a partition being detached concurrently still
 *		counts as a partition of the parent (its rows are still indexed).
 * ----------------------------------------------------------------
 */
Oid
ExecGlobalIndexRoutePartition(Relation parentRel, Relation gidx,
							  IndexTuple itup, bool include_detached)
{
	PartitionDesc partdesc = RelationGetPartitionDesc(parentRel,
													  !include_detached);
	int			part_index;

	part_index = ExecGlobalIndexRouteToIndex(RelationGetPartitionKey(parentRel),
											 partdesc, gidx, itup);

	return part_index >= 0 ? partdesc->oids[part_index] : InvalidOid;
}

/*
 * ExecGlobalIndexRouteToIndex
 *		Like ExecGlobalIndexRoutePartition, but for a given partition
 *		descriptor, returning the partition's index in it (or -1).
 */
int
ExecGlobalIndexRouteToIndex(PartitionKey key, PartitionDesc partdesc,
							Relation gidx, IndexTuple itup)
{
	PartitionBoundInfo boundinfo = partdesc->boundinfo;
	TupleDesc	itupdesc = RelationGetDescr(gidx);
	Datum		values[PARTITION_MAX_KEYS];
	bool		isnull[PARTITION_MAX_KEYS];
	int			part_index = -1;

	if (boundinfo == NULL)
		return -1;

	for (int i = 0; i < key->partnatts; i++)
	{
		int			idxatt = 0;

		for (int j = 0; j < gidx->rd_index->indnatts; j++)
		{
			if (key->partattrs[i] != 0 &&
				gidx->rd_index->indkey.values[j] == key->partattrs[i])
			{
				idxatt = j + 1;
				break;
			}
		}
		if (idxatt == 0)
			elog(ERROR, "global index \"%s\" does not contain the partition key",
				 RelationGetRelationName(gidx));

		values[i] = index_getattr(itup, idxatt, itupdesc, &isnull[i]);
	}

	switch (key->strategy)
	{
		case PARTITION_STRATEGY_HASH:
			{
				uint64		rowHash;

				rowHash = compute_partition_hash_value(key->partnatts,
													   key->partsupfunc,
													   key->partcollation,
													   values, isnull);
				part_index = boundinfo->indexes[rowHash % boundinfo->nindexes];
			}
			break;

		case PARTITION_STRATEGY_LIST:
			if (isnull[0])
			{
				if (partition_bound_accepts_nulls(boundinfo))
					part_index = boundinfo->null_index;
			}
			else
			{
				bool		equal = false;
				int			bound_offset;

				bound_offset = partition_list_bsearch(key->partsupfunc,
													  key->partcollation,
													  boundinfo,
													  values[0], &equal);
				if (bound_offset >= 0 && equal)
					part_index = boundinfo->indexes[bound_offset];
			}
			break;

		case PARTITION_STRATEGY_RANGE:
			{
				bool		equal = false,
							has_null = false;

				/* No range includes NULL; leave that to the default partition */
				for (int i = 0; i < key->partnatts; i++)
				{
					if (isnull[i])
					{
						has_null = true;
						break;
					}
				}
				if (!has_null)
				{
					int			bound_offset;

					bound_offset = partition_range_datum_bsearch(key->partsupfunc,
																 key->partcollation,
																 boundinfo,
																 key->partnatts,
																 values, &equal);
					part_index = boundinfo->indexes[bound_offset + 1];
				}
			}
			break;
	}

	if (part_index < 0)
		part_index = boundinfo->default_index;

	return part_index;
}

/*
 * global_index_keys_equal
 *		Compare the first nkeys columns of two global index entries,
 *		treating two NULLs as equal (only reached with NULLS NOT DISTINCT).
 */
static bool
global_index_keys_equal(Relation gidx, IndexInfo *indexInfo, int nkeys,
						const Datum *values1, const bool *isnull1,
						const Datum *values2, const bool *isnull2)
{
	for (int i = 0; i < nkeys; i++)
	{
		if (isnull1[i] || isnull2[i])
		{
			if (isnull1[i] && isnull2[i])
				continue;
			return false;
		}
		if (!DatumGetBool(OidFunctionCall2Coll(indexInfo->ii_UniqueProcs[i],
											   gidx->rd_indcollation[i],
											   values1[i], values2[i])))
			return false;
	}
	return true;
}

/* An entry of a global index found by ExecCheckGlobalIndexUnique() */
typedef struct GlobalIndexCandidate
{
	Oid			partOid;
	ItemPointerData tid;
} GlobalIndexCandidate;

/* ----------------------------------------------------------------
 *		global_index_find_conflict
 *
 *		Look for a row, other than the one identified by 'heapRel' and
 *		'tupleid' (may be NULL), whose key in UNIQUE global partition index
 *		'gidx' equals 'values'/'isnull'.  If one is found, return true and
 *		its partition and TID in *conflictPart and *conflictTid.
 *
 *		With 'wait', a matching row whose inserting or deleting transaction
 *		is still in progress is waited for, and the search restarted, so the
 *		answer is conclusive.  Without it, such a row counts as a conflict
 *		right away: that is how a speculative insertion (INSERT ... ON
 *		CONFLICT) detects a possible conflict without waiting while it holds
 *		its own speculative insertion lock.
 *
 *		The btree AM cannot do this itself: its uniqueness check fetches the
 *		conflicting TIDs from the one heap it is given, whereas a global index
 *		holds TIDs of many partition heaps.  So the entry is inserted without
 *		a check (UNIQUE_CHECK_NO) and then, as for an exclusion constraint, we
 *		scan the index for other entries with the same key, route each one to
 *		its partition through the stored partition key, and look at the heap
 *		tuple with a dirty snapshot.  A live duplicate raises a unique
 *		violation; one whose inserting or deleting transaction is still in
 *		progress is waited for, and the scan restarted.  Two sessions
 *		inserting the same key concurrently thus wait for each other, and one
 *		of them fails (possibly with a deadlock error).
 *
 *		Entries can be stale (pointing at a dead or recycled heap slot), so
 *		every heap tuple found is rechecked against the key and predicate.
 *
 *		'heapRel' and 'tupleid' identify the new tuple itself, which is
 *		skipped.  'newIndex' selects the error wording used while building
 *		the index.  'indexInfo' must come from BuildGlobalIndexInfo(gidx,
 *		heapRel), i.e. be mapped to heapRel's column layout.
 * ----------------------------------------------------------------
 */
static bool
global_index_find_conflict(Relation gidx, IndexInfo *indexInfo,
						   Relation heapRel, const ItemPointerData *tupleid,
						   const Datum *values, const bool *isnull,
						   EState *estate, bool wait,
						   Oid *conflictPart, ItemPointer conflictTid)
{
	/* uniqueness is on the user's columns, not the routing columns */
	int			nkeys = IndexGlobalNumUserKeys(gidx->rd_index);
	Relation	parentRel;
	ScanKeyData scankeys[INDEX_MAX_KEYS];
	SnapshotData DirtySnapshot;
	IndexScanDesc index_scan;
	ExprContext *econtext;
	TupleTableSlot *save_scantuple;
	ExprState  *predicate = NULL;
	bool		conflict = false;
	List	   *candidates;

	Assert(gidx->rd_index->indglobal && gidx->rd_index->indisunique);
	Assert(indexInfo->ii_UniqueProcs != NULL);

	/* With the default NULLS DISTINCT, a NULL key never conflicts */
	if (!indexInfo->ii_NullsNotDistinct)
	{
		for (int i = 0; i < nkeys; i++)
		{
			if (isnull[i])
				return false;
		}
	}

	for (int i = 0; i < nkeys; i++)
		ScanKeyEntryInitialize(&scankeys[i],
							   isnull[i] ? SK_ISNULL | SK_SEARCHNULL : 0,
							   i + 1,
							   indexInfo->ii_UniqueStrats[i],
							   InvalidOid,
							   gidx->rd_indcollation[i],
							   indexInfo->ii_UniqueProcs[i],
							   values[i]);

	if (indexInfo->ii_Predicate != NIL)
	{
		predicate = indexInfo->ii_PredicateState;
		if (predicate == NULL)
		{
			predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);
			indexInfo->ii_PredicateState = predicate;
		}
	}

	parentRel = table_open(gidx->rd_index->indrelid, AccessShareLock);

	econtext = GetPerTupleExprContext(estate);
	save_scantuple = econtext->ecxt_scantuple;

	InitDirtySnapshot(DirtySnapshot);

retry:

	/*
	 * First collect the other entries with this key, then look at their
	 * rows.  Don't open or lock a sibling partition while the index scan
	 * holds a buffer pin: if the partition is locked by, say, VACUUM FULL,
	 * that command may itself be waiting to clean up the pinned index page,
	 * and the deadlock detector can't see buffer pin waits.
	 *
	 * index_beginscan() needs a heap relation to set up a fetch descriptor;
	 * we discard it and fetch from the owning partition of each entry.
	 */
	candidates = NIL;
	index_scan = index_beginscan(heapRel, gidx, &DirtySnapshot, NULL,
								 nkeys, 0, SO_NONE);
	if (index_scan->xs_heapfetch != NULL)
	{
		table_index_fetch_end(index_scan->xs_heapfetch);
		index_scan->xs_heapfetch = NULL;
	}
	index_scan->xs_want_itup = true;
	index_rescan(index_scan, scankeys, nkeys, NULL, 0);

	while (index_getnext_tid(index_scan, ForwardScanDirection) != NULL)
	{
		GlobalIndexCandidate *cand;
		Oid			partOid;

		CHECK_FOR_INTERRUPTS();

		partOid = ExecGlobalIndexRoutePartition(parentRel, gidx,
												index_scan->xs_itup, true);
		if (!OidIsValid(partOid))
			continue;

		/* Skip the entry of the tuple being checked */
		if (tupleid != NULL &&
			partOid == RelationGetRelid(heapRel) &&
			ItemPointerEquals(&index_scan->xs_heaptid, tupleid))
			continue;

		cand = palloc_object(GlobalIndexCandidate);
		cand->partOid = partOid;
		cand->tid = index_scan->xs_heaptid;
		candidates = lappend(candidates, cand);
	}

	index_endscan(index_scan);

	foreach_ptr(GlobalIndexCandidate, cand, candidates)
	{
		ItemPointerData tid = cand->tid;
		Relation	partRel;
		TupleTableSlot *existing_slot;
		IndexFetchTableData *fetch;
		bool		call_again = false;
		bool		all_dead = false;
		bool		found;
		TransactionId xwait;

		CHECK_FOR_INTERRUPTS();

		partRel = (cand->partOid == RelationGetRelid(heapRel)) ? heapRel :
			table_open(cand->partOid, AccessShareLock);

		/* A stale entry may point past the end of a truncated heap */
		if (ItemPointerGetBlockNumber(&tid) >=
			RelationGetNumberOfBlocks(partRel))
		{
			if (partRel != heapRel)
				table_close(partRel, NoLock);
			continue;
		}

		existing_slot = table_slot_create(partRel, NULL);
		fetch = table_index_fetch_begin(partRel, 0);
		found = table_index_fetch_tuple(fetch, &tid, &DirtySnapshot,
										existing_slot, &call_again, &all_dead);
		table_index_fetch_end(fetch);

		if (found)
		{
			Datum		existing_values[INDEX_MAX_KEYS];
			bool		existing_isnull[INDEX_MAX_KEYS];
			TupleTableSlot *cmp_slot = existing_slot;
			TupleConversionMap *map = NULL;

			/*
			 * indexInfo is mapped to heapRel's column layout; a sibling
			 * partition's may differ, so convert its tuple first.
			 */
			if (partRel != heapRel)
				map = convert_tuples_by_name(RelationGetDescr(partRel),
											 RelationGetDescr(heapRel));
			if (map != NULL)
			{
				cmp_slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRel),
													&TTSOpsVirtual);
				execute_attr_map_slot(map->attrMap, existing_slot, cmp_slot);
			}

			econtext->ecxt_scantuple = cmp_slot;
			if (predicate != NULL && !ExecQual(predicate, econtext))
				found = false;
			else
			{
				FormIndexDatum(indexInfo, cmp_slot, estate,
							   existing_values, existing_isnull);
				found = global_index_keys_equal(gidx, indexInfo, nkeys,
												existing_values, existing_isnull,
												values, isnull);
			}
			econtext->ecxt_scantuple = save_scantuple;
			if (map != NULL)
			{
				ExecDropSingleTupleTableSlot(cmp_slot);
				free_conversion_map(map);
			}
		}
		ExecDropSingleTupleTableSlot(existing_slot);

		if (!found)
		{
			if (partRel != heapRel)
				table_close(partRel, NoLock);
			continue;
		}

		/*
		 * If the conflicting tuple's inserter or deleter is still in
		 * progress, wait for it and start over (unless told not to wait, in
		 * which case it counts as a conflict).
		 */
		xwait = TransactionIdIsValid(DirtySnapshot.xmin) ?
			DirtySnapshot.xmin : DirtySnapshot.xmax;
		if (TransactionIdIsValid(xwait) && wait)
		{
			if (DirtySnapshot.speculativeToken)
				SpeculativeInsertionWait(DirtySnapshot.xmin,
										 DirtySnapshot.speculativeToken);
			else
				XactLockTableWait(xwait, partRel, &tid, XLTW_InsertIndex);
			if (partRel != heapRel)
				table_close(partRel, NoLock);
			list_free_deep(candidates);
			goto retry;
		}

		if (partRel != heapRel)
			table_close(partRel, NoLock);
		conflict = true;
		*conflictPart = cand->partOid;
		*conflictTid = tid;
		break;
	}
	list_free_deep(candidates);

	table_close(parentRel, NoLock);
	return conflict;
}

/* ----------------------------------------------------------------
 *		ExecCheckGlobalIndexUnique
 *
 *		Enforce uniqueness for an entry just inserted into a UNIQUE global
 *		partition index: raise a unique violation if another row has the same
 *		key (see global_index_find_conflict()).  Two sessions inserting the
 *		same key concurrently wait for each other, and one of them fails
 *		(possibly with a deadlock error).
 *
 *		'heapRel' and 'tupleid' identify the new tuple itself, which is
 *		skipped.  'newIndex' selects the error wording used while building
 *		the index.  'indexInfo' must come from BuildGlobalIndexInfo(gidx,
 *		heapRel), i.e. be mapped to heapRel's column layout.
 * ----------------------------------------------------------------
 */
void
ExecCheckGlobalIndexUnique(Relation gidx, IndexInfo *indexInfo,
						   Relation heapRel, const ItemPointerData *tupleid,
						   const Datum *values, const bool *isnull,
						   EState *estate, bool newIndex)
{
	Oid			conflictPart;
	ItemPointerData conflictTid;

	if (global_index_find_conflict(gidx, indexInfo, heapRel, tupleid,
								   values, isnull, estate, true,
								   &conflictPart, &conflictTid))
	{
		Relation	parentRel = table_open(gidx->rd_index->indrelid,
										   AccessShareLock);
		char	   *key_desc = BuildIndexValueDescription(gidx, values, isnull);

		if (newIndex)
			ereport(ERROR,
					(errcode(ERRCODE_UNIQUE_VIOLATION),
					 errmsg("could not create unique index \"%s\"",
							RelationGetRelationName(gidx)),
					 key_desc ?
					 errdetail("Key %s is duplicated.", key_desc) :
					 errdetail("Duplicate keys exist."),
					 errtableconstraint(parentRel,
										RelationGetRelationName(gidx))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_UNIQUE_VIOLATION),
					 errmsg("duplicate key value violates unique constraint \"%s\"",
							RelationGetRelationName(gidx)),
					 key_desc ?
					 errdetail("Key %s already exists.", key_desc) : 0,
					 errtableconstraint(parentRel,
										RelationGetRelationName(gidx))));
	}
}

/* ----------------------------------------------------------------
 *		ExecCheckGlobalIndexConstraints
 *
 *		The ON CONFLICT pre-check for the parent's global indexes, the
 *		counterpart of ExecCheckIndexConstraints() for a partition's own
 *		indexes: check whether the row in 'slot', about to be inserted into
 *		the partition of 'resultRelInfo', conflicts with an existing row in
 *		any partition through a UNIQUE global index that is an arbiter
 *		('arbiterIndexes' NIL means all of them).
 *
 *		Returns true if there is no conflict.  Otherwise returns false and
 *		sets *conflictTid and *conflictPart to the conflicting row, which may
 *		be in a different partition than the one being inserted into.  Rows
 *		of transactions still in progress are waited for, as in
 *		ExecCheckIndexConstraints().
 * ----------------------------------------------------------------
 */
bool
ExecCheckGlobalIndexConstraints(ResultRelInfo *resultRelInfo,
								TupleTableSlot *slot, EState *estate,
								ItemPointer conflictTid, Oid *conflictPart,
								List *arbiterIndexes)
{
	Relation	heapRelation = resultRelInfo->ri_RelationDesc;
	ExprContext *econtext = GetPerTupleExprContext(estate);

	econtext->ecxt_scantuple = slot;

	for (int gi = 0; gi < resultRelInfo->ri_NumGlobalIndices; gi++)
	{
		Relation	gidx = resultRelInfo->ri_GlobalIndexRelationDescs[gi];
		IndexInfo  *ii = resultRelInfo->ri_GlobalIndexRelationInfo[gi];
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];

		if (!gidx->rd_index->indisunique || !ii->ii_ReadyForInserts)
			continue;
		if (arbiterIndexes != NIL &&
			!list_member_oid(arbiterIndexes, RelationGetRelid(gidx)))
			continue;

		/* A row outside a partial index's predicate cannot conflict on it */
		if (ii->ii_Predicate != NIL)
		{
			if (ii->ii_PredicateState == NULL)
				ii->ii_PredicateState = ExecPrepareQual(ii->ii_Predicate, estate);
			if (!ExecQual(ii->ii_PredicateState, econtext))
				continue;
		}

		FormIndexDatum(ii, slot, estate, values, isnull);
		if (global_index_find_conflict(gidx, ii, heapRelation, NULL,
									   values, isnull, estate, true,
									   conflictPart, conflictTid))
			return false;
	}

	return true;
}

/* ----------------------------------------------------------------
 *		ExecOpenGlobalIndexes
 *
 *		When the result relation is a partition, resolve and open the parent's
 *		GLOBAL partition indexes once and cache them on the ResultRelInfo.
 *		Without this, ExecInsertIndexTuples() would re-scan pg_index and reopen
 *		the indexes for every inserted row.  Opened with RowExclusiveLock and
 *		closed in ExecCloseIndices().  Runs in the per-query memory context (it
 *		is called from ExecOpenIndices at executor setup), so the cache arrays
 *		and IndexInfos live for the whole statement.
 * ----------------------------------------------------------------
 */
static void
ExecOpenGlobalIndexes(ResultRelInfo *resultRelInfo)
{
	Relation	resultRelation = resultRelInfo->ri_RelationDesc;
	Oid			parentOid;
	Relation	pgidxrel;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	htup;
	List	   *oids = NIL;
	ListCell   *lc;
	int			n;
	List	   *ancestors;

	resultRelInfo->ri_GlobalIndicesResolved = true;

	/* No error if the pg_inherits entry is missing (ATTACH/DETACH) */
	ancestors = get_partition_ancestors(RelationGetRelid(resultRelation));
	if (ancestors == NIL)
		return;
	parentOid = linitial_oid(ancestors);
	list_free(ancestors);

	/* Collect the parent's global index OIDs (one pg_index scan, not per row). */
	ScanKeyInit(&key, Anum_pg_index_indrelid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(parentOid));
	pgidxrel = table_open(IndexRelationId, AccessShareLock);
	scan = systable_beginscan(pgidxrel, IndexIndrelidIndexId, true, NULL, 1, &key);
	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		Form_pg_index idx = (Form_pg_index) GETSTRUCT(htup);

		if (idx->indglobal)
			oids = lappend_oid(oids, idx->indexrelid);
	}
	systable_endscan(scan);
	table_close(pgidxrel, AccessShareLock);

	n = list_length(oids);
	if (n > 0)
	{
		resultRelInfo->ri_GlobalIndexRelationDescs = palloc_array(Relation, n);
		resultRelInfo->ri_GlobalIndexRelationInfo = palloc_array(IndexInfo *, n);
		n = 0;
		foreach(lc, oids)
		{
			Relation	idxrel = index_open(lfirst_oid(lc), RowExclusiveLock);

			/*
			 * Mapped to this partition's column layout, which may differ
			 * from the parent's; for a UNIQUE index this also sets up the
			 * equality operators ExecCheckGlobalIndexUnique() needs.
			 */
			IndexInfo  *ii = BuildGlobalIndexInfo(idxrel, resultRelation);

			resultRelInfo->ri_GlobalIndexRelationDescs[n] = idxrel;
			resultRelInfo->ri_GlobalIndexRelationInfo[n] = ii;
			n++;
		}
		resultRelInfo->ri_NumGlobalIndices = n;
	}
	list_free(oids);
}

/* ----------------------------------------------------------------
 *		ExecOpenIndices
 *
 *		Find the indices associated with a result relation, open them,
 *		and save information about them in the result ResultRelInfo.
 *
 *		At entry, caller has already opened and locked
 *		resultRelInfo->ri_RelationDesc.
 * ----------------------------------------------------------------
 */
void
ExecOpenIndices(ResultRelInfo *resultRelInfo, bool speculative)
{
	Relation	resultRelation = resultRelInfo->ri_RelationDesc;
	List	   *indexoidlist;
	ListCell   *l;
	int			len,
				i;
	RelationPtr relationDescs;
	IndexInfo **indexInfoArray;

	resultRelInfo->ri_NumIndices = 0;

	/*
	 * If this is a partition, resolve and open the parent's global partition
	 * indexes once (cached on the ResultRelInfo).  Done before the "no local
	 * indexes" fast paths below, because a partition can have global indexes
	 * even when it has no local index of its own.
	 */
	if (resultRelation->rd_rel->relispartition &&
		!resultRelInfo->ri_GlobalIndicesResolved)
		ExecOpenGlobalIndexes(resultRelInfo);

	/* fast path if no indexes */
	if (!RelationGetForm(resultRelation)->relhasindex)
		return;

	/*
	 * Get cached list of index OIDs
	 */
	indexoidlist = RelationGetIndexList(resultRelation);
	len = list_length(indexoidlist);
	if (len == 0)
		return;

	/* This Assert will fail if ExecOpenIndices is called twice */
	Assert(resultRelInfo->ri_IndexRelationDescs == NULL);

	/*
	 * allocate space for result arrays
	 */
	relationDescs = palloc_array(Relation, len);
	indexInfoArray = palloc_array(IndexInfo *, len);

	resultRelInfo->ri_NumIndices = len;
	resultRelInfo->ri_IndexRelationDescs = relationDescs;
	resultRelInfo->ri_IndexRelationInfo = indexInfoArray;

	/*
	 * For each index, open the index relation and save pg_index info. We
	 * acquire RowExclusiveLock, signifying we will update the index.
	 *
	 * Note: we do this even if the index is not indisready; it's not worth
	 * the trouble to optimize for the case where it isn't.
	 */
	i = 0;
	foreach(l, indexoidlist)
	{
		Oid			indexOid = lfirst_oid(l);
		Relation	indexDesc;
		IndexInfo  *ii;

		indexDesc = index_open(indexOid, RowExclusiveLock);

		/* extract index key information from the index's pg_index info */
		ii = BuildIndexInfo(indexDesc);

		/*
		 * If the indexes are to be used for speculative insertion, add extra
		 * information required by unique index entries.
		 */
		if (speculative && ii->ii_Unique && !indexDesc->rd_index->indisexclusion)
			BuildSpeculativeIndexInfo(indexDesc, ii);

		relationDescs[i] = indexDesc;
		indexInfoArray[i] = ii;
		i++;
	}

	list_free(indexoidlist);
}

/* ----------------------------------------------------------------
 *		ExecCloseIndices
 *
 *		Close the index relations stored in resultRelInfo
 * ----------------------------------------------------------------
 */
void
ExecCloseIndices(ResultRelInfo *resultRelInfo)
{
	int			i;
	int			numIndices;
	RelationPtr indexDescs;
	IndexInfo **indexInfos;

	numIndices = resultRelInfo->ri_NumIndices;
	indexDescs = resultRelInfo->ri_IndexRelationDescs;
	indexInfos = resultRelInfo->ri_IndexRelationInfo;

	for (i = 0; i < numIndices; i++)
	{
		/* This Assert will fail if ExecCloseIndices is called twice */
		Assert(indexDescs[i] != NULL);

		/* Give the index a chance to do some post-insert cleanup */
		index_insert_cleanup(indexDescs[i], indexInfos[i]);

		/* Drop lock acquired by ExecOpenIndices */
		index_close(indexDescs[i], RowExclusiveLock);

		/* Mark the index as closed */
		indexDescs[i] = NULL;
	}

	/* Close the cached global partition indexes, if any. */
	for (i = 0; i < resultRelInfo->ri_NumGlobalIndices; i++)
	{
		Relation	gidx = resultRelInfo->ri_GlobalIndexRelationDescs[i];

		if (gidx == NULL)
			continue;
		index_insert_cleanup(gidx, resultRelInfo->ri_GlobalIndexRelationInfo[i]);
		index_close(gidx, RowExclusiveLock);
		resultRelInfo->ri_GlobalIndexRelationDescs[i] = NULL;
	}

	/*
	 * We don't attempt to free the IndexInfo data structures or the arrays,
	 * instead assuming that such stuff will be cleaned up automatically in
	 * FreeExecutorState.
	 */
}

/* ----------------------------------------------------------------
 *		ExecInsertIndexTuples
 *
 *		This routine takes care of inserting index tuples
 *		into all the relations indexing the result relation
 *		when a heap tuple is inserted into the result relation.
 *
 *		When EIIT_IS_UPDATE is set and EIIT_ONLY_SUMMARIZING isn't,
 *		executor is performing an UPDATE that could not use an
 *		optimization like heapam's HOT (in more general terms a
 *		call to table_tuple_update() took place and set
 *		'update_indexes' to TU_All).  Receiving this hint makes
 *		us consider if we should pass down the 'indexUnchanged'
 *		hint in turn.  That's something that we figure out for
 *		each index_insert() call iff EIIT_IS_UPDATE is set.
 *		(When that flag is not set we already know not to pass the
 *		hint to any index.)
 *
 *		If EIIT_ONLY_SUMMARIZING is set, an equivalent optimization to
 *		HOT has been applied and any updated columns are indexed
 *		only by summarizing indexes (or in more general terms a
 *		call to table_tuple_update() took place and set
 *		'update_indexes' to TU_Summarizing). We can (and must)
 *		therefore only update the indexes that have
 *		'amsummarizing' = true.
 *
 *		Unique and exclusion constraints are enforced at the same
 *		time.  This returns a list of index OIDs for any unique or
 *		exclusion constraints that are deferred and that had
 *		potential (unconfirmed) conflicts.  (if EIIT_NO_DUPE_ERROR,
 *		the same is done for non-deferred constraints, but report
 *		if conflict was speculative or deferred conflict to caller)
 *
 *		If 'arbiterIndexes' is nonempty, EIIT_NO_DUPE_ERROR applies only to
 *		those indexes.  NIL means EIIT_NO_DUPE_ERROR applies to all indexes.
 * ----------------------------------------------------------------
 */
List *
ExecInsertIndexTuples(ResultRelInfo *resultRelInfo,
					  EState *estate,
					  uint32 flags,
					  TupleTableSlot *slot,
					  List *arbiterIndexes,
					  bool *specConflict)
{
	ItemPointer tupleid = &slot->tts_tid;
	List	   *result = NIL;
	int			i;
	int			numIndices;
	RelationPtr relationDescs;
	Relation	heapRelation;
	IndexInfo **indexInfoArray;
	ExprContext *econtext;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];

	Assert(ItemPointerIsValid(tupleid));

	/*
	 * Get information from the result relation info structure.
	 */
	numIndices = resultRelInfo->ri_NumIndices;
	relationDescs = resultRelInfo->ri_IndexRelationDescs;
	indexInfoArray = resultRelInfo->ri_IndexRelationInfo;
	heapRelation = resultRelInfo->ri_RelationDesc;

	/* Sanity check: slot must belong to the same rel as the resultRelInfo. */
	Assert(slot->tts_tableOid == RelationGetRelid(heapRelation));

	/*
	 * We will use the EState's per-tuple context for evaluating predicates
	 * and index expressions (creating it if it's not already there).
	 */
	econtext = GetPerTupleExprContext(estate);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/*
	 * for each index, form and insert the index tuple
	 */
	for (i = 0; i < numIndices; i++)
	{
		Relation	indexRelation = relationDescs[i];
		IndexInfo  *indexInfo;
		bool		applyNoDupErr;
		IndexUniqueCheck checkUnique;
		bool		indexUnchanged;
		bool		satisfiesConstraint;

		if (indexRelation == NULL)
			continue;

		indexInfo = indexInfoArray[i];

		/* If the index is marked as read-only, ignore it */
		if (!indexInfo->ii_ReadyForInserts)
			continue;

		/*
		 * Skip processing of non-summarizing indexes if we only update
		 * summarizing indexes
		 */
		if ((flags & EIIT_ONLY_SUMMARIZING) && !indexInfo->ii_Summarizing)
			continue;

		/* Check for partial index */
		if (indexInfo->ii_Predicate != NIL)
		{
			ExprState  *predicate;

			/*
			 * If predicate state not set up yet, create it (in the estate's
			 * per-query context)
			 */
			predicate = indexInfo->ii_PredicateState;
			if (predicate == NULL)
			{
				predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);
				indexInfo->ii_PredicateState = predicate;
			}

			/* Skip this index-update if the predicate isn't satisfied */
			if (!ExecQual(predicate, econtext))
				continue;
		}

		/*
		 * FormIndexDatum fills in its values and isnull parameters with the
		 * appropriate values for the column(s) of the index.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		/* Check whether to apply noDupErr to this index */
		applyNoDupErr = (flags & EIIT_NO_DUPE_ERROR) &&
			(arbiterIndexes == NIL ||
			 list_member_oid(arbiterIndexes,
							 indexRelation->rd_index->indexrelid));

		/*
		 * The index AM does the actual insertion, plus uniqueness checking.
		 *
		 * For an immediate-mode unique index, we just tell the index AM to
		 * throw error if not unique.
		 *
		 * For a deferrable unique index, we tell the index AM to just detect
		 * possible non-uniqueness, and we add the index OID to the result
		 * list if further checking is needed.
		 *
		 * For a speculative insertion (used by INSERT ... ON CONFLICT), do
		 * the same as for a deferrable unique index.
		 */
		if (!indexRelation->rd_index->indisunique)
			checkUnique = UNIQUE_CHECK_NO;
		else if (applyNoDupErr)
			checkUnique = UNIQUE_CHECK_PARTIAL;
		else if (indexRelation->rd_index->indimmediate)
			checkUnique = UNIQUE_CHECK_YES;
		else
			checkUnique = UNIQUE_CHECK_PARTIAL;

		/*
		 * There's definitely going to be an index_insert() call for this
		 * index.  If we're being called as part of an UPDATE statement,
		 * consider if the 'indexUnchanged' = true hint should be passed.
		 */
		indexUnchanged = ((flags & EIIT_IS_UPDATE) &&
						  index_unchanged_by_update(resultRelInfo,
													estate,
													indexInfo,
													indexRelation));

		satisfiesConstraint =
			index_insert(indexRelation, /* index relation */
						 values,	/* array of index Datums */
						 isnull,	/* null flags */
						 tupleid,	/* tid of heap tuple */
						 heapRelation,	/* heap relation */
						 checkUnique,	/* type of uniqueness check to do */
						 indexUnchanged,	/* UPDATE without logical change? */
						 indexInfo);	/* index AM may need this */

		/*
		 * If the index has an associated exclusion constraint, check that.
		 * This is simpler than the process for uniqueness checks since we
		 * always insert first and then check.  If the constraint is deferred,
		 * we check now anyway, but don't throw error on violation or wait for
		 * a conclusive outcome from a concurrent insertion; instead we'll
		 * queue a recheck event.  Similarly, noDupErr callers (speculative
		 * inserters) will recheck later, and wait for a conclusive outcome
		 * then.
		 *
		 * An index for an exclusion constraint can't also be UNIQUE (not an
		 * essential property, we just don't allow it in the grammar), so no
		 * need to preserve the prior state of satisfiesConstraint.
		 */
		if (indexInfo->ii_ExclusionOps != NULL)
		{
			bool		violationOK;
			CEOUC_WAIT_MODE waitMode;

			if (applyNoDupErr)
			{
				violationOK = true;
				waitMode = CEOUC_LIVELOCK_PREVENTING_WAIT;
			}
			else if (!indexRelation->rd_index->indimmediate)
			{
				violationOK = true;
				waitMode = CEOUC_NOWAIT;
			}
			else
			{
				violationOK = false;
				waitMode = CEOUC_WAIT;
			}

			satisfiesConstraint =
				check_exclusion_or_unique_constraint(heapRelation,
													 indexRelation, indexInfo,
													 tupleid, values, isnull,
													 estate, false,
													 waitMode, violationOK, NULL);
		}

		if ((checkUnique == UNIQUE_CHECK_PARTIAL ||
			 indexInfo->ii_ExclusionOps != NULL) &&
			!satisfiesConstraint)
		{
			/*
			 * The tuple potentially violates the uniqueness or exclusion
			 * constraint, so make a note of the index so that we can re-check
			 * it later.  Speculative inserters are told if there was a
			 * speculative conflict, since that always requires a restart.
			 */
			result = lappend_oid(result, RelationGetRelid(indexRelation));
			if (indexRelation->rd_index->indimmediate && specConflict)
				*specConflict = true;
		}
	}

	/*
	 * Global Partition Index maintenance
	 *
	 * If we just inserted into a partition, check whether the parent
	 * partitioned table has any global indexes (indglobal = true).  Such
	 * indexes span all partitions and must be updated for every inserted row.
	 *
	 * The TID stored in a global index entry is the tuple's ctid inside the
	 * partition heap, so it is globally meaningful together with the partition
	 * OID stored in the INCLUDE columns (if any).  Callers that want to
	 * resolve the full row must use the PK columns (also stored as INCLUDE
	 * columns at index-creation time).
	 */
	if (heapRelation->rd_rel->relispartition)
	{
		/*
		 * The parent's global indexes were resolved and opened once by
		 * ExecOpenIndices() and cached on the ResultRelInfo, so here we just
		 * insert into each -- no per-row pg_index scan or index_open.
		 */
		for (int gi = 0; gi < resultRelInfo->ri_NumGlobalIndices; gi++)
		{
			Relation	globalIdxRel = resultRelInfo->ri_GlobalIndexRelationDescs[gi];
			IndexInfo  *globalIdxInfo = resultRelInfo->ri_GlobalIndexRelationInfo[gi];
			Datum		gvalues[INDEX_MAX_KEYS];
			bool		gisnull[INDEX_MAX_KEYS];

			if (!globalIdxInfo->ii_ReadyForInserts)
				continue;

			/* A global btree is never a summarizing index */
			if (flags & EIIT_ONLY_SUMMARIZING)
				continue;

			/* Partial global index: skip rows not satisfying the predicate */
			if (globalIdxInfo->ii_Predicate != NIL)
			{
				ExprState  *predicate = globalIdxInfo->ii_PredicateState;

				if (predicate == NULL)
				{
					predicate = ExecPrepareQual(globalIdxInfo->ii_Predicate,
												estate);
					globalIdxInfo->ii_PredicateState = predicate;
				}
				if (!ExecQual(predicate, econtext))
					continue;
			}

			FormIndexDatum(globalIdxInfo, slot, estate, gvalues, gisnull);

			/*
			 * The btree AM can't check uniqueness across partition heaps, so
			 * insert unchecked and enforce uniqueness ourselves afterwards.
			 */
			index_insert(globalIdxRel,
						 gvalues,
						 gisnull,
						 tupleid,
						 heapRelation,
						 UNIQUE_CHECK_NO,
						 false,
						 globalIdxInfo);

			if (globalIdxRel->rd_index->indisunique)
			{
				/*
				 * During a speculative insertion (INSERT ... ON CONFLICT), a
				 * conflict on an arbiter only flags *specConflict, without
				 * waiting, so that ExecInsert() backs the tuple out and redoes
				 * its pre-check, which then finds (and waits for) the other
				 * row.  Upstream does the same for btree arbiters.
				 */
				if ((flags & EIIT_NO_DUPE_ERROR) &&
					(arbiterIndexes == NIL ||
					 list_member_oid(arbiterIndexes,
									 RelationGetRelid(globalIdxRel))))
				{
					Oid			conflictPart;
					ItemPointerData conflictTid;

					if (global_index_find_conflict(globalIdxRel, globalIdxInfo,
												   heapRelation, tupleid,
												   gvalues, gisnull, estate,
												   false, &conflictPart,
												   &conflictTid))
					{
						Assert(specConflict != NULL);
						*specConflict = true;
					}
				}
				else
					ExecCheckGlobalIndexUnique(globalIdxRel, globalIdxInfo,
											   heapRelation, tupleid,
											   gvalues, gisnull, estate, false);
			}
		}
	}

	return result;
}

/* ----------------------------------------------------------------
 *		ExecCheckIndexConstraints
 *
 *		This routine checks if a tuple violates any unique or
 *		exclusion constraints.  Returns true if there is no conflict.
 *		Otherwise returns false, and the TID of the conflicting
 *		tuple is returned in *conflictTid.
 *
 *		If 'arbiterIndexes' is given, only those indexes are checked.
 *		NIL means all indexes.
 *
 *		Note that this doesn't lock the values in any way, so it's
 *		possible that a conflicting tuple is inserted immediately
 *		after this returns.  This can be used for either a pre-check
 *		before insertion or a re-check after finding a conflict.
 *
 *		'tupleid' should be the TID of the tuple that has been recently
 *		inserted (or can be invalid if we haven't inserted a new tuple yet).
 *		This tuple will be excluded from conflict checking.
 * ----------------------------------------------------------------
 */
bool
ExecCheckIndexConstraints(ResultRelInfo *resultRelInfo, TupleTableSlot *slot,
						  EState *estate, ItemPointer conflictTid,
						  const ItemPointerData *tupleid, List *arbiterIndexes)
{
	int			i;
	int			numIndices;
	RelationPtr relationDescs;
	Relation	heapRelation;
	IndexInfo **indexInfoArray;
	ExprContext *econtext;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	ItemPointerData invalidItemPtr;
	bool		checkedIndex = false;

	ItemPointerSetInvalid(conflictTid);
	ItemPointerSetInvalid(&invalidItemPtr);

	/*
	 * Get information from the result relation info structure.
	 */
	numIndices = resultRelInfo->ri_NumIndices;
	relationDescs = resultRelInfo->ri_IndexRelationDescs;
	indexInfoArray = resultRelInfo->ri_IndexRelationInfo;
	heapRelation = resultRelInfo->ri_RelationDesc;

	/*
	 * We will use the EState's per-tuple context for evaluating predicates
	 * and index expressions (creating it if it's not already there).
	 */
	econtext = GetPerTupleExprContext(estate);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/*
	 * For each index, form index tuple and check if it satisfies the
	 * constraint.
	 */
	for (i = 0; i < numIndices; i++)
	{
		Relation	indexRelation = relationDescs[i];
		IndexInfo  *indexInfo;
		bool		satisfiesConstraint;

		if (indexRelation == NULL)
			continue;

		indexInfo = indexInfoArray[i];

		if (!indexInfo->ii_Unique && !indexInfo->ii_ExclusionOps)
			continue;

		/* If the index is marked as read-only, ignore it */
		if (!indexInfo->ii_ReadyForInserts)
			continue;

		/* When specific arbiter indexes requested, only examine them */
		if (arbiterIndexes != NIL &&
			!list_member_oid(arbiterIndexes,
							 indexRelation->rd_index->indexrelid))
			continue;

		if (!indexRelation->rd_index->indimmediate)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("ON CONFLICT does not support deferrable unique constraints/exclusion constraints as arbiters"),
					 errtableconstraint(heapRelation,
										RelationGetRelationName(indexRelation))));

		checkedIndex = true;

		/* Check for partial index */
		if (indexInfo->ii_Predicate != NIL)
		{
			ExprState  *predicate;

			/*
			 * If predicate state not set up yet, create it (in the estate's
			 * per-query context)
			 */
			predicate = indexInfo->ii_PredicateState;
			if (predicate == NULL)
			{
				predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);
				indexInfo->ii_PredicateState = predicate;
			}

			/* Skip this index-update if the predicate isn't satisfied */
			if (!ExecQual(predicate, econtext))
				continue;
		}

		/*
		 * FormIndexDatum fills in its values and isnull parameters with the
		 * appropriate values for the column(s) of the index.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		satisfiesConstraint =
			check_exclusion_or_unique_constraint(heapRelation, indexRelation,
												 indexInfo, tupleid,
												 values, isnull, estate, false,
												 CEOUC_WAIT, true,
												 conflictTid);
		if (!satisfiesConstraint)
			return false;
	}

	if (arbiterIndexes != NIL && !checkedIndex)
	{
		/*
		 * The arbiters may all be the parent's global indexes, which
		 * ExecCheckGlobalIndexConstraints() checks instead.
		 */
		for (i = 0; i < resultRelInfo->ri_NumGlobalIndices; i++)
		{
			if (list_member_oid(arbiterIndexes,
								RelationGetRelid(resultRelInfo->ri_GlobalIndexRelationDescs[i])))
				checkedIndex = true;
		}
		if (!checkedIndex)
			elog(ERROR, "unexpected failure to find arbiter index");
	}

	return true;
}

/*
 * Check for violation of an exclusion or unique constraint
 *
 * heap: the table containing the new tuple
 * index: the index supporting the constraint
 * indexInfo: info about the index, including the exclusion properties
 * tupleid: heap TID of the new tuple we have just inserted (invalid if we
 *		haven't inserted a new tuple yet)
 * values, isnull: the *index* column values computed for the new tuple
 * estate: an EState we can do evaluation in
 * newIndex: if true, we are trying to build a new index (this affects
 *		only the wording of error messages)
 * waitMode: whether to wait for concurrent inserters/deleters
 * violationOK: if true, don't throw error for violation
 * conflictTid: if not-NULL, the TID of the conflicting tuple is returned here
 *
 * Returns true if OK, false if actual or potential violation
 *
 * 'waitMode' determines what happens if a conflict is detected with a tuple
 * that was inserted or deleted by a transaction that's still running.
 * CEOUC_WAIT means that we wait for the transaction to commit, before
 * throwing an error or returning.  CEOUC_NOWAIT means that we report the
 * violation immediately; so the violation is only potential, and the caller
 * must recheck sometime later.  This behavior is convenient for deferred
 * exclusion checks; we need not bother queuing a deferred event if there is
 * definitely no conflict at insertion time.
 *
 * CEOUC_LIVELOCK_PREVENTING_WAIT is like CEOUC_NOWAIT, but we will sometimes
 * wait anyway, to prevent livelocking if two transactions try inserting at
 * the same time.  This is used with speculative insertions, for INSERT ON
 * CONFLICT statements. (See notes in file header)
 *
 * If violationOK is true, we just report the potential or actual violation to
 * the caller by returning 'false'.  Otherwise we throw a descriptive error
 * message here.  When violationOK is false, a false result is impossible.
 *
 * Note: The indexam is normally responsible for checking unique constraints,
 * so this normally only needs to be used for exclusion constraints.  But this
 * function is also called when doing a "pre-check" for conflicts on a unique
 * constraint, when doing speculative insertion.  Caller may use the returned
 * conflict TID to take further steps.
 */
static bool
check_exclusion_or_unique_constraint(Relation heap, Relation index,
									 IndexInfo *indexInfo,
									 const ItemPointerData *tupleid,
									 const Datum *values, const bool *isnull,
									 EState *estate, bool newIndex,
									 CEOUC_WAIT_MODE waitMode,
									 bool violationOK,
									 ItemPointer conflictTid)
{
	Oid		   *constr_procs;
	uint16	   *constr_strats;
	Oid		   *index_collations = index->rd_indcollation;
	int			indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	IndexScanDesc index_scan;
	ScanKeyData scankeys[INDEX_MAX_KEYS];
	SnapshotData DirtySnapshot;
	int			i;
	bool		conflict;
	bool		found_self;
	ExprContext *econtext;
	TupleTableSlot *existing_slot;
	TupleTableSlot *save_scantuple;

	if (indexInfo->ii_ExclusionOps)
	{
		constr_procs = indexInfo->ii_ExclusionProcs;
		constr_strats = indexInfo->ii_ExclusionStrats;
	}
	else
	{
		constr_procs = indexInfo->ii_UniqueProcs;
		constr_strats = indexInfo->ii_UniqueStrats;
	}

	/*
	 * If this is a WITHOUT OVERLAPS constraint, we must also forbid empty
	 * ranges/multiranges. This must happen before we look for NULLs below, or
	 * a UNIQUE constraint could insert an empty range along with a NULL
	 * scalar part.
	 */
	if (indexInfo->ii_WithoutOverlaps)
	{
		/*
		 * Look up the type from the heap tuple, but check the Datum from the
		 * index tuple.
		 */
		AttrNumber	attno = indexInfo->ii_IndexAttrNumbers[indnkeyatts - 1];

		if (!isnull[indnkeyatts - 1])
		{
			TupleDesc	tupdesc = RelationGetDescr(heap);
			Form_pg_attribute att = TupleDescAttr(tupdesc, attno - 1);
			TypeCacheEntry *typcache = lookup_type_cache(att->atttypid,
														 TYPECACHE_DOMAIN_BASE_INFO);
			char		typtype;

			if (OidIsValid(typcache->domainBaseType))
				typtype = get_typtype(typcache->domainBaseType);
			else
				typtype = typcache->typtype;

			ExecWithoutOverlapsNotEmpty(heap, att->attname,
										values[indnkeyatts - 1],
										typtype, att->atttypid);
		}
	}

	/*
	 * If any of the input values are NULL, and the index uses the default
	 * nulls-are-distinct mode, the constraint check is assumed to pass (i.e.,
	 * we assume the operators are strict).  Otherwise, we interpret the
	 * constraint as specifying IS NULL for each column whose input value is
	 * NULL.
	 */
	if (!indexInfo->ii_NullsNotDistinct)
	{
		for (i = 0; i < indnkeyatts; i++)
		{
			if (isnull[i])
				return true;
		}
	}

	/*
	 * Search the tuples that are in the index for any violations, including
	 * tuples that aren't visible yet.
	 */
	InitDirtySnapshot(DirtySnapshot);

	for (i = 0; i < indnkeyatts; i++)
	{
		ScanKeyEntryInitialize(&scankeys[i],
							   isnull[i] ? SK_ISNULL | SK_SEARCHNULL : 0,
							   i + 1,
							   constr_strats[i],
							   InvalidOid,
							   index_collations[i],
							   constr_procs[i],
							   values[i]);
	}

	/*
	 * Need a TupleTableSlot to put existing tuples in.
	 *
	 * To use FormIndexDatum, we have to make the econtext's scantuple point
	 * to this slot.  Be sure to save and restore caller's value for
	 * scantuple.
	 */
	existing_slot = table_slot_create(heap, NULL);

	econtext = GetPerTupleExprContext(estate);
	save_scantuple = econtext->ecxt_scantuple;
	econtext->ecxt_scantuple = existing_slot;

	/*
	 * May have to restart scan from this point if a potential conflict is
	 * found.
	 */
retry:
	conflict = false;
	found_self = false;
	index_scan = index_beginscan(heap, index,
								 &DirtySnapshot, NULL, indnkeyatts, 0,
								 SO_NONE);
	index_rescan(index_scan, scankeys, indnkeyatts, NULL, 0);

	while (index_getnext_slot(index_scan, ForwardScanDirection, existing_slot))
	{
		TransactionId xwait;
		XLTW_Oper	reason_wait;
		Datum		existing_values[INDEX_MAX_KEYS];
		bool		existing_isnull[INDEX_MAX_KEYS];
		char	   *error_new;
		char	   *error_existing;

		/*
		 * Ignore the entry for the tuple we're trying to check.
		 */
		if (ItemPointerIsValid(tupleid) &&
			ItemPointerEquals(tupleid, &existing_slot->tts_tid))
		{
			if (found_self)		/* should not happen */
				elog(ERROR, "found self tuple multiple times in index \"%s\"",
					 RelationGetRelationName(index));
			found_self = true;
			continue;
		}

		/*
		 * Extract the index column values and isnull flags from the existing
		 * tuple.
		 */
		FormIndexDatum(indexInfo, existing_slot, estate,
					   existing_values, existing_isnull);

		/* If lossy indexscan, must recheck the condition */
		if (index_scan->xs_recheck)
		{
			if (!index_recheck_constraint(index,
										  constr_procs,
										  existing_values,
										  existing_isnull,
										  values))
				continue;		/* tuple doesn't actually match, so no
								 * conflict */
		}

		/*
		 * At this point we have either a conflict or a potential conflict.
		 *
		 * If an in-progress transaction is affecting the visibility of this
		 * tuple, we need to wait for it to complete and then recheck (unless
		 * the caller requested not to).  For simplicity we do rechecking by
		 * just restarting the whole scan --- this case probably doesn't
		 * happen often enough to be worth trying harder, and anyway we don't
		 * want to hold any index internal locks while waiting.
		 */
		xwait = TransactionIdIsValid(DirtySnapshot.xmin) ?
			DirtySnapshot.xmin : DirtySnapshot.xmax;

		if (TransactionIdIsValid(xwait) &&
			(waitMode == CEOUC_WAIT ||
			 (waitMode == CEOUC_LIVELOCK_PREVENTING_WAIT &&
			  DirtySnapshot.speculativeToken &&
			  TransactionIdPrecedes(GetCurrentTransactionId(), xwait))))
		{
			reason_wait = indexInfo->ii_ExclusionOps ?
				XLTW_RecheckExclusionConstr : XLTW_InsertIndex;
			index_endscan(index_scan);
			if (DirtySnapshot.speculativeToken)
				SpeculativeInsertionWait(DirtySnapshot.xmin,
										 DirtySnapshot.speculativeToken);
			else
				XactLockTableWait(xwait, heap,
								  &existing_slot->tts_tid, reason_wait);
			goto retry;
		}

		/*
		 * We have a definite conflict (or a potential one, but the caller
		 * didn't want to wait).  Return it to caller, or report it.
		 */
		if (violationOK)
		{
			conflict = true;
			if (conflictTid)
				*conflictTid = existing_slot->tts_tid;
			break;
		}

		error_new = BuildIndexValueDescription(index, values, isnull);
		error_existing = BuildIndexValueDescription(index, existing_values,
													existing_isnull);
		if (newIndex)
			ereport(ERROR,
					(errcode(ERRCODE_EXCLUSION_VIOLATION),
					 errmsg("could not create exclusion constraint \"%s\"",
							RelationGetRelationName(index)),
					 error_new && error_existing ?
					 errdetail("Key %s conflicts with key %s.",
							   error_new, error_existing) :
					 errdetail("Key conflicts exist."),
					 errtableconstraint(heap,
										RelationGetRelationName(index))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_EXCLUSION_VIOLATION),
					 errmsg("conflicting key value violates exclusion constraint \"%s\"",
							RelationGetRelationName(index)),
					 error_new && error_existing ?
					 errdetail("Key %s conflicts with existing key %s.",
							   error_new, error_existing) :
					 errdetail("Key conflicts with existing key."),
					 errtableconstraint(heap,
										RelationGetRelationName(index))));
	}

	index_endscan(index_scan);

	/*
	 * Ordinarily, at this point the search should have found the originally
	 * inserted tuple (if any), unless we exited the loop early because of
	 * conflict.  However, it is possible to define exclusion constraints for
	 * which that wouldn't be true --- for instance, if the operator is <>. So
	 * we no longer complain if found_self is still false.
	 */

	econtext->ecxt_scantuple = save_scantuple;

	ExecDropSingleTupleTableSlot(existing_slot);

#ifdef USE_INJECTION_POINTS
	if (!conflict)
		INJECTION_POINT("check-exclusion-or-unique-constraint-no-conflict", NULL);
#endif

	return !conflict;
}

/*
 * Check for violation of an exclusion constraint
 *
 * This is a dumbed down version of check_exclusion_or_unique_constraint
 * for external callers. They don't need all the special modes.
 */
void
check_exclusion_constraint(Relation heap, Relation index,
						   IndexInfo *indexInfo,
						   const ItemPointerData *tupleid,
						   const Datum *values, const bool *isnull,
						   EState *estate, bool newIndex)
{
	(void) check_exclusion_or_unique_constraint(heap, index, indexInfo, tupleid,
												values, isnull,
												estate, newIndex,
												CEOUC_WAIT, false, NULL);
}

/*
 * Check existing tuple's index values to see if it really matches the
 * exclusion condition against the new_values.  Returns true if conflict.
 */
static bool
index_recheck_constraint(Relation index, const Oid *constr_procs,
						 const Datum *existing_values, const bool *existing_isnull,
						 const Datum *new_values)
{
	int			indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	int			i;

	for (i = 0; i < indnkeyatts; i++)
	{
		/* Assume the exclusion operators are strict */
		if (existing_isnull[i])
			return false;

		if (!DatumGetBool(OidFunctionCall2Coll(constr_procs[i],
											   index->rd_indcollation[i],
											   existing_values[i],
											   new_values[i])))
			return false;
	}

	return true;
}

/*
 * Check if ExecInsertIndexTuples() should pass indexUnchanged hint.
 *
 * When the executor performs an UPDATE that requires a new round of index
 * tuples, determine if we should pass 'indexUnchanged' = true hint for one
 * single index.
 */
static bool
index_unchanged_by_update(ResultRelInfo *resultRelInfo, EState *estate,
						  IndexInfo *indexInfo, Relation indexRelation)
{
	Bitmapset  *updatedCols;
	Bitmapset  *extraUpdatedCols;
	Bitmapset  *allUpdatedCols;
	bool		hasexpression = false;
	List	   *idxExprs;

	/*
	 * Check cache first
	 */
	if (indexInfo->ii_CheckedUnchanged)
		return indexInfo->ii_IndexUnchanged;
	indexInfo->ii_CheckedUnchanged = true;

	/*
	 * Check for indexed attribute overlap with updated columns.
	 *
	 * Only do this for key columns.  A change to a non-key column within an
	 * INCLUDE index should not be counted here.  Non-key column values are
	 * opaque payload state to the index AM, a little like an extra table TID.
	 *
	 * Note that row-level BEFORE triggers won't affect our behavior, since
	 * they don't affect the updatedCols bitmaps generally.  It doesn't seem
	 * worth the trouble of checking which attributes were changed directly.
	 */
	updatedCols = ExecGetUpdatedCols(resultRelInfo, estate);
	extraUpdatedCols = ExecGetExtraUpdatedCols(resultRelInfo, estate);
	for (int attr = 0; attr < indexInfo->ii_NumIndexKeyAttrs; attr++)
	{
		int			keycol = indexInfo->ii_IndexAttrNumbers[attr];

		if (keycol <= 0)
		{
			/*
			 * Skip expressions for now, but remember to deal with them later
			 * on
			 */
			hasexpression = true;
			continue;
		}

		if (bms_is_member(keycol - FirstLowInvalidHeapAttributeNumber,
						  updatedCols) ||
			bms_is_member(keycol - FirstLowInvalidHeapAttributeNumber,
						  extraUpdatedCols))
		{
			/* Changed key column -- don't hint for this index */
			indexInfo->ii_IndexUnchanged = false;
			return false;
		}
	}

	/*
	 * When we get this far and index has no expressions, return true so that
	 * index_insert() call will go on to pass 'indexUnchanged' = true hint.
	 *
	 * The _absence_ of an indexed key attribute that overlaps with updated
	 * attributes (in addition to the total absence of indexed expressions)
	 * shows that the index as a whole is logically unchanged by UPDATE.
	 */
	if (!hasexpression)
	{
		indexInfo->ii_IndexUnchanged = true;
		return true;
	}

	/*
	 * Need to pass only one bms to expression_tree_walker helper function.
	 * Avoid allocating memory in common case where there are no extra cols.
	 */
	if (!extraUpdatedCols)
		allUpdatedCols = updatedCols;
	else
		allUpdatedCols = bms_union(updatedCols, extraUpdatedCols);

	/*
	 * We have to work slightly harder in the event of indexed expressions,
	 * but the principle is the same as before: try to find columns (Vars,
	 * actually) that overlap with known-updated columns.
	 *
	 * If we find any matching Vars, don't pass hint for index.  Otherwise
	 * pass hint.
	 */
	idxExprs = RelationGetIndexExpressions(indexRelation);
	hasexpression = index_expression_changed_walker((Node *) idxExprs,
													allUpdatedCols);
	list_free(idxExprs);
	if (extraUpdatedCols)
		bms_free(allUpdatedCols);

	if (hasexpression)
	{
		indexInfo->ii_IndexUnchanged = false;
		return false;
	}

	/*
	 * Deliberately don't consider index predicates.  We should even give the
	 * hint when result rel's "updated tuple" has no corresponding index
	 * tuple, which is possible with a partial index (provided the usual
	 * conditions are met).
	 */
	indexInfo->ii_IndexUnchanged = true;
	return true;
}

/*
 * Indexed expression helper for index_unchanged_by_update().
 *
 * Returns true when Var that appears within allUpdatedCols located.
 */
static bool
index_expression_changed_walker(Node *node, Bitmapset *allUpdatedCols)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (bms_is_member(var->varattno - FirstLowInvalidHeapAttributeNumber,
						  allUpdatedCols))
		{
			/* Var was updated -- indicates that we should not hint */
			return true;
		}

		/* Still haven't found a reason to not pass the hint */
		return false;
	}

	return expression_tree_walker(node, index_expression_changed_walker,
								  allUpdatedCols);
}

/*
 * ExecWithoutOverlapsNotEmpty - raise an error if the tuple has an empty
 * range or multirange in the given attribute.
 */
static void
ExecWithoutOverlapsNotEmpty(Relation rel, NameData attname, Datum attval, char typtype, Oid atttypid)
{
	bool		isempty;
	RangeType  *r;
	MultirangeType *mr;

	switch (typtype)
	{
		case TYPTYPE_RANGE:
			r = DatumGetRangeTypeP(attval);
			isempty = RangeIsEmpty(r);
			break;
		case TYPTYPE_MULTIRANGE:
			mr = DatumGetMultirangeTypeP(attval);
			isempty = MultirangeIsEmpty(mr);
			break;
		default:
			elog(ERROR, "WITHOUT OVERLAPS column \"%s\" is not a range or multirange",
				 NameStr(attname));
	}

	/* Report a CHECK_VIOLATION */
	if (isempty)
		ereport(ERROR,
				(errcode(ERRCODE_CHECK_VIOLATION),
				 errmsg("empty WITHOUT OVERLAPS value found in column \"%s\" in relation \"%s\"",
						NameStr(attname), RelationGetRelationName(rel))));
}
