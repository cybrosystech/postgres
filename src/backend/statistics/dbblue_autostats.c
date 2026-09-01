/*-------------------------------------------------------------------------
 *
 * dbblue_autostats.c
 *	  Detection of missing multi-column (extended) statistics.
 *
 * See src/include/statistics/dbblue_autostats.h for the rationale.
 *
 * Structure
 * ---------
 *
 * AutoStatsNoteClauses() is called from clauselist_selectivity_ext(), at the
 * exact point where the planner has determined that a set of clauses belongs
 * to one relation and is about to multiply their selectivities together.  We
 * reuse statext_is_compatible_clause() to extract attribute numbers, so we
 * only ever record column sets that a statistics object could genuinely have
 * estimated -- recommending statistics the planner would then ignore is the
 * classic failure of qual-logging advisors.
 *
 * Counting happens in a per-backend "front cache" first and is flushed to a
 * shared hash table at transaction end.  Planning is hot in the Odoo
 * workload, and taking even a shared LWLock per planned clause list would be
 * visible; batching means shared-memory traffic scales with the number of
 * distinct column sets rather than with the number of plannings.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/statistics/dbblue_autostats.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "statistics/dbblue_autostats.h"
#include "statistics/statistics.h"
#include "storage/lwlock.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

/* GUC variables */
bool		dbblue_autostats_enabled = false;
int			dbblue_autostats_max = 5000;
int			dbblue_autostats_local_max = 512;

/* number of output columns of dbblue_stats_advisor() */
#define DBBLUE_ADVISOR_COLS		9

/*
 * Key identifying one observed column combination.
 *
 * Kept memcmp-comparable for HASH_BLOBS.  There is trailing padding after
 * attnums[], so every key must be memset to zero before its fields are
 * filled in; autostats_init_key() is the only place that builds one.
 */
typedef struct AutoStatsKey
{
	Oid			dbid;
	Oid			relid;
	int16		nattnums;
	int16		attnums[STATS_MAX_DIMENSIONS];	/* ascending, zero-padded */
} AutoStatsKey;

/* Shared hash entry */
typedef struct AutoStatsEntry
{
	AutoStatsKey key;			/* hash key, must be first */
	uint64		plan_count;		/* times estimated independently */
	bool		all_equality;	/* were all clauses equality-shaped? */
	TimestampTz first_seen;
	TimestampTz last_seen;
} AutoStatsEntry;

/* Shared control struct */
typedef struct AutoStatsSharedState
{
	LWLock		lock;			/* protects the hash table */
	uint64		n_dropped;		/* combinations lost to a full hash */
} AutoStatsSharedState;

static AutoStatsSharedState *AutoStatsShared = NULL;
static HTAB *AutoStatsHash = NULL;

/*
 * Per-backend front cache.  Entries are linked into a dirty list when they
 * have counts that have not reached shared memory yet; dynahash entry
 * addresses are stable, so linking them is safe as long as we only ever
 * remove entries by destroying the whole table.
 */
typedef struct AutoStatsLocalEntry
{
	AutoStatsKey key;			/* hash key, must be first */
	uint32		pending;		/* unflushed plan count */
	bool		all_equality;
	bool		is_dirty;
	struct AutoStatsLocalEntry *next_dirty;
} AutoStatsLocalEntry;

static HTAB *AutoStatsLocalHash = NULL;
static AutoStatsLocalEntry *AutoStatsDirtyList = NULL;
static bool AutoStatsXactCallbackSet = false;

static void dbblue_autostats_shmem_request(void *arg);
static void dbblue_autostats_shmem_init(void *arg);
static void autostats_xact_callback(XactEvent event, void *arg);
static void autostats_flush_local(void);
static bool clause_is_equality_shaped(Node *clause);
static char *autostats_live_attname(Oid relid, int16 attnum);
static char *autostats_build_ddl(const AutoStatsKey *key, bool all_equality,
								 const char *nspname, const char *relname);

const ShmemCallbacks DBBlueAutoStatsShmemCallbacks = {
	.request_fn = dbblue_autostats_shmem_request,
	.init_fn = dbblue_autostats_shmem_init,
};


/*
 * dbblue_autostats_shmem_request
 *		Reserve the shared hash table and control struct.
 */
static void
dbblue_autostats_shmem_request(void *arg)
{
	ShmemRequestStruct(.name = "dbblue autostats state",
					   .size = sizeof(AutoStatsSharedState),
					   .ptr = (void **) &AutoStatsShared
		);

	ShmemRequestHash(.name = "dbblue autostats hash",
					 .nelems = dbblue_autostats_max,
					 .ptr = &AutoStatsHash,
					 .hash_info.keysize = sizeof(AutoStatsKey),
					 .hash_info.entrysize = sizeof(AutoStatsEntry),
					 .hash_flags = HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE
		);
}

/*
 * dbblue_autostats_shmem_init
 */
static void
dbblue_autostats_shmem_init(void *arg)
{
	LWLockInitialize(&AutoStatsShared->lock, LWTRANCHE_DBBLUE_AUTOSTATS);
	AutoStatsShared->n_dropped = 0;
}

/*
 * autostats_init_key
 *		Build a hash key from a relation OID and a set of attribute numbers.
 *
 * Returns false if the combination is not one we can represent or care
 * about: fewer than two columns, any system column, or more attributes than
 * a statistics object can hold.  Attribute numbers come out in ascending
 * order, so the same column set always produces the same key regardless of
 * the order the clauses appeared in.
 */
static bool
autostats_init_key(AutoStatsKey *key, Oid relid, Bitmapset *attnums)
{
	int			attnum = -1;
	int			n = 0;

	if (bms_num_members(attnums) < 2)
		return false;

	/*
	 * More than STATS_MAX_DIMENSIONS columns cannot be covered by a single
	 * statistics object.  Rather than silently truncating to a column set
	 * that was never actually queried together, drop the observation.
	 */
	if (bms_num_members(attnums) > STATS_MAX_DIMENSIONS)
		return false;

	/* zero the whole struct, including trailing padding, for memcmp */
	memset(key, 0, sizeof(AutoStatsKey));

	while ((attnum = bms_next_member(attnums, attnum)) >= 0)
	{
		/*
		 * Extended statistics cannot describe system columns.  These should
		 * already have been rejected upstream, but the key layout has no way
		 * to represent them, so be defensive.
		 */
		if (attnum <= 0 || attnum > MaxAttrNumber)
			return false;

		key->attnums[n++] = (int16) attnum;
	}

	key->dbid = MyDatabaseId;
	key->relid = relid;
	key->nattnums = (int16) n;

	return true;
}

/*
 * clause_is_equality_shaped
 *		Would this clause benefit from an MCV list?
 *
 * Multi-column MCV lists pay off for equality and IS NULL; inequality and
 * range clauses get much less out of them, and MCV lists are the expensive
 * kind to build.  This only decides which statistics kinds the advisor
 * suggests, so a name-based operator test is good enough -- we are labelling
 * a recommendation, not making a planning decision.
 */
static bool
clause_is_equality_shaped(Node *clause)
{
	Oid			opno = InvalidOid;

	if (clause == NULL)
		return false;

	if (IsA(clause, RestrictInfo))
		clause = (Node *) ((RestrictInfo *) clause)->clause;

	if (is_andclause(clause))
	{
		ListCell   *lc;

		foreach(lc, ((BoolExpr *) clause)->args)
		{
			if (!clause_is_equality_shaped((Node *) lfirst(lc)))
				return false;
		}
		return true;
	}

	/* "IS NULL", a bare boolean Var and "IS TRUE" are all point lookups */
	if (IsA(clause, NullTest) || IsA(clause, BooleanTest) || IsA(clause, Var))
		return true;

	if (IsA(clause, OpExpr))
		opno = ((OpExpr *) clause)->opno;
	else if (IsA(clause, ScalarArrayOpExpr))
		opno = ((ScalarArrayOpExpr *) clause)->opno;
	else
		return false;

	if (OidIsValid(opno))
	{
		char	   *opname = get_opname(opno);
		bool		is_eq;

		if (opname == NULL)
			return false;

		is_eq = (strcmp(opname, "=") == 0);
		pfree(opname);
		return is_eq;
	}

	return false;
}

/*
 * autostats_note_local
 *		Bump the per-backend counter for one column combination.
 */
static void
autostats_note_local(const AutoStatsKey *key, bool all_equality)
{
	AutoStatsLocalEntry *entry;
	bool		found;

	if (AutoStatsLocalHash == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(AutoStatsKey);
		ctl.entrysize = sizeof(AutoStatsLocalEntry);
		ctl.hcxt = TopMemoryContext;

		AutoStatsLocalHash = hash_create("dbblue autostats local cache",
										 64, &ctl,
										 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	if (!AutoStatsXactCallbackSet)
	{
		RegisterXactCallback(autostats_xact_callback, NULL);
		AutoStatsXactCallbackSet = true;
	}

	/*
	 * Keep the local cache bounded.  Flushing and starting over is cheaper
	 * and simpler than an eviction policy, and this is an advisory counter --
	 * nothing is lost but the (already flushed) local aggregation state.
	 */
	if (hash_get_num_entries(AutoStatsLocalHash) >= dbblue_autostats_local_max)
	{
		autostats_flush_local();
		hash_destroy(AutoStatsLocalHash);
		AutoStatsLocalHash = NULL;
		AutoStatsDirtyList = NULL;
		autostats_note_local(key, all_equality);
		return;
	}

	entry = (AutoStatsLocalEntry *) hash_search(AutoStatsLocalHash, key,
												HASH_ENTER, &found);

	if (!found)
	{
		entry->pending = 0;
		entry->all_equality = all_equality;
		entry->is_dirty = false;
		entry->next_dirty = NULL;
	}
	else if (!all_equality)
	{
		/*
		 * The same column set can show up with different operators.  Treat
		 * "all equality" as a property that any non-equality sighting
		 * clears, so we do not recommend an expensive MCV list on the
		 * strength of one lucky query shape.
		 */
		entry->all_equality = false;
	}

	entry->pending++;

	if (!entry->is_dirty)
	{
		entry->is_dirty = true;
		entry->next_dirty = AutoStatsDirtyList;
		AutoStatsDirtyList = entry;
	}
}

/*
 * autostats_flush_local
 *		Push pending per-backend counts into shared memory.
 *
 * Runs at transaction end, so it must not allocate, must not ereport, and
 * must walk only the entries that actually changed.
 */
static void
autostats_flush_local(void)
{
	AutoStatsLocalEntry *local;
	TimestampTz now;

	if (AutoStatsDirtyList == NULL || AutoStatsHash == NULL)
		return;

	now = GetCurrentTimestamp();

	LWLockAcquire(&AutoStatsShared->lock, LW_EXCLUSIVE);

	for (local = AutoStatsDirtyList; local != NULL; local = local->next_dirty)
	{
		AutoStatsEntry *shared;
		bool		found;

		/*
		 * HASH_ENTER_NULL rather than HASH_ENTER: the table is fixed-size,
		 * and running out of room must not throw an error out of a
		 * transaction callback.
		 */
		shared = (AutoStatsEntry *) hash_search(AutoStatsHash, &local->key,
												HASH_ENTER_NULL, &found);

		if (shared == NULL)
		{
			AutoStatsShared->n_dropped++;
			continue;
		}

		if (!found)
		{
			shared->plan_count = 0;
			shared->all_equality = local->all_equality;
			shared->first_seen = now;
		}
		else if (!local->all_equality)
			shared->all_equality = false;

		shared->plan_count += local->pending;
		shared->last_seen = now;
	}

	LWLockRelease(&AutoStatsShared->lock);

	/* clear the dirty list; entries stay in the local cache */
	for (local = AutoStatsDirtyList; local != NULL;)
	{
		AutoStatsLocalEntry *next = local->next_dirty;

		local->pending = 0;
		local->is_dirty = false;
		local->next_dirty = NULL;
		local = next;
	}

	AutoStatsDirtyList = NULL;
}

/*
 * autostats_xact_callback
 */
static void
autostats_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PARALLEL_ABORT:
			autostats_flush_local();
			break;
		default:
			break;
	}
}

/*
 * AutoStatsNoteClauses
 *		Record a column combination the planner is estimating independently.
 *
 * 'estimatedclauses' holds the 0-based positions of clauses that extended
 * statistics already covered; skipping those means we also catch the
 * partial-coverage case, where a statistics object exists but the query
 * filters on an additional column outside it.
 */
void
AutoStatsNoteClauses(PlannerInfo *root, RelOptInfo *rel, List *clauses,
					 Bitmapset *estimatedclauses)
{
	RangeTblEntry *rte;
	Bitmapset  *attnums = NULL;
	AutoStatsKey key;
	bool		all_equality = true;
	int			listidx;
	int			nleftover = 0;
	ListCell   *lc;

	Assert(rel->rtekind == RTE_RELATION);

	if (!dbblue_autostats_enabled || AutoStatsHash == NULL)
		return;

	rte = planner_rt_fetch(rel->relid, root);

	/* Only ordinary tables and partitions can carry extended statistics. */
	if (rte->relkind != RELKIND_RELATION &&
		rte->relkind != RELKIND_MATVIEW &&
		rte->relkind != RELKIND_PARTITIONED_TABLE)
		return;

	/* Never advise on the system catalogs. */
	if (rte->relid < FirstNormalObjectId)
		return;

	/*
	 * Extract the attributes referenced by every clause a statistics object
	 * could have estimated, mirroring the loop in
	 * statext_mcv_clauselist_selectivity().
	 *
	 * We accumulate all compatible columns, not just the ones left
	 * unestimated, but only report the combination if at least one compatible
	 * clause went unestimated.  That covers both cases with one rule: with no
	 * statistics at all, everything is unestimated and we report the whole
	 * set; with statistics on (a, b) and a query filtering on a, b and c, we
	 * report (a, b, c), which is the object that would actually help.
	 * Reporting the leftover {c} alone would be useless -- a single column is
	 * already covered by pg_statistic.
	 */
	listidx = 0;
	foreach(lc, clauses)
	{
		Node	   *clause = (Node *) lfirst(lc);
		Bitmapset  *clause_attnums = NULL;
		List	   *exprs = NIL;

		if (statext_is_compatible_clause(root, clause, rel->relid,
										 &clause_attnums, &exprs))
		{
			/*
			 * Clauses over expressions rather than plain columns need a
			 * statistics object built on the same expression, which we have
			 * no way to reconstruct here.  Ignore those clauses; the plain
			 * columns in the same list are still worth recording.
			 */
			if (exprs == NIL)
			{
				attnums = bms_add_members(attnums, clause_attnums);

				if (!bms_is_member(listidx, estimatedclauses))
					nleftover++;
			}

			if (all_equality && !clause_is_equality_shaped(clause))
				all_equality = false;
		}

		listidx++;
	}

	/* Existing statistics already covered everything we could advise on. */
	if (nleftover == 0)
		return;

	if (!autostats_init_key(&key, rte->relid, attnums))
		return;

	autostats_note_local(&key, all_equality);
}

/*
 * autostats_live_attname
 *		Name of a column, or NULL if it is gone.
 *
 * get_attname() cannot be used here: for a dropped column the pg_attribute
 * row survives and it returns the "........pg.dropped.N........" placeholder,
 * which would end up in the advisor output as if it were a real column.
 */
static char *
autostats_live_attname(Oid relid, int16 attnum)
{
	HeapTuple	tp;
	Form_pg_attribute att;
	char	   *result = NULL;

	tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
	if (!HeapTupleIsValid(tp))
		return NULL;

	att = (Form_pg_attribute) GETSTRUCT(tp);
	if (!att->attisdropped)
		result = pstrdup(NameStr(att->attname));

	ReleaseSysCache(tp);

	return result;
}

/*
 * autostats_build_ddl
 *		Render the CREATE STATISTICS command that would fix this combination.
 *
 * ndistinct and dependencies are cheap to build and maintain.  An MCV list
 * is the only kind that gives exact answers for common value combinations,
 * and also much the most expensive to build during ANALYZE, so it is
 * suggested only for narrow, equality-shaped combinations.
 */
static char *
autostats_build_ddl(const AutoStatsKey *key, bool all_equality,
					const char *nspname, const char *relname)
{
	StringInfoData buf;
	StringInfoData objname;
	bool		with_mcv = (all_equality && key->nattnums <= 4);
	int			i;

	/*
	 * Name the object after the attribute numbers rather than the column
	 * names: it stays inside NAMEDATALEN, it is stable across renames, and
	 * re-running the advisor produces the same name for the same column set.
	 */
	initStringInfo(&objname);
	appendStringInfoString(&objname, "dbblue_auto");
	appendStringInfo(&objname, "_%u", key->relid);
	for (i = 0; i < key->nattnums; i++)
		appendStringInfo(&objname, "_a%d", key->attnums[i]);

	initStringInfo(&buf);
	appendStringInfo(&buf, "CREATE STATISTICS %s", quote_identifier(objname.data));
	appendStringInfo(&buf, " (ndistinct, dependencies%s) ON ",
					 with_mcv ? ", mcv" : "");

	for (i = 0; i < key->nattnums; i++)
	{
		char	   *attname = autostats_live_attname(key->relid, key->attnums[i]);

		if (attname == NULL)
		{
			/* column dropped since we recorded it; no usable suggestion */
			pfree(buf.data);
			pfree(objname.data);
			return NULL;
		}

		if (i > 0)
			appendStringInfoString(&buf, ", ");
		appendStringInfoString(&buf, quote_identifier(attname));
	}

	appendStringInfo(&buf, " FROM %s.%s;",
					 quote_identifier(nspname), quote_identifier(relname));

	/*
	 * A statistics object holds no data until the table is analyzed, so the
	 * ANALYZE is part of the suggestion, not an afterthought.
	 */
	appendStringInfo(&buf, " ANALYZE %s.%s;",
					 quote_identifier(nspname), quote_identifier(relname));

	pfree(objname.data);

	return buf.data;
}

/*
 * dbblue_stats_advisor
 *		Report the column combinations estimated independently so far.
 */
Datum
dbblue_stats_advisor(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	HASH_SEQ_STATUS hash_seq;
	AutoStatsEntry *entry;

	if (AutoStatsHash == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("dbblue autostats shared memory is not initialized")));

	InitMaterializedSRF(fcinfo, 0);

	LWLockAcquire(&AutoStatsShared->lock, LW_SHARED);

	hash_seq_init(&hash_seq, AutoStatsHash);

	while ((entry = (AutoStatsEntry *) hash_seq_search(&hash_seq)) != NULL)
	{
		Datum		values[DBBLUE_ADVISOR_COLS];
		bool		nulls[DBBLUE_ADVISOR_COLS];
		Datum		attname_datums[STATS_MAX_DIMENSIONS];
		ArrayType  *attname_array;
		char	   *nspname;
		char	   *relname;
		char	   *ddl;
		int			nattnames = 0;
		int			i;

		/*
		 * Entries are keyed by database, and resolving names needs this
		 * backend's catalogs, so only report the current database.
		 */
		if (entry->key.dbid != MyDatabaseId)
			continue;

		relname = get_rel_name(entry->key.relid);
		if (relname == NULL)
			continue;			/* table dropped since we recorded it */

		nspname = get_namespace_name(get_rel_namespace(entry->key.relid));
		if (nspname == NULL)
			continue;

		for (i = 0; i < entry->key.nattnums; i++)
		{
			char	   *attname = autostats_live_attname(entry->key.relid,
														 entry->key.attnums[i]);

			if (attname == NULL)
				break;			/* column dropped */

			attname_datums[nattnames++] = CStringGetTextDatum(attname);
		}

		/*
		 * If any column has been dropped the recorded combination no longer
		 * describes anything anyone can query, so skip the whole entry rather
		 * than advising on a partial column list.
		 */
		if (nattnames != entry->key.nattnums)
			continue;

		attname_array = construct_array_builtin(attname_datums, nattnames,
												TEXTOID);

		memset(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(nspname);
		values[1] = CStringGetTextDatum(relname);
		values[2] = PointerGetDatum(attname_array);
		values[3] = Int32GetDatum(entry->key.nattnums);
		values[4] = Int64GetDatum((int64) entry->plan_count);
		values[5] = BoolGetDatum(entry->all_equality);
		values[6] = TimestampTzGetDatum(entry->first_seen);
		values[7] = TimestampTzGetDatum(entry->last_seen);

		ddl = autostats_build_ddl(&entry->key, entry->all_equality,
								  nspname, relname);
		if (ddl == NULL)
			nulls[8] = true;
		else
			values[8] = CStringGetTextDatum(ddl);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	LWLockRelease(&AutoStatsShared->lock);

	return (Datum) 0;
}

/*
 * dbblue_stats_advisor_reset
 *		Discard everything recorded so far.
 */
Datum
dbblue_stats_advisor_reset(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS hash_seq;
	AutoStatsEntry *entry;

	if (AutoStatsHash == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("dbblue autostats shared memory is not initialized")));

	LWLockAcquire(&AutoStatsShared->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, AutoStatsHash);
	while ((entry = (AutoStatsEntry *) hash_seq_search(&hash_seq)) != NULL)
		hash_search(AutoStatsHash, &entry->key, HASH_REMOVE, NULL);

	AutoStatsShared->n_dropped = 0;

	LWLockRelease(&AutoStatsShared->lock);

	PG_RETURN_VOID();
}
