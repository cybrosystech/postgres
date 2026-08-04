/*-------------------------------------------------------------------------
 *
 * pg_dbblue_trusted_fkey.c
 *	  routines to support manipulation of the pg_dbblue_trusted_fkey catalog
 *
 * A foreign key records that referential integrity was *armed*, not that the
 * data satisfies it -- see pg_dbblue_trusted_fkey.h.  An optimization whose
 * result depends on the constraint actually holding therefore consults this
 * catalog, which records the constraints an administrator has accepted
 * responsibility for.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_dbblue_trusted_fkey.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_dbblue_trusted_fkey.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tuplestore.h"

/*
 * DbblueFkeyIsTrusted
 *		Has this foreign key been declared trustworthy?
 *
 * conrelid and conname must match what was recorded, so that a row left behind
 * by a dropped constraint cannot speak for a different constraint that later
 * reused its OID.
 *
 * This is consulted during planning, so it must not error out on a missing
 * catalog; a false answer merely costs the optimization.
 */
bool
DbblueFkeyIsTrusted(Oid conoid, Oid conrelid, const char *conname)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey;
	HeapTuple	tup;
	bool		result = false;

	if (!OidIsValid(conoid))
		return false;

	rel = table_open(DbblueTrustedFkeyRelationId, AccessShareLock);

	ScanKeyInit(&skey,
				Anum_pg_dbblue_trusted_fkey_conoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(conoid));

	scan = systable_beginscan(rel, DbblueTrustedFkeyIndexId, true,
							  NULL, 1, &skey);

	if (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_dbblue_trusted_fkey form =
			(Form_pg_dbblue_trusted_fkey) GETSTRUCT(tup);

		result = (form->conrelid == conrelid &&
				  strcmp(NameStr(form->conname), conname) == 0);
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return result;
}

/*
 * DbblueFkeyTrustRowExists
 *		Is there a trust declaration for this constraint OID at all?
 *
 * Unlike DbblueFkeyIsTrusted this does not verify the relation and name, which
 * is what withdrawal wants: removing a row left behind by a dropped constraint
 * is harmless, and matching on OID alone keeps this cheap enough for the write
 * path.
 */
bool
DbblueFkeyTrustRowExists(Oid conoid)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey;
	bool		found;

	if (!OidIsValid(conoid))
		return false;

	rel = table_open(DbblueTrustedFkeyRelationId, AccessShareLock);
	ScanKeyInit(&skey, Anum_pg_dbblue_trusted_fkey_conoid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(conoid));
	scan = systable_beginscan(rel, DbblueTrustedFkeyIndexId, true,
							  NULL, 1, &skey);
	found = HeapTupleIsValid(systable_getnext(scan));
	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return found;
}

/*
 * Whether this database has any trust declaration at all, cached for the
 * transaction: -1 not yet looked at, 0 none, 1 some.
 *
 * A database that has never granted trust -- the default, since nothing is
 * trusted until an administrator opts in -- can answer every bypass without
 * examining a single constraint.  Without this the per-constraint memo still
 * costs one catalog lookup per distinct constraint per transaction, which a
 * restore touching many tables pays over and over for no possible benefit.
 */
static int	dbblue_any_trusted = -1;

static bool
dbblue_any_trust_declared(void)
{
	Relation	rel;
	SysScanDesc scan;

	if (dbblue_any_trusted >= 0)
		return dbblue_any_trusted > 0;

	rel = table_open(DbblueTrustedFkeyRelationId, AccessShareLock);
	scan = systable_beginscan(rel, DbblueTrustedFkeyIndexId, true, NULL, 0, NULL);
	dbblue_any_trusted = HeapTupleIsValid(systable_getnext(scan)) ? 1 : 0;
	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return dbblue_any_trusted > 0;
}

/*
 * dbblue_fkey_set_trusted
 *		Record or remove a trust declaration.  Returns true if anything changed.
 */
static bool
dbblue_fkey_set_trusted(Oid conoid, Oid conrelid, const char *conname,
						bool trusted, bool verified)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey;
	HeapTuple	tup;
	bool		changed = false;

	rel = table_open(DbblueTrustedFkeyRelationId, RowExclusiveLock);

	ScanKeyInit(&skey,
				Anum_pg_dbblue_trusted_fkey_conoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(conoid));
	scan = systable_beginscan(rel, DbblueTrustedFkeyIndexId, true,
							  NULL, 1, &skey);
	tup = systable_getnext(scan);

	if (!trusted)
	{
		if (HeapTupleIsValid(tup))
		{
			CatalogTupleDelete(rel, &tup->t_self);
			changed = true;
		}
	}
	else
	{
		Datum		values[Natts_pg_dbblue_trusted_fkey];
		bool		nulls[Natts_pg_dbblue_trusted_fkey];
		NameData	nameval;
		HeapTuple	newtup;

		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));
		values[Anum_pg_dbblue_trusted_fkey_conoid - 1] = ObjectIdGetDatum(conoid);
		values[Anum_pg_dbblue_trusted_fkey_conrelid - 1] = ObjectIdGetDatum(conrelid);
		namestrcpy(&nameval, conname);
		values[Anum_pg_dbblue_trusted_fkey_conname - 1] = NameGetDatum(&nameval);
		values[Anum_pg_dbblue_trusted_fkey_converified - 1] = BoolGetDatum(verified);

		newtup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

		if (HeapTupleIsValid(tup))
		{
			newtup->t_self = tup->t_self;
			CatalogTupleUpdate(rel, &newtup->t_self, newtup);
		}
		else
			CatalogTupleInsert(rel, newtup);

		heap_freetuple(newtup);
		changed = true;
		/* a bypass later in this transaction must now be able to see it */
		dbblue_any_trusted = 1;
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();

	return changed;
}

/*
 * Constraints whose referential-integrity trigger was skipped during this
 * transaction, and which were trusted at the time.  Their trust declarations
 * are withdrawn when the transaction commits: whatever the transaction wrote
 * was not checked against them.
 */
static List *dbblue_pending_untrust = NIL;

/*
 * Constraints already examined this transaction, whether or not they turned
 * out to be trusted.  Without this a bypass that finds nothing to withdraw
 * would re-examine the catalog for every row: a restore run with triggers
 * disabled skips an RI trigger per foreign key per row, and paid a catalog
 * lookup for each one.
 */
static List *dbblue_bypass_seen = NIL;
static bool dbblue_untrust_callback_registered = false;

static bool
dbblue_fkey_delete_row(Oid conoid)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData skey;
	HeapTuple	tup;
	bool		deleted = false;

	rel = table_open(DbblueTrustedFkeyRelationId, RowExclusiveLock);
	ScanKeyInit(&skey, Anum_pg_dbblue_trusted_fkey_conoid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(conoid));
	scan = systable_beginscan(rel, DbblueTrustedFkeyIndexId, true,
							  NULL, 1, &skey);
	if (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		CatalogTupleDelete(rel, &tup->t_self);
		deleted = true;
	}
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return deleted;
}

static void
dbblue_untrust_at_xact_end(XactEvent event, void *arg)
{
	List	   *pending = dbblue_pending_untrust;
	ListCell   *lc;

	/*
	 * Both lists live in TopTransactionContext, so they are freed underneath
	 * us at end of transaction.  Drop the pointers on every event we handle,
	 * never conditionally, or the next transaction reads freed memory.
	 */
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
			/* still safe to write catalogs here */
			dbblue_pending_untrust = NIL;
			dbblue_bypass_seen = NIL;
			dbblue_any_trusted = -1;
			foreach(lc, pending)
			{
				if (dbblue_fkey_delete_row(lfirst_oid(lc)))
					ereport(LOG,
							(errmsg("DBblue: withdrew the trust declaration for "
									"foreign key %u", lfirst_oid(lc)),
							 errdetail("Its referential-integrity trigger did not "
									   "fire for a change made in this transaction, "
									   "so the constraint may no longer hold."),
							 errhint("Run dbblue_trust_foreign_keys() to re-verify "
									 "and restore it.")));
			}
			if (pending != NIL)
				CommandCounterIncrement();
			break;
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			/* nothing was written, so nothing to withdraw */
			dbblue_pending_untrust = NIL;
			dbblue_bypass_seen = NIL;
			dbblue_any_trusted = -1;
			break;
		default:
			break;
	}
}

/*
 * DbblueFkeyNoteBypass
 *		Note that a foreign key's RI trigger was skipped.
 *
 * Called from the trigger machinery when a constraint trigger is passed over
 * because of session_replication_role or because it has been disabled.  Rows
 * written in that window were never checked, so any trust declaration for the
 * constraint stops being warranted and is withdrawn at commit.
 *
 * This runs on the write path, so it must stay cheap and must not error.  Once
 * a constraint is no longer trusted there is nothing left to do, which is what
 * keeps the cost off replication apply after the first transaction.
 */
void
DbblueFkeyNoteBypass(Oid conoid)
{
	MemoryContext oldcxt;

	if (!OidIsValid(conoid))
		return;

	/*
	 * If nothing in this database is trusted there is nothing any bypass can
	 * withdraw, so answer without looking at the constraint at all.  This is
	 * the default state and the one a restore runs in.
	 */
	if (!dbblue_any_trust_declared())
		return;

	/*
	 * Otherwise one catalog lookup per constraint per transaction, no matter
	 * how many rows go by.  Record the constraint as examined before deciding,
	 * so that "not trusted" is remembered too.
	 */
	if (list_member_oid(dbblue_bypass_seen, conoid))
		return;

	oldcxt = MemoryContextSwitchTo(TopTransactionContext);
	dbblue_bypass_seen = lappend_oid(dbblue_bypass_seen, conoid);
	MemoryContextSwitchTo(oldcxt);

	/* not trusted: nothing to withdraw */
	if (!DbblueFkeyTrustRowExists(conoid))
		return;

	oldcxt = MemoryContextSwitchTo(TopTransactionContext);
	dbblue_pending_untrust = lappend_oid(dbblue_pending_untrust, conoid);
	MemoryContextSwitchTo(oldcxt);

	if (!dbblue_untrust_callback_registered)
	{
		RegisterXactCallback(dbblue_untrust_at_xact_end, NULL);
		dbblue_untrust_callback_registered = true;
	}
}

/*
 * dbblue_fkey_count_violations
 *		How many rows of the referencing table have no referenced row?
 *
 * Deliberately phrased as NOT EXISTS rather than the LEFT JOIN ... IS NULL form
 * that RI_Initial_Check() uses, so that the answer cannot be influenced by the
 * very optimization this trust declaration is meant to license.
 */
static int64
dbblue_fkey_count_violations(HeapTuple contup)
{
	Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(contup);
	AttrNumber	conkey[INDEX_MAX_KEYS];
	AttrNumber	confkey[INDEX_MAX_KEYS];
	int			numkeys;
	StringInfoData buf;
	char	   *childname;
	char	   *parentname;
	int64		violations;
	int			ret;

	DeconstructFkConstraintRow(contup, &numkeys, conkey, confkey,
							   NULL, NULL, NULL, NULL, NULL);

	childname = quote_qualified_identifier(
		get_namespace_name(get_rel_namespace(con->conrelid)),
		get_rel_name(con->conrelid));
	parentname = quote_qualified_identifier(
		get_namespace_name(get_rel_namespace(con->confrelid)),
		get_rel_name(con->confrelid));

	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT count(*) FROM %s c WHERE ", childname);

	/*
	 * Only rows whose key is entirely non-null are required to match: under
	 * MATCH SIMPLE any NULL exempts the row, and under MATCH FULL a wholly
	 * NULL key does.  Checking the fully non-null rows is correct either way.
	 */
	for (int i = 0; i < numkeys; i++)
		appendStringInfo(&buf, "%sc.%s IS NOT NULL",
						 i ? " AND " : "",
						 quote_identifier(get_attname(con->conrelid,
													  conkey[i], false)));

	appendStringInfo(&buf, " AND NOT EXISTS (SELECT 1 FROM %s p WHERE ",
					 parentname);
	for (int i = 0; i < numkeys; i++)
		appendStringInfo(&buf, "%sp.%s = c.%s",
						 i ? " AND " : "",
						 quote_identifier(get_attname(con->confrelid,
													  confkey[i], false)),
						 quote_identifier(get_attname(con->conrelid,
													  conkey[i], false)));
	appendStringInfoString(&buf, ")");

	if ((ret = SPI_connect()) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(ret));

	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "unexpected result from foreign key verification query");

	{
		bool		isnull;

		violations = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												SPI_tuptable->tupdesc, 1,
												&isnull));
		if (isnull)
			violations = 0;
	}

	SPI_finish();
	pfree(buf.data);

	return violations;
}

/*
 * Shared implementation of dbblue_trust_foreign_keys() and
 * dbblue_untrust_foreign_keys().
 */
static Datum
dbblue_fkey_trust_worker(FunctionCallInfo fcinfo, Oid onerel, bool trust,
						 bool verify)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	conrel;
	SysScanDesc scan;
	ScanKeyData skey;
	HeapTuple	tup;
	List	   *candidates = NIL;
	ListCell   *lc;

	InitMaterializedSRF(fcinfo, 0);

	/*
	 * A whole-database pass also tidies up: dropping a constraint leaves its
	 * declaration behind, harmlessly, since a declaration only speaks for the
	 * relation and name it recorded.  Clear those out while we are here.
	 */
	if (!OidIsValid(onerel))
	{
		Relation	trel;
		SysScanDesc tscan;
		HeapTuple	ttup;
		List	   *stale = NIL;

		trel = table_open(DbblueTrustedFkeyRelationId, AccessShareLock);
		tscan = systable_beginscan(trel, InvalidOid, false, NULL, 0, NULL);
		while (HeapTupleIsValid(ttup = systable_getnext(tscan)))
		{
			Form_pg_dbblue_trusted_fkey trow =
				(Form_pg_dbblue_trusted_fkey) GETSTRUCT(ttup);

			if (!SearchSysCacheExists1(CONSTROID,
									   ObjectIdGetDatum(trow->conoid)))
				stale = lappend_oid(stale, trow->conoid);
		}
		systable_endscan(tscan);
		table_close(trel, AccessShareLock);

		foreach(lc, stale)
			dbblue_fkey_delete_row(lfirst_oid(lc));
		if (stale != NIL)
			CommandCounterIncrement();
		list_free(stale);
	}

	/*
	 * Collect the constraints of interest first and close the scan before
	 * doing any work on them.  Verifying runs a full query through SPI and
	 * recording the result writes another catalog, neither of which should
	 * happen underneath an open scan of pg_constraint.
	 */
	conrel = table_open(ConstraintRelationId, AccessShareLock);

	if (OidIsValid(onerel))
	{
		ScanKeyInit(&skey, Anum_pg_constraint_conrelid,
					BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(onerel));
		scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId,
								  true, NULL, 1, &skey);
	}
	else
		scan = systable_beginscan(conrel, InvalidOid, false, NULL, 0, NULL);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tup);

		if (con->contype != CONSTRAINT_FOREIGN)
			continue;
		/* a clone describes only its own partition; trust the parent instead */
		if (OidIsValid(con->conparentid))
			continue;
		/* only the owner may speak for a table's integrity */
		if (!object_ownercheck(RelationRelationId, con->conrelid, GetUserId()))
			continue;

		candidates = lappend_oid(candidates, con->oid);
	}

	systable_endscan(scan);
	table_close(conrel, AccessShareLock);

	foreach(lc, candidates)
	{
		Oid			conoid = lfirst_oid(lc);
		HeapTuple	contup;
		Form_pg_constraint con;
		Oid			conrelid;
		Oid			confrelid;
		NameData	conname;
		Datum		values[5];
		bool		nulls[5] = {false, false, false, false, false};
		int64		violations = -1;
		const char *action;

		contup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid));
		if (!HeapTupleIsValid(contup))
			continue;			/* vanished under us */
		con = (Form_pg_constraint) GETSTRUCT(contup);
		conrelid = con->conrelid;
		confrelid = con->confrelid;
		namestrcpy(&conname, NameStr(con->conname));

		if (trust && verify)
			violations = dbblue_fkey_count_violations(contup);

		ReleaseSysCache(contup);

		if (!trust)
		{
			action = dbblue_fkey_set_trusted(conoid, conrelid,
											 NameStr(conname), false, false)
				? "untrusted" : "was not trusted";
		}
		else if (violations > 0)
		{
			/* refuse, and clear any declaration that is no longer warranted */
			dbblue_fkey_set_trusted(conoid, conrelid, NameStr(conname),
									false, false);
			action = "refused: violating rows found";
		}
		else
		{
			dbblue_fkey_set_trusted(conoid, conrelid, NameStr(conname),
									true, verify);
			action = verify ? "trusted (verified)" : "trusted (asserted)";
		}

		values[0] = ObjectIdGetDatum(conrelid);
		values[1] = NameGetDatum(&conname);
		values[2] = ObjectIdGetDatum(confrelid);
		values[3] = CStringGetTextDatum(action);
		if (violations < 0)
			nulls[4] = true;
		else
			values[4] = Int64GetDatum(violations);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	list_free(candidates);

	return (Datum) 0;
}

Datum
dbblue_trust_foreign_keys(PG_FUNCTION_ARGS)
{
	return dbblue_fkey_trust_worker(fcinfo, InvalidOid, true, true);
}

Datum
dbblue_trust_foreign_keys_rel(PG_FUNCTION_ARGS)
{
	return dbblue_fkey_trust_worker(fcinfo, PG_GETARG_OID(0), true, true);
}

Datum
dbblue_untrust_foreign_keys(PG_FUNCTION_ARGS)
{
	return dbblue_fkey_trust_worker(fcinfo, InvalidOid, false, false);
}

Datum
dbblue_untrust_foreign_keys_rel(PG_FUNCTION_ARGS)
{
	return dbblue_fkey_trust_worker(fcinfo, PG_GETARG_OID(0), false, false);
}
