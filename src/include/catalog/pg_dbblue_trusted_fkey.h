/*-------------------------------------------------------------------------
 *
 * pg_dbblue_trusted_fkey.h
 *	  DBblue: foreign keys the administrator declares trustworthy
 *
 * A foreign key is enforced by triggers, so the catalog's record that one is
 * validated and enforced says that checking was *armed*, not that the data
 * satisfies it: session_replication_role = replica, ALTER TABLE ... DISABLE
 * TRIGGER, and a BEFORE trigger that cancels a cascaded delete all admit rows
 * the constraint forbids while leaving it validated and enforced.  Nothing the
 * planner can read distinguishes that from a clean database.
 *
 * PostgreSQL therefore uses foreign keys only where being wrong costs
 * performance rather than correctness -- selectivity estimation.  An
 * optimization that changes results needs a stronger warrant, so it takes one
 * from here: a row in this catalog means someone has declared this particular
 * constraint trustworthy, ideally after dbblue_trust_foreign_keys() scanned the
 * data and found no violating rows.  This mirrors RELY constraints in other
 * systems, where an informational constraint may drive a rewrite only once the
 * administrator has accepted responsibility for it.
 *
 * conrelid and conname are stored alongside conoid so that a stale row -- one
 * left behind by a dropped constraint whose OID was later reused -- cannot be
 * mistaken for a trust declaration about a different constraint.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_dbblue_trusted_fkey.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DBBLUE_TRUSTED_FKEY_H
#define PG_DBBLUE_TRUSTED_FKEY_H

#include "catalog/genbki.h"
#include "catalog/pg_dbblue_trusted_fkey_d.h"	/* IWYU pragma: export */

/* ----------------
 *		pg_dbblue_trusted_fkey definition.
 * ----------------
 */
BEGIN_CATALOG_STRUCT

CATALOG(pg_dbblue_trusted_fkey,8336,DbblueTrustedFkeyRelationId)
{
	Oid			conoid BKI_LOOKUP(pg_constraint);	/* the FK constraint */
	Oid			conrelid BKI_LOOKUP(pg_class);	/* its referencing relation */
	NameData	conname;		/* its name, to catch OID reuse */
	bool		converified;	/* was the data actually scanned? */
} FormData_pg_dbblue_trusted_fkey;

END_CATALOG_STRUCT

typedef FormData_pg_dbblue_trusted_fkey *Form_pg_dbblue_trusted_fkey;

DECLARE_UNIQUE_INDEX_PKEY(pg_dbblue_trusted_fkey_conoid_index, 8337, DbblueTrustedFkeyIndexId, pg_dbblue_trusted_fkey, btree(conoid oid_ops));

extern bool DbblueFkeyIsTrusted(Oid conoid, Oid conrelid, const char *conname);

#endif							/* PG_DBBLUE_TRUSTED_FKEY_H */
