/*-------------------------------------------------------------------------
 *
 * pg_dbblue_matview.h
 *	  DBblue: catalog for incremental refresh of materialized views
 *
 * One row per (matview, source_table) pair.  Stores the pre-generated
 * delta SQL for insert and delete operations so the trigger code can
 * retrieve and prepare them without re-deriving from the Query tree on
 * every invocation.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_dbblue_matview.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DBBLUE_MATVIEW_H
#define PG_DBBLUE_MATVIEW_H

#include "catalog/genbki.h"
#include "catalog/pg_dbblue_matview_d.h"	/* IWYU pragma: export */

/* ----------------
 *		pg_dbblue_matview definition.
 * ----------------
 */
BEGIN_CATALOG_STRUCT

CATALOG(pg_dbblue_matview,8330,DbblueMatviewRelationId)
{
	Oid		mvrelid BKI_LOOKUP(pg_class);	/* matview OID */
	Oid		srctable BKI_LOOKUP(pg_class);	/* source table OID */

#ifdef CATALOG_VARLEN
	text	ins_sql BKI_FORCE_NOT_NULL;		/* INSERT delta SQL */
	text	del_sql BKI_FORCE_NOT_NULL;		/* DELETE delta (UPDATE) SQL */
	text	cln_sql BKI_FORCE_NOT_NULL;		/* cleanup SQL: DELETE WHERE __mv_count__ <= 0 */
	text	having_sql;						/* HAVING update SQL (NULL if no HAVING) */
#endif
} FormData_pg_dbblue_matview;

END_CATALOG_STRUCT

typedef FormData_pg_dbblue_matview *Form_pg_dbblue_matview;

DECLARE_TOAST(pg_dbblue_matview, 8331, 8332);

DECLARE_UNIQUE_INDEX_PKEY(pg_dbblue_matview_mvrelid_srctable_index, 8333, DbblueMatviewIndexId, pg_dbblue_matview, btree(mvrelid oid_ops, srctable oid_ops));
DECLARE_INDEX(pg_dbblue_matview_mvrelid_index, 8334, DbblueMatviewMvrelidIndexId, pg_dbblue_matview, btree(mvrelid oid_ops));

#endif							/* PG_DBBLUE_MATVIEW_H */
