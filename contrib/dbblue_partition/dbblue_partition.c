/*-------------------------------------------------------------------------
 * dbblue_partition.c
 *
 * Native PostgreSQL extension: table partitioning for Odoo/dbblue workloads.
 *
 * Replaces the Odoo Python 'partition_manager' module with a C extension
 * that runs entirely inside PostgreSQL via SPI.  All behaviour is controlled
 * through GUC parameters prefixed with "dbblue_partition.".
 *
 * GUC parameters
 * ──────────────
 *   dbblue_partition.enabled          bool    false
 *       Gate switch; functions raise ERROR when false.
 *
 *   dbblue_partition.default_strategy string  'monthly'
 *       Used when the caller passes NULL for the strategy argument.
 *       Accepted values: 'monthly', 'yearly'.
 *
 *   dbblue_partition.advance_count    int     3
 *       Number of future periods to pre-create after partitioning.
 *       Also used by dbblue_ensure_partitions() when advance is omitted.
 *
 *   dbblue_partition.backup_suffix    string  '_backup'
 *       Suffix appended to the original table name for the backup copy.
 *
 * SQL interface (defined in dbblue_partition--1.0.sql)
 * ─────────────────────────────────────────────────────
 *   dbblue_partition_table(table, field [, strategy])  → text
 *   dbblue_drop_backup(table)                          → void
 *   dbblue_ensure_partitions(table [, advance])        → int
 *   dbblue_partition_info(table)                       → setof record
 *
 * contrib/dbblue_partition/dbblue_partition.c
 *
 * Copyright (c) 2026, Cybrosys Technologies
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC_EXT(
					.name = "dbblue_partition",
					.version = PG_VERSION
);

/* ── GUC backing variables ───────────────────────────────────────────── */
static bool  dbblue_partition_enabled          = false;
static char *dbblue_partition_default_strategy = NULL;
static int   dbblue_partition_advance_count    = 3;
static char *dbblue_partition_backup_suffix    = NULL;

/* GUCs for dbblue_auto_partition() – set these instead of passing args */
static char *dbblue_partition_table_guc  = NULL;
static char *dbblue_partition_column_guc = NULL;
static char *dbblue_partition_range_guc  = NULL;

/* ── Internal FK capture structures ──────────────────────────────────── */
typedef struct
{
	char	   *column_name;
	char	   *foreign_table;
	char	   *foreign_column;
	char	   *delete_rule;
	char	   *update_rule;
} OutgoingFK;

typedef struct
{
	char	   *source_table;
	char	   *constraint_name;
	char	   *local_column;
	char	   *referenced_column;
	char	   *delete_rule;
	char	   *update_rule;
} IncomingFK;

typedef struct
{
	char	   *partition_name;
	char	   *start_ts;
	char	   *end_ts;
} PartitionPeriod;

typedef struct
{
	char	   *view_name;
	char	   *view_def;
} DependentView;

typedef struct
{
	char	   *index_name;
	char	   *index_def;
} IndexDef;

/* ── Forward declarations ────────────────────────────────────────────── */
void		_PG_init(void);
PG_FUNCTION_INFO_V1(dbblue_partition_table);
PG_FUNCTION_INFO_V1(dbblue_drop_backup);
PG_FUNCTION_INFO_V1(dbblue_ensure_partitions);
PG_FUNCTION_INFO_V1(dbblue_partition_info);
PG_FUNCTION_INFO_V1(dbblue_auto_partition);

/* ── Helpers ─────────────────────────────────────────────────────────── */

/*
 * Execute SQL via SPI.  Aborts the transaction on failure.
 * ok_code is the SPI_OK_* constant for the expected operation type.
 * Pass -1 to skip the result-type check (for DDL statements where
 * SPI_OK_UTILITY is returned).
 */
static void
spi_exec(const char *sql, int ok_code)
{
	int			ret = SPI_execute(sql, false, 0);

	if (ret < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("dbblue_partition: SPI error %d", ret),
				 errdetail("SQL: %s", sql)));
	if (ok_code >= 0 && ret != ok_code)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("dbblue_partition: unexpected SPI result %d (expected %d)",
						ret, ok_code),
				 errdetail("SQL: %s", sql)));
}

/*
 * Execute a read-only SELECT via SPI.  Returns SPI_processed.
 */
static uint64
spi_select(const char *sql)
{
	int ret = SPI_execute(sql, true, 0);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("dbblue_partition: SELECT failed"),
				 errdetail("SQL: %s", sql)));
	return SPI_processed;
}

/*
 * Copy a nullable SPI column value into caller_ctx.
 * Returns NULL if the column is NULL.
 */
static char *
spi_getval(HeapTuple tup, TupleDesc desc, int col, MemoryContext caller_ctx)
{
	char	   *raw  = SPI_getvalue(tup, desc, col);
	char	   *copy;

	if (!raw)
		return NULL;
	/* SPI_getvalue pallocs in CurrentMemoryContext (= SPI ctx after connect).
	 * Copy into caller's context so it survives the next SPI_execute. */
	copy = MemoryContextStrdup(caller_ctx, raw);
	pfree(raw);
	return copy;
}


/* ── GUC registration ────────────────────────────────────────────────── */

void
_PG_init(void)
{
	DefineCustomBoolVariable(
							 "dbblue_partition.enabled",
							 "Enable dbblue partition management functions.",
							 "When false (default) dbblue_partition_table() raises an error.",
							 &dbblue_partition_enabled,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable(
							   "dbblue_partition.default_strategy",
							   "Default partitioning strategy when not supplied by caller.",
							   "Accepted values: 'monthly', 'yearly'.",
							   &dbblue_partition_default_strategy,
							   "monthly",
							   PGC_USERSET,
							   0,
							   NULL, NULL, NULL);

	DefineCustomIntVariable(
							"dbblue_partition.advance_count",
							"Future periods to pre-create partitions for.",
							"Used by dbblue_ensure_partitions() when advance is not specified.",
							&dbblue_partition_advance_count,
							3,
							0, 24,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomStringVariable(
							   "dbblue_partition.backup_suffix",
							   "Suffix appended to the original table name for the backup.",
							   NULL,
							   &dbblue_partition_backup_suffix,
							   "_backup",
							   PGC_USERSET,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable(
								   "dbblue_partition.table",
								   "Target table for dbblue_auto_partition().",
								   "Set to the unquoted table name before calling dbblue_auto_partition().",
								   &dbblue_partition_table_guc,
								   "",
								   PGC_USERSET,
								   0,
								   NULL, NULL, NULL);

	DefineCustomStringVariable(
								   "dbblue_partition.column",
								   "Partition column for dbblue_auto_partition().",
								   "Must be a NOT NULL date/timestamp column on dbblue_partition.table.",
								   &dbblue_partition_column_guc,
								   "",
								   PGC_USERSET,
								   0,
								   NULL, NULL, NULL);

	DefineCustomStringVariable(
								   "dbblue_partition.range",
								   "Partition granularity for dbblue_auto_partition().",
								   "Accepted values: 'monthly' (default) or 'yearly'.",
								   &dbblue_partition_range_guc,
								   "monthly",
								   PGC_USERSET,
								   0,
								   NULL, NULL, NULL);

	MarkGUCPrefixReserved("dbblue_partition");
}


/* ═══════════════════════════════════════════════════════════════════════
 * dbblue_partition_table(p_table text, p_field text, p_strategy text)
 *
 * Main entry point.  Converts an ordinary Odoo table into a
 * range-partitioned table, preserving all FK relationships and fixing
 * sequence ownership so that the backup table can be dropped cleanly.
 * ═══════════════════════════════════════════════════════════════════════ */
Datum
dbblue_partition_table(PG_FUNCTION_ARGS)
{
	char	   *table_name;
	char	   *field_name;
	char	   *strategy;
	char	   *backup_table;
	char	   *part_table;		/* intermediate partitioned table name */
	char	   *seq_name;

	OutgoingFK *out_fks   = NULL;
	int			n_out_fks = 0;
	IncomingFK *in_fks    = NULL;
	int			n_in_fks  = 0;
	PartitionPeriod *periods = NULL;
	int			n_periods = 0;
	DependentView *dep_views = NULL;
	int			n_dep_views = 0;

	MemoryContext caller_ctx;
	StringInfoData sql;
	uint64		nrows;
	int			i;

	/* ── Guard ──────────────────────────────────────────────────────── */
	if (!dbblue_partition_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("dbblue_partition: feature is disabled"),
				 errhint("SET dbblue_partition.enabled = on")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("dbblue_partition: table_name and partition_field must not be NULL")));

	table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	field_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
	strategy   = (!PG_ARGISNULL(2))
				 ? text_to_cstring(PG_GETARG_TEXT_PP(2))
				 : dbblue_partition_default_strategy;

	if (strcmp(strategy, "monthly") != 0 && strcmp(strategy, "yearly") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dbblue_partition: invalid strategy \"%s\"", strategy),
				 errhint("Valid values: 'monthly', 'yearly'")));

	/* Derived names */
	backup_table = psprintf("%s%s", table_name, dbblue_partition_backup_suffix);
	part_table   = psprintf("%s_partitioned", table_name);
	seq_name     = psprintf("%s_id_seq", table_name);

	caller_ctx = CurrentMemoryContext;

	/* ── Open SPI ───────────────────────────────────────────────────── */
	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errmsg("dbblue_partition: SPI_connect failed")));

	initStringInfo(&sql);

	/* ── Step 1a: Capture outgoing FKs (table → other tables) ──────── */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT a.attname,"
		"       ref_cl.relname,"
		"       ref_a.attname,"
		"       CASE con.confdeltype"
		"           WHEN 'a' THEN 'NO ACTION'  WHEN 'r' THEN 'RESTRICT'"
		"           WHEN 'c' THEN 'CASCADE'    WHEN 'n' THEN 'SET NULL'"
		"           WHEN 'd' THEN 'SET DEFAULT' END,"
		"       CASE con.confupdtype"
		"           WHEN 'a' THEN 'NO ACTION'  WHEN 'r' THEN 'RESTRICT'"
		"           WHEN 'c' THEN 'CASCADE'    WHEN 'n' THEN 'SET NULL'"
		"           WHEN 'd' THEN 'SET DEFAULT' END"
		" FROM  pg_constraint con"
		" JOIN  pg_class     cl     ON cl.oid     = con.conrelid"
		" JOIN  pg_class     ref_cl ON ref_cl.oid = con.confrelid"
		" JOIN  pg_namespace nsp    ON nsp.oid    = cl.relnamespace"
		" CROSS JOIN LATERAL unnest(con.conkey, con.confkey) AS cols(la, ra)"
		" JOIN  pg_attribute a     ON a.attrelid     = con.conrelid    AND a.attnum  = cols.la"
		" JOIN  pg_attribute ref_a ON ref_a.attrelid = con.confrelid   AND ref_a.attnum = cols.ra"
		" WHERE con.contype = 'f'"
		"   AND cl.relname  = '%s'"
		"   AND nsp.nspname = 'public'"
		" ORDER BY a.attname",
		table_name);

	nrows = spi_select(sql.data);
	if (nrows > 0)
	{
		out_fks = MemoryContextAlloc(caller_ctx, nrows * sizeof(OutgoingFK));
		for (i = 0; i < (int) nrows; i++)
		{
			HeapTuple	tup  = SPI_tuptable->vals[i];
			TupleDesc	desc = SPI_tuptable->tupdesc;

			out_fks[i].column_name    = spi_getval(tup, desc, 1, caller_ctx);
			out_fks[i].foreign_table  = spi_getval(tup, desc, 2, caller_ctx);
			out_fks[i].foreign_column = spi_getval(tup, desc, 3, caller_ctx);
			out_fks[i].delete_rule    = spi_getval(tup, desc, 4, caller_ctx);
			out_fks[i].update_rule    = spi_getval(tup, desc, 5, caller_ctx);
		}
		n_out_fks = (int) nrows;
	}

	/* ── Step 1b: Capture incoming FKs (other tables → table) ──────── */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT src_cl.relname,"
		"       con.conname,"
		"       a.attname,"
		"       ref_a.attname,"
		"       CASE con.confdeltype"
		"           WHEN 'a' THEN 'NO ACTION'  WHEN 'r' THEN 'RESTRICT'"
		"           WHEN 'c' THEN 'CASCADE'    WHEN 'n' THEN 'SET NULL'"
		"           WHEN 'd' THEN 'SET DEFAULT' END,"
		"       CASE con.confupdtype"
		"           WHEN 'a' THEN 'NO ACTION'  WHEN 'r' THEN 'RESTRICT'"
		"           WHEN 'c' THEN 'CASCADE'    WHEN 'n' THEN 'SET NULL'"
		"           WHEN 'd' THEN 'SET DEFAULT' END"
		" FROM  pg_constraint con"
		" JOIN  pg_class     src_cl ON src_cl.oid = con.conrelid"
		" JOIN  pg_class     ref_cl ON ref_cl.oid = con.confrelid"
		" JOIN  pg_namespace nsp    ON nsp.oid    = ref_cl.relnamespace"
		" CROSS JOIN LATERAL unnest(con.conkey, con.confkey) AS cols(la, ra)"
		" JOIN  pg_attribute a     ON a.attrelid     = con.conrelid    AND a.attnum  = cols.la"
		" JOIN  pg_attribute ref_a ON ref_a.attrelid = con.confrelid   AND ref_a.attnum = cols.ra"
		" WHERE con.contype  = 'f'"
		"   AND ref_cl.relname = '%s'"
		"   AND nsp.nspname  = 'public'"
		" ORDER BY src_cl.relname, con.conname",
		table_name);

	nrows = spi_select(sql.data);
	if (nrows > 0)
	{
		in_fks = MemoryContextAlloc(caller_ctx, nrows * sizeof(IncomingFK));
		for (i = 0; i < (int) nrows; i++)
		{
			HeapTuple	tup  = SPI_tuptable->vals[i];
			TupleDesc	desc = SPI_tuptable->tupdesc;

			in_fks[i].source_table      = spi_getval(tup, desc, 1, caller_ctx);
			in_fks[i].constraint_name   = spi_getval(tup, desc, 2, caller_ctx);
			in_fks[i].local_column      = spi_getval(tup, desc, 3, caller_ctx);
			in_fks[i].referenced_column = spi_getval(tup, desc, 4, caller_ctx);
			in_fks[i].delete_rule       = spi_getval(tup, desc, 5, caller_ctx);
			in_fks[i].update_rule       = spi_getval(tup, desc, 6, caller_ctx);
		}
		n_in_fks = (int) nrows;
	}

	/* ── Step 1c: Capture dependent views ──────────────────────────── */
	/*
	 * Views that reference table_name must be dropped and recreated after
	 * the rename swap.  Capture their definitions now (they still reference
	 * table_name) so recreating them automatically points at the new
	 * partitioned table.
	 */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"WITH RECURSIVE vd AS ("
		"  SELECT DISTINCT c.oid AS void, c.relname AS vname, 0 AS d"
		"  FROM pg_class c"
		"  JOIN pg_namespace cn  ON cn.oid  = c.relnamespace"
		"  JOIN pg_rewrite   r   ON r.ev_class = c.oid"
		"  JOIN pg_depend    dep ON dep.objid = r.oid"
		"  JOIN pg_class     t   ON t.oid   = dep.refobjid"
		"  JOIN pg_namespace tn  ON tn.oid  = t.relnamespace"
		"  WHERE c.relkind = 'v'"
		"    AND t.relname  = '%s'"
		"    AND tn.nspname = 'public'"
		"    AND cn.nspname = 'public'"
		"    AND dep.deptype = 'n'"
		"  UNION ALL"
		"  SELECT DISTINCT c.oid, c.relname, v.d + 1"
		"  FROM pg_class     c"
		"  JOIN pg_namespace cn  ON cn.oid  = c.relnamespace"
		"  JOIN pg_rewrite   r   ON r.ev_class = c.oid"
		"  JOIN pg_depend    dep ON dep.objid = r.oid"
		"  JOIN vd           v   ON v.void   = dep.refobjid"
		"  WHERE c.relkind = 'v'"
		"    AND cn.nspname = 'public'"
		"    AND dep.deptype = 'n'"
		")"
		"SELECT DISTINCT ON (void)"
		"  vname,"
		"  pg_get_viewdef(void, true)"
		" FROM vd ORDER BY void, d",
		table_name);

	nrows = spi_select(sql.data);
	if (nrows > 0)
	{
		dep_views = MemoryContextAlloc(caller_ctx, nrows * sizeof(DependentView));
		for (i = 0; i < (int) nrows; i++)
		{
			HeapTuple	tup  = SPI_tuptable->vals[i];
			TupleDesc	desc = SPI_tuptable->tupdesc;

			dep_views[i].view_name = spi_getval(tup, desc, 1, caller_ctx);
			dep_views[i].view_def  = spi_getval(tup, desc, 2, caller_ctx);
		}
		n_dep_views = (int) nrows;
	}

	/* ── Step 2: Create partitioned parent table ────────────────────── */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"CREATE TABLE IF NOT EXISTS %s ("
		"    LIKE %s"
		"        INCLUDING DEFAULTS"
		"        INCLUDING STORAGE"
		"        INCLUDING COMMENTS"
		"        EXCLUDING CONSTRAINTS,"
		"    PRIMARY KEY (id, %s)"
		") PARTITION BY RANGE (%s)",
		part_table, table_name, field_name, field_name);
	spi_exec(sql.data, SPI_OK_UTILITY);

	/* ── Step 3: Detect existing data periods ───────────────────────── */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT DISTINCT date_trunc('%s', %s) AS period"
		" FROM %s"
		" ORDER BY period",
		(strcmp(strategy, "monthly") == 0) ? "month" : "year",
		field_name,
		table_name);

	nrows = spi_select(sql.data);

	if (nrows > 0)
	{
		periods = MemoryContextAlloc(caller_ctx, nrows * sizeof(PartitionPeriod));
		for (i = 0; i < (int) nrows; i++)
		{
			HeapTuple	tup     = SPI_tuptable->vals[i];
			TupleDesc	desc    = SPI_tuptable->tupdesc;
			char	   *period  = spi_getval(tup, desc, 1, caller_ctx);

			if (!period)
				ereport(ERROR,
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						 errmsg("dbblue_partition: NULL value found in partition field \"%s\"",
								field_name),
						 errhint("Ensure the partition field has no NULL values before partitioning.")));

			if (strcmp(strategy, "monthly") == 0)
			{
				int y, m, y_end, m_end;

				/* period format from date_trunc: 'YYYY-MM-DD HH24:MI:SS+TZ' */
				if (sscanf(period, "%d-%d-", &y, &m) != 2)
					ereport(ERROR,
							(errmsg("dbblue_partition: cannot parse period \"%s\"", period)));

				m_end = (m == 12) ? 1 : m + 1;
				y_end = (m == 12) ? y + 1 : y;

				periods[i].partition_name = MemoryContextStrdup(caller_ctx,
					psprintf("%s_%04d_%02d", part_table, y, m));
				periods[i].start_ts = MemoryContextStrdup(caller_ctx,
					psprintf("%04d-%02d-01 00:00:00", y, m));
				periods[i].end_ts   = MemoryContextStrdup(caller_ctx,
					psprintf("%04d-%02d-01 00:00:00", y_end, m_end));
			}
			else  /* yearly */
			{
				int y;

				if (sscanf(period, "%d-", &y) != 1)
					ereport(ERROR,
							(errmsg("dbblue_partition: cannot parse period \"%s\"", period)));

				periods[i].partition_name = MemoryContextStrdup(caller_ctx,
					psprintf("%s_%04d", part_table, y));
				periods[i].start_ts = MemoryContextStrdup(caller_ctx,
					psprintf("%04d-01-01 00:00:00", y));
				periods[i].end_ts   = MemoryContextStrdup(caller_ctx,
					psprintf("%04d-01-01 00:00:00", y + 1));
			}
		}
		n_periods = (int) nrows;
	}

	/* ── Step 4: Create child partitions ────────────────────────────── */
	for (i = 0; i < n_periods; i++)
	{
		resetStringInfo(&sql);
		appendStringInfo(&sql,
			"CREATE TABLE IF NOT EXISTS %s"
			" PARTITION OF %s"
			" FOR VALUES FROM ('%s') TO ('%s')",
			periods[i].partition_name,
			part_table,
			periods[i].start_ts,
			periods[i].end_ts);
		spi_exec(sql.data, SPI_OK_UTILITY);
	}

	/* ── Step 5: Copy data ──────────────────────────────────────────── */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO %s SELECT * FROM %s",
		part_table, table_name);
	{
		int ret = SPI_execute(sql.data, false, 0);

		if (ret != SPI_OK_INSERT)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("dbblue_partition: data copy failed"),
					 errhint("Ensure no NULL values exist in the partition field \"%s\".",
							 field_name)));
	}

	/* ── Step 6: Rename swap ────────────────────────────────────────── */
	resetStringInfo(&sql);
	appendStringInfo(&sql, "ALTER TABLE %s RENAME TO %s",
					 table_name, backup_table);
	spi_exec(sql.data, SPI_OK_UTILITY);

	resetStringInfo(&sql);
	appendStringInfo(&sql, "ALTER TABLE %s RENAME TO %s",
					 part_table, table_name);
	spi_exec(sql.data, SPI_OK_UTILITY);

	/*
	 * ── Step 5b: Recreate dependent views ───────────────────────────────
	 *
	 * After the rename swap PostgreSQL leaves view definitions unchanged,
	 * so they still reference backup_table by name.  Drop them deepest-first
	 * (avoids dependency errors), then recreate from the pre-rename
	 * definitions which reference table_name — now pointing at the new
	 * partitioned table automatically.
	 */
	for (i = n_dep_views - 1; i >= 0; i--)
	{
		resetStringInfo(&sql);
		appendStringInfo(&sql, "DROP VIEW IF EXISTS \"%s\" CASCADE",
						 dep_views[i].view_name);
		spi_exec(sql.data, SPI_OK_UTILITY);
	}
	for (i = 0; i < n_dep_views; i++)
	{
		resetStringInfo(&sql);
		appendStringInfo(&sql, "CREATE VIEW \"%s\" AS %s",
						 dep_views[i].view_name, dep_views[i].view_def);
		spi_exec(sql.data, SPI_OK_UTILITY);
	}

	/*
	 * ── Step 7: Fix sequence ownership ─────────────────────────────────
	 *
	 * After the rename swap, seq_name (e.g. sale_order_id_seq) is still
	 * OWNED BY backup_table.id — PostgreSQL does not rename sequences when
	 * tables are renamed.  The new partitioned table and all its child
	 * partitions already have DEFAULT nextval(seq_name) because the LIKE
	 * clause copied it from the original table.
	 *
	 * Simply change the OWNED BY to the new partitioned parent.  This
	 * decouples the sequence lifetime from the backup table so that a later
	 * DROP TABLE backup_table does not cascade to the sequence or to any
	 * column DEFAULTs — without touching the sequence value or settings.
	 */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"ALTER SEQUENCE %s OWNED BY %s.id",
		seq_name, table_name);
	spi_exec(sql.data, SPI_OK_UTILITY);

	/*
	 * ── Step 8: Re-point incoming FKs to the new partitioned table ─────
	 *
	 * After the rename swap, FK constraints on other tables (e.g.
	 * sale_order_line.order_id REFERENCES sale_order(id)) still reference
	 * the renamed backup_table OID — PostgreSQL follows renames silently.
	 * Drop each such constraint and re-create it pointing to table_name.
	 *
	 * NOTE: Re-creating FKs that reference only the 'id' column of a table
	 * whose PK is (id, partition_field) requires the custom patch in
	 * tablecmds.c (transformFkeyCheckAttrs) that allows FK references to a
	 * subset of a partitioned table's composite primary key.
	 */
	for (i = 0; i < n_in_fks; i++)
	{
		resetStringInfo(&sql);
		appendStringInfo(&sql,
			"ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s",
			in_fks[i].source_table,
			in_fks[i].constraint_name);
		spi_exec(sql.data, SPI_OK_UTILITY);

		resetStringInfo(&sql);
		appendStringInfo(&sql,
			"ALTER TABLE %s"
			" ADD CONSTRAINT %s"
			" FOREIGN KEY (%s)"
			" REFERENCES %s(%s)"
			" ON DELETE %s"
			" ON UPDATE %s",
			in_fks[i].source_table,
			in_fks[i].constraint_name,
			in_fks[i].local_column,
			table_name,
			in_fks[i].referenced_column,
			in_fks[i].delete_rule,
			in_fks[i].update_rule);
		spi_exec(sql.data, SPI_OK_UTILITY);
	}

	/*
	 * ── Step 9: Add outgoing FKs to the partitioned parent ─────────────
	 *
	 * In PG 11+ FK constraints ON a partitioned table are supported and
	 * propagate automatically to all existing and future child partitions.
	 * We add them to the parent only — no per-child duplicates needed.
	 * (The old Python module added them per-child; that was correct for
	 * older PG versions but redundant/wrong in PG 12+.)
	 */
	for (i = 0; i < n_out_fks; i++)
	{
		char *con_name = psprintf("%s_%s_fkey",
								  table_name, out_fks[i].column_name);

		resetStringInfo(&sql);
		appendStringInfo(&sql,
			"ALTER TABLE %s"
			" ADD CONSTRAINT %s"
			" FOREIGN KEY (%s)"
			" REFERENCES %s(%s)"
			" ON DELETE %s"
			" ON UPDATE %s",
			table_name,
			con_name,
			out_fks[i].column_name,
			out_fks[i].foreign_table,
			out_fks[i].foreign_column,
			out_fks[i].delete_rule,
			out_fks[i].update_rule);
		spi_exec(sql.data, SPI_OK_UTILITY);
	}

	/* ── Step 10: Register in catalog ───────────────────────────────── */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"INSERT INTO dbblue_partition_catalog"
		"    (table_name, partition_field, strategy, backup_table)"
		" VALUES ('%s', '%s', '%s', '%s')"
		" ON CONFLICT (table_name) DO UPDATE SET"
		"    partition_field    = EXCLUDED.partition_field,"
		"    strategy           = EXCLUDED.strategy,"
		"    backup_table       = EXCLUDED.backup_table,"
		"    partitioned_at     = now(),"
		"    last_maintained_at = NULL",
		table_name, field_name, strategy, backup_table);
	spi_exec(sql.data, SPI_OK_INSERT);

	for (i = 0; i < n_periods; i++)
	{
		resetStringInfo(&sql);
		appendStringInfo(&sql,
			"INSERT INTO dbblue_partition_registry"
			"    (catalog_id, partition_name, range_start, range_end)"
			" SELECT id, '%s', '%s'::timestamptz, '%s'::timestamptz"
			"   FROM dbblue_partition_catalog"
			"  WHERE table_name = '%s'"
			" ON CONFLICT (partition_name) DO NOTHING",
			periods[i].partition_name,
			periods[i].start_ts,
			periods[i].end_ts,
			table_name);
		SPI_execute(sql.data, false, 0);
	}

	/* ── Step 11: Pre-create future partitions ──────────────────────── */
	SPI_finish();

	/* Call dbblue_ensure_partitions for the advance period */
	{
		Datum		args[2];
		char		nulls[2] = {' ', ' '};
		Oid			argtypes[2] = {TEXTOID, INT4OID};
		FmgrInfo	flinfo;
		LOCAL_FCINFO(fcinfo_inner, 2);

		(void) args;
		(void) nulls;
		(void) argtypes;

		/*
		 * Simpler approach: re-connect to SPI and run the ensure logic
		 * inline rather than doing an indirect function call.
		 */
		if (dbblue_partition_advance_count > 0 &&
			SPI_connect() == SPI_OK_CONNECT)
		{
			initStringInfo(&sql);

			for (i = 1; i <= dbblue_partition_advance_count; i++)
			{
				char *period_sfx, *start_ts, *end_ts, *part_name;

				resetStringInfo(&sql);
				if (strcmp(strategy, "monthly") == 0)
					appendStringInfo(&sql,
						"SELECT to_char(date_trunc('month', now())"
						"           + ('%d months'::interval), 'YYYY_MM'),"
						"       (date_trunc('month', now())"
						"           + ('%d months'::interval))::text,"
						"       (date_trunc('month', now())"
						"           + ('%d months'::interval))::text",
						i, i, i + 1);
				else
					appendStringInfo(&sql,
						"SELECT to_char(date_trunc('year', now())"
						"           + ('%d years'::interval), 'YYYY'),"
						"       (date_trunc('year', now())"
						"           + ('%d years'::interval))::text,"
						"       (date_trunc('year', now())"
						"           + ('%d years'::interval))::text",
						i, i, i + 1);

				if (spi_select(sql.data) == 0)
					continue;

				period_sfx = spi_getval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc, 1, caller_ctx);
				start_ts   = spi_getval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc, 2, caller_ctx);
				end_ts     = spi_getval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc, 3, caller_ctx);

				if (!period_sfx || !start_ts || !end_ts)
					continue;

				part_name = psprintf("%s_partitioned_%s", table_name, period_sfx);

				resetStringInfo(&sql);
				appendStringInfo(&sql,
					"CREATE TABLE IF NOT EXISTS %s"
					" PARTITION OF %s"
					" FOR VALUES FROM ('%s') TO ('%s')",
					part_name, table_name, start_ts, end_ts);
				if (SPI_execute(sql.data, false, 0) != SPI_OK_UTILITY)
					continue;

				/* Set DEFAULT on the new future partition */
				resetStringInfo(&sql);
				appendStringInfo(&sql,
					"ALTER TABLE %s ALTER COLUMN id SET DEFAULT nextval('%s')",
					part_name, seq_name);
				SPI_execute(sql.data, false, 0);

				/* Register */
				resetStringInfo(&sql);
				appendStringInfo(&sql,
					"INSERT INTO dbblue_partition_registry"
					"    (catalog_id, partition_name, range_start, range_end)"
					" SELECT id, '%s', '%s'::timestamptz, '%s'::timestamptz"
					"   FROM dbblue_partition_catalog WHERE table_name = '%s'"
					" ON CONFLICT (partition_name) DO NOTHING",
					part_name, start_ts, end_ts, table_name);
				SPI_execute(sql.data, false, 0);
			}

			SPI_finish();
		}
		(void) flinfo;
		(void) fcinfo_inner;
	}

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"Table \"%s\" partitioned by \"%s\" (strategy: %s). "
		"Backup: \"%s\". "
		"Data partitions: %d. "
		"Sequence: \"%s\" OWNED BY %s.id. "
		"Incoming FKs re-pointed: %d. "
		"Outgoing FKs on parent: %d. "
		"Views recreated: %d.",
		table_name, field_name, strategy,
		backup_table,
		n_periods,
		seq_name, table_name,
		n_in_fks,
		n_out_fks,
		n_dep_views)));
}


/* ═══════════════════════════════════════════════════════════════════════
 * dbblue_drop_backup(p_table text)
 *
 * Safely drops the backup table.  Because dbblue_partition_table() creates
 * the sequence OWNED BY table_name.id (not the backup), a plain DROP TABLE
 * on the backup will NOT cascade to the sequence or the column DEFAULTs.
 *
 * This function verifies the catalog entry exists before proceeding.
 * ═══════════════════════════════════════════════════════════════════════ */
Datum
dbblue_drop_backup(PG_FUNCTION_ARGS)
{
	char	   *table_name  = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *backup_table;
	StringInfoData sql;

	backup_table = psprintf("%s%s", table_name, dbblue_partition_backup_suffix);

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errmsg("dbblue_partition: SPI_connect failed")));

	initStringInfo(&sql);

	/* Verify this table was partitioned by us */
	appendStringInfo(&sql,
		"SELECT 1 FROM dbblue_partition_catalog WHERE table_name = '%s'",
		table_name);
	if (spi_select(sql.data) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("dbblue_partition: table \"%s\" not in dbblue catalog",
						table_name),
				 errhint("Only use dbblue_drop_backup() for tables partitioned "
						 "by dbblue_partition_table().")));

	/* Verify backup table exists */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT 1 FROM pg_class c"
		" JOIN pg_namespace n ON n.oid = c.relnamespace"
		" WHERE c.relname = '%s' AND n.nspname = 'public'",
		backup_table);
	if (spi_select(sql.data) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("dbblue_partition: backup table \"%s\" does not exist",
						backup_table)));

	/*
	 * Safe to drop: sequence is OWNED BY table_name.id, not backup_table.
	 * No CASCADE needed — the only dependency was the sequence which is
	 * already re-owned.
	 */
	resetStringInfo(&sql);
	appendStringInfo(&sql, "DROP TABLE %s", backup_table);
	spi_exec(sql.data, SPI_OK_UTILITY);

	/* Clear backup_table in catalog */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE dbblue_partition_catalog"
		" SET backup_table = NULL, last_maintained_at = now()"
		" WHERE table_name = '%s'",
		table_name);
	SPI_execute(sql.data, false, 0);

	SPI_finish();

	PG_RETURN_VOID();
}


/* ═══════════════════════════════════════════════════════════════════════
 * dbblue_ensure_partitions(p_table text, p_advance int)
 *
 * Creates future child partitions for the given table.
 * Returns the number of new partitions created (0 if all already exist).
 * ═══════════════════════════════════════════════════════════════════════ */
Datum
dbblue_ensure_partitions(PG_FUNCTION_ARGS)
{
	char	   *table_name;
	int			advance;
	char	   *strategy;
	char	   *seq_name;
	MemoryContext caller_ctx = CurrentMemoryContext;
	StringInfoData sql;
	int			i, created = 0;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("dbblue_partition: table_name must not be NULL")));

	table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	advance    = PG_ARGISNULL(1) ? dbblue_partition_advance_count
								 : PG_GETARG_INT32(1);
	seq_name   = psprintf("%s_id_seq", table_name);

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errmsg("dbblue_partition: SPI_connect failed")));

	initStringInfo(&sql);

	/* Lookup strategy from catalog */
	appendStringInfo(&sql,
		"SELECT strategy"
		" FROM dbblue_partition_catalog"
		" WHERE table_name = '%s'",
		table_name);
	if (spi_select(sql.data) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("dbblue_partition: table \"%s\" not in dbblue catalog",
						table_name)));

	strategy   = MemoryContextStrdup(caller_ctx,
					SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));

	for (i = 1; i <= advance; i++)
	{
		char   *period_sfx, *start_ts, *end_ts, *part_name;
		int		ret;

		resetStringInfo(&sql);
		if (strcmp(strategy, "monthly") == 0)
			appendStringInfo(&sql,
				"SELECT to_char(date_trunc('month', now())"
				"           + ('%d months'::interval), 'YYYY_MM'),"
				"       (date_trunc('month', now())"
				"           + ('%d months'::interval))::text,"
				"       (date_trunc('month', now())"
				"           + ('%d months'::interval))::text",
				i, i, i + 1);
		else
			appendStringInfo(&sql,
				"SELECT to_char(date_trunc('year', now())"
				"           + ('%d years'::interval), 'YYYY'),"
				"       (date_trunc('year', now())"
				"           + ('%d years'::interval))::text,"
				"       (date_trunc('year', now())"
				"           + ('%d years'::interval))::text",
				i, i, i + 1);

		if (spi_select(sql.data) == 0)
			continue;

		period_sfx = MemoryContextStrdup(caller_ctx,
						SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
		start_ts   = MemoryContextStrdup(caller_ctx,
						SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2));
		end_ts     = MemoryContextStrdup(caller_ctx,
						SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3));

		if (!period_sfx || !start_ts || !end_ts)
			continue;

		part_name = psprintf("%s_partitioned_%s", table_name, period_sfx);

		resetStringInfo(&sql);
		appendStringInfo(&sql,
			"CREATE TABLE IF NOT EXISTS %s"
			" PARTITION OF %s"
			" FOR VALUES FROM ('%s') TO ('%s')",
			part_name, table_name, start_ts, end_ts);
		ret = SPI_execute(sql.data, false, 0);
		if (ret != SPI_OK_UTILITY)
			continue;

		/* Set sequence DEFAULT on new partition */
		resetStringInfo(&sql);
		appendStringInfo(&sql,
			"ALTER TABLE %s ALTER COLUMN id SET DEFAULT nextval('%s')",
			part_name, seq_name);
		SPI_execute(sql.data, false, 0);

		/* Register */
		resetStringInfo(&sql);
		appendStringInfo(&sql,
			"INSERT INTO dbblue_partition_registry"
			"    (catalog_id, partition_name, range_start, range_end)"
			" SELECT id, '%s', '%s'::timestamptz, '%s'::timestamptz"
			"   FROM dbblue_partition_catalog WHERE table_name = '%s'"
			" ON CONFLICT (partition_name) DO NOTHING",
			part_name, start_ts, end_ts, table_name);
		SPI_execute(sql.data, false, 0);

		created++;
	}

	/* Update last_maintained_at */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"UPDATE dbblue_partition_catalog"
		" SET last_maintained_at = now()"
		" WHERE table_name = '%s'",
		table_name);
	SPI_execute(sql.data, false, 0);

	SPI_finish();

	PG_RETURN_INT32(created);
}


/* ═══════════════════════════════════════════════════════════════════════
 * dbblue_partition_info(p_table text)
 *   → SETOF (partition_name text, range_start text, range_end text,
 *            live_rows bigint, dead_rows bigint, size text)
 * ═══════════════════════════════════════════════════════════════════════ */
Datum
dbblue_partition_info(PG_FUNCTION_ARGS)
{
	char	   *table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	ReturnSetInfo *rsinfo  = (ReturnSetInfo *) fcinfo->resultinfo;
	StringInfoData sql;
	uint64		nrows;
	int			i;

	InitMaterializedSRF(fcinfo, 0);

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errmsg("dbblue_partition: SPI_connect failed")));

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT r.partition_name,"
		"       r.range_start::text,"
		"       r.range_end::text,"
		"       COALESCE(s.n_live_tup, 0),"
		"       COALESCE(s.n_dead_tup, 0),"
		"       COALESCE(pg_size_pretty(pg_relation_size(r.partition_name::regclass)), '?')"
		" FROM  dbblue_partition_registry r"
		" JOIN  dbblue_partition_catalog  c ON c.id = r.catalog_id"
		" LEFT JOIN pg_stat_user_tables   s ON s.relname = r.partition_name"
		" WHERE c.table_name = '%s'"
		" ORDER BY r.range_start",
		table_name);

	nrows = spi_select(sql.data);

	for (i = 0; i < (int) nrows; i++)
	{
		HeapTuple	tup    = SPI_tuptable->vals[i];
		TupleDesc	desc   = SPI_tuptable->tupdesc;
		Datum		values[6];
		bool		nulls[6] = {false, false, false, false, false, false};
		char	   *v;

		v = SPI_getvalue(tup, desc, 1);
		values[0] = v ? CStringGetTextDatum(v) : (nulls[0] = true, (Datum) 0);

		v = SPI_getvalue(tup, desc, 2);
		values[1] = v ? CStringGetTextDatum(v) : (nulls[1] = true, (Datum) 0);

		v = SPI_getvalue(tup, desc, 3);
		values[2] = v ? CStringGetTextDatum(v) : (nulls[2] = true, (Datum) 0);

		v = SPI_getvalue(tup, desc, 4);
		values[3] = v ? Int64GetDatum(atoll(v)) : (nulls[3] = true, (Datum) 0);

		v = SPI_getvalue(tup, desc, 5);
		values[4] = v ? Int64GetDatum(atoll(v)) : (nulls[4] = true, (Datum) 0);

		v = SPI_getvalue(tup, desc, 6);
		values[5] = v ? CStringGetTextDatum(v) : (nulls[5] = true, (Datum) 0);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	SPI_finish();

	return (Datum) 0;
}


/* ═══════════════════════════════════════════════════════════════════════
 * dbblue_auto_partition()
 *
 * Zero-argument entry point driven entirely by GUC parameters:
 *
 *   SET dbblue_partition.enabled = on;
 *   SET dbblue_partition.table   = 'sale_order';
 *   SET dbblue_partition.column  = 'date_order';
 *   SET dbblue_partition.range   = 'monthly';   -- or 'yearly'
 *   SELECT dbblue_auto_partition();
 *
 * Equivalent to calling:
 *   SELECT dbblue_partition_table(table, column, range);
 *   SELECT dbblue_drop_backup(table);
 * plus index recreation — matching the full Python partition_manager flow.
 *
 * The backup table is dropped automatically (unlike dbblue_partition_table
 * which keeps it for manual verification).  Non-unique, non-PK indexes are
 * recreated on the partitioned parent after the backup is dropped so that
 * the original index names are available.
 * ═══════════════════════════════════════════════════════════════════════ */
Datum
dbblue_auto_partition(PG_FUNCTION_ARGS)
{
	const char *table_name;
	const char *column_name;
	const char *strategy;
	StringInfoData sql;
	MemoryContext caller_ctx;
	IndexDef   *idx_defs   = NULL;
	int			n_idx_defs = 0;
	uint64		nrows;
	int			i;
	char	   *part_result = NULL;

	/* ── Guard ──────────────────────────────────────────────────────── */
	if (!dbblue_partition_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("dbblue_partition: feature is disabled"),
				 errhint("SET dbblue_partition.enabled = on")));

	/* ── Read GUCs ──────────────────────────────────────────────────── */
	table_name  = (dbblue_partition_table_guc  && dbblue_partition_table_guc[0])
				  ? dbblue_partition_table_guc  : NULL;
	column_name = (dbblue_partition_column_guc && dbblue_partition_column_guc[0])
				  ? dbblue_partition_column_guc : NULL;
	strategy    = (dbblue_partition_range_guc  && dbblue_partition_range_guc[0])
				  ? dbblue_partition_range_guc  : dbblue_partition_default_strategy;

	if (!table_name)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dbblue_partition.table is not set"),
				 errhint("SET dbblue_partition.table = 'your_table_name'")));

	if (!column_name)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dbblue_partition.column is not set"),
				 errhint("SET dbblue_partition.column = 'your_date_column'")));

	if (strcmp(strategy, "monthly") != 0 && strcmp(strategy, "yearly") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dbblue_partition: invalid range \"%s\"", strategy),
				 errhint("Valid values: 'monthly', 'yearly'")));

	caller_ctx = CurrentMemoryContext;

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errmsg("dbblue_partition: SPI_connect failed")));

	initStringInfo(&sql);

	/*
	 * ── Capture non-PK non-unique index defs BEFORE partitioning ────────
	 *
	 * After the rename swap the original table name is taken by the new
	 * partitioned table, so pg_get_indexdef() would reference it.  Capture
	 * the defs now while they still reference the plain (pre-partition) table
	 * so executing them after the backup is dropped recreates identical
	 * indexes on the partitioned parent.  PostgreSQL then automatically
	 * creates matching child indexes on every existing and future partition.
	 *
	 * Unique indexes are skipped: PG requires unique indexes on partitioned
	 * tables to include all partition key columns; the original unique indexes
	 * do not, so they cannot be recreated as-is.
	 */
	appendStringInfo(&sql,
		"SELECT i.relname, pg_get_indexdef(i.oid)"
		" FROM  pg_class     t"
		" JOIN  pg_index     ix ON ix.indrelid  = t.oid"
		" JOIN  pg_class     i  ON i.oid        = ix.indexrelid"
		" JOIN  pg_namespace n  ON n.oid         = t.relnamespace"
		" WHERE t.relname   = '%s'"
		"   AND n.nspname   = 'public'"
		"   AND t.relkind   = 'r'"
		"   AND NOT ix.indisprimary"
		"   AND NOT ix.indisunique",
		table_name);

	nrows = spi_select(sql.data);
	if (nrows > 0)
	{
		idx_defs = MemoryContextAlloc(caller_ctx, nrows * sizeof(IndexDef));
		for (i = 0; i < (int) nrows; i++)
		{
			HeapTuple	tup  = SPI_tuptable->vals[i];
			TupleDesc	desc = SPI_tuptable->tupdesc;

			idx_defs[i].index_name = spi_getval(tup, desc, 1, caller_ctx);
			idx_defs[i].index_def  = spi_getval(tup, desc, 2, caller_ctx);
		}
		n_idx_defs = (int) nrows;
	}

	/* ── Call dbblue_partition_table() ─────────────────────────────── */
	/*
	 * This handles: FK capture, partitioned-table creation, data copy,
	 * rename swap, view recreation, sequence fix, FK re-pointing, and
	 * pre-creating future partitions.  The backup table is NOT dropped by
	 * this function — we drop it below after capturing index defs.
	 */
	resetStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT dbblue_partition_table('%s', '%s', '%s')",
		table_name, column_name, strategy);
	{
		int ret = SPI_execute(sql.data, false, 0);

		if (ret != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("dbblue_partition: dbblue_partition_table() failed (SPI %d)",
							ret)));
		if (SPI_processed > 0)
			part_result = MemoryContextStrdup(caller_ctx,
				SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
	}

	/* ── Drop the backup table ──────────────────────────────────────── */
	/*
	 * At this point the backup is safe to remove: sequence re-owned,
	 * views recreated, FKs re-pointed (all done inside partition_table).
	 * Dropping it also frees the original index names so Step 3 can
	 * recreate them on the new partitioned parent.
	 */
	resetStringInfo(&sql);
	appendStringInfo(&sql, "SELECT dbblue_drop_backup('%s')", table_name);
	{
		int ret = SPI_execute(sql.data, false, 0);

		if (ret != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("dbblue_partition: dbblue_drop_backup() failed (SPI %d)",
							ret)));
	}

	/* ── Recreate indexes on the partitioned parent ─────────────────── */
	/*
	 * Index names are now free (backup dropped above).  Creating each index
	 * on the partitioned parent causes PostgreSQL to automatically create
	 * matching child indexes on every existing and future partition.
	 */
	for (i = 0; i < n_idx_defs; i++)
	{
		if (!idx_defs[i].index_def)
			continue;

		{
			int ret = SPI_execute(idx_defs[i].index_def, false, 0);

			if (ret != SPI_OK_UTILITY)
				ereport(WARNING,
						(errmsg("dbblue_partition: could not recreate index \"%s\"",
								idx_defs[i].index_name
								? idx_defs[i].index_name : "?")));
		}
	}

	SPI_finish();

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"%s "
		"Backup dropped automatically. "
		"Indexes recreated: %d.",
		part_result ? part_result : "(no result from partition_table)",
		n_idx_defs)));
}
