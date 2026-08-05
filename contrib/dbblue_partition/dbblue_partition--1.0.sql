/* contrib/dbblue_partition/dbblue_partition--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION dbblue_partition" to load this file. \quit

/*
 * dbblue_partition 1.0 -- the whole extension, in one script.
 *
 * There are no 1.x--1.y upgrade scripts.  An extension script is read only by
 * CREATE EXTENSION or ALTER EXTENSION ... UPDATE, so a database already at the
 * current version never needs one; the pre-release chain was folded into this
 * file once every deployed database had reached it.  A future release adds
 * dbblue_partition--1.0--1.1.sql beside this one.
 *
 * Nothing here was retyped.  Every definition was extracted verbatim from the
 * development script that last changed it, and the result was verified by
 * diffing pg_get_functiondef() for all 16 objects against a database built by
 * replaying the full chain -- byte for byte identical.  The internal
 * provenance, kept because it records which development revision each object
 * settled in:
 *
 *		dbblue_partition_catalog          1.0 (+ the 1.4 column, inlined)
 *		dbblue_partition_compat_state     1.6
 *		dbblue_partition_enabled_check    1.0
 *		dbblue_partition_shadowed_objects 1.6
 *		dbblue_partition_partman_schema   1.0
 *		dbblue_partition_resolve_table    1.0
 *		dbblue_partition_dependent_views  1.0
 *		dbblue_partition_convert          1.4
 *		dbblue_partition_status           1.4
 *		dbblue_partition_drop_backup      1.0
 *		dbblue_partition_undo             1.4
 *		dbblue_partition_odoo_compat      1.6
 *		dbblue_partition_odoo_compat_remove 1.6
 *		dbblue_partition_odoo_provision   1.6
 *		dbblue_partition_odoo_deprovision 1.6
 *		dbblue_partition_odoo_reconnect   1.6
 *		dbblue_partition_odoo_compat_check 1.6
 *		dbblue_partition_model            1.4 (+ the auto-reconnect block, spliced)
 *
 * A fresh install also provisions the Odoo role automatically; see the
 * auto-provisioning block at the foot of this file for what that does, why
 * it is the only way to make conversions restart-free, and how to turn it
 * off.
 */
/* ------------------------------------------------------------------------
 * State catalog: one row per converted table.
 * Registered with pg_extension_config_dump so pg_dump carries it.
 * ------------------------------------------------------------------------
 */
CREATE TABLE @extschema@.dbblue_partition_catalog (
	parent_schema		name NOT NULL,
	parent_table		name NOT NULL,
	control_column		name NOT NULL,
	partition_interval	interval NOT NULL,
	backup_table		name NOT NULL,
	template_table		name NOT NULL,
	state				text NOT NULL DEFAULT 'migrating'
						CHECK (state IN ('migrating', 'complete')),
	rows_at_conversion	bigint NOT NULL,
	rows_moved			bigint NOT NULL DEFAULT 0,
	renamed_indexes		jsonb NOT NULL DEFAULT '[]'::jsonb,
	fks_to_validate		jsonb NOT NULL DEFAULT '[]'::jsonb,
	converted_at		timestamptz NOT NULL DEFAULT now(),
	completed_at		timestamptz,
	matviews_to_refresh	jsonb NOT NULL DEFAULT '[]'::jsonb,
	PRIMARY KEY (parent_schema, parent_table)
);

SELECT pg_catalog.pg_extension_config_dump('dbblue_partition_catalog', '');

/* ------------------------------------------------------------------------
 * dbblue_partition_compat_state
 *
 * When, and at what scope, each role's search_path was pointed at
 * dbblue_compat.  PostgreSQL does not record when a pg_db_role_setting row
 * was written, and without that timestamp "which live connections predate
 * the setting?" is unanswerable -- which is exactly the question that
 * matters, because such a connection cannot see the compatibility views and
 * will make Odoo fail.
 *
 * Registered with pg_extension_config_dump so pg_dump carries it, like
 * dbblue_partition_catalog.  Note the table is per-database while
 * cluster-wide provisioning is not, so a row may legitimately be absent in a
 * database that is nevertheless covered; the check function treats an absent
 * row as "unknown" rather than "none".
 * ------------------------------------------------------------------------
 */
CREATE TABLE @extschema@.dbblue_partition_compat_state (
	role_name		name NOT NULL PRIMARY KEY,
	scope			text NOT NULL,
	configured_at	timestamptz NOT NULL,
	CONSTRAINT dbblue_partition_compat_state_scope_check
		CHECK (scope IN ('cluster', 'database'))
);

SELECT pg_catalog.pg_extension_config_dump('@extschema@.dbblue_partition_compat_state', '');

/* ------------------------------------------------------------------------
 * dbblue_partition_enabled_check
 *
 * All mutating entry points refuse to run unless the operator has set
 * dbblue_partition.enabled = on.  The GUC name deliberately avoids
 * reserved SQL keywords so SET/SHOW always work.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_enabled_check()
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	IF NOT coalesce(nullif(current_setting('dbblue_partition.enabled', true), ''), 'off')::boolean THEN
		RAISE EXCEPTION 'dbblue_partition is disabled'
			USING HINT = 'Run SET dbblue_partition.enabled = on; to enable it for this session.',
				  ERRCODE = 'object_not_in_prerequisite_state';
	END IF;
END
$$;

/* ------------------------------------------------------------------------
 * dbblue_partition_shadowed_objects
 *
 * Internal helper.  Listing pg_catalog explicitly in search_path is what
 * lets dbblue_compat shadow the catalog, but it also stops pg_catalog from
 * being searched first for everything else, so an object in public sharing a
 * builtin's name can start winning resolution.  The search_path order is
 * forced (current_schema must stay public, and dbblue_compat must precede
 * pg_catalog), so report such objects rather than silently changing
 * semantics.
 *
 * Factored out here because three callers now need it.  Behaviour is
 * unchanged from 1.5: matching is by *name*, so it over-reports for
 * overloads that differ in signature -- pg_trgm's "%" (text,text) and "<->"
 * (text,text) never actually capture pg_catalog's numeric modulo or
 * geometric distance operators, because PostgreSQL resolves operators by
 * argument type across all visible candidates.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_shadowed_objects()
RETURNS text
LANGUAGE sql
STABLE
SET search_path = pg_catalog, pg_temp
AS $$
	SELECT string_agg(DISTINCT sh, ', ')
	FROM (
		SELECT p.proname AS sh
		FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'public'
		  AND EXISTS (SELECT 1 FROM pg_proc p2
					  JOIN pg_namespace n2 ON n2.oid = p2.pronamespace
					  WHERE n2.nspname = 'pg_catalog' AND p2.proname = p.proname)
		UNION ALL
		SELECT o.oprname
		FROM pg_operator o JOIN pg_namespace n ON n.oid = o.oprnamespace
		WHERE n.nspname = 'public'
		  AND EXISTS (SELECT 1 FROM pg_operator o2
					  JOIN pg_namespace n2 ON n2.oid = o2.oprnamespace
					  WHERE n2.nspname = 'pg_catalog' AND o2.oprname = o.oprname)
		UNION ALL
		SELECT t.typname
		FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname = 'public'
		  AND EXISTS (SELECT 1 FROM pg_type t2
					  JOIN pg_namespace n2 ON n2.oid = t2.typnamespace
					  WHERE n2.nspname = 'pg_catalog' AND t2.typname = t.typname)
	) s
$$;

REVOKE ALL ON FUNCTION @extschema@.dbblue_partition_shadowed_objects() FROM PUBLIC;

/* ------------------------------------------------------------------------
 * dbblue_partition_partman_schema
 *
 * pg_partman is relocatable at install time, so its schema cannot be
 * hardcoded; resolve it from the catalogs.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_partman_schema()
RETURNS name
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	v_schema	name;
BEGIN
	SELECT n.nspname INTO v_schema
	FROM pg_extension e
	JOIN pg_namespace n ON n.oid = e.extnamespace
	WHERE e.extname = 'pg_partman';

	IF v_schema IS NULL THEN
		RAISE EXCEPTION 'the pg_partman extension is not installed'
			USING HINT = 'Run CREATE EXTENSION pg_partman; first.';
	END IF;

	RETURN v_schema;
END
$$;

/* ------------------------------------------------------------------------
 * dbblue_partition_resolve_table
 *
 * Map an Odoo model name to its table name: dots become underscores
 * ('sale.order' -> 'sale_order').  A plain table name passes through
 * unchanged, preserving case so quoted identifiers keep working.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_resolve_table(p_model text)
RETURNS name
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
RETURN replace(p_model, '.', '_')::name;

/* ------------------------------------------------------------------------
 * dbblue_partition_dependent_views
 *
 * The transitive closure of relations whose rewrite rules depend on
 * p_relid: regular views, materialized views, and tables carrying rules.
 * Views reference tables by OID, so they must be dropped before the rename
 * swap and recreated against the new table afterwards.  The caller decides
 * what to do with non-view entries (we refuse, explicitly).
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_dependent_views(p_relid oid)
RETURNS TABLE (view_oid oid, view_schema name, view_name name, view_kind "char", depth int)
LANGUAGE sql
STABLE
SET search_path = pg_catalog, pg_temp
AS $$
	WITH RECURSIVE deps AS (
		SELECT DISTINCT r.ev_class AS viewoid, 1 AS lvl
		FROM pg_depend d
		JOIN pg_rewrite r ON r.oid = d.objid
		WHERE d.classid = 'pg_rewrite'::regclass
		  AND d.refclassid = 'pg_class'::regclass
		  AND d.refobjid = p_relid
		  AND r.ev_class <> p_relid
		UNION
		SELECT DISTINCT r2.ev_class, deps.lvl + 1
		FROM deps
		JOIN pg_depend d2 ON d2.classid = 'pg_rewrite'::regclass
						 AND d2.refclassid = 'pg_class'::regclass
						 AND d2.refobjid = deps.viewoid
		JOIN pg_rewrite r2 ON r2.oid = d2.objid
		WHERE r2.ev_class <> deps.viewoid
	)
	SELECT c.oid, n.nspname, c.relname, c.relkind, max(deps.lvl)
	FROM deps
	JOIN pg_class c ON c.oid = deps.viewoid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	GROUP BY c.oid, n.nspname, c.relname, c.relkind
$$;

/* ------------------------------------------------------------------------
 * dbblue_partition_convert
 *
 * The structural half of the conversion, run in a single transaction:
 * validate, lock, capture every dependent object, rename the original to
 * <table>_old, create the partitioned replacement under the original name,
 * hand the partition lifecycle to pg_partman, and reattach everything.
 *
 * No data is moved here; the caller (dbblue_partition_model) does that in
 * batches afterwards.  Any failure rolls this transaction back and leaves
 * the original table untouched.
 *
 * Everything that cannot be carried over raises an ERROR or a WARNING;
 * nothing is dropped silently.
 * ------------------------------------------------------------------------
 */
CREATE OR REPLACE FUNCTION @extschema@.dbblue_partition_convert(
	p_schema name,
	p_table name,
	p_control name,
	p_interval text,
	p_premake int)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
SET datestyle = 'ISO'
AS $$
DECLARE
	v_relid			oid;
	v_relkind		"char";
	v_persistence	"char";
	v_owner			oid;
	v_acl			aclitem[];
	v_reloptions	text[];
	v_replident		"char";
	v_replident_idx	name;
	v_rowsecurity	boolean;
	v_forcerowsec	boolean;
	v_table_comment	text;
	v_qualified		text;	-- format('%I.%I') of the original/new table
	v_qualified_old	text;	-- format('%I.%I') of the backup table
	v_raw			text;	-- unquoted 'schema.table' form pg_partman expects
	v_backup		name;
	v_template		name;
	v_partman		name;
	v_interval		interval;
	v_control_type	text;
	v_control_typid	oid;
	v_mv_refresh	jsonb := '[]'::jsonb;
	v_control_attnum smallint;
	v_control_generated "char";
	v_rowcount		bigint;
	v_nullcount		bigint;
	v_min_control	text;
	v_span			bigint;
	v_pk_cols		name[];
	v_pk_conname	name;
	v_pk_comment	text;
	v_new_pk_cols	name[];
	v_incoming		jsonb;
	v_outgoing		jsonb;
	v_indexes		jsonb;
	v_triggers		jsonb;
	v_policies		jsonb;
	v_grants		text[];
	v_pubs			jsonb;
	v_views			jsonb;
	v_seqs			jsonb;
	v_renames		jsonb := '[]'::jsonb;
	v_validate		jsonb := '[]'::jsonb;
	v_unique_sets	text[] := '{}';	-- sorted column sets backed by a parent-level unique index
	v_needed		text[];
	v_bad			text;
	v_new_name		text;
	v_n				int;
	v_ord			int := 0;
	r				record;
	r2				record;
BEGIN
	----------------------------------------------------------------------
	-- 1. Validate before touching anything
	----------------------------------------------------------------------
	IF p_schema IS NULL OR p_table IS NULL OR p_control IS NULL THEN
		RAISE EXCEPTION 'schema, table and control column must all be non-NULL';
	END IF;

	IF position('.' in p_schema) > 0 OR position('.' in p_table) > 0 THEN
		RAISE EXCEPTION 'schema and table names must not contain a dot: %.%', p_schema, p_table
			USING DETAIL = 'pg_partman identifies partition sets by the string ''schema.table''.';
	END IF;

	IF length(p_table) > 52 THEN
		RAISE EXCEPTION 'table name "%" is too long to partition (52 characters maximum)', p_table
			USING DETAIL = 'Room is needed for the _old backup suffix, the dbblue_tpl_ template prefix and pg_partman partition suffixes.';
	END IF;

	v_interval := p_interval::interval;
	IF v_interval <= '0'::interval THEN
		RAISE EXCEPTION 'partition interval must be positive, not %', p_interval;
	END IF;

	IF p_premake IS NULL OR p_premake < 1 THEN
		RAISE EXCEPTION 'p_premake must be at least 1';
	END IF;

	v_partman := @extschema@.dbblue_partition_partman_schema();

	v_qualified := format('%I.%I', p_schema, p_table);
	v_raw := p_schema || '.' || p_table;
	v_relid := to_regclass(v_qualified);
	IF v_relid IS NULL THEN
		RAISE EXCEPTION 'table %.% does not exist', p_schema, p_table;
	END IF;

	SELECT c.relkind, c.relpersistence, c.relowner, c.relacl, c.reloptions,
		   c.relreplident, c.relrowsecurity, c.relforcerowsecurity
	INTO v_relkind, v_persistence, v_owner, v_acl, v_reloptions,
		 v_replident, v_rowsecurity, v_forcerowsec
	FROM pg_class c WHERE c.oid = v_relid;

	IF v_relkind = 'p' THEN
		RAISE EXCEPTION 'table % is already partitioned', v_qualified;
	ELSIF v_relkind <> 'r' THEN
		RAISE EXCEPTION 'relation % is not an ordinary table (relkind "%")', v_qualified, v_relkind;
	END IF;

	IF EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = v_relid AND c.relispartition) THEN
		RAISE EXCEPTION 'table % is itself a partition of another table', v_qualified;
	END IF;

	IF v_persistence = 't' THEN
		RAISE EXCEPTION 'temporary tables cannot be partitioned by dbblue_partition';
	END IF;

	IF NOT (pg_has_role(v_owner, 'USAGE')
			OR EXISTS (SELECT 1 FROM pg_roles
					   WHERE rolname = current_user AND rolsuper)) THEN
		RAISE EXCEPTION 'must be owner of table %', v_qualified;
	END IF;

	IF EXISTS (SELECT 1 FROM pg_inherits
			   WHERE inhparent = v_relid OR inhrelid = v_relid) THEN
		RAISE EXCEPTION 'table % participates in inheritance and cannot be converted', v_qualified;
	END IF;

	IF EXISTS (SELECT 1 FROM pg_rewrite
			   WHERE ev_class = v_relid AND rulename <> '_RETURN') THEN
		RAISE EXCEPTION 'table % has rewrite rules, which cannot be carried to a partitioned table', v_qualified;
	END IF;

	SELECT string_agg(format('%s %I', c.conname,
							 (SELECT string_agg(a.attname, ', ')
							  FROM unnest(c.conkey) k
							  JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k)), ', ')
	INTO v_bad
	FROM pg_constraint c
	WHERE c.conrelid = v_relid AND c.contype = 'x';
	IF v_bad IS NOT NULL THEN
		RAISE EXCEPTION 'table % has exclusion constraints (%) that cannot be carried over', v_qualified, v_bad
			USING HINT = 'Drop them before converting, or do not partition this table.';
	END IF;

	-- Control column checks
	SELECT format_type(a.atttypid, a.atttypmod), a.attnum, a.attgenerated
	INTO v_control_type, v_control_attnum, v_control_generated
	FROM pg_attribute a
	WHERE a.attrelid = v_relid AND a.attname = p_control
	  AND a.attnum > 0 AND NOT a.attisdropped;

	SELECT a.atttypid INTO v_control_typid
	FROM pg_attribute a
	WHERE a.attrelid = v_relid AND a.attname = p_control
	  AND a.attnum > 0 AND NOT a.attisdropped;

	IF v_control_attnum IS NULL THEN
		RAISE EXCEPTION 'column "%" of table % does not exist', p_control, v_qualified;
	END IF;

	/*
	 * Compare the type itself, not its rendered name: format_type() includes
	 * any precision modifier, so a timestamp(3) column would otherwise be
	 * rejected even though it partitions perfectly well.
	 */
	IF v_control_typid NOT IN ('date'::regtype, 'timestamp'::regtype,
							   'timestamptz'::regtype) THEN
		RAISE EXCEPTION 'column "%" has type %, but range partitioning needs date, timestamp or timestamptz',
			p_control, v_control_type;
	END IF;

	IF v_control_generated <> '' THEN
		RAISE EXCEPTION 'column "%" is a generated column and cannot be a partition key', p_control;
	END IF;

	-- Primary key is required: it anchors Odoo FKs and the composite PK.
	SELECT con.conname,
		   obj_description(con.oid, 'pg_constraint'),
		   (SELECT array_agg(a.attname ORDER BY o.ord)
			FROM unnest(con.conkey) WITH ORDINALITY o(attnum, ord)
			JOIN pg_attribute a ON a.attrelid = v_relid AND a.attnum = o.attnum)
	INTO v_pk_conname, v_pk_comment, v_pk_cols
	FROM pg_constraint con
	WHERE con.conrelid = v_relid AND con.contype = 'p';

	IF v_pk_conname IS NULL THEN
		RAISE EXCEPTION 'table % has no primary key', v_qualified
			USING DETAIL = 'dbblue_partition expects an Odoo-style table with a primary key.';
	END IF;

	-- The template table is namespaced dbblue_tpl_* rather than suffixed:
	-- Odoo has real models named <table>_template (sale_order_template,
	-- mail_template, product_template, ...) that a suffix would collide with.
	v_backup := (p_table || '_old')::name;
	v_template := ('dbblue_tpl_' || p_table)::name;
	v_qualified_old := format('%I.%I', p_schema, v_backup);

	IF to_regclass(v_qualified_old) IS NOT NULL THEN
		RAISE EXCEPTION 'backup table % already exists', v_qualified_old
			USING HINT = 'A previous conversion may be unfinished; see dbblue_partition_status().';
	END IF;
	IF to_regclass(format('%I.%I', p_schema, v_template)) IS NOT NULL THEN
		RAISE EXCEPTION 'template table %.% already exists', p_schema, v_template;
	END IF;

	EXECUTE format('SELECT count(*) FROM %I.part_config WHERE parent_table = %L', v_partman, v_raw)
	INTO v_n;
	IF v_n > 0 THEN
		RAISE EXCEPTION 'table % is already managed by pg_partman', v_raw;
	END IF;

	----------------------------------------------------------------------
	-- 2. Lock out concurrent writers before capturing anything
	----------------------------------------------------------------------
	EXECUTE format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE', p_schema, p_table);

	EXECUTE format('SELECT count(*) FROM %I.%I WHERE %I IS NULL', p_schema, p_table, p_control)
	INTO v_nullcount;
	IF v_nullcount > 0 THEN
		RAISE EXCEPTION 'column "%" contains % NULL value(s) and cannot become the partition key',
			p_control, v_nullcount
			USING HINT = 'Backfill the column first; it will become part of the primary key.';
	END IF;

	EXECUTE format('SELECT count(*) FROM %I.%I', p_schema, p_table) INTO v_rowcount;
	RAISE NOTICE 'dbblue_partition: converting % (% rows) to range partitions of % on "%"',
		v_qualified, v_rowcount, p_interval, p_control;
	RAISE DEBUG 'dbblue_partition: rough duration estimate: % seconds',
		greatest(1, v_rowcount / 100000);

	----------------------------------------------------------------------
	-- 3. Capture every dependent object as SQL-ready text, before any DDL
	----------------------------------------------------------------------

	/*
	 * Dependent object closure.  Plain views and materialized views are both
	 * captured and recreated below; anything else carrying rules cannot be
	 * reconstructed, so refuse before touching the table.
	 */
	SELECT string_agg(format('%I.%I (relation with rules)',
							 dv.view_schema, dv.view_name), ', ')
	INTO v_bad
	FROM @extschema@.dbblue_partition_dependent_views(v_relid) dv
	WHERE dv.view_kind NOT IN ('v', 'm');
	IF v_bad IS NOT NULL THEN
		RAISE EXCEPTION 'objects depending on % cannot be recreated automatically: %', v_qualified, v_bad
			USING HINT = 'Drop them, convert the table, then recreate them.';
	END IF;

	SELECT coalesce(jsonb_agg(jsonb_build_object(
			'schema', dv.view_schema,
			'name', dv.view_name,
			-- pg_get_viewdef() ends with a semicolon; strip it so the
			-- matview path can append WITH [NO] DATA after the query.
			'def', rtrim(pg_get_viewdef(dv.view_oid), E' \t\n;'),
			'owner', pg_get_userbyid(c.relowner),
			'reloptions', to_jsonb(c.reloptions),
			'comment', obj_description(dv.view_oid, 'pg_class'),
			'kind', dv.view_kind,
			'populated', c.relispopulated,
			'indexes', (SELECT coalesce(jsonb_agg(pg_get_indexdef(i.indexrelid)), '[]'::jsonb)
						FROM pg_index i WHERE i.indrelid = dv.view_oid),
			'grants', (SELECT coalesce(jsonb_agg(
						format('GRANT %s ON TABLE %I.%I TO %s%s',
							   a.privilege_type, dv.view_schema, dv.view_name,
							   CASE WHEN a.grantee = 0 THEN 'PUBLIC'
									ELSE a.grantee::regrole::text END,
							   CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION'
									ELSE '' END)), '[]'::jsonb)
					   FROM aclexplode(c.relacl) a),
			'depth', dv.depth) ORDER BY dv.depth), '[]'::jsonb)
	INTO v_views
	FROM @extschema@.dbblue_partition_dependent_views(v_relid) dv
	JOIN pg_class c ON c.oid = dv.view_oid;

	-- Incoming FKs (other tables, or the table itself, referencing us).
	-- Self-referencing FKs are flagged: after the rename swap their captured
	-- table name denotes the NEW table, so there is nothing to drop there --
	-- the old constraint stays on the backup, harmlessly pointing at itself.
	SELECT coalesce(jsonb_agg(jsonb_build_object(
			'contable', con.conrelid::regclass::text,
			'conname', con.conname,
			'condef', pg_get_constraintdef(con.oid),
			'validated', con.convalidated,
			'selfref', (con.conrelid = con.confrelid),
			'comment', obj_description(con.oid, 'pg_constraint'),
			'refcols', (SELECT to_jsonb(array_agg(a.attname ORDER BY o.ord))
						FROM unnest(con.confkey) WITH ORDINALITY o(attnum, ord)
						JOIN pg_attribute a ON a.attrelid = con.confrelid
										   AND a.attnum = o.attnum))), '[]'::jsonb)
	INTO v_incoming
	FROM pg_constraint con
	WHERE con.confrelid = v_relid AND con.contype = 'f' AND con.conparentid = 0;

	-- Outgoing FKs (we reference other tables); CREATE TABLE LIKE does not
	-- copy them.  Self-referencing FKs are captured as incoming above.
	SELECT coalesce(jsonb_agg(jsonb_build_object(
			'conname', con.conname,
			'condef', pg_get_constraintdef(con.oid),
			'validated', con.convalidated,
			'comment', obj_description(con.oid, 'pg_constraint'))), '[]'::jsonb)
	INTO v_outgoing
	FROM pg_constraint con
	WHERE con.conrelid = v_relid AND con.contype = 'f'
	  AND con.confrelid <> v_relid AND con.conparentid = 0;

	-- Every index, with its backing constraint if any.  Only the leading
	-- indnkeyatts columns count for partition key coverage (INCLUDE columns
	-- do not satisfy the partitioned-unique requirement).
	SELECT coalesce(jsonb_agg(jsonb_build_object(
			'name', ic.relname,
			'def', pg_get_indexdef(i.indexrelid),
			'is_unique', i.indisunique,
			'is_primary', i.indisprimary,
			'conname', con.conname,
			'condef', CASE WHEN con.oid IS NOT NULL
						   THEN pg_get_constraintdef(con.oid) END,
			'comment', obj_description(i.indexrelid, 'pg_class'),
			'concomment', CASE WHEN con.oid IS NOT NULL
							   THEN obj_description(con.oid, 'pg_constraint') END,
			'keycols', (SELECT to_jsonb(array_agg(a.attname ORDER BY s))
						FROM generate_series(0, i.indnkeyatts - 1) s
						JOIN pg_attribute a ON a.attrelid = v_relid
										   AND a.attnum = i.indkey[s]),
			'has_expr', (i.indexprs IS NOT NULL),
			'contains_control', COALESCE(
				(SELECT bool_or(i.indkey[s] = v_control_attnum)
				 FROM generate_series(0, i.indnkeyatts - 1) s), false))),
		   '[]'::jsonb)
	INTO v_indexes
	FROM pg_index i
	JOIN pg_class ic ON ic.oid = i.indexrelid
	-- Only index-OWNING constraints here: a foreign key's conindid points at
	-- the referenced unique index too, and for a self-referencing FK its
	-- conrelid also matches, which would duplicate the index in the capture.
	LEFT JOIN pg_constraint con ON con.conindid = i.indexrelid
							   AND con.conrelid = v_relid
							   AND con.contype IN ('p', 'u')
	WHERE i.indrelid = v_relid;

	-- User triggers (internal ones belong to FKs, which are rebuilt)
	SELECT coalesce(jsonb_agg(jsonb_build_object(
			'name', t.tgname,
			'def', pg_get_triggerdef(t.oid),
			'enabled', t.tgenabled)), '[]'::jsonb)
	INTO v_triggers
	FROM pg_trigger t
	WHERE t.tgrelid = v_relid AND NOT t.tgisinternal;

	-- Row level security policies
	SELECT coalesce(jsonb_agg(jsonb_build_object(
			'name', pol.polname,
			'permissive', pol.polpermissive,
			'cmd', CASE pol.polcmd
					WHEN 'r' THEN 'SELECT' WHEN 'a' THEN 'INSERT'
					WHEN 'w' THEN 'UPDATE' WHEN 'd' THEN 'DELETE'
					ELSE 'ALL' END,
			'roles', (SELECT to_jsonb(array_agg(
						CASE WHEN m = 0 THEN 'PUBLIC'
							 ELSE m::regrole::text END))
					  FROM unnest(pol.polroles) m),
			'qual', pg_get_expr(pol.polqual, v_relid),
			'check', pg_get_expr(pol.polwithcheck, v_relid))), '[]'::jsonb)
	INTO v_policies
	FROM pg_policy pol
	WHERE pol.polrelid = v_relid;

	-- Table and column grants, replayed verbatim on the new table
	SELECT coalesce(array_agg(g.stmt), '{}') INTO v_grants
	FROM (
		SELECT format('GRANT %s ON TABLE %I.%I TO %s%s',
					  a.privilege_type, p_schema, p_table,
					  CASE WHEN a.grantee = 0 THEN 'PUBLIC'
						   ELSE a.grantee::regrole::text END,
					  CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION'
						   ELSE '' END) AS stmt
		FROM aclexplode(v_acl) a
		UNION ALL
		SELECT format('GRANT %s (%I) ON TABLE %I.%I TO %s%s',
					  a.privilege_type, att.attname, p_schema, p_table,
					  CASE WHEN a.grantee = 0 THEN 'PUBLIC'
						   ELSE a.grantee::regrole::text END,
					  CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION'
						   ELSE '' END)
		FROM pg_attribute att
		CROSS JOIN LATERAL aclexplode(att.attacl) a
		WHERE att.attrelid = v_relid AND att.attnum > 0
		  AND NOT att.attisdropped AND att.attacl IS NOT NULL
	) g;

	-- Publication membership (logical replication)
	SELECT coalesce(jsonb_agg(jsonb_build_object(
			'pubname', p.pubname,
			'collist', (SELECT to_jsonb(array_agg(a.attname ORDER BY o.ord))
						FROM unnest(pr.prattrs) WITH ORDINALITY o(attnum, ord)
						JOIN pg_attribute a ON a.attrelid = v_relid
										   AND a.attnum = o.attnum),
			'qual', pg_get_expr(pr.prqual, pr.prrelid))), '[]'::jsonb)
	INTO v_pubs
	FROM pg_publication_rel pr
	JOIN pg_publication p ON p.oid = pr.prpubid
	WHERE pr.prrelid = v_relid;

	-- Replica identity detail
	IF v_replident = 'i' THEN
		SELECT ic.relname INTO v_replident_idx
		FROM pg_index i
		JOIN pg_class ic ON ic.oid = i.indexrelid
		WHERE i.indrelid = v_relid AND i.indisreplident;
	END IF;

	-- Sequences feeding columns: serial-style defaults keep their sequence
	-- (which must be re-owned); identity columns get a fresh sequence on
	-- the new table that must be synchronized.  (Never assume the sequence
	-- is named <table>_id_seq.)
	SELECT coalesce(jsonb_agg(jsonb_build_object(
			'col', s.attname,
			'seq', s.seq,
			'identity', s.attidentity)), '[]'::jsonb)
	INTO v_seqs
	FROM (
		SELECT att.attname, att.attidentity,
			   pg_get_serial_sequence(v_qualified, att.attname) AS seq
		FROM pg_attribute att
		WHERE att.attrelid = v_relid AND att.attnum > 0 AND NOT att.attisdropped
	) s
	WHERE s.seq IS NOT NULL;

	v_table_comment := obj_description(v_relid, 'pg_class');

	IF exists (SELECT 1 FROM jsonb_array_elements(v_pubs)) THEN
		RAISE WARNING 'table % is part of a logical replication publication; moved rows will be re-published as inserts',
			v_qualified
			USING HINT = 'Consider publish_via_partition_root = true on the publication, and deduplicate on subscribers.';
	END IF;

	IF v_persistence = 'u' THEN
		RAISE WARNING 'table % is UNLOGGED; the partitioned parent will be LOGGED but its partitions will stay UNLOGGED via the template table',
			v_qualified;
	END IF;

	----------------------------------------------------------------------
	-- 4. Rename the original out of the way (indexes first, so the new
	--    table can reuse the original index names; index names are
	--    schema-wide, constraint names are per-table)
	----------------------------------------------------------------------
	FOR r IN SELECT * FROM jsonb_to_recordset(v_indexes)
			 AS x(name name)
	LOOP
		v_new_name := left(r.name, 59) || '_old';
		v_n := 1;
		WHILE to_regclass(format('%I.%I', p_schema, v_new_name)) IS NOT NULL LOOP
			v_n := v_n + 1;
			v_new_name := left(r.name, 57) || '_old' || v_n;
			IF v_n > 9 THEN
				RAISE EXCEPTION 'could not find a free name to rename index %.%', p_schema, r.name;
			END IF;
		END LOOP;
		EXECUTE format('ALTER INDEX %I.%I RENAME TO %I', p_schema, r.name, v_new_name);
		v_renames := v_renames || jsonb_build_object('from', r.name, 'to', v_new_name);
	END LOOP;

	EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', p_schema, p_table, v_backup);

	----------------------------------------------------------------------
	-- 5. Create the partitioned replacement under the original name
	----------------------------------------------------------------------
	EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL EXCLUDING INDEXES) PARTITION BY RANGE (%I)',
				   p_schema, p_table, p_schema, v_backup, p_control);

	EXECUTE format('ALTER TABLE %I.%I OWNER TO %s',
				   p_schema, p_table, v_owner::regrole::text);

	-- Composite PK: original PK columns plus the partition key
	v_new_pk_cols := v_pk_cols;
	IF NOT p_control = ANY (v_pk_cols) THEN
		v_new_pk_cols := v_pk_cols || p_control;
		RAISE WARNING 'primary key of % widens from (%) to (%): "%" is unique per partition only',
			v_qualified, array_to_string(v_pk_cols, ', '),
			array_to_string(v_new_pk_cols, ', '),
			array_to_string(v_pk_cols, ', ')
			USING DETAIL = 'A unique index on a partitioned table must include the partition key. Odoo ids come from a sequence, so cross-partition duplicates do not occur in normal operation.',
				  HINT = 'Monitoring: SELECT ' || array_to_string(v_pk_cols, ', ') || ', count(*) FROM '
						 || v_qualified || ' GROUP BY ' || array_to_string(v_pk_cols, ', ')
						 || ' HAVING count(*) > 1;';
	END IF;

	EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I PRIMARY KEY (%s)',
				   p_schema, p_table, v_pk_conname,
				   (SELECT string_agg(quote_ident(c), ', ') FROM unnest(v_new_pk_cols) c));
	IF v_pk_comment IS NOT NULL THEN
		EXECUTE format('COMMENT ON CONSTRAINT %I ON %I.%I IS %L',
					   v_pk_conname, p_schema, p_table, v_pk_comment);
	END IF;

	v_unique_sets := v_unique_sets ||
		(SELECT array_agg(c ORDER BY c) FROM unnest(v_new_pk_cols) c)::text;

	----------------------------------------------------------------------
	-- 6. Recreate indexes and unique constraints
	----------------------------------------------------------------------
	FOR r IN SELECT * FROM jsonb_to_recordset(v_indexes)
			 AS x(name name, def text, is_unique boolean, is_primary boolean,
				  conname name, condef text, comment text, concomment text,
				  keycols text[], has_expr boolean, contains_control boolean)
	LOOP
		CONTINUE WHEN r.is_primary;	-- rebuilt above, composite

		IF NOT r.is_unique THEN
			-- Plain index: recreate on the parent; it cascades to every
			-- partition.  The captured definition still names the original
			-- table, which is now the new parent.
			EXECUTE r.def;
			IF r.comment IS NOT NULL THEN
				EXECUTE format('COMMENT ON INDEX %I.%I IS %L', p_schema, r.name, r.comment);
			END IF;
		ELSIF r.contains_control THEN
			-- Unique and includes the partition key: valid on the parent.
			IF r.conname IS NOT NULL THEN
				EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
							   p_schema, p_table, r.conname, r.condef);
				IF r.concomment IS NOT NULL THEN
					EXECUTE format('COMMENT ON CONSTRAINT %I ON %I.%I IS %L',
								   r.conname, p_schema, p_table, r.concomment);
				END IF;
			ELSE
				EXECUTE r.def;
				IF r.comment IS NOT NULL THEN
					EXECUTE format('COMMENT ON INDEX %I.%I IS %L', p_schema, r.name, r.comment);
				END IF;
			END IF;
			IF NOT r.has_expr THEN
				v_unique_sets := v_unique_sets ||
					(SELECT array_agg(c ORDER BY c) FROM unnest(r.keycols) c)::text;
			END IF;
		ELSE
			-- Unique but does not include the partition key: PostgreSQL
			-- cannot enforce it globally on a partitioned table.  Apply it
			-- per partition through pg_partman's template table (created
			-- below); the index definition is transplanted onto the
			-- template by swapping the ON clause, the same way pg_partman
			-- itself does.
			RAISE WARNING 'unique constraint/index "%" on (%) does not include "%": it will be enforced per partition, not globally',
				r.name, array_to_string(r.keycols, ', '), p_control
				USING DETAIL = 'PostgreSQL requires every global unique index on a partitioned table to include the partition key.';
		END IF;
	END LOOP;

	----------------------------------------------------------------------
	-- 7. Template table: carries per-partition unique indexes, UNLOGGED
	--    state and storage options to every child pg_partman creates
	----------------------------------------------------------------------
	EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I)',
				   p_schema, v_template, p_schema, p_table);
	EXECUTE format('ALTER TABLE %I.%I OWNER TO %s',
				   p_schema, v_template, v_owner::regrole::text);
	EXECUTE format('COMMENT ON TABLE %I.%I IS %L', p_schema, v_template,
				   'pg_partman template for ' || v_qualified || ' (dbblue_partition); do not drop');

	IF v_persistence = 'u' THEN
		EXECUTE format('ALTER TABLE %I.%I SET UNLOGGED', p_schema, v_template);
	END IF;
	IF v_reloptions IS NOT NULL THEN
		EXECUTE format('ALTER TABLE %I.%I SET (%s)', p_schema, v_template,
					   array_to_string(v_reloptions, ', '));
	END IF;

	v_n := 0;
	FOR r IN SELECT * FROM jsonb_to_recordset(v_indexes)
			 AS x(name name, def text, is_unique boolean, is_primary boolean,
				  contains_control boolean)
	LOOP
		CONTINUE WHEN r.is_primary OR NOT r.is_unique OR r.contains_control;
		v_n := v_n + 1;
		EXECUTE format('CREATE UNIQUE INDEX %I ON %I.%I %s',
					   left(p_table, 50) || '_tpluq' || v_n,
					   p_schema, v_template,
					   substring(r.def from ' USING .*'));
	END LOOP;

	----------------------------------------------------------------------
	-- 8. Hand the partition lifecycle to pg_partman.  Starting from the
	--    oldest existing row makes the set gapless from day one; the
	--    DEFAULT partition catches anything else, so the table is writable
	--    the moment this function returns -- even when it was empty.
	----------------------------------------------------------------------
	EXECUTE format('SELECT min(%I)::text FROM %I.%I', p_control, p_schema, v_backup)
	INTO v_min_control;

	/*
	 * pg_partman materializes one partition per interval from
	 * p_start_partition all the way to now + premake, with no cap, and it
	 * does it inside this transaction while the table is locked.  A single
	 * row with a nonsense old date -- an Odoo import that backfilled
	 * create_date to the epoch, say -- would therefore create tens of
	 * thousands of child tables and their index sets before the conversion
	 * could finish.  Refuse instead, and name the row that caused it.
	 */
	IF v_min_control IS NOT NULL THEN
		v_span := (SELECT count(*) FROM generate_series(
					   date_trunc('day', v_min_control::timestamptz),
					   now(), p_interval::interval));
		IF v_span > 2000 THEN
			RAISE EXCEPTION 'partitioning %.% by % of % would create % partitions (oldest value is %)',
				p_schema, p_table, p_interval, p_control, v_span, v_min_control
				USING HINT = 'Use a coarser interval, or correct out-of-range values in the control column first: SELECT min('
					|| quote_ident(p_control) || ') FROM ' || v_qualified || ';',
					 ERRCODE = 'invalid_parameter_value';
		ELSIF v_span > 200 THEN
			RAISE WARNING 'partitioning %.% creates % partitions (oldest % value is %)',
				p_schema, p_table, v_span, p_control, v_min_control
				USING HINT = 'A coarser p_interval keeps the partition count manageable.';
		END IF;
	END IF;

	EXECUTE format(
		'SELECT %I.create_parent(p_parent_table := %L, p_control := %L, '
		'p_interval := %L, p_premake := %s, p_start_partition := %L, '
		'p_default_table := true, p_template_table := %L, p_jobmon := false)',
		v_partman, v_raw, p_control, p_interval, p_premake,
		v_min_control, p_schema || '.' || v_template);

	----------------------------------------------------------------------
	-- 9. Outgoing FKs, added NOT VALID and validated after the data move
	--    (the move itself runs with FK triggers suppressed when possible)
	----------------------------------------------------------------------
	FOR r IN SELECT * FROM jsonb_to_recordset(v_outgoing)
			 AS x(conname name, condef text, validated boolean, comment text)
	LOOP
		EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s%s',
					   p_schema, p_table, r.conname, r.condef,
					   CASE WHEN r.validated THEN ' NOT VALID' ELSE '' END);
		IF r.comment IS NOT NULL THEN
			EXECUTE format('COMMENT ON CONSTRAINT %I ON %I.%I IS %L',
						   r.conname, p_schema, p_table, r.comment);
		END IF;
		IF r.validated THEN
			v_validate := v_validate ||
				jsonb_build_object('tbl', v_qualified, 'con', r.conname);
		END IF;
	END LOOP;

	----------------------------------------------------------------------
	-- 10. Incoming FKs.  Odoo emits single-column FKs against id; the
	--     DBblue core patch accepts them against the composite PK.  A FK
	--     referencing a column set with no parent-level unique index needs
	--     one (columns + partition key) created here.  All are added NOT
	--     VALID now (so new writes are checked immediately) and validated
	--     once the data move completes.
	----------------------------------------------------------------------
	v_n := 0;
	FOR r IN SELECT * FROM jsonb_to_recordset(v_incoming)
			 AS x(contable text, conname name, condef text, validated boolean,
				  selfref boolean, comment text, refcols text[])
	LOOP
		-- The parent-level unique index this FK needs covers the referenced
		-- columns plus the partition key (the only surplus column the core
		-- patch tolerates).
		IF p_control = ANY (r.refcols) THEN
			v_needed := r.refcols;
		ELSE
			v_needed := r.refcols || p_control::text;
		END IF;

		IF NOT ((SELECT array_agg(c ORDER BY c) FROM unnest(r.refcols) c)::text = ANY (v_unique_sets)
				OR (SELECT array_agg(c ORDER BY c) FROM unnest(v_needed) c)::text = ANY (v_unique_sets)) THEN
			v_n := v_n + 1;
			v_new_name := left(p_table, 50) || '_fkuq' || v_n;
			EXECUTE format('CREATE UNIQUE INDEX %I ON %I.%I (%s)',
						   v_new_name, p_schema, p_table,
						   (SELECT string_agg(quote_ident(c), ', ')
							FROM unnest(v_needed) c));
			v_unique_sets := v_unique_sets ||
				(SELECT array_agg(c ORDER BY c) FROM unnest(v_needed) c)::text;
			RAISE WARNING 'created unique index % on (%) to back foreign key % from %; uniqueness of (%) alone is per partition only',
				v_new_name, array_to_string(v_needed, ', '),
				r.conname, r.contable, array_to_string(r.refcols, ', ');
		END IF;

		IF NOT r.selfref THEN
			EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', r.contable, r.conname);
		END IF;
		EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s%s',
					   r.contable, r.conname, r.condef,
					   CASE WHEN r.validated THEN ' NOT VALID' ELSE '' END);
		IF r.comment IS NOT NULL THEN
			EXECUTE format('COMMENT ON CONSTRAINT %I ON %s IS %L',
						   r.conname, r.contable, r.comment);
		END IF;
		IF r.validated THEN
			v_validate := v_validate ||
				jsonb_build_object('tbl', r.contable, 'con', r.conname);
		END IF;
	END LOOP;

	----------------------------------------------------------------------
	-- 11. Triggers, RLS, privileges, publications, replica identity
	----------------------------------------------------------------------
	FOR r IN SELECT * FROM jsonb_to_recordset(v_triggers)
			 AS x(name name, def text, enabled "char")
	LOOP
		EXECUTE r.def;
		IF r.enabled = 'D' THEN
			EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER %I', p_schema, p_table, r.name);
		ELSIF r.enabled = 'R' THEN
			EXECUTE format('ALTER TABLE %I.%I ENABLE REPLICA TRIGGER %I', p_schema, p_table, r.name);
		ELSIF r.enabled = 'A' THEN
			EXECUTE format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER %I', p_schema, p_table, r.name);
		END IF;
	END LOOP;

	FOR r IN SELECT * FROM jsonb_to_recordset(v_policies)
			 AS x(name name, permissive boolean, cmd text, roles text[],
				  qual text, "check" text)
	LOOP
		EXECUTE format('CREATE POLICY %I ON %I.%I AS %s FOR %s TO %s%s%s',
					   r.name, p_schema, p_table,
					   CASE WHEN r.permissive THEN 'PERMISSIVE' ELSE 'RESTRICTIVE' END,
					   r.cmd,
					   array_to_string(r.roles, ', '),
					   CASE WHEN r.qual IS NOT NULL
							THEN format(' USING (%s)', r.qual) ELSE '' END,
					   CASE WHEN r."check" IS NOT NULL
							THEN format(' WITH CHECK (%s)', r."check") ELSE '' END);
	END LOOP;
	IF v_rowsecurity THEN
		EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY', p_schema, p_table);
	END IF;
	IF v_forcerowsec THEN
		EXECUTE format('ALTER TABLE %I.%I FORCE ROW LEVEL SECURITY', p_schema, p_table);
	END IF;

	FOR v_ord IN 1 .. coalesce(array_length(v_grants, 1), 0)
	LOOP
		EXECUTE v_grants[v_ord];
	END LOOP;

	FOR r IN SELECT * FROM jsonb_to_recordset(v_pubs)
			 AS x(pubname name, collist text[], qual text)
	LOOP
		-- Publication membership follows the relation OID through the
		-- rename, so the backup is still the member: replace it with the
		-- new parent, or subscribers would see (and fail on) the batched
		-- deletes from the backup table during the data move.
		EXECUTE format('ALTER PUBLICATION %I DROP TABLE %I.%I',
					   r.pubname, p_schema, v_backup);
		EXECUTE format('ALTER PUBLICATION %I ADD TABLE %I.%I%s%s',
					   r.pubname, p_schema, p_table,
					   CASE WHEN r.collist IS NOT NULL
							THEN format(' (%s)', (SELECT string_agg(quote_ident(c), ', ')
												  FROM unnest(r.collist) c))
						ELSE '' END,
					   CASE WHEN r.qual IS NOT NULL
							THEN format(' WHERE (%s)', r.qual) ELSE '' END);
	END LOOP;

	IF v_replident = 'f' THEN
		EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL', p_schema, p_table);
	ELSIF v_replident = 'n' THEN
		EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY NOTHING', p_schema, p_table);
	ELSIF v_replident = 'i' THEN
		IF to_regclass(format('%I.%I', p_schema, v_replident_idx)) IS NOT NULL THEN
			EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY USING INDEX %I',
						   p_schema, p_table, v_replident_idx);
		ELSE
			RAISE WARNING 'REPLICA IDENTITY index "%" could not be carried to the partitioned table; falling back to FULL',
				v_replident_idx;
			EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL', p_schema, p_table);
		END IF;
	END IF;

	----------------------------------------------------------------------
	-- 12. Sequences: re-own serial sequences to the new table so dropping
	--     the backup cannot cascade to them; synchronize fresh identity
	--     sequences so new ids continue where the old table left off.
	----------------------------------------------------------------------
	FOR r IN SELECT * FROM jsonb_to_recordset(v_seqs)
			 AS x(col name, seq text, "identity" "char")
	LOOP
		IF r."identity" = '' OR r."identity" IS NULL THEN
			EXECUTE format('ALTER SEQUENCE %s OWNED BY %I.%I.%I',
						   r.seq, p_schema, p_table, r.col);
		ELSE
			EXECUTE format('SELECT setval(pg_get_serial_sequence(%L, %L), s.last_value, s.is_called) FROM %s s',
						   v_qualified, r.col, r.seq);
		END IF;
	END LOOP;

	----------------------------------------------------------------------
	-- 13. Recreate dependent views (outermost last), their ownership,
	--     privileges and comments
	----------------------------------------------------------------------
	FOR r IN SELECT * FROM jsonb_to_recordset(v_views)
			 AS x("schema" name, name name, def text, owner text,
				  reloptions text[], comment text, grants jsonb, depth int,
				  kind "char", populated boolean, indexes jsonb)
			 ORDER BY depth DESC
	LOOP
		EXECUTE format('DROP %s %I.%I',
					   CASE r.kind WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END,
					   r."schema", r.name);
	END LOOP;

	FOR r IN SELECT * FROM jsonb_to_recordset(v_views)
			 AS x("schema" name, name name, def text, owner text,
				  reloptions text[], comment text, grants jsonb, depth int,
				  kind "char", populated boolean, indexes jsonb)
			 ORDER BY depth ASC
	LOOP
		IF r.kind = 'm' THEN
			/*
			 * A materialized view holds its own copy of the data, so
			 * recreating it re-runs the query.  Preserve whether it was
			 * populated: an unpopulated matview must stay unpopulated,
			 * since querying one raises an error by design.
			 */
			/*
			 * WITH NO DATA unconditionally: at this point the new
			 * partitioned table is still empty, so refreshing here would
			 * populate the view with nothing and then claim it is
			 * populated.  Anything that was populated is queued for a
			 * refresh once the rows have been migrated.
			 */
			EXECUTE format('CREATE MATERIALIZED VIEW %I.%I%s AS %s WITH NO DATA',
						   r."schema", r.name,
						   CASE WHEN r.reloptions IS NOT NULL
								THEN format(' WITH (%s)', array_to_string(r.reloptions, ', '))
							ELSE '' END,
						   r.def);
			IF r.populated THEN
				v_mv_refresh := v_mv_refresh ||
					jsonb_build_object('schema', r."schema", 'name', r.name);
			END IF;
			EXECUTE format('ALTER MATERIALIZED VIEW %I.%I OWNER TO %I',
						   r."schema", r.name, r.owner);
			-- indexes on a matview are ordinary indexes and must be replayed
			FOR r2 IN SELECT value #>> '{}' AS stmt FROM jsonb_array_elements(r.indexes)
			LOOP
				EXECUTE r2.stmt;
			END LOOP;
			IF r.comment IS NOT NULL THEN
				EXECUTE format('COMMENT ON MATERIALIZED VIEW %I.%I IS %L',
							   r."schema", r.name, r.comment);
			END IF;
			RAISE NOTICE 'dbblue_partition: materialized view % recreated%',
				format('%I.%I', r."schema", r.name),
				CASE WHEN r.populated THEN '; queued for refresh after the data move'
					 ELSE ' (unpopulated, as before)' END;
		ELSE
			EXECUTE format('CREATE VIEW %I.%I%s AS %s',
						   r."schema", r.name,
						   CASE WHEN r.reloptions IS NOT NULL
								THEN format(' WITH (%s)', array_to_string(r.reloptions, ', '))
							ELSE '' END,
						   r.def);
			EXECUTE format('ALTER VIEW %I.%I OWNER TO %I', r."schema", r.name, r.owner);
			IF r.comment IS NOT NULL THEN
				EXECUTE format('COMMENT ON VIEW %I.%I IS %L', r."schema", r.name, r.comment);
			END IF;
		END IF;
		FOR r2 IN SELECT value #>> '{}' AS stmt FROM jsonb_array_elements(r.grants)
		LOOP
			EXECUTE r2.stmt;
		END LOOP;
	END LOOP;

	IF v_table_comment IS NOT NULL THEN
		EXECUTE format('COMMENT ON TABLE %I.%I IS %L', p_schema, p_table, v_table_comment);
	END IF;

	----------------------------------------------------------------------
	-- 14. Record the conversion; the data move updates this row
	----------------------------------------------------------------------
	INSERT INTO @extschema@.dbblue_partition_catalog
		(parent_schema, parent_table, control_column, partition_interval,
		 backup_table, template_table, state, rows_at_conversion,
		 renamed_indexes, fks_to_validate, matviews_to_refresh)
	VALUES
		(p_schema, p_table, p_control, v_interval,
		 v_backup, v_template, 'migrating', v_rowcount,
		 v_renames, v_validate, v_mv_refresh);

	RAISE NOTICE 'dbblue_partition: % converted; % row(s) wait in % and will be moved in batches',
		v_qualified, v_rowcount, v_qualified_old;
END
$$;

REVOKE ALL ON FUNCTION @extschema@.dbblue_partition_convert(name, name, name, text, int) FROM PUBLIC;

/* ------------------------------------------------------------------------
 * dbblue_partition_status
 *
 * Inspection across everything dbblue_partition manages.  Never throws
 * for dropped objects: partition sets or backups removed out-of-band show
 * up as zero/false rather than an error.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_status(
	p_model text DEFAULT NULL,
	p_schema text DEFAULT 'public')
RETURNS TABLE (
	parent_schema name,
	parent_table name,
	control_column name,
	partition_interval interval,
	state text,
	rows_at_conversion bigint,
	rows_moved bigint,
	partition_count bigint,
	total_size text,
	default_partition_rows bigint,
	backup_table name,
	backup_exists boolean,
	backup_rows_remaining bigint,
	rows_not_visible_to_odoo bigint,
	converted_at timestamptz,
	completed_at timestamptz)
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	v_cat		record;
	v_relid		oid;
	v_default	oid;
	v_backup	oid;
BEGIN
	FOR v_cat IN
		SELECT * FROM @extschema@.dbblue_partition_catalog c
		WHERE (p_model IS NULL
			   OR c.parent_table = @extschema@.dbblue_partition_resolve_table(p_model))
		  AND c.parent_schema = p_schema
		ORDER BY c.parent_schema, c.parent_table
	LOOP
		parent_schema := v_cat.parent_schema;
		parent_table := v_cat.parent_table;
		control_column := v_cat.control_column;
		partition_interval := v_cat.partition_interval;
		state := v_cat.state;
		rows_at_conversion := v_cat.rows_at_conversion;
		rows_moved := v_cat.rows_moved;
		converted_at := v_cat.converted_at;
		completed_at := v_cat.completed_at;

		v_relid := to_regclass(format('%I.%I', v_cat.parent_schema, v_cat.parent_table));
		IF v_relid IS NULL THEN
			partition_count := 0;
			default_partition_rows := 0;
		ELSE
			SELECT count(*) INTO partition_count
			FROM pg_inherits WHERE inhparent = v_relid;

			/*
			 * A partitioned parent has no storage of its own, so
			 * pg_total_relation_size() on it returns 0 -- reliably
			 * confusing.  Report the size of the whole partition set.
			 */
			SELECT pg_size_pretty(coalesce(sum(pg_total_relation_size(i.inhrelid)), 0))
			INTO total_size
			FROM pg_inherits i WHERE i.inhparent = v_relid;

			SELECT i.inhrelid INTO v_default
			FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			WHERE i.inhparent = v_relid
			  AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT';
			IF v_default IS NULL
			   OR NOT has_table_privilege(v_default, 'SELECT') THEN
				default_partition_rows := 0;
			ELSE
				EXECUTE format('SELECT count(*) FROM %s', v_default::regclass)
				INTO default_partition_rows;
			END IF;
		END IF;

		backup_table := v_cat.backup_table;
		-- Catalog lookup instead of a ::regclass cast: a dropped backup
		-- must report false here, not break the whole status function --
		-- and a reused name of a different relkind must not count.
		SELECT c.oid INTO v_backup
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = v_cat.parent_schema
		  AND c.relname = v_cat.backup_table
		  AND c.relkind = 'r';
		backup_exists := v_backup IS NOT NULL;
		IF backup_exists AND has_table_privilege(v_backup, 'SELECT') THEN
			EXECUTE format('SELECT count(*) FROM %I.%I', v_cat.parent_schema, v_cat.backup_table)
			INTO backup_rows_remaining;
		ELSE
			backup_rows_remaining := 0;
		END IF;

		/*
		 * While a conversion is unfinished, rows still in the backup are
		 * invisible to the application.  Say so as a number rather than
		 * leaving it to be inferred from state = 'migrating'.
		 */
		rows_not_visible_to_odoo := CASE WHEN v_cat.state = 'complete'
										 THEN 0 ELSE backup_rows_remaining END;

		RETURN NEXT;
	END LOOP;
END
$$;

/* ------------------------------------------------------------------------
 * dbblue_partition_drop_backup
 *
 * Drop <table>_old once the conversion is verified.  Refuses while the
 * migration is unfinished or the backup still holds rows, unless forced.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_drop_backup(
	p_model text,
	p_schema text DEFAULT 'public',
	p_force boolean DEFAULT false)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	v_table		name;
	v_cat		@extschema@.dbblue_partition_catalog%ROWTYPE;
	v_rows		bigint;
BEGIN
	PERFORM @extschema@.dbblue_partition_enabled_check();

	v_table := @extschema@.dbblue_partition_resolve_table(p_model);

	SELECT * INTO v_cat
	FROM @extschema@.dbblue_partition_catalog
	WHERE parent_schema = p_schema AND parent_table = v_table;
	IF NOT FOUND THEN
		RAISE EXCEPTION 'table %.% is not managed by dbblue_partition', p_schema, v_table;
	END IF;

	IF v_cat.state <> 'complete' AND NOT p_force THEN
		RAISE EXCEPTION 'conversion of %.% is not complete (state: %); not dropping its backup',
			p_schema, v_table, v_cat.state
			USING HINT = 'Finish it with CALL dbblue_partition_model(...), or pass p_force => true.';
	END IF;

	IF to_regclass(format('%I.%I', p_schema, v_cat.backup_table)) IS NULL THEN
		RAISE EXCEPTION 'backup table %.% no longer exists', p_schema, v_cat.backup_table;
	END IF;

	EXECUTE format('SELECT count(*) FROM %I.%I', p_schema, v_cat.backup_table)
	INTO v_rows;
	IF v_rows > 0 AND NOT p_force THEN
		RAISE EXCEPTION 'backup table %.% still holds % row(s); not dropping it',
			p_schema, v_cat.backup_table, v_rows
			USING HINT = 'Resume the conversion, or pass p_force => true to discard these rows.';
	END IF;

	EXECUTE format('DROP TABLE %I.%I', p_schema, v_cat.backup_table);
	RAISE NOTICE 'dbblue_partition: dropped backup table %.%', p_schema, v_cat.backup_table;
END
$$;

REVOKE ALL ON FUNCTION @extschema@.dbblue_partition_drop_backup(text, text, boolean) FROM PUBLIC;

/* ------------------------------------------------------------------------
 * dbblue_partition_undo
 *
 * Reverse a conversion while the backup table still exists: move all rows
 * back into the original (still unpartitioned) table structure, restore
 * its name, indexes, FKs and views, and deregister from pg_partman.  Runs
 * in a single transaction, so a failure rolls everything back.
 * ------------------------------------------------------------------------
 */
CREATE OR REPLACE PROCEDURE @extschema@.dbblue_partition_undo(
	p_model text,
	p_schema text DEFAULT 'public')
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	v_table			name;
	v_partman		name;
	v_raw			text;
	v_qualified		text;
	v_cat			@extschema@.dbblue_partition_catalog%ROWTYPE;
	v_relid			oid;
	v_backup_relid	oid;
	v_incoming		jsonb;
	v_views			jsonb;
	v_pubs			jsonb;
	v_bad			text;
	v_cols			text;
	v_has_identity	boolean;
	v_parent_count	bigint;
	v_backup_before	bigint;
	v_backup_after	bigint;
	r				record;
	r2				record;
BEGIN
	PERFORM @extschema@.dbblue_partition_enabled_check();

	v_table := @extschema@.dbblue_partition_resolve_table(p_model);
	v_partman := @extschema@.dbblue_partition_partman_schema();
	v_raw := p_schema || '.' || v_table;
	v_qualified := pg_catalog.format('%I.%I', p_schema, v_table);

	SELECT * INTO v_cat
	FROM @extschema@.dbblue_partition_catalog
	WHERE parent_schema = p_schema AND parent_table = v_table
	FOR UPDATE;
	IF NOT FOUND THEN
		RAISE EXCEPTION 'table % is not managed by dbblue_partition', v_qualified;
	END IF;

	v_relid := pg_catalog.to_regclass(v_qualified);
	v_backup_relid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', p_schema, v_cat.backup_table));
	IF v_relid IS NULL THEN
		RAISE EXCEPTION 'partitioned table % no longer exists', v_qualified;
	END IF;
	IF v_backup_relid IS NULL THEN
		RAISE EXCEPTION 'cannot undo: backup table %.% no longer exists', p_schema, v_cat.backup_table
			USING DETAIL = 'The original table structure lives in the backup table; without it there is nothing to restore into.';
	END IF;

	EXECUTE pg_catalog.format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE', p_schema, v_table);
	EXECUTE pg_catalog.format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE', p_schema, v_cat.backup_table);

	----------------------------------------------------------------------
	-- Capture what currently points at the partitioned table
	----------------------------------------------------------------------
	SELECT pg_catalog.string_agg(pg_catalog.format('%I.%I', dv.view_schema, dv.view_name), ', ')
	INTO v_bad
	FROM @extschema@.dbblue_partition_dependent_views(v_relid) dv
	WHERE dv.view_kind NOT IN ('v', 'm');
	IF v_bad IS NOT NULL THEN
		RAISE EXCEPTION 'objects depending on % cannot be recreated automatically: %', v_qualified, v_bad;
	END IF;

	SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
			'schema', dv.view_schema,
			'name', dv.view_name,
			'def', pg_catalog.rtrim(pg_catalog.pg_get_viewdef(dv.view_oid), E' \t\n;'),
			'kind', dv.view_kind,
			'populated', c.relispopulated,
			'indexes', (SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.pg_get_indexdef(ix.indexrelid)), '[]'::jsonb)
						FROM pg_catalog.pg_index ix WHERE ix.indrelid = dv.view_oid),
			'owner', pg_catalog.pg_get_userbyid(c.relowner),
			'reloptions', pg_catalog.to_jsonb(c.reloptions),
			'comment', pg_catalog.obj_description(dv.view_oid, 'pg_class'),
			'grants', (SELECT COALESCE(pg_catalog.jsonb_agg(
						pg_catalog.format('GRANT %s ON TABLE %I.%I TO %s%s',
							   a.privilege_type, dv.view_schema, dv.view_name,
							   CASE WHEN a.grantee = 0 THEN 'PUBLIC'
									ELSE a.grantee::regrole::text END,
							   CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION'
									ELSE '' END)), '[]'::jsonb)
					   FROM pg_catalog.aclexplode(c.relacl) a),
			'depth', dv.depth) ORDER BY dv.depth), '[]'::jsonb)
	INTO v_views
	FROM @extschema@.dbblue_partition_dependent_views(v_relid) dv
	JOIN pg_catalog.pg_class c ON c.oid = dv.view_oid;

	SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
			'contable', con.conrelid::regclass::text,
			'conname', con.conname,
			'condef', pg_catalog.pg_get_constraintdef(con.oid),
			'validated', con.convalidated,
			'selfref', (con.conrelid = con.confrelid),
			'comment', pg_catalog.obj_description(con.oid, 'pg_constraint'))), '[]'::jsonb)
	INTO v_incoming
	FROM pg_catalog.pg_constraint con
	WHERE con.confrelid = v_relid AND con.contype = 'f' AND con.conparentid = 0;

	SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
			'pubname', p.pubname)), '[]'::jsonb)
	INTO v_pubs
	FROM pg_catalog.pg_publication_rel pr
	JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
	WHERE pr.prrelid = v_relid;

	----------------------------------------------------------------------
	-- Detach dependents, move the data back, and swap the names
	----------------------------------------------------------------------
	FOR r IN SELECT * FROM pg_catalog.jsonb_to_recordset(v_views)
			 AS x("schema" name, name name, depth int, kind "char")
			 ORDER BY depth DESC
	LOOP
		EXECUTE pg_catalog.format('DROP %s %I.%I',
								  CASE r.kind WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END,
								  r."schema", r.name);
	END LOOP;

	FOR r IN SELECT * FROM pg_catalog.jsonb_to_recordset(v_incoming)
			 AS x(contable text, conname name)
	LOOP
		EXECUTE pg_catalog.format('ALTER TABLE %s DROP CONSTRAINT %I', r.contable, r.conname);
	END LOOP;

	-- Sequences: serial sequences go back to the restored table; identity
	-- sequences on the backup are synchronized from the parent's.
	FOR r IN
		SELECT att.attname, att.attidentity,
			   pg_catalog.pg_get_serial_sequence(v_qualified, att.attname) AS seq
		FROM pg_catalog.pg_attribute att
		WHERE att.attrelid = v_relid AND att.attnum > 0 AND NOT att.attisdropped
		  AND pg_catalog.pg_get_serial_sequence(v_qualified, att.attname) IS NOT NULL
	LOOP
		IF r.attidentity = '' OR r.attidentity IS NULL THEN
			EXECUTE pg_catalog.format('ALTER SEQUENCE %s OWNED BY %I.%I.%I',
									  r.seq, p_schema, v_cat.backup_table, r.attname);
		ELSE
			EXECUTE pg_catalog.format(
				'SELECT pg_catalog.setval(pg_catalog.pg_get_serial_sequence(%L, %L), s.last_value, s.is_called) FROM %s s',
				pg_catalog.format('%I.%I', p_schema, v_cat.backup_table), r.attname, r.seq);
		END IF;
	END LOOP;

	/*
	 * The backup froze the table's shape at conversion time.  If columns
	 * were added or dropped on the live table since -- which any Odoo
	 * module update can do -- the shapes no longer match.
	 *
	 * When the backup is empty (always true once the conversion reached
	 * 'complete', which is enforced before that state is set), reconciling
	 * it is lossless: adding a column to an empty table invents no data,
	 * and dropping one destroys none.  Do it automatically so a module
	 * update between conversion and undo is not an obstacle.
	 *
	 * When the backup still holds rows -- an interrupted conversion --
	 * dropping a column would destroy real data, so refuse and say exactly
	 * which statements would reconcile it.
	 */
	EXECUTE pg_catalog.format('SELECT pg_catalog.count(*) FROM %I.%I',
							  p_schema, v_cat.backup_table)
	INTO v_backup_before;

	FOR r IN
		SELECT COALESCE(p.attname, b.attname) AS attname,
			   (b.attname IS NULL) AS only_on_parent,
			   p.coldef
		FROM (SELECT a.attname,
					 pg_catalog.format('%I %s%s%s%s%s',
						 a.attname,
						 pg_catalog.format_type(a.atttypid, a.atttypmod),
						 CASE WHEN a.attcollation <> 0
							   AND a.attcollation <> t.typcollation
							  THEN ' COLLATE ' || pg_catalog.quote_ident(co.collname)
							  ELSE '' END,
						 CASE WHEN a.attgenerated = 's'
							  THEN ' GENERATED ALWAYS AS (' ||
								   pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) ||
								   ') STORED'
							  ELSE '' END,
						 -- for a generated column pg_attrdef holds the
						 -- generation expression, already emitted above
						 CASE WHEN a.attgenerated = '' AND ad.adbin IS NOT NULL
							  THEN ' DEFAULT ' ||
								   pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)
							  ELSE '' END,
						 CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END)
						 AS coldef
			  FROM pg_catalog.pg_attribute a
			  JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
			  LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation
			  LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid
												AND ad.adnum = a.attnum
			  WHERE a.attrelid = v_relid AND a.attnum > 0
				AND NOT a.attisdropped) p
		FULL JOIN (SELECT attname, attgenerated FROM pg_catalog.pg_attribute
				   WHERE attrelid = v_backup_relid AND attnum > 0
					 AND NOT attisdropped) b
			USING (attname)
		WHERE p.attname IS NULL OR b.attname IS NULL
	LOOP
		IF v_backup_before > 0 THEN
			v_bad := pg_catalog.concat_ws('; ', v_bad,
				pg_catalog.format('column %I exists only on %s', r.attname,
					CASE WHEN r.only_on_parent THEN v_qualified
						 ELSE pg_catalog.format('%I.%I', p_schema, v_cat.backup_table) END));
		ELSIF r.only_on_parent THEN
			EXECUTE pg_catalog.format('ALTER TABLE %I.%I ADD COLUMN %s',
									  p_schema, v_cat.backup_table, r.coldef);
			RAISE NOTICE 'dbblue_partition: added column % to % to match the live table',
				pg_catalog.quote_ident(r.attname),
				pg_catalog.format('%I.%I', p_schema, v_cat.backup_table);
		ELSE
			EXECUTE pg_catalog.format('ALTER TABLE %I.%I DROP COLUMN %I',
									  p_schema, v_cat.backup_table, r.attname);
			RAISE NOTICE 'dbblue_partition: dropped column % from % to match the live table',
				pg_catalog.quote_ident(r.attname),
				pg_catalog.format('%I.%I', p_schema, v_cat.backup_table);
		END IF;
	END LOOP;

	IF v_bad IS NOT NULL THEN
		RAISE EXCEPTION 'cannot undo: the table''s columns changed after conversion (%)', v_bad
			USING DETAIL = pg_catalog.format('%s still holds %s row(s), so reconciling its columns automatically could destroy data.',
											 pg_catalog.format('%I.%I', p_schema, v_cat.backup_table), v_backup_before),
				  HINT = 'Finish or roll back the migration first: CALL dbblue_partition_model(...) to resume.';
	END IF;

	-- Move every row back.  Generated columns are recomputed; identity
	-- values are preserved.
	SELECT pg_catalog.string_agg(pg_catalog.quote_ident(att.attname), ', '),
		   pg_catalog.bool_or(att.attidentity = 'a')
	INTO v_cols, v_has_identity
	FROM pg_catalog.pg_attribute att
	WHERE att.attrelid = v_relid AND att.attnum > 0
	  AND NOT att.attisdropped AND att.attgenerated = '';

	SET LOCAL row_security = off;
	BEGIN
		SET LOCAL session_replication_role = replica;
	EXCEPTION WHEN insufficient_privilege THEN
		RAISE WARNING 'insufficient privilege for session_replication_role = replica; triggers on the restored table will fire for every row moved back';
	END;

	EXECUTE pg_catalog.format('SELECT pg_catalog.count(*) FROM %I.%I', p_schema, v_table)
	INTO v_parent_count;
	EXECUTE pg_catalog.format('SELECT pg_catalog.count(*) FROM %I.%I', p_schema, v_cat.backup_table)
	INTO v_backup_before;

	EXECUTE pg_catalog.format('INSERT INTO %I.%I (%s)%s SELECT %s FROM %I.%I',
							  p_schema, v_cat.backup_table, v_cols,
							  CASE WHEN v_has_identity THEN ' OVERRIDING SYSTEM VALUE' ELSE '' END,
							  v_cols, p_schema, v_table);

	EXECUTE pg_catalog.format('SELECT pg_catalog.count(*) FROM %I.%I', p_schema, v_cat.backup_table)
	INTO v_backup_after;
	IF v_backup_after <> v_backup_before + v_parent_count THEN
		RAISE EXCEPTION 'row count mismatch while undoing: expected %, found %',
			v_backup_before + v_parent_count, v_backup_after;
	END IF;

	-- Deregister from pg_partman, then drop the partition set and template
	EXECUTE pg_catalog.format('DELETE FROM %I.part_config_sub WHERE sub_parent = %L', v_partman, v_raw);
	EXECUTE pg_catalog.format('DELETE FROM %I.part_config WHERE parent_table = %L', v_partman, v_raw);
	EXECUTE pg_catalog.format('DROP TABLE %I.%I', p_schema, v_table);
	IF pg_catalog.to_regclass(pg_catalog.format('%I.%I', p_schema, v_cat.template_table)) IS NOT NULL THEN
		EXECUTE pg_catalog.format('DROP TABLE %I.%I', p_schema, v_cat.template_table);
	END IF;

	-- Restore the original name and index names
	EXECUTE pg_catalog.format('ALTER TABLE %I.%I RENAME TO %I', p_schema, v_cat.backup_table, v_table);
	FOR r IN SELECT * FROM pg_catalog.jsonb_to_recordset(v_cat.renamed_indexes)
			 AS x("from" name, "to" name)
	LOOP
		EXECUTE pg_catalog.format('ALTER INDEX %I.%I RENAME TO %I', p_schema, r."to", r."from");
	END LOOP;

	----------------------------------------------------------------------
	-- Reattach dependents to the restored table
	----------------------------------------------------------------------
	FOR r IN SELECT * FROM pg_catalog.jsonb_to_recordset(v_incoming)
			 AS x(contable text, conname name, condef text, validated boolean,
				  selfref boolean, comment text)
	LOOP
		-- A self-referencing FK usually still exists on the restored table
		-- (convert leaves the original on the backup); re-add only the ones
		-- created after conversion.
		IF r.selfref AND EXISTS (
			SELECT 1 FROM pg_constraint
			WHERE conrelid = v_backup_relid AND conname = r.conname) THEN
			CONTINUE;
		END IF;
		/*
		 * The captured definition already carries NOT VALID for constraints
		 * that were unvalidated, so one that legitimately tolerated
		 * pre-existing violations stays that way.  But a conversion
		 * re-adds incoming FKs as NOT VALID and only validates them at the
		 * very end, so undoing an *interrupted* conversion would otherwise
		 * leave a constraint permanently unvalidated even though it was
		 * valid before any of this started.  Validate those.
		 */
		EXECUTE pg_catalog.format('ALTER TABLE %s ADD CONSTRAINT %I %s',
								  r.contable, r.conname, r.condef);
		IF r.condef LIKE '%NOT VALID' AND v_cat.state <> 'complete' THEN
			BEGIN
				EXECUTE pg_catalog.format('ALTER TABLE %s VALIDATE CONSTRAINT %I',
										  r.contable, r.conname);
			EXCEPTION WHEN OTHERS THEN
				RAISE WARNING 'could not validate constraint % on % after undo: %',
					pg_catalog.quote_ident(r.conname), r.contable, SQLERRM;
			END;
		END IF;
		IF r.comment IS NOT NULL THEN
			EXECUTE pg_catalog.format('COMMENT ON CONSTRAINT %I ON %s IS %L',
									  r.conname, r.contable, r.comment);
		END IF;
	END LOOP;

	FOR r IN SELECT * FROM pg_catalog.jsonb_to_recordset(v_views)
			 AS x("schema" name, name name, def text, owner text,
				  reloptions text[], comment text, grants jsonb, depth int,
				  kind "char", populated boolean, indexes jsonb)
			 ORDER BY depth ASC
	LOOP
		IF r.kind = 'm' THEN
			/*
			 * By this point the rows are already back in the restored
			 * table, so a materialized view can be populated immediately --
			 * unlike during a conversion, where the new table is still
			 * empty when its dependents are recreated.
			 */
			EXECUTE pg_catalog.format('CREATE MATERIALIZED VIEW %I.%I%s AS %s WITH %s',
						   r."schema", r.name,
						   CASE WHEN r.reloptions IS NOT NULL
								THEN pg_catalog.format(' WITH (%s)', pg_catalog.array_to_string(r.reloptions, ', '))
							ELSE '' END,
						   r.def,
						   CASE WHEN r.populated THEN 'DATA' ELSE 'NO DATA' END);
			EXECUTE pg_catalog.format('ALTER MATERIALIZED VIEW %I.%I OWNER TO %I',
									  r."schema", r.name, r.owner);
			FOR r2 IN SELECT value #>> '{}' AS stmt FROM pg_catalog.jsonb_array_elements(r.indexes)
			LOOP
				EXECUTE r2.stmt;
			END LOOP;
			IF r.comment IS NOT NULL THEN
				EXECUTE pg_catalog.format('COMMENT ON MATERIALIZED VIEW %I.%I IS %L',
										  r."schema", r.name, r.comment);
			END IF;
			CONTINUE;
		END IF;

		EXECUTE pg_catalog.format('CREATE VIEW %I.%I%s AS %s',
					   r."schema", r.name,
					   CASE WHEN r.reloptions IS NOT NULL
							THEN pg_catalog.format(' WITH (%s)', pg_catalog.array_to_string(r.reloptions, ', '))
						ELSE '' END,
					   r.def);
		EXECUTE pg_catalog.format('ALTER VIEW %I.%I OWNER TO %I', r."schema", r.name, r.owner);
		FOR r2 IN SELECT value #>> '{}' AS stmt FROM pg_catalog.jsonb_array_elements(r.grants)
		LOOP
			EXECUTE r2.stmt;
		END LOOP;
		IF r.comment IS NOT NULL THEN
			EXECUTE pg_catalog.format('COMMENT ON VIEW %I.%I IS %L', r."schema", r.name, r.comment);
		END IF;
	END LOOP;

	-- Re-add publication membership only where it is actually gone: on
	-- clusters converted before publications were swapped to the parent,
	-- the backup (now restored) may still be the member.
	FOR r IN SELECT * FROM pg_catalog.jsonb_to_recordset(v_pubs) AS x(pubname name)
	LOOP
		IF NOT EXISTS (
			SELECT 1
			FROM pg_publication_rel pr
			JOIN pg_publication p ON p.oid = pr.prpubid
			WHERE p.pubname = r.pubname AND pr.prrelid = v_backup_relid) THEN
			EXECUTE pg_catalog.format('ALTER PUBLICATION %I ADD TABLE %I.%I',
									  r.pubname, p_schema, v_table);
		END IF;
	END LOOP;

	DELETE FROM @extschema@.dbblue_partition_catalog
	WHERE parent_schema = p_schema AND parent_table = v_table;

	RAISE NOTICE 'dbblue_partition: % restored as a plain table with % row(s)',
		v_qualified, v_backup_after;
END
$$;

REVOKE ALL ON PROCEDURE @extschema@.dbblue_partition_undo(text, text) FROM PUBLIC;

/* ------------------------------------------------------------------------
 * dbblue_partition_odoo_compat
 *
 * Create the compatibility views, and configure the role's search_path only
 * if it is not already effective.
 *
 * The views are the half that reaches live sessions: search_path holds
 * names, resolved per query, so creating dbblue_compat makes it visible to
 * every already-open session that names it.  Where the role is provisioned
 * (see dbblue_partition_odoo_provision) this function therefore has nothing
 * to say about restarting, and says so -- the 1.5 behaviour of always
 * telling the operator to restart Odoo was wrong in the good case and easy
 * to ignore in the bad one.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_odoo_compat(p_role name DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	v_cols			text;
	v_shadow		text;
	v_db_cfg		text;
	v_cluster_cfg	text;
	v_effective		text;
	v_already		boolean;
BEGIN
	PERFORM @extschema@.dbblue_partition_enabled_check();

	-- Build the column list dynamically so the view survives pg_class
	-- layout changes across PostgreSQL versions.  Only relkind is
	-- translated, and only for single-column range partitioning on
	-- "create_date" — the shape dbblue_partition_model() creates.
	SELECT string_agg(
		CASE WHEN a.attname = 'relkind' THEN
			'CASE WHEN c.relkind = ''p'' AND COALESCE(pt.ok, false) '
			'THEN ''r''::"char" ELSE c.relkind END AS relkind'
		ELSE 'c.' || quote_ident(a.attname) END,
		', ' ORDER BY a.attnum)
	INTO v_cols
	FROM pg_attribute a
	WHERE a.attrelid = 'pg_catalog.pg_class'::regclass
	  AND a.attnum > 0 AND NOT a.attisdropped;

	CREATE SCHEMA IF NOT EXISTS dbblue_compat;
	GRANT USAGE ON SCHEMA dbblue_compat TO PUBLIC;

	-- DROP + CREATE rather than CREATE OR REPLACE: replacing a view can
	-- only append columns, so a pg_class layout change (typically after a
	-- major-version upgrade) would make the replace fail and leave a stale
	-- view in place.
	DROP VIEW IF EXISTS dbblue_compat.pg_class;

	EXECUTE format(
		'CREATE VIEW dbblue_compat.pg_class AS '
		'SELECT %s FROM pg_catalog.pg_class c '
		'LEFT JOIN LATERAL ('
		'    SELECT true AS ok '
		'    FROM pg_catalog.pg_partitioned_table pt '
		'    JOIN pg_catalog.pg_attribute pa '
		'      ON pa.attrelid = pt.partrelid AND pa.attnum = pt.partattrs[0] '
		'    WHERE pt.partrelid = c.oid '
		'      AND pt.partstrat = ''r'' '
		'      AND pt.partnatts = 1 '
		'      AND pa.attname = ''create_date'''
		') pt ON true', v_cols);

	EXECUTE 'GRANT SELECT ON dbblue_compat.pg_class TO PUBLIC';

	/*
	 * Hide the per-partition child rows of a foreign key that touches a
	 * DBblue-partitioned table, so Odoo sees exactly the foreign-key
	 * topology it created: one row per constrained column.  See the 1.5
	 * script for the full rationale.
	 */
	DROP VIEW IF EXISTS dbblue_compat.pg_constraint;

	CREATE VIEW dbblue_compat.pg_constraint AS
	SELECT c.*
	FROM pg_catalog.pg_constraint c
	WHERE c.conparentid = 0			-- top-level constraints: always visible
	   OR c.contype <> 'f'			-- only foreign-key inheritance is masked
	   OR NOT EXISTS (				-- ...and only for DBblue-shaped tables
			SELECT 1
			FROM pg_catalog.pg_inherits i
			JOIN pg_catalog.pg_partitioned_table pt
			  ON pt.partrelid = i.inhparent
			JOIN pg_catalog.pg_attribute pa
			  ON pa.attrelid = pt.partrelid
			 AND pa.attnum = pt.partattrs[0]
			WHERE i.inhrelid IN (c.conrelid, c.confrelid)
			  AND pt.partstrat = 'r'
			  AND pt.partnatts = 1
			  AND pa.attname = 'create_date');

	GRANT SELECT ON dbblue_compat.pg_constraint TO PUBLIC;

	IF p_role IS NULL THEN
		RAISE NOTICE 'dbblue_partition: compatibility views dbblue_compat.pg_class and dbblue_compat.pg_constraint are in place; provision a role once with SELECT dbblue_partition_odoo_provision(''<odoo role>'')';
		RETURN;
	END IF;

	IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = p_role) THEN
		RAISE EXCEPTION 'role "%" does not exist', p_role;
	END IF;

	/*
	 * Effective search_path default for this role in this database.  A
	 * per-database row overrides the cluster-wide one entirely, so check
	 * precedence rather than merely looking for any row mentioning
	 * dbblue_compat: a per-database override that does *not* mention it
	 * beats a cluster-wide default that does.
	 */
	SELECT array_to_string(s.setconfig, ' ') INTO v_db_cfg
	FROM pg_db_role_setting s
	JOIN pg_roles r ON r.oid = s.setrole
	JOIN pg_database d ON d.oid = s.setdatabase
	WHERE r.rolname = p_role AND d.datname = current_database();

	SELECT array_to_string(s.setconfig, ' ') INTO v_cluster_cfg
	FROM pg_db_role_setting s
	JOIN pg_roles r ON r.oid = s.setrole
	WHERE r.rolname = p_role AND s.setdatabase = 0;

	v_effective := coalesce(v_db_cfg, v_cluster_cfg);
	v_already := v_effective IS NOT NULL
			 AND v_effective LIKE '%dbblue_compat%'
			 AND v_effective LIKE '%pg_catalog%';

	IF v_already THEN
		/*
		 * Nothing to change, and nothing to restart: the views just created
		 * are already reachable through the search_path this role's sessions
		 * started with, including sessions opened long before this call.
		 */
		RAISE NOTICE 'dbblue_partition: role "%" already resolves dbblue_compat, so its existing connections can see the compatibility views immediately -- no reconnect needed',
			p_role;
		RETURN;
	END IF;

	v_shadow := @extschema@.dbblue_partition_shadowed_objects();
	IF v_shadow IS NOT NULL THEN
		RAISE WARNING 'schema public contains object(s) whose name also exists in pg_catalog: %', v_shadow
			USING DETAIL = 'Role "' || p_role || '" resolves public before pg_catalog, so these now shadow the builtin of the same name.',
				  HINT = 'Move them to another schema, or schema-qualify their callers.';
	END IF;

	EXECUTE format(
		'ALTER ROLE %I IN DATABASE %I SET search_path = "$user", public, dbblue_compat, pg_catalog',
		p_role, current_database());

	INSERT INTO @extschema@.dbblue_partition_compat_state AS st
		(role_name, scope, configured_at)
	VALUES (p_role, 'database', clock_timestamp())
	ON CONFLICT (role_name) DO UPDATE
		SET scope = EXCLUDED.scope, configured_at = EXCLUDED.configured_at;

	/*
	 * A WARNING, not a NOTICE: this is the one path that genuinely requires
	 * operator action, and as a NOTICE among seven other lines it was
	 * routinely missed -- leaving Odoo issuing CREATE TABLE over a live
	 * partitioned table.
	 */
	RAISE WARNING 'role "%" was not provisioned in advance, so its already-open connections cannot see the compatibility views', p_role
		USING DETAIL = 'ALTER ROLE ... SET search_path applies at session start only. Until those connections are replaced, Odoo reports: relation "..." already exists.',
			  HINT = 'Run SELECT dbblue_partition_odoo_reconnect(); and, to avoid this on future conversions, SELECT dbblue_partition_odoo_provision(''' || p_role || ''');';
END
$$;

REVOKE ALL ON FUNCTION @extschema@.dbblue_partition_odoo_compat(name) FROM PUBLIC;

/* ------------------------------------------------------------------------
 * dbblue_partition_odoo_compat_remove
 *
 * Unchanged in scope -- one database -- but it must now also delete the
 * recorded per-database state, or a later check would keep dating stale
 * connections from a configuration that no longer exists.  The cluster-wide
 * default is deliberately left alone; use
 * dbblue_partition_odoo_deprovision() for that.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_odoo_compat_remove(p_role name DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	v_left	text;
BEGIN
	PERFORM @extschema@.dbblue_partition_enabled_check();

	IF p_role IS NOT NULL THEN
		IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = p_role) THEN
			RAISE EXCEPTION 'role "%" does not exist', p_role;
		END IF;
		EXECUTE format('ALTER ROLE %I IN DATABASE %I RESET search_path',
					   p_role, current_database());
		DELETE FROM @extschema@.dbblue_partition_compat_state
		WHERE role_name = p_role AND scope = 'database';
	END IF;

	DROP VIEW IF EXISTS dbblue_compat.pg_constraint;
	DROP VIEW IF EXISTS dbblue_compat.pg_class;
	DROP SCHEMA IF EXISTS dbblue_compat;

	SELECT string_agg(quote_ident(r.rolname), ', ') INTO v_left
	FROM pg_db_role_setting s
	JOIN pg_database d ON d.oid = s.setdatabase
	JOIN pg_roles r ON r.oid = s.setrole
	WHERE d.datname = current_database()
	  AND array_to_string(s.setconfig, ' ') LIKE '%dbblue_compat%';
	IF v_left IS NOT NULL THEN
		RAISE WARNING 'role(s) % still list dbblue_compat in search_path for database "%"',
			v_left, current_database()
			USING HINT = 'Reset each one with SELECT dbblue_partition_odoo_compat_remove(''<role>''), or cluster-wide with SELECT dbblue_partition_odoo_deprovision(''<role>'').';
	END IF;

	RAISE NOTICE 'dbblue_partition: Odoo compatibility views removed%',
		CASE WHEN p_role IS NOT NULL
			 THEN format('; per-database search_path of role "%s" reset', p_role) ELSE '' END;
END
$$;

REVOKE ALL ON FUNCTION @extschema@.dbblue_partition_odoo_compat_remove(name) FROM PUBLIC;

/* ------------------------------------------------------------------------
 * dbblue_partition_odoo_provision
 *
 * Point a role's search_path at dbblue_compat cluster-wide, once, before
 * Odoo connects.  This is the step that removes the restart requirement for
 * every later conversion; see the header of this script for why.
 *
 * Run it from any database in the cluster -- it only writes a role setting,
 * so the extension does not need to be installed anywhere else:
 *
 *		SELECT dbblue_partition_odoo_provision('odoo');
 *
 * With no argument it provisions the current database's owner, which in a
 * normal Odoo deployment is the Odoo db_user.
 *
 * A per-database setting overrides a cluster-wide one *entirely*, so a
 * leftover per-database row from a pre-1.6 conversion would mask this one
 * and freeze that database on whatever it says.  p_clear_per_database (on by
 * default) removes those rows for this role wherever they mention
 * dbblue_compat, leaving one cluster-wide setting as the single source of
 * truth.
 *
 * Existing sessions keep the search_path they started with -- that is
 * unavoidable and is precisely why this runs at provisioning time.  If Odoo
 * is already connected when you run this, reconnect it once with
 * dbblue_partition_odoo_reconnect(); from then on nothing needs restarting
 * again.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_odoo_provision(
	p_role name DEFAULT NULL,
	p_clear_per_database boolean DEFAULT true)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	v_role		name;
	v_shadow	text;
	v_cleared	text;
BEGIN
	PERFORM @extschema@.dbblue_partition_enabled_check();

	IF p_role IS NULL THEN
		SELECT pg_get_userbyid(d.datdba) INTO v_role
		FROM pg_database d WHERE d.datname = current_database();
	ELSE
		v_role := p_role;
	END IF;

	IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_role) THEN
		RAISE EXCEPTION 'role "%" does not exist', v_role;
	END IF;

	v_shadow := @extschema@.dbblue_partition_shadowed_objects();
	IF v_shadow IS NOT NULL THEN
		RAISE WARNING 'schema public contains object(s) whose name also exists in pg_catalog: %', v_shadow
			USING DETAIL = 'Role "' || v_role || '" resolves public before pg_catalog, so these now shadow the builtin of the same name.',
				  HINT = 'Move them to another schema, or schema-qualify their callers.';
	END IF;

	/*
	 * No IN DATABASE: this must cover databases that do not exist yet,
	 * because Odoo creates its own from the web UI.  dbblue_compat is
	 * allowed to be absent -- an unresolvable search_path entry is silently
	 * skipped, so the setting is inert until an extension creates the schema.
	 */
	EXECUTE format(
		'ALTER ROLE %I SET search_path = "$user", public, dbblue_compat, pg_catalog',
		v_role);

	IF p_clear_per_database THEN
		SELECT string_agg(quote_ident(d.datname), ', ' ORDER BY d.datname)
		INTO v_cleared
		FROM pg_db_role_setting s
		JOIN pg_database d ON d.oid = s.setdatabase
		JOIN pg_roles r ON r.oid = s.setrole
		WHERE r.rolname = v_role
		  AND array_to_string(s.setconfig, ' ') LIKE '%dbblue_compat%';

		IF v_cleared IS NOT NULL THEN
			-- Cannot be done set-wise: ALTER ROLE takes one database at a time.
			DECLARE
				r record;
			BEGIN
				FOR r IN
					SELECT d.datname
					FROM pg_db_role_setting s
					JOIN pg_database d ON d.oid = s.setdatabase
					JOIN pg_roles ro ON ro.oid = s.setrole
					WHERE ro.rolname = v_role
					  AND array_to_string(s.setconfig, ' ') LIKE '%dbblue_compat%'
				LOOP
					EXECUTE format('ALTER ROLE %I IN DATABASE %I RESET search_path',
								   v_role, r.datname);
				END LOOP;
			END;
			RAISE NOTICE 'dbblue_partition: removed per-database search_path override(s) for role "%" in %; the cluster-wide setting now applies everywhere',
				v_role, v_cleared;
		END IF;
	END IF;

	INSERT INTO @extschema@.dbblue_partition_compat_state AS st
		(role_name, scope, configured_at)
	VALUES (v_role, 'cluster', clock_timestamp())
	ON CONFLICT (role_name) DO UPDATE
		SET scope = EXCLUDED.scope, configured_at = EXCLUDED.configured_at;

	RAISE NOTICE 'dbblue_partition: role "%" is provisioned cluster-wide; every database it connects to from now on -- including ones created later -- will see DBblue-partitioned tables as regular tables, with no restart needed after a conversion',
		v_role;

	IF EXISTS (SELECT 1 FROM pg_stat_activity a
			   WHERE a.usename = v_role AND a.pid <> pg_backend_pid()) THEN
		RAISE WARNING 'role "%" already has open session(s) that keep the search_path they started with', v_role
			USING DETAIL = 'Sessions opened before this call cannot see dbblue_compat, so Odoo would still fail on a partitioned table.',
				  HINT = 'Reconnect them once with SELECT dbblue_partition_odoo_reconnect(); after that no further reconnects are needed.';
	END IF;
END
$$;

REVOKE ALL ON FUNCTION @extschema@.dbblue_partition_odoo_provision(name, boolean) FROM PUBLIC;

/* ------------------------------------------------------------------------
 * dbblue_partition_odoo_deprovision
 *
 * Undo dbblue_partition_odoo_provision(): drop the cluster-wide search_path
 * default for the role.  Deliberately separate from
 * dbblue_partition_odoo_compat_remove(), which is scoped to one database --
 * silently resetting a cluster-wide setting from a per-database call would
 * affect every other database behind the operator's back.
 *
 * Leaves the views alone: a search_path naming a schema that no longer
 * exists is harmless, and other databases may still need them.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_odoo_deprovision(
	p_role name DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	v_role	name;
BEGIN
	PERFORM @extschema@.dbblue_partition_enabled_check();

	IF p_role IS NULL THEN
		SELECT pg_get_userbyid(d.datdba) INTO v_role
		FROM pg_database d WHERE d.datname = current_database();
	ELSE
		v_role := p_role;
	END IF;

	IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_role) THEN
		RAISE EXCEPTION 'role "%" does not exist', v_role;
	END IF;

	EXECUTE format('ALTER ROLE %I RESET search_path', v_role);

	DELETE FROM @extschema@.dbblue_partition_compat_state
	WHERE role_name = v_role AND scope = 'cluster';

	RAISE NOTICE 'dbblue_partition: cluster-wide search_path default removed for role "%"; its new sessions will no longer see dbblue_compat',
		v_role;
END
$$;

REVOKE ALL ON FUNCTION @extschema@.dbblue_partition_odoo_deprovision(name) FROM PUBLIC;

/* ------------------------------------------------------------------------
 * dbblue_partition_odoo_reconnect
 *
 * Force a role's stale pooled connections to be re-established, so a
 * deployment that was not provisioned in advance can pick up the
 * compatibility layer without stopping Odoo.
 *
 * This is not an Odoo restart and does not need one.  Odoo's
 * ConnectionPool.borrow() calls reset() on every candidate connection and,
 * on OperationalError, closes and discards it before opening a fresh one --
 * so a terminated backend is replaced transparently on the next request,
 * with the correct search_path.
 *
 * By default only *provably* stale connections are terminated: those that
 * started before the search_path was configured (dbblue_partition_compat_state,
 * falling back to the most recent conversion's completed_at).  When neither
 * timestamp is known the function refuses rather than guess; pass
 * p_force => true to terminate every other session of that role in this
 * database.
 *
 * Filtering is by role, not by application_name: Odoo's db_app_name is
 * configurable, so matching 'odoo%' would silently miss renamed deployments.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_odoo_reconnect(
	p_role name DEFAULT NULL,
	p_force boolean DEFAULT false)
RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	v_role	name;
	v_since	timestamptz;
	v_n		bigint;
BEGIN
	PERFORM @extschema@.dbblue_partition_enabled_check();

	IF p_role IS NULL THEN
		SELECT pg_get_userbyid(d.datdba) INTO v_role
		FROM pg_database d WHERE d.datname = current_database();
	ELSE
		v_role := p_role;
	END IF;

	IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_role) THEN
		RAISE EXCEPTION 'role "%" does not exist', v_role;
	END IF;

	SELECT st.configured_at INTO v_since
	FROM @extschema@.dbblue_partition_compat_state st
	WHERE st.role_name = v_role;

	IF v_since IS NULL THEN
		SELECT max(c.completed_at) INTO v_since
		FROM @extschema@.dbblue_partition_catalog c;
	END IF;

	IF v_since IS NULL AND NOT p_force THEN
		RAISE EXCEPTION 'cannot tell which connections are stale: no recorded search_path configuration and no completed conversion in this database'
			USING HINT = 'Run SELECT dbblue_partition_odoo_reconnect(NULL, true) to terminate every other session of this role in this database.',
				  ERRCODE = 'object_not_in_prerequisite_state';
	END IF;

	SELECT count(*) INTO v_n
	FROM (
		SELECT pg_terminate_backend(a.pid)
		FROM pg_stat_activity a
		WHERE a.datname = current_database()
		  AND a.usename = v_role
		  AND a.pid <> pg_backend_pid()
		  AND (p_force OR a.backend_start < v_since)
	) t;

	IF v_n = 0 THEN
		RAISE NOTICE 'dbblue_partition: no stale connections for role "%" in database "%"',
			v_role, current_database();
	ELSE
		RAISE NOTICE 'dbblue_partition: terminated % connection(s) for role "%" in database "%"; the client pool reconnects on its next request and will see the compatibility views',
			v_n, v_role, current_database();
	END IF;

	RETURN v_n;
END
$$;

REVOKE ALL ON FUNCTION @extschema@.dbblue_partition_odoo_reconnect(name, boolean) FROM PUBLIC;

/* ------------------------------------------------------------------------
 * dbblue_partition_odoo_compat_check
 *
 * Report whether the Odoo compatibility layer is actually in effect --
 * including for the clients that are connected right now, which 1.5 could
 * not see.  It reported "active" for a database where every Odoo connection
 * predated the search_path change and was therefore certain to fail.
 *
 * Run it after any restore, upgrade, provisioning or conversion:
 *
 *		SELECT * FROM dbblue_partition_odoo_compat_check();
 *
 * Recreated rather than replaced because 1.6 adds output columns.
 * ------------------------------------------------------------------------
 */

CREATE FUNCTION @extschema@.dbblue_partition_odoo_compat_check(
	p_role name DEFAULT NULL)
RETURNS TABLE (
	checked_role			name,
	compat_view_present		boolean,
	role_configured			boolean,
	provisioning_scope		text,
	stale_connections		bigint,
	masked_tables			bigint,
	masked_fk_children		bigint,
	shadowed_objects		text,
	verdict					text)
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	v_db_cfg		text;
	v_cluster_cfg	text;
	v_effective		text;
	v_class_view	boolean;
	v_con_view		boolean;
	v_since			timestamptz;
BEGIN
	IF p_role IS NULL THEN
		SELECT pg_get_userbyid(d.datdba) INTO checked_role
		FROM pg_database d WHERE d.datname = current_database();
	ELSE
		checked_role := p_role;
	END IF;

	v_class_view := to_regclass('dbblue_compat.pg_class') IS NOT NULL;
	v_con_view   := to_regclass('dbblue_compat.pg_constraint') IS NOT NULL;
	compat_view_present := v_class_view AND v_con_view;

	-- Precedence: a per-database row overrides the cluster-wide one.
	SELECT array_to_string(s.setconfig, ' ') INTO v_db_cfg
	FROM pg_db_role_setting s
	JOIN pg_roles r ON r.oid = s.setrole
	JOIN pg_database d ON d.oid = s.setdatabase
	WHERE r.rolname = checked_role AND d.datname = current_database();

	SELECT array_to_string(s.setconfig, ' ') INTO v_cluster_cfg
	FROM pg_db_role_setting s
	JOIN pg_roles r ON r.oid = s.setrole
	WHERE r.rolname = checked_role AND s.setdatabase = 0;

	v_effective := coalesce(v_db_cfg, v_cluster_cfg);

	role_configured := v_effective IS NOT NULL
		AND v_effective LIKE '%dbblue_compat%'
		AND v_effective LIKE '%pg_catalog%';

	provisioning_scope := CASE
		WHEN NOT role_configured THEN 'none'
		WHEN v_db_cfg IS NOT NULL THEN 'database'
		ELSE 'cluster'
	END;

	-- Tables the views are there to mask: range-partitioned on create_date.
	SELECT count(*) INTO masked_tables
	FROM pg_partitioned_table pt
	JOIN pg_attribute pa ON pa.attrelid = pt.partrelid
						AND pa.attnum = pt.partattrs[0]
	WHERE pt.partstrat = 'r' AND pt.partnatts = 1
	  AND pa.attname = 'create_date';

	-- Inherited foreign-key rows the pg_constraint view hides.  Grows with
	-- every new partition and every new foreign key, which is why this is a
	-- view and not a one-time repair.
	SELECT count(*) INTO masked_fk_children
	FROM pg_constraint c
	WHERE c.contype = 'f' AND c.conparentid <> 0
	  AND EXISTS (
			SELECT 1
			FROM pg_inherits i
			JOIN pg_partitioned_table pt ON pt.partrelid = i.inhparent
			JOIN pg_attribute pa ON pa.attrelid = pt.partrelid
								AND pa.attnum = pt.partattrs[0]
			WHERE i.inhrelid IN (c.conrelid, c.confrelid)
			  AND pt.partstrat = 'r' AND pt.partnatts = 1
			  AND pa.attname = 'create_date');

	/*
	 * Connections that provably cannot see the views: they started before
	 * the search_path was configured, and a session never re-reads its
	 * defaults.  PostgreSQL does not timestamp pg_db_role_setting, hence
	 * dbblue_partition_compat_state; fall back to the newest conversion when
	 * no row exists (e.g. provisioning was done from another database), and
	 * report NULL rather than 0 when neither is known, because "none" and
	 * "cannot tell" are very different answers here.
	 */
	SELECT st.configured_at INTO v_since
	FROM @extschema@.dbblue_partition_compat_state st
	WHERE st.role_name = checked_role;

	IF v_since IS NULL THEN
		SELECT max(c.completed_at) INTO v_since
		FROM @extschema@.dbblue_partition_catalog c;
	END IF;

	IF v_since IS NULL THEN
		stale_connections := NULL;
	ELSE
		SELECT count(*) INTO stale_connections
		FROM pg_stat_activity a
		WHERE a.datname = current_database()
		  AND a.usename = checked_role
		  AND a.pid <> pg_backend_pid()
		  AND a.backend_start < v_since;
	END IF;

	shadowed_objects := @extschema@.dbblue_partition_shadowed_objects();

	verdict := CASE
		WHEN masked_tables = 0 THEN
			'no DBblue-partitioned tables in this database; the compatibility layer is not needed'
		WHEN v_class_view AND NOT v_con_view THEN
			'INCOMPLETE: dbblue_compat.pg_class is present but dbblue_compat.pg_constraint is not, so Odoo still sees one foreign key per partition and will fail with "cannot drop inherited constraint"; run SELECT ' ||
			'dbblue_partition_odoo_compat(' || quote_literal(checked_role) || ')'
		WHEN NOT compat_view_present THEN
			'INACTIVE: the compatibility views do not exist; run SELECT dbblue_partition_odoo_compat(' ||
			quote_literal(checked_role) || ')'
		WHEN NOT role_configured THEN
			'INACTIVE: role ' || quote_ident(checked_role) ||
			' has no search_path naming dbblue_compat (typical after a restore, which does not carry pg_db_role_setting); run SELECT ' ||
			'dbblue_partition_odoo_provision(' || quote_literal(checked_role) || ')'
		WHEN coalesce(stale_connections, 0) > 0 THEN
			'INACTIVE FOR RUNNING CLIENTS: everything is in place, but ' ||
			stale_connections || ' connection(s) by ' || quote_ident(checked_role) ||
			' opened before the search_path was configured and cannot see the views; Odoo will fail with ' ||
			'"relation already exists" until they are replaced. Run SELECT dbblue_partition_odoo_reconnect(' ||
			quote_literal(checked_role) || ')'
		WHEN provisioning_scope = 'database' THEN
			'active for this database, but configured per-database only: a conversion in a database created later will need a reconnect. Run SELECT ' ||
			'dbblue_partition_odoo_provision(' || quote_literal(checked_role) ||
			') to make it cluster-wide and restart-free everywhere'
		ELSE
			'active and provisioned cluster-wide: Odoo sees partitioned tables as regular tables with one foreign-key row per column, and future conversions need no reconnect'
	END;

	RETURN NEXT;
END
$$;

REVOKE ALL ON FUNCTION @extschema@.dbblue_partition_odoo_compat_check(name) FROM PUBLIC;

/* ------------------------------------------------------------------------
 * dbblue_partition_model
 *
 * The one call an Odoo user makes:
 *
 *		CALL dbblue_partition_model('sale.order', 'create_date', '1 month');
 *
 * Runs the structural conversion in one transaction, then moves the data
 * from <table>_old into the partition set in batches, committing between
 * batches (so it must be CALLed outside a transaction block unless
 * p_single_transaction is true).  Restartable: calling it again for a
 * table whose state is 'migrating' resumes the data move.
 *
 * This procedure deliberately has no SET search_path clause: a procedure
 * with one cannot execute COMMIT.  Every object reference in its body is
 * schema-qualified instead, exactly like pg_partman's own procedures.
 * ------------------------------------------------------------------------
 */
CREATE OR REPLACE PROCEDURE @extschema@.dbblue_partition_model(
	p_model text,
	p_control text DEFAULT 'create_date',
	p_interval text DEFAULT '1 year',
	p_schema text DEFAULT 'public',
	p_premake int DEFAULT 4,
	p_batch_interval interval DEFAULT NULL,
	p_single_transaction boolean DEFAULT true,
	p_analyze boolean DEFAULT true,
	p_odoo_compat boolean DEFAULT true)
LANGUAGE plpgsql
AS $$
DECLARE
	v_table			name;
	v_partman		name;
	v_raw			text;
	v_qualified		text;
	v_backup_raw	text;
	v_cat			@extschema@.dbblue_partition_catalog%ROWTYPE;
	v_moved			bigint;
	v_total_moved	bigint := 0;
	v_remaining		bigint;
	v_new_count		bigint;
	v_batches		int := 0;
	v_ignored_cols	text[];
	v_has_identity	boolean;
	v_old_role		text;
	v_old_rowsec	text;
	v_old_datestyle	text;
	v_can_replica	boolean := true;
	v_db_owner		name;
	v_move_error	text;
	v_moved_total	bigint;
	r				record;
BEGIN
	PERFORM @extschema@.dbblue_partition_enabled_check();

	/*
	 * The batched migration commits between batches, which a procedure can
	 * only do when it was not called inside an explicit transaction block.
	 * Probe for that here, with nothing yet at stake: committing now is a
	 * no-op at the top level, but fails immediately inside a transaction
	 * block.  Without the probe, convert() would first take ACCESS
	 * EXCLUSIVE and rebuild every dependent object -- minutes of work on a
	 * large table -- only for the first COMMIT to fail afterwards.
	 */
	IF NOT p_single_transaction THEN
		COMMIT;
	END IF;

	v_table := @extschema@.dbblue_partition_resolve_table(p_model);
	v_partman := @extschema@.dbblue_partition_partman_schema();
	v_raw := p_schema || '.' || v_table;
	v_qualified := pg_catalog.format('%I.%I', p_schema, v_table);

	SELECT * INTO v_cat
	FROM @extschema@.dbblue_partition_catalog
	WHERE parent_schema = p_schema AND parent_table = v_table;

	IF v_cat.state = 'complete' THEN
		RAISE EXCEPTION 'table % is already partitioned by dbblue_partition', v_qualified;
	ELSIF v_cat.state = 'migrating' THEN
		RAISE NOTICE 'dbblue_partition: resuming interrupted data migration for % (control, interval and premake are taken from the catalog)', v_qualified;
		IF pg_catalog.to_regclass(v_qualified) IS NULL THEN
			RAISE EXCEPTION 'cannot resume: partitioned table % no longer exists', v_qualified;
		END IF;
		IF pg_catalog.to_regclass(pg_catalog.format('%I.%I', p_schema, v_cat.backup_table)) IS NULL THEN
			RAISE EXCEPTION 'cannot resume: backup table %.% no longer exists', p_schema, v_cat.backup_table;
		END IF;
	ELSE
		PERFORM @extschema@.dbblue_partition_convert(p_schema::name, v_table,
													 p_control::name, p_interval,
													 p_premake);
		SELECT * INTO v_cat
		FROM @extschema@.dbblue_partition_catalog
		WHERE parent_schema = p_schema AND parent_table = v_table;
	END IF;

	v_backup_raw := p_schema || '.' || v_cat.backup_table;

	-- Structural phase done: make it durable and release ACCESS EXCLUSIVE
	-- before the long data move, unless the caller wants one transaction.
	IF NOT p_single_transaction THEN
		COMMIT;
	END IF;

	----------------------------------------------------------------------
	-- Batched data move, delegated to pg_partman's partition_data_time():
	-- per-batch row locking with retry, and partitions created on demand
	-- for whatever the data needs.  Generated columns cannot be inserted
	-- and identity columns need OVERRIDING SYSTEM VALUE, so both are
	-- computed here and passed down.
	----------------------------------------------------------------------
	SELECT COALESCE(pg_catalog.array_agg(a.attname::text)
							   FILTER (WHERE a.attgenerated <> ''), '{}'),
		   pg_catalog.bool_or(a.attidentity = 'a')
	INTO v_ignored_cols, v_has_identity
	FROM pg_catalog.pg_attribute a
	WHERE a.attrelid = pg_catalog.to_regclass(v_qualified)
	  AND a.attnum > 0 AND NOT a.attisdropped;

	-- Suppress user triggers and FK checks for the migrated rows (they are
	-- not new data), and refuse silent row-level-security filtering during
	-- the move.  Both settings are restored afterwards.
	v_old_role := pg_catalog.current_setting('session_replication_role');
	v_old_rowsec := pg_catalog.current_setting('row_security');
	v_old_datestyle := pg_catalog.current_setting('datestyle');
	BEGIN
		SET session_replication_role = replica;
	EXCEPTION WHEN insufficient_privilege THEN
		v_can_replica := false;
		RAISE WARNING 'insufficient privilege for session_replication_role = replica; triggers and FK checks will fire for every migrated row';
	END;
	SET row_security = off;
	-- pg_partman's partition_data_time() compares partition boundaries as
	-- text (its workaround for timestamp vs timestamptz comparisons); that
	-- is only correct when timestamps render in lexicographically ordered
	-- form, i.e. under ISO DateStyle.  Session-level SET because COMMITs
	-- follow; restored below.
	SET datestyle = 'ISO';

	-- NOTE: p_lock_wait must stay 0.  pg_partman 5.4.3 has a format() bug in
	-- partition_data_time()'s p_lock_wait > 0 branch ("%6$L" with only five
	-- arguments), reported upstream rather than patched in the vendored
	-- copy.  Nothing else can write to the backup table anyway: it lost its
	-- original name, so no application session ever sees it.
	LOOP
		-- A failure mid-move must not leak replica/row_security/datestyle
		-- into the caller's session: the earlier batch COMMITs made those
		-- SETs durable, and settings restored inside a transaction that
		-- subsequently aborts revert to the migration values.  So catch,
		-- restore, COMMIT the restore, and only then re-raise.
		BEGIN
			v_moved := NULL;
			EXECUTE pg_catalog.format(
				'SELECT %I.partition_data_time(p_parent_table := %L, '
				'p_batch_count := 1, p_batch_interval := %L, p_lock_wait := 0, '
				'p_analyze := false, p_source_table := %L, '
				'p_ignored_columns := %L, p_override_system_value := %L)',
				v_partman, v_raw,
				COALESCE(p_batch_interval, v_cat.partition_interval),
				v_backup_raw, v_ignored_cols, v_has_identity)
			INTO v_moved;
		EXCEPTION WHEN OTHERS THEN
			v_move_error := pg_catalog.format('%s (%s)', SQLERRM, SQLSTATE);
		END;

		/*
		 * partition_data_time() signals a lock-wait timeout by returning -1,
		 * but only from its p_lock_wait > 0 branch, and p_lock_wait is pinned
		 * to 0 above, so that cannot happen here.  Treat it as an error
		 * rather than silently looping if a future pg_partman changes this.
		 */
		IF v_moved = -1 AND v_move_error IS NULL THEN
			v_move_error := 'partition_data_time() could not obtain row locks';
		END IF;

		IF v_move_error IS NOT NULL THEN
			IF v_can_replica THEN
				EXECUTE pg_catalog.format('SET session_replication_role = %L', v_old_role);
			END IF;
			EXECUTE pg_catalog.format('SET row_security = %L', v_old_rowsec);
			EXECUTE pg_catalog.format('SET datestyle = %L', v_old_datestyle);
			IF NOT p_single_transaction THEN
				COMMIT;
			END IF;
			RAISE EXCEPTION 'data move for % failed: %; conversion stays resumable by repeating the same CALL',
				v_qualified, v_move_error;
		END IF;

		EXIT WHEN v_moved IS NULL OR v_moved = 0;

		v_total_moved := v_total_moved + v_moved;
		v_batches := v_batches + 1;

		UPDATE @extschema@.dbblue_partition_catalog
		SET rows_moved = rows_moved + v_moved
		WHERE parent_schema = p_schema AND parent_table = v_table;

		IF NOT p_single_transaction THEN
			COMMIT;
		END IF;

		IF v_batches % 20 = 0 THEN
			RAISE NOTICE 'dbblue_partition: % rows moved into % so far', v_total_moved, v_qualified;
		END IF;
	END LOOP;

	IF v_can_replica THEN
		EXECUTE pg_catalog.format('SET session_replication_role = %L', v_old_role);
	END IF;
	EXECUTE pg_catalog.format('SET row_security = %L', v_old_rowsec);
	EXECUTE pg_catalog.format('SET datestyle = %L', v_old_datestyle);

	-- Persist the restored settings before the finalize checks: if a check
	-- below raises, its transaction rolls back, and settings restored
	-- inside it would revert to the migration values.
	IF NOT p_single_transaction THEN
		COMMIT;
	END IF;

	----------------------------------------------------------------------
	-- Finalize: prove nothing was lost, plug any partition holes,
	-- validate the re-pointed FKs, and mark the conversion complete.
	----------------------------------------------------------------------
	EXECUTE pg_catalog.format('SELECT pg_catalog.count(*) FROM %I.%I',
							  p_schema, v_cat.backup_table)
	INTO v_remaining;
	IF v_remaining > 0 THEN
		RAISE EXCEPTION '% row(s) remain in %; conversion is incomplete and resumable',
			v_remaining, v_backup_raw;
	END IF;

	-- Row accounting: every row captured at conversion must have been moved
	-- exactly once (application sessions never see the backup, so nothing
	-- else can drain it).  The live count is only advisory: the application
	-- may legitimately delete rows during a batched migration, so a lower
	-- live count warns instead of wedging the conversion forever.
	SELECT rows_moved INTO v_moved_total
	FROM @extschema@.dbblue_partition_catalog
	WHERE parent_schema = p_schema AND parent_table = v_table;
	IF v_moved_total < v_cat.rows_at_conversion THEN
		/*
		 * Report, but do not raise.  The backup is provably empty (checked
		 * just above), so every row has left it; a shortfall here means rows
		 * were counted differently, not lost -- a BEFORE ROW trigger that
		 * suppressed or redirected rows during the move is enough, and that
		 * happens whenever the caller lacks the privilege to set
		 * session_replication_role.  Raising would abort this transaction,
		 * leave state = 'migrating' with an empty backup, and make every
		 * retry fail at exactly the same point: unrecoverable without
		 * hand-editing the catalog.
		 */
		RAISE WARNING 'dbblue_partition: % row(s) moved out of % captured at conversion of %',
			v_moved_total, v_cat.rows_at_conversion, v_qualified
			USING DETAIL = 'The backup table is empty, so no row remains behind; the difference is in how rows were counted, most often a BEFORE ROW trigger that fired during the move.',
				  HINT = 'Compare counts before dropping the backup: SELECT count(*) FROM ' || v_qualified || ';';
	END IF;

	EXECUTE pg_catalog.format('SELECT pg_catalog.count(*) FROM %I.%I',
							  p_schema, v_table)
	INTO v_new_count;
	IF v_new_count < v_cat.rows_at_conversion THEN
		RAISE WARNING 'table % holds % row(s) but % were captured at conversion; concurrent sessions deleted rows during the migration',
			v_qualified, v_new_count, v_cat.rows_at_conversion;
	END IF;

	EXECUTE pg_catalog.format('SELECT %I.partition_gap_fill(%L)', v_partman, v_raw);

	/*
	 * Now that every row has landed, refresh the materialized views that
	 * were populated before the conversion.  They were recreated WITH NO
	 * DATA on purpose: refreshing during the structural swap would have
	 * populated them from an empty table.
	 */
	FOR r IN SELECT x."schema", x.name
			 FROM pg_catalog.jsonb_to_recordset(v_cat.matviews_to_refresh)
			 AS x("schema" name, name name)
	LOOP
		BEGIN
			EXECUTE pg_catalog.format('REFRESH MATERIALIZED VIEW %I.%I',
									  r."schema", r.name);
			RAISE NOTICE 'dbblue_partition: refreshed materialized view %',
				pg_catalog.format('%I.%I', r."schema", r.name);
		EXCEPTION WHEN OTHERS THEN
			RAISE WARNING 'could not refresh materialized view %: %',
				pg_catalog.format('%I.%I', r."schema", r.name), SQLERRM
				USING HINT = 'Run REFRESH MATERIALIZED VIEW manually.';
		END;
	END LOOP;

	FOR r IN SELECT x.tbl, x.con
			 FROM pg_catalog.jsonb_to_recordset(v_cat.fks_to_validate)
				  AS x(tbl text, con name)
	LOOP
		EXECUTE pg_catalog.format('ALTER TABLE %s VALIDATE CONSTRAINT %I', r.tbl, r.con);
	END LOOP;

	UPDATE @extschema@.dbblue_partition_catalog
	SET state = 'complete', completed_at = pg_catalog.clock_timestamp()
	WHERE parent_schema = p_schema AND parent_table = v_table;

	IF p_analyze THEN
		EXECUTE pg_catalog.format('ANALYZE %I.%I', p_schema, v_table);
	END IF;

	----------------------------------------------------------------------
	-- DBblue serves Odoo.  Two things have to happen here, in this order:
	--
	--   1. create the compatibility views, so pg_class reports relkind 'r'
	--      for this table and pg_constraint hides the per-partition foreign
	--      key children PostgreSQL just created;
	--   2. make sure the Odoo role's connections can actually reach them.
	--
	-- (2) is the step that used to be left to the operator, and the reason
	-- a conversion could appear to succeed and still break Odoo.  A session
	-- reads its search_path default exactly once, at startup, so a
	-- connection opened before the role was configured can never see
	-- dbblue_compat: it keeps reading the real pg_catalog.pg_class, sees
	-- relkind 'p', concludes the model's table is missing, and issues
	-- CREATE TABLE over the table this procedure has just partitioned --
	--
	--     psycopg2.errors.DuplicateTable: relation "..." already exists
	--
	-- Terminating those connections is the only cure, and this is the right
	-- moment for it: the conversion is finished, and any Odoo request
	-- touching this table was already blocked behind our ACCESS EXCLUSIVE
	-- lock, so less is in flight now than at any later point.  Odoo treats
	-- a dead pooled connection as routine -- ConnectionPool.borrow() resets
	-- each candidate and discards the ones that raise -- so it reconnects on
	-- its next request with the correct search_path.  This is not, and does
	-- not need, an Odoo restart.
	--
	-- Where the role was provisioned before Odoo ever connected (see
	-- dbblue_partition_odoo_provision) nothing is stale and both steps are
	-- silent no-ops.  Where staleness cannot be dated -- no recorded
	-- configuration and no earlier conversion in this database -- the
	-- reconnect declines rather than guess, and says so.
	--
	-- Neither step may fail the conversion: this transaction also carries
	-- the foreign-key validations and the state update, so an error here
	-- would wedge the conversion in 'migrating' on every retry.
	----------------------------------------------------------------------
	IF p_odoo_compat THEN
		v_db_owner := nullif(
			pg_catalog.current_setting('dbblue_partition.odoo_role', true), '');
		IF v_db_owner IS NULL THEN
			SELECT pg_catalog.pg_get_userbyid(d.datdba) INTO v_db_owner
			FROM pg_catalog.pg_database d
			WHERE d.datname = pg_catalog.current_database();
		END IF;

		BEGIN
			PERFORM @extschema@.dbblue_partition_odoo_compat(v_db_owner);
		EXCEPTION WHEN OTHERS THEN
			RAISE WARNING 'dbblue_partition: could not configure Odoo compatibility for role "%" automatically: %; run SELECT dbblue_partition_odoo_compat(...) manually',
				v_db_owner, SQLERRM;
		END;

		IF coalesce(nullif(
				pg_catalog.current_setting('dbblue_partition.auto_reconnect', true), ''),
				'on')::boolean
		THEN
			BEGIN
				PERFORM @extschema@.dbblue_partition_odoo_reconnect(v_db_owner);
			EXCEPTION WHEN OTHERS THEN
				RAISE WARNING 'dbblue_partition: could not re-establish stale connections for role "%" automatically: %', v_db_owner, SQLERRM
					USING HINT = 'Odoo may fail with "relation already exists" until you run SELECT dbblue_partition_odoo_reconnect().';
			END;
		END IF;
	END IF;

	RAISE NOTICE 'dbblue_partition: % complete; % row(s) moved in % batch(es); backup kept as %; drop it with dbblue_partition_drop_backup(%)',
		v_qualified, v_total_moved, v_batches, v_backup_raw, p_model;
END
$$;

REVOKE ALL ON PROCEDURE @extschema@.dbblue_partition_model(text, text, text, text, int, interval, boolean, boolean, boolean) FROM PUBLIC;


/* ------------------------------------------------------------------------
 * Automatic provisioning on fresh install
 *
 * The compatibility layer has two halves with very different timing, and
 * getting that wrong is the single most common way to break an Odoo
 * database with this extension:
 *
 *	- the views (dbblue_compat.pg_class, .pg_constraint) are visible to
 *	  every *already open* session the moment they are created, because
 *	  search_path holds names that are resolved per query and a name that
 *	  resolves to nothing is silently skipped;
 *	- the search_path itself is read only when a session starts, so it can
 *	  never be pushed into a connection that is already open.
 *
 * So the search_path must be in place before Odoo connects.  If it is set
 * later -- which is what happens when the conversion configures it -- every
 * pooled Odoo connection keeps the old one, keeps reading the real
 * pg_catalog.pg_class, sees relkind 'p', decides the model's table is
 * missing and issues CREATE TABLE over the live partitioned table:
 *
 *		psycopg2.errors.DuplicateTable: relation "sale_order" already exists
 *
 * Hence this block: installing the extension is the earliest moment the
 * extension can act, so it sets the role's search_path here, cluster-wide.
 * The dbblue_compat schema does not exist yet and does not need to -- it is
 * ignored until a conversion creates it, and picked up live at that moment.
 *
 * Cluster-wide (ALTER ROLE without IN DATABASE) is deliberate: Odoo creates
 * its own databases from the web UI, and you cannot target a database that
 * does not exist yet.  A cluster-wide default is inherited by every database
 * the role ever connects to, including future ones.
 *
 * Recommended deployment, which needs no restart at any point:
 *
 *		-- before Odoo is started, as superuser:
 *		ALTER SYSTEM SET dbblue_partition.odoo_role = 'odoo';
 *		SELECT pg_reload_conf();
 *		psql -d template1 -c 'CREATE EXTENSION dbblue_partition CASCADE'
 *		-- and in odoo.conf:    db_template = template1
 *
 * Every database Odoo then creates already carries this extension and an
 * already-provisioned role, so conversions are transparent from the first
 * one.  (Odoo defaults db_template to template0, which is why that setting
 * is needed; note it also drops Odoo's LC_COLLATE 'C', so make that choice
 * deliberately.)
 *
 * Settings, both read here and both optional:
 *
 *	dbblue_partition.odoo_role       role to provision.  Defaults to the
 *	                                 owner of the database being installed
 *	                                 into, which is right for a normal Odoo
 *	                                 deployment but wrong for template1,
 *	                                 whose owner is usually postgres -- so
 *	                                 set it explicitly for the template1
 *	                                 route.
 *	dbblue_partition.auto_provision  set to off to skip this entirely and
 *	                                 provision by hand later with
 *	                                 dbblue_partition_odoo_provision().
 *
 * Two things this block deliberately does not do.  It does not consult
 * dbblue_partition.enabled: that guard exists to stop conversions from
 * running unintentionally, and at install time it is invariably off, so
 * honouring it here would mean this never runs.  And it does not create the
 * dbblue_compat schema or its views, because every object an extension
 * script creates is recorded as a member of the extension and could then
 * never be dropped by dbblue_partition_odoo_compat_remove().  Writing a row
 * into our own table is fine.
 *
 * Nothing here can fail the install: a missing role or insufficient
 * privileges downgrade to a WARNING naming the manual command.
 * ------------------------------------------------------------------------
 */
DO $dbblue_auto$
DECLARE
	v_role	name;
	v_auto	text;
BEGIN
	v_auto := nullif(
		pg_catalog.current_setting('dbblue_partition.auto_provision', true), '');

	IF v_auto IS NOT NULL AND NOT v_auto::boolean THEN
		RAISE NOTICE 'dbblue_partition: auto-provisioning is off; run SELECT dbblue_partition_odoo_provision(''<odoo role>'') before starting Odoo, or conversions will need a reconnect';
		RETURN;
	END IF;

	v_role := nullif(
		pg_catalog.current_setting('dbblue_partition.odoo_role', true), '');

	IF v_role IS NULL THEN
		SELECT pg_catalog.pg_get_userbyid(d.datdba) INTO v_role
		FROM pg_catalog.pg_database d
		WHERE d.datname = pg_catalog.current_database();
	END IF;

	IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname = v_role) THEN
		RAISE WARNING 'dbblue_partition: role "%" does not exist, so the Odoo search_path was not configured', v_role
			USING HINT = 'Set dbblue_partition.odoo_role, or run SELECT dbblue_partition_odoo_provision(''<odoo role>'') once the role exists.';
		RETURN;
	END IF;

	BEGIN
		EXECUTE pg_catalog.format(
			'ALTER ROLE %I SET search_path = "$user", public, dbblue_compat, pg_catalog',
			v_role);
	EXCEPTION WHEN insufficient_privilege THEN
		RAISE WARNING 'dbblue_partition: not permitted to set the search_path of role "%"', v_role
			USING DETAIL = 'ALTER ROLE ... SET requires superuser, or the role altering itself.',
				  HINT = 'Run SELECT dbblue_partition_odoo_provision(''' || v_role || '''); as a superuser before starting Odoo.';
		RETURN;
	END;

	INSERT INTO @extschema@.dbblue_partition_compat_state AS st
		(role_name, scope, configured_at)
	VALUES (v_role, 'cluster', pg_catalog.clock_timestamp())
	ON CONFLICT (role_name) DO UPDATE
		SET scope = EXCLUDED.scope, configured_at = EXCLUDED.configured_at;

	RAISE NOTICE 'dbblue_partition: role "%" provisioned cluster-wide; it will see DBblue-partitioned tables as regular tables in every database it connects to, and conversions will not need Odoo to be restarted',
		v_role;

	/*
	 * The one case that still needs action: the role is already connected,
	 * and those sessions keep the search_path they started with.
	 */
	IF EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a
			   WHERE a.usename = v_role
				 AND a.pid <> pg_catalog.pg_backend_pid()) THEN
		RAISE NOTICE 'dbblue_partition: role "%" already has open session(s), which cannot see a search_path set after they started; they will be re-established automatically at the end of the first conversion, so no action is needed now',
			v_role;
	END IF;
END
$dbblue_auto$;
