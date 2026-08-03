/* contrib/dbblue_partition/dbblue_partition--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION dbblue_partition" to load this file. \quit

/*
 * dbblue_partition
 *
 * Odoo-aware, in-place conversion of an existing table to native range
 * partitioning.  pg_partman deliberately refuses in-place conversion (its
 * create_parent() requires an already-partitioned, usually empty parent);
 * this extension fills exactly that gap and delegates the partition
 * lifecycle (gapless premake, DEFAULT partition, gap fill, batched data
 * move, retention, maintenance) to an unmodified, vendored pg_partman.
 *
 * The extension is off by default, per DBblue convention:
 *
 *		SET dbblue_partition.enabled = on;
 *
 * Entry points:
 *
 *		CALL dbblue_partition_model('sale.order', 'create_date', '1 month');
 *		SELECT * FROM dbblue_partition_status();
 *		SELECT dbblue_partition_drop_backup('sale.order');
 *		CALL dbblue_partition_undo('sale.order');
 *
 * Documented, deliberate trade-offs (do not "fix" these):
 *
 * 1. A partitioned table's PK must include the partition key, so the PK
 *    becomes (id, create_date).  Odoo's single-column FKs referencing id
 *    are accepted by a DBblue core patch (transformFkeyCheckAttrs), but the
 *    FK is then backed by a non-unique key: if duplicate ids ever appear
 *    across partitions, referential actions misbehave.  Odoo draws id from
 *    a sequence, so this does not happen in normal operation.  Run
 *        SELECT id, count(*) FROM <table> GROUP BY id HAVING count(*) > 1;
 *    after migrations, setval() changes, or restores.
 *
 * 2. UNIQUE constraints that do not include the partition column cannot be
 *    global on a partitioned table.  They are applied per-partition via
 *    pg_partman's template table, which is weaker; a WARNING is emitted.
 *
 * 3. The batched data move commits between batches, so the conversion as a
 *    whole is not atomic (the structural swap is).  The original rows stay
 *    in <table>_old until moved, and the conversion is resumable.  Pass
 *    p_single_transaction := true to get the old all-or-nothing behaviour
 *    at the price of holding ACCESS EXCLUSIVE for the whole copy.
 *
 * 4. If the table is in a logical replication publication, moved rows are
 *    re-published as inserts; a WARNING is emitted.
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
	PRIMARY KEY (parent_schema, parent_table)
);

SELECT pg_catalog.pg_extension_config_dump('dbblue_partition_catalog', '');

GRANT SELECT ON @extschema@.dbblue_partition_catalog TO PUBLIC;


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
CREATE FUNCTION @extschema@.dbblue_partition_convert(
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
	v_control_attnum smallint;
	v_control_generated "char";
	v_rowcount		bigint;
	v_nullcount		bigint;
	v_min_control	text;
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

	IF v_control_attnum IS NULL THEN
		RAISE EXCEPTION 'column "%" of table % does not exist', p_control, v_qualified;
	END IF;

	IF v_control_type NOT IN ('date', 'timestamp without time zone', 'timestamp with time zone') THEN
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

	-- Dependent view closure; refuse anything that is not a plain view.
	SELECT string_agg(format('%I.%I (%s)', dv.view_schema, dv.view_name,
							 CASE dv.view_kind WHEN 'm' THEN 'materialized view'
							 ELSE 'relation with rules' END), ', ')
	INTO v_bad
	FROM @extschema@.dbblue_partition_dependent_views(v_relid) dv
	WHERE dv.view_kind <> 'v';
	IF v_bad IS NOT NULL THEN
		RAISE EXCEPTION 'objects depending on % cannot be recreated automatically: %', v_qualified, v_bad
			USING HINT = 'Drop them, convert the table, then recreate them.';
	END IF;

	SELECT coalesce(jsonb_agg(jsonb_build_object(
			'schema', dv.view_schema,
			'name', dv.view_name,
			'def', pg_get_viewdef(dv.view_oid),
			'owner', pg_get_userbyid(c.relowner),
			'reloptions', to_jsonb(c.reloptions),
			'comment', obj_description(dv.view_oid, 'pg_class'),
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

	-- Incoming FKs (other tables, or the table itself, referencing us)
	SELECT coalesce(jsonb_agg(jsonb_build_object(
			'contable', con.conrelid::regclass::text,
			'conname', con.conname,
			'condef', pg_get_constraintdef(con.oid),
			'validated', con.convalidated,
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
	LEFT JOIN pg_constraint con ON con.conindid = i.indexrelid
							   AND con.conrelid = v_relid
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
				  comment text, refcols text[])
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

		EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', r.contable, r.conname);
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
				  reloptions text[], comment text, grants jsonb, depth int)
			 ORDER BY depth DESC
	LOOP
		EXECUTE format('DROP VIEW %I.%I', r."schema", r.name);
	END LOOP;

	FOR r IN SELECT * FROM jsonb_to_recordset(v_views)
			 AS x("schema" name, name name, def text, owner text,
				  reloptions text[], comment text, grants jsonb, depth int)
			 ORDER BY depth ASC
	LOOP
		EXECUTE format('CREATE VIEW %I.%I%s AS %s',
					   r."schema", r.name,
					   CASE WHEN r.reloptions IS NOT NULL
							THEN format(' WITH (%s)', array_to_string(r.reloptions, ', '))
						ELSE '' END,
					   r.def);
		EXECUTE format('ALTER VIEW %I.%I OWNER TO %I', r."schema", r.name, r.owner);
		FOR r2 IN SELECT value #>> '{}' AS stmt FROM jsonb_array_elements(r.grants)
		LOOP
			EXECUTE r2.stmt;
		END LOOP;
		IF r.comment IS NOT NULL THEN
			EXECUTE format('COMMENT ON VIEW %I.%I IS %L', r."schema", r.name, r.comment);
		END IF;
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
		 renamed_indexes, fks_to_validate)
	VALUES
		(p_schema, p_table, p_control, v_interval,
		 v_backup, v_template, 'migrating', v_rowcount,
		 v_renames, v_validate);

	RAISE NOTICE 'dbblue_partition: % converted; % row(s) wait in % and will be moved in batches',
		v_qualified, v_rowcount, v_qualified_old;
END
$$;

REVOKE ALL ON FUNCTION @extschema@.dbblue_partition_convert(name, name, name, text, int) FROM PUBLIC;


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
CREATE PROCEDURE @extschema@.dbblue_partition_model(
	p_model text,
	p_control text DEFAULT 'create_date',
	p_interval text DEFAULT '1 month',
	p_schema text DEFAULT 'public',
	p_premake int DEFAULT 4,
	p_batch_interval interval DEFAULT NULL,
	p_single_transaction boolean DEFAULT false,
	p_analyze boolean DEFAULT true)
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
	v_lock_fail		int := 0;
	v_ignored_cols	text[];
	v_has_identity	boolean;
	v_old_role		text;
	v_old_rowsec	text;
	v_old_datestyle	text;
	v_can_replica	boolean := true;
	r				record;
BEGIN
	PERFORM @extschema@.dbblue_partition_enabled_check();

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
		RAISE NOTICE 'dbblue_partition: resuming interrupted data migration for %', v_qualified;
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
		EXECUTE pg_catalog.format(
			'SELECT %I.partition_data_time(p_parent_table := %L, '
			'p_batch_count := 1, p_batch_interval := %L, p_lock_wait := 0, '
			'p_analyze := false, p_source_table := %L, '
			'p_ignored_columns := %L, p_override_system_value := %L)',
			v_partman, v_raw,
			COALESCE(p_batch_interval, v_cat.partition_interval),
			v_backup_raw, v_ignored_cols, v_has_identity)
		INTO v_moved;

		IF v_moved = -1 THEN
			v_lock_fail := v_lock_fail + 1;
			IF v_lock_fail > 10 THEN
				RAISE EXCEPTION 'could not obtain row locks on % after 10 attempts; conversion stays resumable by repeating the same CALL',
					v_backup_raw;
			END IF;
			CONTINUE;
		END IF;
		v_lock_fail := 0;

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

	EXECUTE pg_catalog.format('SELECT pg_catalog.count(*) FROM %I.%I',
							  p_schema, v_table)
	INTO v_new_count;
	IF v_new_count < v_cat.rows_at_conversion THEN
		RAISE EXCEPTION 'row count mismatch after migration: % has % rows but % were captured at conversion',
			v_qualified, v_new_count, v_cat.rows_at_conversion
			USING HINT = 'The backup table has not been touched; investigate before dropping it.';
	END IF;

	EXECUTE pg_catalog.format('SELECT %I.partition_gap_fill(%L)', v_partman, v_raw);

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

	RAISE NOTICE 'dbblue_partition: % complete; % row(s) moved in % batch(es); backup kept as %; drop it with dbblue_partition_drop_backup(%)',
		v_qualified, v_total_moved, v_batches, v_backup_raw, p_model;
END
$$;

REVOKE ALL ON PROCEDURE @extschema@.dbblue_partition_model(text, text, text, text, int, interval, boolean, boolean) FROM PUBLIC;


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
	default_partition_rows bigint,
	backup_table name,
	backup_exists boolean,
	backup_rows_remaining bigint,
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

			SELECT i.inhrelid INTO v_default
			FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			WHERE i.inhparent = v_relid
			  AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT';
			IF v_default IS NULL THEN
				default_partition_rows := 0;
			ELSE
				EXECUTE format('SELECT count(*) FROM %s', v_default::regclass)
				INTO default_partition_rows;
			END IF;
		END IF;

		backup_table := v_cat.backup_table;
		-- to_regclass() instead of a ::regclass cast: a dropped backup must
		-- report false here, not break the whole status function.
		backup_exists := to_regclass(format('%I.%I', v_cat.parent_schema, v_cat.backup_table)) IS NOT NULL;
		IF backup_exists THEN
			EXECUTE format('SELECT count(*) FROM %I.%I', v_cat.parent_schema, v_cat.backup_table)
			INTO backup_rows_remaining;
		ELSE
			backup_rows_remaining := 0;
		END IF;

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
CREATE PROCEDURE @extschema@.dbblue_partition_undo(
	p_model text,
	p_schema text DEFAULT 'public')
LANGUAGE plpgsql
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
	WHERE dv.view_kind <> 'v';
	IF v_bad IS NOT NULL THEN
		RAISE EXCEPTION 'objects depending on % cannot be recreated automatically: %', v_qualified, v_bad;
	END IF;

	SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
			'schema', dv.view_schema,
			'name', dv.view_name,
			'def', pg_catalog.pg_get_viewdef(dv.view_oid),
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
			 AS x("schema" name, name name, depth int)
			 ORDER BY depth DESC
	LOOP
		EXECUTE pg_catalog.format('DROP VIEW %I.%I', r."schema", r.name);
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
			 AS x(contable text, conname name, condef text, comment text)
	LOOP
		EXECUTE pg_catalog.format('ALTER TABLE %s ADD CONSTRAINT %I %s',
								  r.contable, r.conname, r.condef);
		IF r.comment IS NOT NULL THEN
			EXECUTE pg_catalog.format('COMMENT ON CONSTRAINT %I ON %s IS %L',
									  r.conname, r.contable, r.comment);
		END IF;
	END LOOP;

	FOR r IN SELECT * FROM pg_catalog.jsonb_to_recordset(v_views)
			 AS x("schema" name, name name, def text, owner text,
				  reloptions text[], comment text, grants jsonb, depth int)
			 ORDER BY depth ASC
	LOOP
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

	FOR r IN SELECT * FROM pg_catalog.jsonb_to_recordset(v_pubs) AS x(pubname name)
	LOOP
		EXECUTE pg_catalog.format('ALTER PUBLICATION %I ADD TABLE %I.%I',
								  r.pubname, p_schema, v_table);
	END LOOP;

	DELETE FROM @extschema@.dbblue_partition_catalog
	WHERE parent_schema = p_schema AND parent_table = v_table;

	RAISE NOTICE 'dbblue_partition: % restored as a plain table with % row(s)',
		v_qualified, v_backup_after;
END
$$;

REVOKE ALL ON PROCEDURE @extschema@.dbblue_partition_undo(text, text) FROM PUBLIC;
