/* contrib/dbblue_partition/dbblue_partition--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION dbblue_partition UPDATE TO '1.3'" to load this file. \quit

/*
 * 1.3: a converted table keeps its primary key on its own columns -- (id)
 * for Odoo -- as a DBblue GLOBAL index instead of widening it to
 * (id, <control>).  The key is then enforced across all partitions, Odoo's
 * "GROUP BY id" queries that select other columns of the table are accepted
 * again (functional dependency needs the primary key to be exactly (id)),
 * and incoming foreign keys are backed by a real unique key.
 * dbblue_partition_undo skips the helper index the conversion now creates.
 */

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
 *
 * 1.3: the primary key keeps its own columns as a DBblue GLOBAL index,
 * which enforces it across all partitions, instead of widening to
 * (id, <control>).  A plain partitioned index on the same columns sits
 * beside it, because global indexes are not used to find rows to lock,
 * update or delete (foreign key checks, Odoo's writes by id).  Tables that
 * cannot have a global primary key keep the widened one.
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
SET dbblue_auto_global_index = on
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
	v_global_pk		boolean;
	v_no_global		text;	-- why the primary key cannot be global
	v_pk_local		name;
	v_old_cmm		text;
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

	/*
	 * A global primary key covers the partitions without them having a key
	 * of their own.  That rules it out for UNLOGGED partitions (crash
	 * recovery would empty them but not the global index) and for a table
	 * whose partitions need a primary key as their replica identity: one
	 * that is published, or names an index as its replica identity.
	 */
	IF p_control = ANY (v_pk_cols) THEN
		v_no_global := 'the primary key already contains the partition key';
	ELSIF v_persistence = 'u' THEN
		v_no_global := 'the table is UNLOGGED';
	ELSIF v_replident = 'i' THEN
		v_no_global := 'the table uses REPLICA IDENTITY USING INDEX';
	ELSIF v_replident <> 'f'
		  AND (EXISTS (SELECT 1 FROM jsonb_array_elements(v_pubs))
			   OR EXISTS (SELECT 1 FROM pg_publication WHERE puballtables)
			   OR EXISTS (SELECT 1 FROM pg_publication_namespace pn
						  JOIN pg_namespace n ON n.oid = pn.pnnspid
						  WHERE n.nspname = p_schema)) THEN
		v_no_global := 'the table is published for logical replication';
	END IF;
	v_global_pk := v_no_global IS NULL;

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

	-- Primary key: global on its own columns where possible (see above),
	-- otherwise composite, the original columns plus the partition key
	v_new_pk_cols := v_pk_cols;
	IF v_global_pk THEN
		/*
		 * pg_partman requires a NOT NULL control column, which the widened
		 * key used to imply; the NULL check above guarantees the data fits.
		 */
		EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN %I SET NOT NULL',
					   p_schema, p_table, p_control);
		RAISE NOTICE 'dbblue_partition: primary key of % stays (%) as a GLOBAL index, unique across all partitions',
			v_qualified, array_to_string(v_pk_cols, ', ');
	ELSIF NOT p_control = ANY (v_pk_cols) THEN
		v_new_pk_cols := v_pk_cols || p_control;
		RAISE WARNING 'primary key of % widens from (%) to (%): "%" is unique per partition only',
			v_qualified, array_to_string(v_pk_cols, ', '),
			array_to_string(v_new_pk_cols, ', '),
			array_to_string(v_pk_cols, ', ')
			USING DETAIL = 'It cannot be a global index because ' || v_no_global
						   || ', and a unique index on a partitioned table must otherwise include the partition key. Odoo ids come from a sequence, so cross-partition duplicates do not occur in normal operation.',
				  HINT = 'Monitoring: SELECT ' || array_to_string(v_pk_cols, ', ') || ', count(*) FROM '
						 || v_qualified || ' GROUP BY ' || array_to_string(v_pk_cols, ', ')
						 || ' HAVING count(*) > 1;';
	END IF;

	-- DBblue announces the conversion to GLOBAL itself; the NOTICE above
	-- says the same in this extension's terms
	v_old_cmm := current_setting('client_min_messages');
	IF v_global_pk THEN
		PERFORM set_config('client_min_messages', 'warning', true);
	END IF;
	EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I PRIMARY KEY (%s)',
				   p_schema, p_table, v_pk_conname,
				   (SELECT string_agg(quote_ident(c), ', ') FROM unnest(v_new_pk_cols) c));
	PERFORM set_config('client_min_messages', v_old_cmm, true);
	IF v_pk_comment IS NOT NULL THEN
		EXECUTE format('COMMENT ON CONSTRAINT %I ON %I.%I IS %L',
					   v_pk_conname, p_schema, p_table, v_pk_comment);
	END IF;

	/*
	 * Global indexes are not used to find the rows a statement locks,
	 * updates or deletes (EvalPlanQual rechecks need the partitions' own
	 * scans), so without a per-partition index on the key every foreign key
	 * check and every UPDATE/DELETE ... WHERE id = ... would scan whole
	 * partitions.  The widened key provided that index; provide it here.
	 */
	IF v_global_pk THEN
		v_pk_local := (left(p_table, 50) || '_pklocal')::name;
		EXECUTE format('CREATE INDEX %I ON %I.%I (%s)',
					   v_pk_local, p_schema, p_table,
					   (SELECT string_agg(quote_ident(c), ', ') FROM unnest(v_pk_cols) c));
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
	 * An empty table has no oldest row to anchor on.  Left NULL,
	 * pg_partman's own default kicks in -- CURRENT_TIMESTAMP - (interval *
	 * p_premake) -- which backfills p_premake *historical* partitions on
	 * top of the p_premake future ones, even though there is no data to put
	 * in them.  Anchor on now() instead, so an empty table gets only the
	 * current partition plus p_premake ahead, exactly like p_premake alone
	 * would suggest.
	 */
	IF v_min_control IS NULL THEN
		v_min_control := now()::text;
	END IF;

	/*
	 * pg_partman materializes one partition per interval from
	 * p_start_partition all the way to now + premake, with no cap, and it
	 * does it inside this transaction while the table is locked.  A single
	 * row with a nonsense old date -- an Odoo import that backfilled
	 * create_date to the epoch, say -- would therefore create tens of
	 * thousands of child tables and their index sets before the conversion
	 * could finish.  Refuse instead, and name the row that caused it.
	 */
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
	-- 10. Incoming FKs.  Odoo emits single-column FKs against id, which
	--     the global PK backs (or, when widened, the DBblue core patch
	--     accepts against the composite PK).  A FK
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
 * dbblue_partition_undo
 *
 * Reverse a conversion while the backup table still exists: move all rows
 * back into the original (still unpartitioned) table structure, restore
 * its name, indexes, FKs and views, and deregister from pg_partman.  Runs
 * in a single transaction, so a failure rolls everything back.
 *
 * 1.1: indexes and unique constraints created on the partitioned table
 * after the conversion (by an Odoo module update, typically as DBblue
 * GLOBAL indexes) are recreated on the restored table as ordinary ones;
 * 1.0 dropped them with the partitioned table.
 *
 * 1.3: the "_pklocal" index the conversion puts beside a global primary key
 * is not recreated; the restored table's primary key covers it.
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
	v_extra			jsonb;
	v_comments		jsonb;
	v_restored		name[];
	r				record;
	r2				record;
BEGIN
	-- PERFORM @extschema@.dbblue_partition_enabled_check();

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

	/*
	 * Indexes and unique / primary key / exclusion constraints created on
	 * the partitioned table after the conversion -- typically by an Odoo
	 * module update, whose CREATE UNIQUE INDEX or ADD CONSTRAINT ... UNIQUE
	 * DBblue turned into GLOBAL indexes.  The restored table only has what
	 * the backup had at conversion time, and dropping the partitioned table
	 * drops these, so capture them to recreate them, as ordinary (non-global)
	 * objects, once the backup is back under the original name.
	 *
	 * Skipped: everything the restored table will have under the same name
	 * (the backup's indexes, renamed back below), and the indexes the
	 * conversion itself created, which only made sense on the partitioned
	 * table: the "_fkuq" unique indexes backing incoming foreign keys and
	 * the "_pklocal" index beside a global primary key.
	 */
	SELECT pg_catalog.array_agg(COALESCE(rn."from", ci.relname))
	INTO v_restored
	FROM pg_catalog.pg_index ix
	JOIN pg_catalog.pg_class ci ON ci.oid = ix.indexrelid
	LEFT JOIN pg_catalog.jsonb_to_recordset(v_cat.renamed_indexes)
		AS rn("from" name, "to" name) ON rn."to" = ci.relname
	WHERE ix.indrelid = v_backup_relid;

	SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
			'name', ci.relname,
			'conname', con.conname,
			-- a constraint is re-added as a constraint (its definition never
			-- shows a global index's hidden partition key column); a plain
			-- index loses GLOBAL and ON ONLY, which a plain table rejects
			'stmt', CASE WHEN con.oid IS NOT NULL
						 THEN pg_catalog.format('ALTER TABLE %s ADD CONSTRAINT %I %s',
												v_qualified, con.conname,
												pg_catalog.pg_get_constraintdef(con.oid))
						 ELSE pg_catalog.regexp_replace(
								  pg_catalog.regexp_replace(
									  pg_catalog.pg_get_indexdef(ix.indexrelid),
									  '^CREATE (UNIQUE )?INDEX GLOBAL ', 'CREATE \1INDEX '),
								  ' ON ONLY ', ' ON ') END,
			'comment', CASE WHEN con.oid IS NOT NULL
							THEN pg_catalog.obj_description(con.oid, 'pg_constraint')
							ELSE pg_catalog.obj_description(ix.indexrelid, 'pg_class') END)
			ORDER BY ci.relname), '[]'::jsonb)
	INTO v_extra
	FROM pg_catalog.pg_index ix
	JOIN pg_catalog.pg_class ci ON ci.oid = ix.indexrelid
	LEFT JOIN pg_catalog.pg_constraint con
		ON con.conindid = ix.indexrelid AND con.conrelid = v_relid
	   AND con.contype IN ('p', 'u', 'x')
	WHERE ix.indrelid = v_relid
	  AND NOT (ci.relname = ANY (COALESCE(v_restored, '{}')))
	  AND ci.relname NOT LIKE pg_catalog.left(v_table, 50) || '\_fkuq%'
	  AND ci.relname <> pg_catalog.left(v_table, 50) || '_pklocal';

	/*
	 * Comments of the partitioned table's constraints and indexes, which an
	 * Odoo update may have set (Odoo keeps a constraint's definition there
	 * and compares it on the next update).  Copied below onto the restored
	 * table's object of the same name where that one has none.
	 */
	SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
			'name', ci.relname,
			'conname', con.conname,
			'comment', COALESCE(pg_catalog.obj_description(con.oid, 'pg_constraint'),
								pg_catalog.obj_description(ix.indexrelid, 'pg_class')))), '[]'::jsonb)
	INTO v_comments
	FROM pg_catalog.pg_index ix
	JOIN pg_catalog.pg_class ci ON ci.oid = ix.indexrelid
	LEFT JOIN pg_catalog.pg_constraint con
		ON con.conindid = ix.indexrelid AND con.conrelid = v_relid
	   AND con.contype IN ('p', 'u', 'x')
	WHERE ix.indrelid = v_relid
	  AND COALESCE(pg_catalog.obj_description(con.oid, 'pg_constraint'),
				   pg_catalog.obj_description(ix.indexrelid, 'pg_class')) IS NOT NULL;

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

	/*
	 * Recreate the indexes and constraints added after the conversion (see
	 * above).  The rows come from a table that enforced them, so this should
	 * not fail; if it does anyway, say so and leave the restore otherwise
	 * complete, so that the data is not held hostage by one index.
	 */
	FOR r IN SELECT * FROM pg_catalog.jsonb_to_recordset(v_extra)
			 AS x(name name, conname name, stmt text, comment text)
	LOOP
		BEGIN
			EXECUTE r.stmt;
			IF r.comment IS NOT NULL AND r.conname IS NOT NULL THEN
				EXECUTE pg_catalog.format('COMMENT ON CONSTRAINT %I ON %s IS %L',
										  r.conname, v_qualified, r.comment);
			ELSIF r.comment IS NOT NULL THEN
				EXECUTE pg_catalog.format('COMMENT ON INDEX %I.%I IS %L',
										  p_schema, r.name, r.comment);
			END IF;
			RAISE NOTICE 'dbblue_partition: recreated % % on the restored table',
				CASE WHEN r.conname IS NOT NULL THEN 'constraint' ELSE 'index' END,
				pg_catalog.quote_ident(r.name);
		EXCEPTION WHEN OTHERS THEN
			RAISE WARNING 'could not recreate % on %: %',
				pg_catalog.quote_ident(r.name), v_qualified, SQLERRM
				USING DETAIL = pg_catalog.format('Statement: %s', r.stmt);
		END;
	END LOOP;

	-- Carry over comments the restored objects of the same name lack
	FOR r IN SELECT * FROM pg_catalog.jsonb_to_recordset(v_comments)
			 AS x(name name, conname name, comment text)
	LOOP
		IF r.conname IS NOT NULL THEN
			IF EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c
					   WHERE c.conrelid = v_qualified::regclass AND c.conname = r.conname
						 AND pg_catalog.obj_description(c.oid, 'pg_constraint') IS NULL) THEN
				EXECUTE pg_catalog.format('COMMENT ON CONSTRAINT %I ON %s IS %L',
										  r.conname, v_qualified, r.comment);
			END IF;
		ELSIF EXISTS (SELECT 1 FROM pg_catalog.pg_index i
					  JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
					  WHERE i.indrelid = v_qualified::regclass AND c.relname = r.name
						AND pg_catalog.obj_description(c.oid, 'pg_class') IS NULL) THEN
			EXECUTE pg_catalog.format('COMMENT ON INDEX %I.%I IS %L',
									  p_schema, r.name, r.comment);
		END IF;
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
