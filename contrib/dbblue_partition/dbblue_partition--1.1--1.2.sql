/* contrib/dbblue_partition/dbblue_partition--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION dbblue_partition UPDATE TO '1.2'" to load this file. \quit

/*
 * 1.2: dbblue_partition_model() finishes the Odoo integration on its own.
 *
 * After a successful conversion it now calls
 * dbblue_partition_odoo_compat(<database owner>) automatically (an Odoo
 * database is always owned by the Odoo db_user), so the one manual step
 * disappears.  Opt out with p_odoo_compat => false.
 *
 * The parameter list changed, so the old procedure must be dropped first:
 * CREATE OR REPLACE would create an ambiguous overload instead.
 */
DROP PROCEDURE @extschema@.dbblue_partition_model(text, text, text, text, int, interval, boolean, boolean);

CREATE PROCEDURE @extschema@.dbblue_partition_model(
	p_model text,
	p_control text DEFAULT 'create_date',
	p_interval text DEFAULT '1 month',
	p_schema text DEFAULT 'public',
	p_premake int DEFAULT 4,
	p_batch_interval interval DEFAULT NULL,
	p_single_transaction boolean DEFAULT false,
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
	v_lock_fail		int := 0;
	v_ignored_cols	text[];
	v_has_identity	boolean;
	v_old_role		text;
	v_old_rowsec	text;
	v_old_datestyle	text;
	v_can_replica	boolean := true;
	v_db_owner		name;
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

	----------------------------------------------------------------------
	-- DBblue serves Odoo: unless disabled, configure the Odoo
	-- compatibility view for the database owner (in Odoo deployments the
	-- Odoo db_user owns its database), so an unmodified Odoo keeps
	-- recognising the now-partitioned table.  Failure to do so must not
	-- fail the conversion itself.
	----------------------------------------------------------------------
	IF p_odoo_compat THEN
		SELECT pg_catalog.pg_get_userbyid(d.datdba) INTO v_db_owner
		FROM pg_catalog.pg_database d
		WHERE d.datname = pg_catalog.current_database();
		BEGIN
			PERFORM @extschema@.dbblue_partition_odoo_compat(v_db_owner);
			RAISE NOTICE 'dbblue_partition: Odoo compatibility configured for role "%"; restart Odoo so its connections pick it up', v_db_owner;
		EXCEPTION WHEN insufficient_privilege THEN
			RAISE WARNING 'dbblue_partition: could not configure Odoo compatibility for role "%" automatically; run SELECT dbblue_partition_odoo_compat(...) as a superuser', v_db_owner;
		END;
	END IF;

	RAISE NOTICE 'dbblue_partition: % complete; % row(s) moved in % batch(es); backup kept as %; drop it with dbblue_partition_drop_backup(%)',
		v_qualified, v_total_moved, v_batches, v_backup_raw, p_model;
END
$$;

REVOKE ALL ON PROCEDURE @extschema@.dbblue_partition_model(text, text, text, text, int, interval, boolean, boolean, boolean) FROM PUBLIC;
