/* contrib/dbblue_partition/dbblue_partition--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION dbblue_partition UPDATE TO '1.1'" to load this file. \quit

/*
 * 1.1: dbblue_partition_undo recreates, on the restored plain table, the
 * indexes and unique constraints that were created on the partitioned table
 * after the conversion.  An Odoo module update re-issues its CREATE UNIQUE
 * INDEX / ADD CONSTRAINT ... UNIQUE statements on the partitioned table,
 * where DBblue makes them GLOBAL indexes; 1.0's undo dropped those together
 * with the partitioned table and the restored table was left without them.
 */

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
	 * (the backup's indexes, renamed back below), and the "_fkuq" unique
	 * indexes the conversion itself created to back incoming foreign keys,
	 * which only made sense on the partitioned table.
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
	  AND ci.relname NOT LIKE pg_catalog.left(v_table, 50) || '\_fkuq%';

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
