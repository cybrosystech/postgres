/* contrib/dbblue_partition/dbblue_partition--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION dbblue_partition UPDATE TO '1.2'" to load this file. \quit

/*
 * 1.2: dbblue_partition_estimate predicts how long dbblue_partition_model
 * will take, before it is called, so an application can hold its screen
 * for that long (or show a progress bar against it) while the conversion
 * runs.
 */

/* ------------------------------------------------------------------------
 * dbblue_partition_estimate
 *
 * Takes the same arguments as dbblue_partition_model and returns the
 * expected duration of that exact call, with a per-phase breakdown.
 *
 * The prediction is a cost model fed by measurements on this server and
 * this table, not by built-in constants:
 *
 *   - sequential read speed: a TID range scan of up to 128MB of the table;
 *   - per-partition DDL cost: CREATE TABLE ... (LIKE ... INCLUDING ALL),
 *     which builds the same index set every partition will get;
 *   - per-row move cost: p_sample_rows real rows moved into that scratch
 *     table with the same DELETE ... RETURNING / INSERT statement
 *     pg_partman runs per batch (every index maintained, WAL written);
 *   - per-partition ANALYZE cost: ANALYZE of the filled scratch table.
 *
 * The scratch work runs in a subtransaction that is always rolled back, so
 * nothing persists.  The table itself is only read (ACCESS SHARE): calling
 * this never blocks the application.  Row counts, column bounds and the
 * size of FK-related tables come from the catalogs and planner statistics,
 * so the function returns in about a second even for very large tables.
 *
 * What the model counts, mirroring dbblue_partition_model:
 *   structural swap: NULL check, row count, min(control) on the backup,
 *     and creation of every partition from the oldest row up to premake;
 *   data move: every row deleted from the backup and inserted into its
 *     partition; per batch, pg_partman's range DELETE scans the whole
 *     backup and compares the control column of every row not yet moved
 *     as text, so the batch count multiplies with the row count (dead rows
 *     keep the backup at full size until VACUUM), and its min() is one
 *     more full scan when no index leads on the control column;
 *   finalize: count(*) of backup and new table, VALIDATE of every
 *     re-created FK (a join of both tables, per partition for outgoing
 *     FKs), ANALYZE of the tree and of every partition.
 *
 * Accuracy is limited by what cannot be known in advance: cache state,
 * concurrent load, checkpoints, autovacuum, standby round trips.  The
 * sample runs warm, so a table much larger than RAM moves slower than
 * predicted; the notes column says so when it applies.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_estimate(
	p_model text,
	p_control text DEFAULT 'create_date',
	p_interval text DEFAULT '1 year',
	p_schema text DEFAULT 'public',
	p_premake int DEFAULT 4,
	p_batch_interval interval DEFAULT NULL,
	p_single_transaction boolean DEFAULT true,
	p_analyze boolean DEFAULT true,
	p_sample_rows int DEFAULT 50000)
RETURNS TABLE (
	table_name text,
	row_count bigint,
	table_size text,
	index_count int,
	partition_count int,
	batch_count int,
	control_indexed boolean,
	fks_to_validate int,
	estimated_seconds numeric,
	estimated_seconds_high numeric,
	table_locked_seconds numeric,
	breakdown jsonb,
	notes text[])
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog, pg_temp
SET datestyle = 'ISO'
AS $$
DECLARE
	v_table			name;
	v_qualified		text;
	v_relid			oid;
	v_relkind		"char";
	v_reltuples		real;
	v_relpages		int;
	v_pages			bigint;
	v_block_size	int := current_setting('block_size')::int;
	v_interval		interval;
	v_batch_int		interval;
	v_control_attnum smallint;
	v_control_typid	oid;
	v_null_frac		real;
	v_has_nulls		boolean := false;
	v_min			timestamptz;
	v_max			timestamptz;
	v_scratch		name;
	v_scratch_src	name;
	v_analyze_secs	float8 := 0;
	v_stat_rows		float8 := 300 * current_setting('default_statistics_target')::int;
	v_cols			text;
	v_override		text := '';
	v_scan_pages	bigint := 0;
	v_scan_start	bigint;
	v_scan_secs		float8 := 0;
	v_ddl_secs		float8 := 0;
	v_row_secs		float8 := 0;
	v_sampled		bigint := 0;
	v_t0			timestamptz;
	v_pages_per_sec	float8;
	v_par_scan		float8;
	v_fk_rows		float8 := 0;
	v_pk_col		name;
	v_join_row_secs	float8 := 0;
	v_decoded		text;
	v_half			bigint;
	v_moved			bigint;
	v_filter_row_secs float8 := 0;
	v_part_rows		float8;
	v_part_pages	float8;
	v_sample_pages	float8;
	v_s_struct		float8;
	v_s_move		float8;
	v_s_batch_scans	float8;
	v_s_counts		float8;
	v_s_fks			float8;
	v_s_analyze		float8;
	v_total			float8;
	v_notes			text[] := '{}';
BEGIN
	v_table := @extschema@.dbblue_partition_resolve_table(p_model);
	v_qualified := format('%I.%I', p_schema, v_table);
	v_relid := to_regclass(v_qualified);
	IF v_relid IS NULL THEN
		RAISE EXCEPTION 'table %.% does not exist', p_schema, v_table;
	END IF;

	SELECT c.relkind, c.reltuples, c.relpages
	INTO v_relkind, v_reltuples, v_relpages
	FROM pg_class c WHERE c.oid = v_relid;
	IF v_relkind = 'p' THEN
		RAISE EXCEPTION 'table % is already partitioned', v_qualified;
	ELSIF v_relkind <> 'r' THEN
		RAISE EXCEPTION 'relation % is not an ordinary table (relkind "%")', v_qualified, v_relkind;
	END IF;
	IF EXISTS (SELECT 1 FROM @extschema@.dbblue_partition_catalog c
			   WHERE c.parent_schema = p_schema AND c.parent_table = v_table) THEN
		RAISE EXCEPTION 'table % is already registered with dbblue_partition', v_qualified
			USING HINT = 'See dbblue_partition_status() for an unfinished conversion.';
	END IF;

	SELECT a.attnum, a.atttypid INTO v_control_attnum, v_control_typid
	FROM pg_attribute a
	WHERE a.attrelid = v_relid AND a.attname = p_control
	  AND a.attnum > 0 AND NOT a.attisdropped;
	IF v_control_attnum IS NULL THEN
		RAISE EXCEPTION 'column "%" of table % does not exist', p_control, v_qualified;
	END IF;
	IF v_control_typid NOT IN ('date'::regtype, 'timestamp'::regtype,
							   'timestamptz'::regtype) THEN
		RAISE EXCEPTION 'column "%" has type %, but range partitioning needs date, timestamp or timestamptz',
			p_control, format_type(v_control_typid, NULL);
	END IF;

	v_interval := p_interval::interval;
	v_batch_int := coalesce(p_batch_interval, v_interval);
	IF v_interval <= '0'::interval OR v_batch_int <= '0'::interval THEN
		RAISE EXCEPTION 'partition and batch intervals must be positive';
	END IF;
	IF p_sample_rows IS NULL OR p_sample_rows < 100 THEN
		RAISE EXCEPTION 'p_sample_rows must be at least 100';
	END IF;

	----------------------------------------------------------------------
	-- Table shape, from the catalogs
	----------------------------------------------------------------------
	table_name := v_qualified;
	v_pages := pg_relation_size(v_relid) / v_block_size;
	table_size := pg_size_pretty(pg_total_relation_size(v_relid));

	-- The cumulative statistics track committed inserts and deletes
	-- continuously; after a stats reset, scale the last ANALYZE/VACUUM
	-- density to the current size, as the planner does.  Count only when
	-- the table was never analyzed.
	IF pg_stat_get_live_tuples(v_relid) > 0 THEN
		row_count := pg_stat_get_live_tuples(v_relid);
	ELSIF v_reltuples >= 0 AND v_relpages > 0 THEN
		row_count := round(v_reltuples::float8 / v_relpages * v_pages);
	ELSIF v_reltuples >= 0 AND v_pages = 0 THEN
		row_count := 0;
	ELSE
		EXECUTE format('SELECT count(*) FROM ONLY %s', v_qualified) INTO row_count;
		v_notes := v_notes || 'table was never analyzed; rows were counted, column bounds may be inexact'::text;
	END IF;

	SELECT count(*) INTO index_count FROM pg_index i WHERE i.indrelid = v_relid;

	control_indexed := EXISTS (
		SELECT 1 FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid
		JOIN pg_am am ON am.oid = ic.relam
		WHERE i.indrelid = v_relid AND am.amname = 'btree'
		  AND i.indkey[0] = v_control_attnum AND i.indpred IS NULL);

	-- Oldest and newest control values: exact through an index, otherwise
	-- from the planner statistics (a full scan is what this avoids).
	IF control_indexed OR row_count = 0 THEN
		EXECUTE format('SELECT min(%1$I)::timestamptz, max(%1$I)::timestamptz FROM ONLY %2$s',
					   p_control, v_qualified)
		INTO v_min, v_max;
		EXECUTE format('SELECT EXISTS (SELECT 1 FROM ONLY %s WHERE %I IS NULL)',
					   v_qualified, p_control)
		INTO v_has_nulls;
	ELSE
		SELECT s.null_frac,
			   least((s.histogram_bounds::text::timestamptz[])[1],
					 (SELECT min(v) FROM unnest(s.most_common_vals::text::timestamptz[]) v)),
			   greatest((s.histogram_bounds::text::timestamptz[])[array_upper(s.histogram_bounds::text::timestamptz[], 1)],
						(SELECT max(v) FROM unnest(s.most_common_vals::text::timestamptz[]) v))
		INTO v_null_frac, v_min, v_max
		FROM pg_stats s
		WHERE s.schemaname = p_schema AND s.tablename = v_table
		  AND s.attname = p_control AND NOT s.inherited;
		IF v_min IS NULL THEN
			EXECUTE format('SELECT min(%1$I)::timestamptz, max(%1$I)::timestamptz FROM ONLY %2$s',
						   p_control, v_qualified)
			INTO v_min, v_max;
		END IF;
		v_has_nulls := v_null_frac > 0;
	END IF;
	IF v_has_nulls THEN
		v_notes := v_notes || format('column "%s" contains NULLs; dbblue_partition_model will refuse to convert', p_control);
	END IF;

	-- One partition per interval from the oldest row to now, plus premake
	-- ahead -- what create_parent() materializes during the swap.
	partition_count := (SELECT count(*) FROM generate_series(
							date_trunc('day', coalesce(v_min, now())), now(), v_interval))
					   + greatest(p_premake, 1);
	IF partition_count > 2000 THEN
		v_notes := v_notes || format('%s partitions: dbblue_partition_model refuses more than 2000; use a coarser interval', partition_count);
	END IF;

	-- One batch per batch window between the oldest and newest row.  The
	-- windows follow partition boundaries, not the oldest row, so the span
	-- usually straddles one more of them than its length suggests.
	batch_count := CASE WHEN row_count = 0 OR v_min IS NULL THEN 0
					ELSE (SELECT count(*) FROM generate_series(v_min, v_max, v_batch_int))::int + 1 END;
	IF NOT control_indexed AND batch_count > 0 THEN
		v_notes := v_notes || format('no index leads on "%s": every batch starts with a full scan for min(%s); CREATE INDEX CONCURRENTLY on it first to remove that cost',
									 p_control, p_control);
	END IF;
	IF batch_count > 24 THEN
		v_notes := v_notes || format('%s batches: each one rescans every row still in the backup (pg_partman compares "%s" as text, so no index helps); a coarser p_interval moves the same rows in fewer, cheaper passes',
									 batch_count, p_control);
	END IF;

	-- Every validated FK touching the table is re-added NOT VALID and
	-- validated after the move: a join of the referencing and the
	-- referenced table.  An outgoing FK is validated partition by
	-- partition, each joined against the whole referenced table.
	SELECT count(*),
		   coalesce(sum(CASE WHEN con.conrelid = v_relid
							 THEN row_count + greatest(batch_count, 1) *
								  CASE WHEN con.confrelid = v_relid THEN row_count
									   ELSE coalesce(nullif(pg_stat_get_live_tuples(pc.oid), 0), greatest(pc.reltuples, 0)) END
							 ELSE coalesce(nullif(pg_stat_get_live_tuples(fc.oid), 0), greatest(fc.reltuples, 0))
								  + row_count END), 0)
	INTO fks_to_validate, v_fk_rows
	FROM pg_constraint con
	JOIN pg_class fc ON fc.oid = con.conrelid
	JOIN pg_class pc ON pc.oid = con.confrelid
	WHERE con.contype = 'f' AND con.convalidated
	  AND (con.conrelid = v_relid OR con.confrelid = v_relid);

	SELECT a.attname INTO v_pk_col
	FROM pg_constraint con
	JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = con.conkey[1]
	WHERE con.conrelid = v_relid AND con.contype = 'p';
	IF v_pk_col IS NULL THEN
		RAISE EXCEPTION 'table % has no primary key', v_qualified
			USING DETAIL = 'dbblue_partition expects an Odoo-style table with a primary key.';
	END IF;

	IF EXISTS (SELECT 1 FROM @extschema@.dbblue_partition_dependent_views(v_relid) dv
			   JOIN pg_class c ON c.oid = dv.view_oid
			   WHERE dv.view_kind = 'm' AND c.relispopulated) THEN
		v_notes := v_notes || 'populated materialized views depend on the table; their REFRESH at the end is not included'::text;
	END IF;

	----------------------------------------------------------------------
	-- Measure this server on this table.  Everything below is rolled back.
	----------------------------------------------------------------------
	v_scratch := ('dbblue_est_' || left(v_table, 40))::name;
	v_scratch_src := ('dbblue_est_src_' || left(v_table, 40))::name;
	IF to_regclass(format('%I.%I', p_schema, v_scratch)) IS NOT NULL
	   OR to_regclass(format('%I.%I', p_schema, v_scratch_src)) IS NOT NULL THEN
		RAISE EXCEPTION 'relation %.% or %.% already exists', p_schema, v_scratch, p_schema, v_scratch_src;
	END IF;

	-- Generated columns are recomputed and identity values carried over,
	-- exactly as the data move does.
	SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum),
		   CASE WHEN bool_or(a.attidentity = 'a') THEN 'OVERRIDING SYSTEM VALUE' ELSE '' END
	INTO v_cols, v_override
	FROM pg_attribute a
	WHERE a.attrelid = v_relid AND a.attnum > 0 AND NOT a.attisdropped
	  AND a.attgenerated = '';

	BEGIN
		IF v_pages > 0 THEN
			-- Up to 128MB from the middle of the heap, sequentially and in
			-- one process: parallel worker startup makes a short parallel
			-- scan's speed too noisy to extrapolate from.  The rollback
			-- below undoes the SET.
			SET LOCAL max_parallel_workers_per_gather = 0;
			v_scan_pages := least(v_pages, 16384);
			v_scan_start := (v_pages - v_scan_pages) / 2;
			-- The conversion scans the table and its backup over and over,
			-- so the warm speed is the one that matters: keep the faster of
			-- two runs.
			FOR i IN 1..2 LOOP
				v_t0 := clock_timestamp();
				EXECUTE format('SELECT count(*) FROM ONLY %s WHERE ctid >= %L::tid AND ctid < %L::tid',
							   v_qualified, format('(%s,0)', v_scan_start),
							   format('(%s,0)', v_scan_start + v_scan_pages));
				v_scan_secs := least(coalesce(nullif(v_scan_secs, 0), 'Infinity'),
									 extract(epoch FROM clock_timestamp() - v_t0));
			END LOOP;
		END IF;

		-- Stand-in for the backup the move drains.  Without INCLUDING,
		-- generated and identity columns become plain ones, so SELECT *
		-- copies them verbatim.
		EXECUTE format('CREATE TABLE %I.%I (LIKE %s)',
					   p_schema, v_scratch_src, v_qualified);
		IF row_count > 0 AND p_sample_rows > 0 THEN
			EXECUTE format('INSERT INTO %I.%I SELECT * FROM ONLY %s LIMIT %s',
						   p_schema, v_scratch_src, v_qualified, p_sample_rows);
			GET DIAGNOSTICS v_sampled = ROW_COUNT;
		END IF;

		-- Stand-in for one partition: the full index set.
		v_t0 := clock_timestamp();
		EXECUTE format('CREATE TABLE %I.%I (LIKE %s INCLUDING ALL)',
					   p_schema, v_scratch, v_qualified);
		v_ddl_secs := extract(epoch FROM clock_timestamp() - v_t0);

		/*
		 * Each measurement below runs twice and the faster run counts: a
		 * sample this short is easily slowed by a burst of unrelated
		 * activity (autovacuum, a checkpoint) that a conversion lasting
		 * minutes averages out.
		 */
		IF v_sampled > 0 THEN
			-- The statement partition_data_time() runs for every batch, on
			-- each half of the sample in turn.
			v_half := pg_relation_size(format('%I.%I', p_schema, v_scratch_src)::regclass)
					  / v_block_size / 2;
			FOR i IN 1..2 LOOP
				v_t0 := clock_timestamp();
				EXECUTE format('WITH d AS (DELETE FROM %I.%I WHERE ctid %s %L::tid RETURNING *) '
							   'INSERT INTO %I.%I (%s) %s SELECT %s FROM d',
							   p_schema, v_scratch_src,
							   CASE i WHEN 1 THEN '<' ELSE '>=' END, format('(%s,0)', v_half),
							   p_schema, v_scratch, v_cols, v_override, v_cols);
				GET DIAGNOSTICS v_moved = ROW_COUNT;
				IF v_moved > 0 THEN
					v_row_secs := least(coalesce(nullif(v_row_secs, 0), 'Infinity'),
										extract(epoch FROM clock_timestamp() - v_t0) / v_moved);
				END IF;
			END LOOP;

			-- Every batch's range DELETE compares the control column as
			-- text (pg_partman's partition_data_time), so it evaluates this
			-- filter, both casts, on every row still in the backup, index
			-- or not.  The range below passes the first test and fails the
			-- second, like the not-yet-moved rows of a real batch.
			v_decoded := format(CASE WHEN v_control_typid = 'timestamp'::regtype
									 THEN '%I::timestamptz::text' ELSE '%I::text' END,
								p_control);
			FOR i IN 1..2 LOOP
				v_t0 := clock_timestamp();
				EXECUTE format('SELECT count(*) FROM ONLY %I.%I WHERE %s >= %L AND %s < %L',
							   p_schema, v_scratch, v_decoded, '0001-01-01',
							   v_decoded, '0001-01-02');
				v_filter_row_secs := least(coalesce(nullif(v_filter_row_secs, 0), 'Infinity'),
										   extract(epoch FROM clock_timestamp() - v_t0) / v_sampled);
			END LOOP;

			IF p_analyze THEN
				FOR i IN 1..2 LOOP
					v_t0 := clock_timestamp();
					EXECUTE format('ANALYZE %I.%I', p_schema, v_scratch);
					v_analyze_secs := least(coalesce(nullif(v_analyze_secs, 0), 'Infinity'),
											extract(epoch FROM clock_timestamp() - v_t0));
				END LOOP;
			END IF;

			-- The anti-join VALIDATE CONSTRAINT runs, on the primary key.
			IF fks_to_validate > 0 THEN
				FOR i IN 1..2 LOOP
					v_t0 := clock_timestamp();
					EXECUTE format('SELECT count(*) FROM ONLY %1$I.%2$I fk '
								   'LEFT JOIN ONLY %1$I.%2$I pk ON pk.%3$I = fk.%3$I '
								   'WHERE pk.%3$I IS NULL AND fk.%3$I IS NOT NULL',
								   p_schema, v_scratch, v_pk_col);
					v_join_row_secs := least(coalesce(nullif(v_join_row_secs, 0), 'Infinity'),
											 extract(epoch FROM clock_timestamp() - v_t0) / (2 * v_sampled));
				END LOOP;
			END IF;
		END IF;

		RAISE EXCEPTION USING ERRCODE = 'DBE01';
	EXCEPTION WHEN SQLSTATE 'DBE01' THEN
		NULL;
	END;

	IF v_sampled > 0 AND v_sampled < least(p_sample_rows, row_count) THEN
		v_notes := v_notes || format('only %s rows could be sampled', v_sampled);
	END IF;

	-- Floor the measured speed: a handful of cached pages reads in
	-- microseconds and would extrapolate to an absurd rate.
	v_pages_per_sec := CASE WHEN v_scan_pages >= 128 AND v_scan_secs > 0
							THEN v_scan_pages / v_scan_secs ELSE 50000 END;

	-- A full scan the planner may parallelize (count(*), min()) takes about
	-- half the serial time; workers add little beyond the second.
	v_par_scan := v_pages / v_pages_per_sec;
	IF current_setting('max_parallel_workers_per_gather')::int > 0
	   AND v_pages * v_block_size >= pg_size_bytes(current_setting('min_parallel_table_scan_size')) THEN
		v_par_scan := v_par_scan / 2;
	END IF;

	IF v_pages::float8 * v_block_size >
	   pg_size_bytes(current_setting('effective_cache_size')) THEN
		v_notes := v_notes || 'table is larger than effective_cache_size; reads will come from disk and run slower than the warm sample'::text;
	END IF;

	----------------------------------------------------------------------
	-- Cost model
	----------------------------------------------------------------------
	-- The factors below were calibrated against real conversions of
	-- Odoo-shaped tables (1-3M rows, 5-7 indexes, yearly and monthly
	-- partitions, one to sixty batches).

	-- Structural swap: NULL check, count(*), min(control) on the backup
	-- (a scan without an index), and pg_partman creating every partition
	-- -- about one measured CREATE TABLE ... LIKE each -- plus the parent,
	-- template and dependent-object rebuild.
	v_s_struct := (2 + CASE WHEN control_indexed THEN 0 ELSE 1 END) * v_par_scan
				  + v_ddl_secs * (partition_count + 5);

	-- A batch moves more rows than the sample, into larger indexes and
	-- through a larger (spilling) CTE, which costs more per row the more
	-- the batch outgrows the sample.  Batched mode adds a COMMIT per
	-- batch, which waits for WAL flush and any sync standby.
	v_s_move := (1 + 0.11 * ln(greatest(row_count::float8 / greatest(batch_count, 1)
										/ greatest(v_sampled / 2.0, 1), 1)))
				* row_count * v_row_secs
				+ batch_count * CASE WHEN p_single_transaction THEN 0.02 ELSE 0.05 END;

	-- Per batch, the range DELETE reads the whole backup serially and runs
	-- its text filter on every row not yet moved: rows * (batches + 1) / 2
	-- evaluations over the migration.  Without an index, min(control)
	-- adds a (parallel) scan per batch too.
	v_s_batch_scans := batch_count * (v_pages / v_pages_per_sec
									  + CASE WHEN control_indexed THEN 0 ELSE v_par_scan END)
					   + v_filter_row_secs * row_count * (batch_count + 1) / 2.0;

	-- count(*) of the emptied backup (still full size) and the new table.
	v_s_counts := 2.0 * v_par_scan;

	-- Each validation joins the referencing and the referenced table,
	-- costing about what the sample join did per row.
	v_s_fks := v_fk_rows * v_join_row_secs;

	-- ANALYZE of a partitioned table analyzes the tree and then every
	-- leaf holding rows: one measured ANALYZE each, scaled to the rows
	-- (CPU) and, more weakly, the pages (reads) a partition holds; both
	-- are capped by the sample size of 300 * statistics target.
	IF p_analyze AND v_sampled > 0 THEN
		v_part_rows := row_count::float8 / greatest(batch_count, 1);
		v_part_pages := v_pages::float8 / greatest(batch_count, 1);
		v_sample_pages := greatest(v_sampled::float8 * v_pages / greatest(row_count, 1), 1);
		v_s_analyze := v_analyze_secs * (batch_count + 1)
					   * least(v_part_rows, v_stat_rows) / least(v_sampled, v_stat_rows)
					   * (1 + 0.35 * greatest(least(v_part_pages, v_stat_rows)
											  / least(v_sample_pages, v_stat_rows) - 1, 0));
	ELSE
		v_s_analyze := 0;
	END IF;

	v_total := v_s_struct + v_s_move + v_s_batch_scans + v_s_counts + v_s_fks + v_s_analyze;

	estimated_seconds := round(v_total::numeric, 1);
	-- The calibration runs landed within 5% of the estimate on a quiet
	-- server, and within 15% while other work competed with the sample;
	-- the high figure is what a caller should wait for before assuming
	-- trouble.
	estimated_seconds_high := round((v_total * 1.25)::numeric, 1);
	table_locked_seconds := round(CASE WHEN p_single_transaction THEN v_total
									   ELSE v_s_struct END::numeric, 1);
	breakdown := jsonb_build_object(
		'structural_swap', round(v_s_struct::numeric, 1),
		'data_move', round(v_s_move::numeric, 1),
		'batch_scans', round(v_s_batch_scans::numeric, 1),
		'row_count_checks', round(v_s_counts::numeric, 1),
		'fk_validation', round(v_s_fks::numeric, 1),
		'analyze', round(v_s_analyze::numeric, 1),
		'measured', jsonb_build_object(
			'read_mb_per_sec', round((v_pages_per_sec * v_block_size / 1048576)::numeric),
			'move_rows_per_sec', CASE WHEN v_row_secs > 0 THEN round((1 / v_row_secs)::numeric) END,
			'partition_ddl_ms', round((v_ddl_secs * 1000)::numeric, 1),
			'analyze_ms', round((v_analyze_secs * 1000)::numeric, 1),
			'fk_join_rows_per_sec', CASE WHEN v_join_row_secs > 0 THEN round((1 / v_join_row_secs)::numeric) END,
			'batch_filter_rows_per_sec', CASE WHEN v_filter_row_secs > 0 THEN round((1 / v_filter_row_secs)::numeric) END,
			'sampled_rows', v_sampled));
	notes := v_notes;
	RETURN NEXT;
END
$$;

REVOKE ALL ON FUNCTION @extschema@.dbblue_partition_estimate(text, text, text, text, int, interval, boolean, boolean, int) FROM PUBLIC;
