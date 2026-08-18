-- dbblue_columnar regression test: columnar results must be byte-identical to
-- heap results across the engine's shape matrix. Self-validating (compares
-- enable_columnar_scan on vs off), so the expected output is stable 'ok's.
-- Requires shared_preload_libraries='dbblue_columnar'.
CREATE EXTENSION dbblue_columnar;

SET max_parallel_workers_per_gather = 0;
SET client_min_messages = warning;

-- deterministic dataset: ints (keys), nullable int, text (varlena), numeric, date
CREATE TABLE t (
	id    int,
	k7    int,          -- 7 distinct
	grp   int,          -- 13 distinct
	txt   text,
	amt   numeric,
	d     date,
	pid   int           -- nullable (every 5th row)
);
INSERT INTO t
SELECT g,
	   g % 7,
	   g % 13,
	   'v' || (g % 17),
	   ((g % 100) + 1) * 1.25,
	   DATE '2024-01-01' + (g % 365),
	   CASE WHEN g % 5 = 0 THEN NULL ELSE g % 23 END
FROM generate_series(1, 50000) g;

-- set all pages all-visible deterministically so blocks are servable
VACUUM (DISABLE_PAGE_SKIPPING) t;
SELECT dbblue_columnar_add('t', ARRAY['id','k7','grp','txt','amt','d','pid']);

-- assert the store actually built blocks (else the differential below could
-- silently pass by falling back to heap on both sides)
SELECT dbblue_columnar_populate('t') > 0 AS blocks_built;

-- does the plan for q use the named custom node?
CREATE FUNCTION uses_node(q text, node text) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE line text; found boolean := false;
BEGIN
	FOR line IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
		IF position(node IN line) > 0 THEN found := true; END IF;
	END LOOP;
	RETURN found;
END $$;

-- differential helper: run q with columnar on and off, must be identical
CREATE FUNCTION agree(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE n bigint;
BEGIN
	EXECUTE 'SET dbblue_columnar.enable_columnar_scan = on';
	EXECUTE format('CREATE TEMP TABLE _c AS %s', q);
	EXECUTE 'SET dbblue_columnar.enable_columnar_scan = off';
	EXECUTE format('CREATE TEMP TABLE _h AS %s', q);
	EXECUTE 'SET dbblue_columnar.enable_columnar_scan = on';
	EXECUTE '(TABLE _c EXCEPT ALL TABLE _h) UNION ALL (TABLE _h EXCEPT ALL TABLE _c)';
	GET DIAGNOSTICS n = ROW_COUNT;
	DROP TABLE _c; DROP TABLE _h;
	RETURN CASE WHEN n = 0 THEN 'ok' ELSE n || ' MISMATCH' END;
END $$;

SET dbblue_columnar.enable_columnar_scan = on;

-- the columnar nodes must actually be chosen (guards the differential tests)
SELECT uses_node($$SELECT id, amt FROM t WHERE k7 = 3$$, 'DBBlueColumnarScan') AS scan_used;
SELECT uses_node($$SELECT grp, sum(amt) FROM t GROUP BY grp$$, 'DBBlueColumnarAgg') AS grouped_agg_used;
SELECT uses_node($$SELECT count(*) FROM t$$, 'DBBlueColumnarAgg') AS meta_agg_used;
-- global aggregate (zero GROUP BY) with a transition agg must use the fused
-- node too, not fall back to scan-serve + a plain Aggregate (the plain Agg has
-- no HashAgg overhead, so the fused cost must not be over-priced for it).
SELECT uses_node($$SELECT sum(amt) FROM t WHERE k7 = 4$$, 'DBBlueColumnarAgg') AS scalar_agg_used;

-- ---- scan: predicate pushdown (all extractable shapes) ----
SELECT 'scan-eq       ', agree($$SELECT id, amt, txt FROM t WHERE k7 = 3 ORDER BY id$$);
SELECT 'scan-range    ', agree($$SELECT id, d FROM t WHERE d >= DATE '2024-06-01' AND id < 2000 ORDER BY id$$);
SELECT 'scan-in       ', agree($$SELECT id FROM t WHERE grp IN (1,5,9) ORDER BY id$$);
SELECT 'scan-nulltest ', agree($$SELECT id, pid FROM t WHERE pid IS NULL ORDER BY id$$);
SELECT 'scan-notnull  ', agree($$SELECT id FROM t WHERE pid IS NOT NULL AND k7 = 2 ORDER BY id$$);
SELECT 'scan-text     ', agree($$SELECT id, txt FROM t WHERE txt = 'v3' ORDER BY id$$);

-- ---- metadata aggregate mode (scalar count/min/max, no WHERE) ----
SELECT 'meta-count    ', agree($$SELECT count(*) a, count(pid) b FROM t$$);
SELECT 'meta-minmax   ', agree($$SELECT min(d) a, max(d) b, min(id) c, max(id) e, min(txt) f, max(txt) g FROM t$$);

-- ---- grouped aggregate pushdown ----
SELECT 'grp-1key-sum  ', agree($$SELECT grp, sum(amt) s, count(*) c FROM t GROUP BY grp ORDER BY grp$$);
SELECT 'grp-2key      ', agree($$SELECT k7, grp, sum(amt) s, avg(amt) a FROM t GROUP BY k7, grp ORDER BY k7, grp$$);
SELECT 'grp-filter    ', agree($$SELECT grp, sum(amt) s FROM t WHERE d >= DATE '2024-04-01' GROUP BY grp ORDER BY grp$$);
SELECT 'grp-minmaxtext', agree($$SELECT k7, min(txt) a, max(txt) b, min(amt) c, max(amt) e FROM t GROUP BY k7 ORDER BY k7$$);
SELECT 'grp-nullkey   ', agree($$SELECT pid, count(*) c, sum(amt) s FROM t GROUP BY pid ORDER BY pid$$);
SELECT 'grp-avgint    ', agree($$SELECT k7, avg(grp) a, sum(grp) s FROM t GROUP BY k7 ORDER BY k7$$);
SELECT 'grp-scalar-agg', agree($$SELECT sum(amt) s, avg(amt) a, count(*) c, count(pid) c2 FROM t WHERE k7 = 4$$);
SELECT 'grp-zero-rows ', agree($$SELECT sum(amt) s, min(d) m, count(*) c FROM t WHERE id < 0$$);
SELECT 'grp-zero-grp  ', agree($$SELECT grp, sum(amt) FROM t WHERE id < 0 GROUP BY grp$$);
SELECT 'grp-nogby-agg ', agree($$SELECT grp FROM t GROUP BY grp ORDER BY grp$$);
SELECT 'grp-saop      ', agree($$SELECT grp, count(*) c FROM t WHERE k7 = ANY(ARRAY[1,3,5]) GROUP BY grp ORDER BY grp$$);

-- ---- correctness under a rescan (LATERAL nestloop, columnar agg on inner) ----
SELECT 'rescan        ', agree($$SELECT o.k, g.s FROM (VALUES (1),(3),(5)) o(k)
	JOIN LATERAL (SELECT k7 kk, sum(amt) s FROM t GROUP BY k7) g ON g.kk = o.k ORDER BY o.k$$);

-- ---- parallel partial-aggregate pushdown: workers aggregate partial states,
-- core Finalize Aggregate combines. Force the path (small table needs parallel
-- enabled + the fused agg preferred over scan-serve/hashagg) and prove results
-- stay byte-identical to heap. ----
SET max_parallel_workers_per_gather = 4;
SET min_parallel_table_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET enable_seqscan = off;
SET enable_hashagg = off;
-- the parallel partial-agg node must actually be chosen (else the checks below
-- could pass by silently running some other path)
SELECT uses_node($$SELECT k7, sum(amt) FROM t GROUP BY k7$$,
				 'Parallel Custom Scan (DBBlueColumnarAgg)') AS par_agg_used;
SELECT 'par-1key      ', agree($$SELECT k7, sum(amt) s, count(*) c, avg(amt) a FROM t GROUP BY k7 ORDER BY k7$$);
SELECT 'par-2key      ', agree($$SELECT k7, grp, sum(amt) s, min(amt) mn, max(amt) mx FROM t GROUP BY k7, grp ORDER BY k7, grp$$);
SELECT 'par-nullkey   ', agree($$SELECT pid, count(*) c, sum(amt) s FROM t GROUP BY pid ORDER BY pid$$);
SELECT 'par-filter    ', agree($$SELECT k7, sum(amt) FILTER (WHERE amt > 50) s, count(*) FILTER (WHERE pid IS NOT NULL) c FROM t GROUP BY k7 ORDER BY k7$$);
SELECT 'par-where     ', agree($$SELECT grp, sum(amt) s FROM t WHERE k7 > 2 GROUP BY grp ORDER BY grp$$);
SELECT 'par-exprkey   ', agree($$SELECT date_trunc('month', d) m, sum(amt) s FROM t GROUP BY 1 ORDER BY 1$$);
SELECT 'par-stddev    ', agree($$SELECT k7, round(stddev(amt),8) sd, round(variance(amt),8) v FROM t GROUP BY k7 ORDER BY k7$$);
RESET enable_seqscan;
RESET enable_hashagg;
RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
SET max_parallel_workers_per_gather = 0;

-- ---- eager-aggregation fusion (aggregate-below-join): PG core's eager
-- aggregation pushes a partial aggregate below a join; our fused
-- DBBlueColumnarAgg should be that below-join aggregate. Force it and prove the
-- results stay byte-identical to heap, including a many-to-many FAN-OUT join
-- (the partial states are multiplied by the join, then a Finalize Aggregate
-- above re-combines - the correctness-critical case). Uses grp (13 distinct,
-- above min_eager_agg_group_size) as the join/group key so core offers it. ----
CREATE TABLE dt (id int PRIMARY KEY, seq int);
INSERT INTO dt SELECT g, g * 10 FROM generate_series(0, 12) g;			-- unique PK
CREATE TABLE dtf (id int, seq int);
INSERT INTO dtf SELECT g, g * 10 FROM generate_series(0, 12) g, generate_series(1, 2);	-- 2x fan-out
ANALYZE dt;
ANALYZE dtf;
SET enable_eager_aggregate = on;
SET max_parallel_workers_per_gather = 4;
SET min_parallel_table_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET enable_seqscan = off;
SET enable_hashagg = off;
-- our fused node must be the below-join aggregate
SELECT uses_node($$SELECT t.grp, dt.seq, sum(t.amt) FROM t LEFT JOIN dt ON dt.id = t.grp GROUP BY t.grp, dt.seq, dt.id$$,
				 'Custom Scan (DBBlueColumnarAgg)') AS eager_fused_used;
SELECT 'eager-leftjoin  ', agree($$SELECT t.grp, dt.seq, sum(t.amt) s, count(*) c, avg(t.amt) a FROM t LEFT JOIN dt ON dt.id = t.grp GROUP BY t.grp, dt.seq, dt.id ORDER BY t.grp$$);
SELECT 'eager-innerjoin ', agree($$SELECT t.grp, dt.seq, min(t.amt) mn, max(t.amt) mx, count(*) c FROM t JOIN dt ON dt.id = t.grp GROUP BY t.grp, dt.seq, dt.id ORDER BY t.grp$$);
SELECT 'eager-where     ', agree($$SELECT t.grp, sum(t.amt) s FROM t LEFT JOIN dt ON dt.id = t.grp WHERE t.k7 > 3 GROUP BY t.grp, dt.id ORDER BY t.grp$$);
SELECT 'eager-fanout    ', agree($$SELECT t.grp, sum(t.amt) s, count(*) c FROM t JOIN dtf ON dtf.id = t.grp GROUP BY t.grp ORDER BY t.grp$$);
SELECT 'eager-fanout-left', agree($$SELECT t.grp, count(*) c, sum(t.amt) s, avg(t.amt) a FROM t LEFT JOIN dtf ON dtf.id = t.grp GROUP BY t.grp ORDER BY t.grp$$);
RESET enable_seqscan;
RESET enable_hashagg;
RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET enable_eager_aggregate;
SET max_parallel_workers_per_gather = 0;
DROP TABLE dt;
DROP TABLE dtf;

DROP FUNCTION agree(text);
DROP FUNCTION uses_node(text, text);
DROP TABLE t;
DROP EXTENSION dbblue_columnar;
