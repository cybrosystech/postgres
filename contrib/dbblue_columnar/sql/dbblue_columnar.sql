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
-- text/varchar group keys: not bit-equal, so each value is canonicalized through
-- the intern table (equal bytes -> one pointer) and the fixed hash key stays
-- byte-comparable. Matches core HashAgg under the (deterministic) DB collation.
SELECT uses_node($$SELECT txt, sum(amt) FROM t GROUP BY txt$$, 'DBBlueColumnarAgg') AS text_key_agg_used;
SELECT 'grp-textkey   ', agree($$SELECT txt, sum(amt) s, count(*) c FROM t GROUP BY txt ORDER BY txt$$);
SELECT 'grp-text+int  ', agree($$SELECT k7, txt, sum(amt) s FROM t GROUP BY k7, txt ORDER BY k7, txt$$);
SELECT 'grp-textexpr  ', agree($$SELECT upper(txt) u, count(*) c FROM t GROUP BY upper(txt) ORDER BY 1$$);
SELECT 'grp-textfilter', agree($$SELECT txt, sum(amt) s FROM t WHERE k7 > 2 GROUP BY txt ORDER BY txt$$);
-- memory-aware gate: the grouped agg has no spill, so it must DECLINE when the
-- estimated groups would not fit hash_mem (work_mem * hash_mem_multiplier) and
-- let a spill-capable HashAgg run - not error at runtime. Self-contained + ANALYZEd
-- so the group-count estimate is accurate; a small work_mem makes the 20000-distinct
-- id key overflow deterministically while the 50-distinct key still fits and uses
-- the node. (Without the gate the high-card query ERRORs with a numeric sum.)
CREATE TABLE mg (id int, lc int, amt numeric) WITH (autovacuum_enabled = off);
INSERT INTO mg SELECT g, g % 50, g * 1.5 FROM generate_series(1, 20000) g;
VACUUM (DISABLE_PAGE_SKIPPING) mg;
SELECT dbblue_columnar_add('mg', ARRAY['id','lc','amt']);
SELECT dbblue_columnar_populate('mg') > 0 AS mg_built;
ANALYZE mg;
SET work_mem = '1MB';
SELECT uses_node($$SELECT id, sum(amt) FROM mg GROUP BY id$$, 'DBBlueColumnarAgg') AS highcard_declines;
SELECT 'mg-highcard   ', agree($$SELECT id, sum(amt) s FROM mg GROUP BY id ORDER BY id$$);
SELECT uses_node($$SELECT lc, sum(amt) FROM mg GROUP BY lc$$, 'DBBlueColumnarAgg') AS lowcard_still_used;
SELECT 'mg-lowcard    ', agree($$SELECT lc, sum(amt) s FROM mg GROUP BY lc ORDER BY lc$$);
RESET work_mem;
-- array_agg/string_agg have aggtransspace = -1 (not 0); the memory-gate size
-- estimate must read it as a SIGNED int32 - a raw Size assignment wraps -1 to
-- SIZE_MAX and crashes hash_agg_entry_size (pg_nextpower2). Just planning these
-- exercises the gate; agree() also checks the result.
SELECT 'mg-arrayagg   ', agree($$SELECT lc, array_agg(id ORDER BY id) a FROM mg GROUP BY lc ORDER BY lc$$);
SELECT 'mg-stringagg  ', agree($$SELECT lc, string_agg(id::text, ',' ORDER BY id) s FROM mg GROUP BY lc ORDER BY lc$$);
DROP TABLE mg;

-- ---- multi-pass hash-partition spill (serial) ----
-- When a low row estimate lets the columnar agg be chosen but the store actually
-- holds far more distinct groups than fit a small work_mem, the node must SPILL -
-- re-scan the store aggregating one hash-partition per pass - not error. Force it:
-- lie about reltuples (autovacuum off) so the plan-time gate passes, then a small
-- work_mem overflows on the real cardinality. Byte-identical to core HashAgg.
CREATE TABLE sp (g int, t text, amt numeric) WITH (autovacuum_enabled = off);
INSERT INTO sp SELECT i, 'k' || i, (i % 100) * 1.5 FROM generate_series(1, 60000) i;
VACUUM (DISABLE_PAGE_SKIPPING) sp;
SELECT dbblue_columnar_add('sp', ARRAY['g','t','amt']);
SELECT dbblue_columnar_populate('sp') > 0 AS sp_built;
ANALYZE sp;
UPDATE pg_class SET reltuples = 500 WHERE relname = 'sp';
SET work_mem = '256kB';
SELECT uses_node($$SELECT g, sum(amt) FROM sp GROUP BY g$$, 'DBBlueColumnarAgg') AS spill_fires;
SELECT 'sp-int-spill  ', agree($$SELECT g, sum(amt) s, count(*) c, avg(amt) a FROM sp GROUP BY g ORDER BY g$$);
SELECT 'sp-text-spill ', agree($$SELECT t, sum(amt) s, count(*) c FROM sp GROUP BY t ORDER BY t$$);
SELECT 'sp-multi-spill', agree($$SELECT g, t, count(*) c FROM sp GROUP BY g, t ORDER BY g, t$$);
SELECT 'sp-minmax-spl ', agree($$SELECT g % 3000 m, min(t) a, max(t) b, sum(amt) s FROM sp GROUP BY g % 3000 ORDER BY 1$$);
RESET work_mem;
DROP TABLE sp;

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

-- ---- expression group key WITH heap fallback: dirty some rows so the fused
-- node interleaves columnar blocks with heap-range fallback in one scan, and
-- the per-row group-key expression is evaluated over BOTH. Regression for a
-- crash where the heap path pointed the expr context at an un-deformed raw heap
-- slot (EEOP_SCAN_VAR "attnum < tts_nvalid" assert) - the block path populated a
-- virtual scratch slot but the heap path did not. Must not crash; the result
-- must equal heap. (Runs last: the UPDATE invalidates blocks for t.) ----
UPDATE t SET amt = amt WHERE id < 500;
SET max_parallel_workers_per_gather = 4;
SET min_parallel_table_scan_size = 0;
SET parallel_setup_cost = 0;
SET enable_seqscan = off;
SET enable_hashagg = off;
SELECT uses_node($$SELECT date_trunc('year', d) FROM t GROUP BY 1$$,
				 'DBBlueColumnarAgg') AS exprkey_heapfb_fused;
SELECT 'exprkey-heapfb  ', agree($$SELECT date_trunc('year', d) y, count(*) c, sum(amt) s, min(amt) mn FROM t GROUP BY 1 ORDER BY 1$$);
RESET enable_seqscan;
RESET enable_hashagg;
RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;
SET max_parallel_workers_per_gather = 0;

-- ---- M6 VM fast-path RE-STAMP: after a vacuum bumps a block's covering
-- visibility-map page LSN, a byte-identical block fails the cheap VM proof and
-- would pay the per-page proof on EVERY later scan. The serve path now re-arms
-- (re-stamps) the VM stamp in place once it has proven the block unchanged, so
-- the next scan is fast again. Deterministic: autovacuum off + explicit VACUUM
-- fully control the VM state. Correctness never depends on the stamp. ----
CREATE TABLE rs (id int, k int, amt numeric, d date) WITH (autovacuum_enabled = off);
INSERT INTO rs SELECT g, g % 7, ((g % 100) + 1) * 1.25, DATE '2024-01-01' + (g % 365)
FROM generate_series(1, 50000) g;
VACUUM (DISABLE_PAGE_SKIPPING) rs;                       -- all pages all-visible
SELECT dbblue_columnar_add('rs', ARRAY['id','k','amt','d']);
SELECT dbblue_columnar_populate('rs') > 1 AS rs_multiblock;  -- need >1 block

-- read three DBBlueColumnarScan counters from ONE EXPLAIN ANALYZE (a second
-- scan would already see the re-stamp, so all three must come from one run)
CREATE FUNCTION cstat(q text, OUT vm bigint, OUT restamped bigint, OUT heap bigint)
LANGUAGE plpgsql AS $$
DECLARE line text;
BEGIN
	vm := 0; restamped := 0; heap := 0;
	FOR line IN EXECUTE
		'EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF, SUMMARY OFF) ' || q
	LOOP
		IF position('Columnar VM-Validated Blocks:' IN line) > 0 THEN
			vm := substring(line from '([0-9]+)$')::bigint;
		ELSIF position('Columnar Blocks Re-stamped:' IN line) > 0 THEN
			restamped := substring(line from '([0-9]+)$')::bigint;
		ELSIF position('Heap Fallback Ranges:' IN line) > 0 THEN
			heap := substring(line from '([0-9]+)$')::bigint;
		END IF;
	END LOOP;
END $$;

SET enable_seqscan = off;                   -- force the columnar scan node
SELECT uses_node($$SELECT id, amt FROM rs WHERE id >= 0$$,
				 'DBBlueColumnarScan') AS rs_scan_used;

-- baseline: freshly built + all-visible => every block proven by the cheap VM
-- proof; nothing re-stamped, no heap fallback
SELECT 'rs-baseline-fast ' t, vm > 0 AND restamped = 0 AND heap = 0 AS ok
FROM cstat($q$SELECT id, amt FROM rs WHERE id >= 0$q$);

-- touch ONE row + vacuum: re-sets all-visible but advances the shared VM page
-- LSN, so EVERY block on that VM page fails the cheap proof (the fragility)
UPDATE rs SET amt = amt WHERE id = 1;
VACUUM (DISABLE_PAGE_SKIPPING) rs;

-- first scan after invalidation: cheap proof fails for all (vm = 0); the block
-- holding id=1 genuinely changed -> heap fallback; every other block is proven
-- unchanged and RE-STAMPED in place
SELECT 'rs-heal-run1     ' t, vm = 0 AND restamped > 0 AND heap > 0 AS ok
FROM cstat($q$SELECT id, amt FROM rs WHERE id >= 0$q$);

-- second scan: the re-stamped blocks pass the cheap proof again (vm > 0),
-- nothing left to re-stamp; the changed block still falls back to heap
SELECT 'rs-heal-run2     ' t, vm > 0 AND restamped = 0 AND heap > 0 AS ok
FROM cstat($q$SELECT id, amt FROM rs WHERE id >= 0$q$);

-- correctness never depends on the stamp: columnar == heap throughout
SELECT 'rs-correct       ' t, agree($q$SELECT id, amt FROM rs WHERE id >= 0 ORDER BY id$q$) c;

-- re-stamp OFF: the block stays on the slow proof, no recovery across scans
SET dbblue_columnar.enable_restamp = off;
UPDATE rs SET amt = amt WHERE id = 2;
VACUUM (DISABLE_PAGE_SKIPPING) rs;
SELECT 'rs-off-run1      ' t, vm = 0 AND restamped = 0 AS ok
FROM cstat($q$SELECT id, amt FROM rs WHERE id >= 0$q$);
SELECT 'rs-off-run2      ' t, vm = 0 AND restamped = 0 AS ok
FROM cstat($q$SELECT id, amt FROM rs WHERE id >= 0$q$);
-- turn it back on: healing resumes on the next scan
SET dbblue_columnar.enable_restamp = on;
SELECT 'rs-on-again      ' t, vm = 0 AND restamped > 0 AS ok
FROM cstat($q$SELECT id, amt FROM rs WHERE id >= 0$q$);
SELECT 'rs-on-recovered  ' t, vm > 0 AND restamped = 0 AS ok
FROM cstat($q$SELECT id, amt FROM rs WHERE id >= 0$q$);

-- SAME re-stamp on the refresh path: invalidate, then a POPULATE (not a query)
-- must re-arm the cold blocks it reuses (dbbc_restamp_block, shared with the
-- serve path), so the VERY FIRST scan afterwards is already fast - vm > 0 with
-- nothing left for the serve path to re-stamp
UPDATE rs SET amt = amt WHERE id = 3;
VACUUM (DISABLE_PAGE_SKIPPING) rs;
SELECT dbblue_columnar_populate('rs') > 0 AS rs_repopulated;
SELECT 'rs-refresh-armed ' t, vm > 0 AND restamped = 0 AND heap = 0 AS ok
FROM cstat($q$SELECT id, amt FROM rs WHERE id >= 0$q$);
RESET enable_seqscan;

DROP FUNCTION cstat(text);
DROP TABLE rs;

-- ---- in-node dimension hash-join (DBBlueColumnarAgg dim-join sub-mode) ----
-- Aggregate a fact (t) LEFT JOIN a small unique-key dimension entirely in the
-- engine (no join node); the dimension is probed live under the query snapshot.
-- dj_dim covers only grp 0..6, so grp 7..12 exercise the LEFT-join NULL path;
-- t carries stale blocks here (the earlier UPDATE), so the per-row probe runs on
-- BOTH the columnar and heap-fallback paths in one scan. Differential vs the
-- plain plan must be byte-identical. eager_aggregate off isolates the fused node.
CREATE TABLE dj_dim (id int PRIMARY KEY, seq int, label text) WITH (autovacuum_enabled = off);
INSERT INTO dj_dim SELECT g, (g * 3) % 20, 'lbl' || (g % 3) FROM generate_series(0, 6) g;
ANALYZE dj_dim;
SET dbblue_columnar.enable_dimjoin_agg = on;
SET enable_eager_aggregate = off;
SELECT uses_node($$SELECT t.grp, dj.seq, count(*) FROM t LEFT JOIN dj_dim dj ON dj.id = t.grp GROUP BY t.grp, dj.seq, dj.id$$,
				 'DBBlueColumnarAgg') AS dimjoin_fires;
SELECT 'dj-basic     ', agree($$SELECT t.grp, dj.seq, count(*) c, sum(t.amt) s FROM t LEFT JOIN dj_dim dj ON dj.id = t.grp GROUP BY t.grp, dj.seq, dj.id ORDER BY t.grp, dj.seq$$);
SELECT 'dj-exprkey   ', agree($$SELECT date_trunc('month', t.d) m, t.grp, dj.seq, count(*) c, sum(t.amt) s FROM t LEFT JOIN dj_dim dj ON dj.id = t.grp WHERE t.k7 > 0 GROUP BY 1, t.grp, dj.seq, dj.id ORDER BY 1, t.grp$$);
SELECT 'dj-filter    ', agree($$SELECT t.grp, dj.seq, count(*) FILTER (WHERE t.amt > 50) c, sum(t.amt) s FROM t LEFT JOIN dj_dim dj ON dj.id = t.grp GROUP BY t.grp, dj.seq, dj.id ORDER BY t.grp, dj.seq$$);
SELECT 'dj-scalaragg ', agree($$SELECT dj.seq, sum(t.amt) s, count(*) c FROM t LEFT JOIN dj_dim dj ON dj.id = t.grp GROUP BY dj.seq, dj.id ORDER BY dj.seq$$);
-- Phase 2: INNER join + a dimension-side WHERE filter. Only passing dim rows
-- enter the in-node hash; a fact row whose grp matches no passing dim row is
-- dropped (INNER) - grp 7..12 (no dim at all) and a grp whose dim was filtered
-- out alike. A "LEFT JOIN ... WHERE dj.col" reduces to the same inner join.
-- At this fixture's tiny scale PG's plain HashAggregate over a Hash Join is
-- cost-competitive (it collapses the FD group keys down to t.grp), so force the
-- join methods off to exercise the fused node deterministically - at production
-- scale the cost router picks it. Correctness is still the on-vs-off differential.
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_nestloop = off;
SELECT uses_node($$SELECT t.grp, dj.seq, count(*) FROM t JOIN dj_dim dj ON dj.id = t.grp WHERE dj.seq > 5 GROUP BY t.grp, dj.seq, dj.id$$,
				 'DBBlueColumnarAgg') AS dimjoin_filter_fires;
SELECT 'dj2-inner-flt ', agree($$SELECT t.grp, dj.seq, count(*) c, sum(t.amt) s FROM t JOIN dj_dim dj ON dj.id = t.grp WHERE dj.seq > 5 GROUP BY t.grp, dj.seq, dj.id ORDER BY t.grp, dj.seq$$);
SELECT 'dj2-left-where', agree($$SELECT t.grp, dj.seq, count(*) c, sum(t.amt) s FROM t LEFT JOIN dj_dim dj ON dj.id = t.grp WHERE dj.seq > 5 GROUP BY t.grp, dj.seq, dj.id ORDER BY t.grp, dj.seq$$);
SELECT 'dj2-keyrange  ', agree($$SELECT t.grp, dj.seq, sum(t.amt) s, count(*) c FROM t JOIN dj_dim dj ON dj.id = t.grp WHERE dj.id <= 3 GROUP BY t.grp, dj.seq, dj.id ORDER BY t.grp$$);
SELECT 'dj2-twoquals  ', agree($$SELECT t.grp, sum(t.amt) s FROM t JOIN dj_dim dj ON dj.id = t.grp WHERE dj.seq > 3 AND dj.id < 6 GROUP BY t.grp, dj.id ORDER BY t.grp$$);
SELECT 'dj2-empty     ', agree($$SELECT t.grp, dj.seq, count(*) c FROM t JOIN dj_dim dj ON dj.id = t.grp WHERE dj.seq > 999 GROUP BY t.grp, dj.seq, dj.id ORDER BY t.grp$$);
-- group by a TEXT dimension column (FD on the dim key, not in GROUP BY): the
-- probe supplies the label, interned so equal labels across dims share a group.
SELECT uses_node($$SELECT dj.label, sum(t.amt) FROM t JOIN dj_dim dj ON dj.id = t.grp WHERE dj.seq > 3 GROUP BY dj.label$$,
				 'DBBlueColumnarAgg') AS dimjoin_textkey_fires;
SELECT 'dj2-dimtext   ', agree($$SELECT dj.label, sum(t.amt) s, count(*) c FROM t JOIN dj_dim dj ON dj.id = t.grp WHERE dj.seq > 3 GROUP BY dj.label ORDER BY dj.label$$);
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_nestloop;
-- parallel: Finalize -> Gather -> Parallel Custom Scan (DBBlueColumnarAgg); each
-- worker builds its own dim hash under the shared snapshot. Still byte-identical.
SET max_parallel_workers_per_gather = 4;
SET min_parallel_table_scan_size = 0;
SET parallel_setup_cost = 0;
SELECT uses_node($$SELECT t.grp, dj.seq, count(*), sum(t.amt) FROM t LEFT JOIN dj_dim dj ON dj.id = t.grp GROUP BY t.grp, dj.seq, dj.id$$,
				 'DBBlueColumnarAgg') AS dimjoin_parallel_fires;
SELECT 'dj-parallel  ', agree($$SELECT t.grp, dj.seq, count(*) c, sum(t.amt) s FROM t LEFT JOIN dj_dim dj ON dj.id = t.grp GROUP BY t.grp, dj.seq, dj.id ORDER BY t.grp, dj.seq$$);
RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;
SET max_parallel_workers_per_gather = 0;
-- runtime dim cap: a dimension whose planner estimate under-counts it must fail
-- cleanly (not OOM). Force the estimate below a tiny cap via pg_class so the gate
-- plans the dim-join; the build then overruns the cap and errors with a hint.
-- (dj_dim has autovacuum off so the forced estimate is not re-ANALYZEd away.)
SET dbblue_columnar.dimjoin_max_dim_rows = 3;
UPDATE pg_class SET reltuples = 2 WHERE relname = 'dj_dim';
SELECT t.grp, dj.seq, count(*) FROM t LEFT JOIN dj_dim dj ON dj.id = t.grp
GROUP BY t.grp, dj.seq, dj.id;
RESET dbblue_columnar.dimjoin_max_dim_rows;
ANALYZE dj_dim;
RESET enable_eager_aggregate;
SET dbblue_columnar.enable_dimjoin_agg = off;
DROP TABLE dj_dim;

DROP FUNCTION agree(text);
DROP FUNCTION uses_node(text, text);
DROP TABLE t;
DROP EXTENSION dbblue_columnar;
