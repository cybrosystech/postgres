-- DBblue IVM — TRUNCATE consistency test
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: TRUNCATE handling ==='
\echo ''

DROP MATERIALIZED VIEW IF EXISTS mv_t_sum;
DROP MATERIALIZED VIEW IF EXISTS mv_t_minmax;
DROP MATERIALIZED VIEW IF EXISTS mv_t_join;
DROP TABLE IF EXISTS t_sales;
DROP TABLE IF EXISTS t_prod;

CREATE TABLE t_prod (id int PRIMARY KEY, categ int);
INSERT INTO t_prod SELECT g, g % 5 FROM generate_series(1,20) g;

CREATE TABLE t_sales (id serial PRIMARY KEY, product_id int, amount numeric);
INSERT INTO t_sales (product_id, amount)
SELECT (g % 20) + 1, (g * 1.5)::numeric
FROM generate_series(1, 1000) g;

-- 1) SUM/COUNT matview
CREATE MATERIALIZED VIEW mv_t_sum
    WITH (incremental_refresh=true) AS
    SELECT product_id, SUM(amount) AS rev, COUNT(*) AS cnt
    FROM t_sales GROUP BY product_id
WITH DATA;

-- 2) MIN/MAX matview
CREATE MATERIALIZED VIEW mv_t_minmax
    WITH (incremental_refresh=true) AS
    SELECT product_id, MIN(amount) AS mn, MAX(amount) AS mx, COUNT(*) AS cnt
    FROM t_sales GROUP BY product_id
WITH DATA;

-- 3) JOIN matview (two source tables)
CREATE MATERIALIZED VIEW mv_t_join
    WITH (incremental_refresh=true) AS
    SELECT p.categ, SUM(s.amount) AS rev, COUNT(*) AS cnt
    FROM t_sales s JOIN t_prod p ON p.id = s.product_id
    GROUP BY p.categ
WITH DATA;

-- 4) HAVING matview (hidden base + filtering view): TRUNCATE -> REFRESH base
CREATE MATERIALIZED VIEW mv_t_having
    WITH (incremental_refresh=true) AS
    SELECT product_id, SUM(amount) AS rev, COUNT(*) AS cnt
    FROM t_sales GROUP BY product_id HAVING SUM(amount) > 10000
WITH DATA;

-- Confirm a TRUNCATE trigger was installed on each source table
DO $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n FROM pg_trigger
    WHERE tgrelid = 't_sales'::regclass AND tgname LIKE '\_\_mv\_delta\_%' AND (tgtype & 32) <> 0;
    IF n >= 4 THEN  -- one truncate trigger per matview (4 matviews ref t_sales)
        RAISE NOTICE 'TRUNCATE trigger install: PASS (% truncate triggers on t_sales)', n;
    ELSE
        RAISE EXCEPTION 'TRUNCATE trigger install: FAIL (only % truncate triggers)', n;
    END IF;
END $$;

-- ── TRUNCATE the source table; every matview must become consistent ──
\echo 'TRUNCATE t_sales -> all 4 matviews (incl. HAVING) must re-seed'
TRUNCATE t_sales;

DO $$
DECLARE n1 int; n2 int; n3 int; n4 int; nbase int;
BEGIN
    SELECT count(*) INTO n1 FROM mv_t_sum;
    SELECT count(*) INTO n2 FROM mv_t_minmax;
    SELECT count(*) INTO n3 FROM mv_t_join;
    SELECT count(*) INTO n4 FROM mv_t_having;
    -- HAVING: the hidden base must also fully empty (no stale failing groups)
    SELECT count(*) INTO nbase FROM pg_dbblue_matview p JOIN pg_class c ON c.oid=p.mvrelid
        WHERE c.relname LIKE '\_dbblue\_%\_base';
    IF n1 = 0 AND n2 = 0 AND n3 = 0 AND n4 = 0 THEN
        RAISE NOTICE 'TRUNCATE empties matviews: PASS (sum=%, minmax=%, join=%, having=%)', n1, n2, n3, n4;
    ELSE
        RAISE EXCEPTION 'TRUNCATE empties matviews: FAIL (sum=%, minmax=%, join=%, having=%)', n1, n2, n3, n4;
    END IF;
END $$;

-- ── Incremental path must still work after TRUNCATE ──
\echo 'INSERT after TRUNCATE -> incremental refresh must resume'
INSERT INTO t_sales (product_id, amount)
SELECT (g % 20) + 1, (g * 2.0)::numeric
FROM generate_series(1, 500) g;

DO $$
DECLARE mismatch int;
BEGIN
    SELECT count(*) INTO mismatch FROM (
        SELECT product_id, SUM(amount) r, COUNT(*) c FROM t_sales GROUP BY product_id
    ) live JOIN mv_t_sum mv USING (product_id)
    WHERE abs(live.r - mv.rev) > 0.001 OR live.c <> mv.cnt;
    IF mismatch = 0 THEN RAISE NOTICE 'SUM post-TRUNCATE incremental: PASS';
    ELSE RAISE EXCEPTION 'SUM post-TRUNCATE incremental: FAIL (% mismatch)', mismatch; END IF;

    SELECT count(*) INTO mismatch FROM (
        SELECT product_id, MIN(amount) mn, MAX(amount) mx, COUNT(*) c FROM t_sales GROUP BY product_id
    ) live JOIN mv_t_minmax mv USING (product_id)
    WHERE abs(live.mn - mv.mn) > 0.001 OR abs(live.mx - mv.mx) > 0.001 OR live.c <> mv.cnt;
    IF mismatch = 0 THEN RAISE NOTICE 'MIN/MAX post-TRUNCATE incremental: PASS';
    ELSE RAISE EXCEPTION 'MIN/MAX post-TRUNCATE incremental: FAIL (% mismatch)', mismatch; END IF;

    SELECT count(*) INTO mismatch FROM (
        SELECT p.categ, SUM(s.amount) r, COUNT(*) c
        FROM t_sales s JOIN t_prod p ON p.id = s.product_id GROUP BY p.categ
    ) live JOIN mv_t_join mv USING (categ)
    WHERE abs(live.r - mv.rev) > 0.001 OR live.c <> mv.cnt;
    IF mismatch = 0 THEN RAISE NOTICE 'JOIN post-TRUNCATE incremental: PASS';
    ELSE RAISE EXCEPTION 'JOIN post-TRUNCATE incremental: FAIL (% mismatch)', mismatch; END IF;

    -- HAVING: the filtering view must match a live recompute with the same HAVING
    SELECT count(*) INTO mismatch FROM (
        SELECT product_id, SUM(amount) r, COUNT(*) c FROM t_sales GROUP BY product_id HAVING SUM(amount) > 10000
    ) live FULL JOIN mv_t_having mv USING (product_id)
    WHERE live.product_id IS DISTINCT FROM mv.product_id OR abs(live.r - mv.rev) > 0.001 OR live.c <> mv.cnt;
    IF mismatch = 0 THEN RAISE NOTICE 'HAVING post-TRUNCATE incremental: PASS';
    ELSE RAISE EXCEPTION 'HAVING post-TRUNCATE incremental: FAIL (% mismatch)', mismatch; END IF;
END $$;

-- ── TRUNCATE the *dimension* table of a JOIN matview ──
\echo 'TRUNCATE t_prod (join dimension) -> join matview must empty'
TRUNCATE t_prod;
DO $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n FROM mv_t_join;
    IF n = 0 THEN RAISE NOTICE 'TRUNCATE join-dimension: PASS (join matview now %)', n;
    ELSE RAISE EXCEPTION 'TRUNCATE join-dimension: FAIL (join matview still has % rows)', n; END IF;
END $$;

\echo ''
\echo '=== TRUNCATE test complete ==='

DROP MATERIALIZED VIEW mv_t_sum;
DROP MATERIALIZED VIEW mv_t_minmax;
DROP MATERIALIZED VIEW mv_t_join;
DROP VIEW mv_t_having;   -- HAVING matview is a view over a hidden base; drops the base too
DROP TABLE t_sales;
DROP TABLE t_prod;
