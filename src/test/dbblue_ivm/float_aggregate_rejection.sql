-- DBblue IVM — float SUM/AVG via recompute (M2).
--
-- Additive SUM/AVG maintain a running total; floating-point addition is not
-- associative, so a float running total would drift from a true recompute over
-- many deltas.  Rather than reject, float SUM/AVG are now routed to the
-- recompute engine (incr_needs_recompute), which recomputes each affected group
-- from scratch — no accumulation, no drift, always == a full REFRESH.  MIN/MAX
-- (comparison only) and COUNT over floats, and numeric/integer SUM/AVG, remain
-- on the exact additive path.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: float aggregate via recompute ==='
\echo ''

DROP TABLE IF EXISTS fagg CASCADE;
CREATE TABLE fagg(id serial primary key, g int, r4 real, r8 double precision, n numeric, i int);
INSERT INTO fagg(g,r4,r8,n,i) SELECT g%3, g*1.1, g*1.1, g*1.1, g FROM generate_series(1,30) g;

CREATE MATERIALIZED VIEW f_i WITH (incremental_refresh=true) AS
  SELECT g, sum(r4) s4, avg(r8) a8, sum(r8*2) se, min(r8) mn, max(r4) mx,
         sum(n) sn, avg(i) ai, count(*) c
  FROM fagg GROUP BY g;
CREATE MATERIALIZED VIEW f_o AS
  SELECT g, sum(r4) s4, avg(r8) a8, sum(r8*2) se, min(r8) mn, max(r4) mx,
         sum(n) sn, avg(i) ai, count(*) c
  FROM fagg GROUP BY g;

-- churn: inserts, deletes, updates that move rows between groups
INSERT INTO fagg(g,r4,r8,n,i) VALUES (0,5.5,5.5,5.5,5),(1,9.9,9.9,9.9,9);
DELETE FROM fagg WHERE id IN (3,6,9);
UPDATE fagg SET g = (g+1)%3, r8 = r8 + 0.25 WHERE id IN (12,15,18);
REFRESH MATERIALIZED VIEW f_o;

DO $$
DECLARE d int;
BEGIN
  -- float columns compared at 6-digit precision (recompute == refresh, both
  -- single-pass aggregations over the same rows)
  SELECT count(*) INTO d FROM (
    (SELECT g, round(s4::numeric,6), round(a8::numeric,6), round(se::numeric,6),
            round(mn::numeric,6), round(mx::numeric,6), sn, round(ai::numeric,6), c FROM f_i
     EXCEPT
     SELECT g, round(s4::numeric,6), round(a8::numeric,6), round(se::numeric,6),
            round(mn::numeric,6), round(mx::numeric,6), sn, round(ai::numeric,6), c FROM f_o)
    UNION ALL
    (SELECT g, round(s4::numeric,6), round(a8::numeric,6), round(se::numeric,6),
            round(mn::numeric,6), round(mx::numeric,6), sn, round(ai::numeric,6), c FROM f_o
     EXCEPT
     SELECT g, round(s4::numeric,6), round(a8::numeric,6), round(se::numeric,6),
            round(mn::numeric,6), round(mx::numeric,6), sn, round(ai::numeric,6), c FROM f_i)) z;
  IF d = 0 THEN RAISE NOTICE 'float SUM/AVG via recompute == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'float aggregate recompute: FAIL (% rows differ)', d; END IF;
END $$;

DROP TABLE fagg CASCADE;
\echo ''
\echo '=== float aggregate recompute test complete ==='
