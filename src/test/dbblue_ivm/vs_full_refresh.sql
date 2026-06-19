-- DBblue IVM — gold-standard oracle: incremental == full REFRESH.
--
-- The contract for incremental refresh is exact: after any sequence of base
-- table changes, an incremental matview must contain EXACTLY what a stock
-- non-incremental matview of the identical definition contains after a full
-- REFRESH MATERIALIZED VIEW.  A full REFRESH is stock PostgreSQL and is the
-- 100%-guaranteed source of truth, so we compare directly against it (not
-- against a hand-written re-query).
--
-- For each shape we build an incremental MV and a normal MV from the SAME
-- definition string, apply identical DML to the base tables, REFRESH the normal
-- MV, and require zero symmetric difference over the visible columns.  Run on
-- both the hand path and the deparse path (set dbblue_ivm_deparse_delta).
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: incremental == full REFRESH (gold-standard oracle) ==='
\echo ''

DROP TABLE IF EXISTS vt, vp CASCADE;
CREATE TABLE vp(id int PRIMARY KEY, categ int);
INSERT INTO vp SELECT g, g%4 FROM generate_series(1,20) g;
CREATE TABLE vt(id serial PRIMARY KEY, g int, amt numeric, qty int, st text, product_id int);
INSERT INTO vt(g,amt,qty,st,product_id)
  SELECT i%5, (i+0.7)::numeric, i, (ARRAY['done','new','cancel'])[1+i%3], (i%20)+1
  FROM generate_series(1,200) i;

-- Build incremental + normal from one definition, mutate base tables, REFRESH the
-- normal MV (the oracle), return the symmetric difference over `cols`.
CREATE FUNCTION _vs(qdef text, cols text) RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int; kind text;
BEGIN
  EXECUTE 'CREATE MATERIALIZED VIEW _inc WITH (incremental_refresh=true) AS '||qdef;
  EXECUTE 'CREATE MATERIALIZED VIEW _norm AS '||qdef;
  -- identical DML hitting both base tables (idempotent across repeated calls)
  INSERT INTO vt(g,amt,qty,st,product_id)
    SELECT i%5,(i+0.31)::numeric,i,'done',(i%20)+1 FROM generate_series(201,400) i;
  DELETE FROM vt WHERE id % 7 = 0;
  UPDATE vt SET amt = amt + 5, st = 'done' WHERE id % 5 = 0;
  INSERT INTO vp VALUES (21,2) ON CONFLICT DO NOTHING;
  INSERT INTO vt(g,amt,qty,st,product_id) VALUES (1,99,1,'done',21);
  DELETE FROM vp WHERE id = 3;
  EXECUTE 'REFRESH MATERIALIZED VIEW _norm';
  EXECUTE format('SELECT (SELECT count(*) FROM (SELECT %1$s FROM _inc EXCEPT SELECT %1$s FROM _norm) a)
                       + (SELECT count(*) FROM (SELECT %1$s FROM _norm EXCEPT SELECT %1$s FROM _inc) b)',
                 cols) INTO n;
  -- A HAVING incremental matview is exposed as a filtering VIEW over a renamed
  -- base; everything else is a materialized view.  Drop by actual relkind.
  SELECT relkind::text INTO kind FROM pg_class WHERE relname = '_inc';
  IF kind = 'v' THEN EXECUTE 'DROP VIEW _inc';
  ELSE EXECUTE 'DROP MATERIALIZED VIEW _inc'; END IF;
  EXECUTE 'DROP MATERIALIZED VIEW _norm';
  RETURN n;
END $$;

-- Check every shape on the CURRENT GUC setting; raise on any divergence.
CREATE FUNCTION _vs_all(tag text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  shapes text[][] := ARRAY[
    ['SELECT g AS k, SUM(amt) s, COUNT(*) c, AVG(amt) a FROM vt GROUP BY g', 'k,s,c,a'],
    ['SELECT g AS k, SUM(amt*qty) sx, COUNT(*) c FROM vt GROUP BY g', 'k,sx,c'],
    ['SELECT g AS k, SUM(floor(amt)) sf, COUNT(*) c FROM vt GROUP BY g', 'k,sf,c'],
    ['SELECT g AS k, SUM(CASE WHEN st=''done'' THEN amt ELSE 0 END) d, AVG(COALESCE(amt,0)) a, COUNT(*) c FROM vt GROUP BY g', 'k,d,a,c'],
    ['SELECT g AS k, MIN(amt) mn, MAX(amt) mx, COUNT(*) c FROM vt GROUP BY g', 'k,mn,mx,c'],
    ['SELECT p.categ k, SUM(t.amt) s, COUNT(*) c, AVG(t.amt) a FROM vt t JOIN vp p ON p.id=t.product_id GROUP BY p.categ', 'k,s,c,a'],
    ['SELECT p.categ k, SUM(CASE WHEN t.st=''done'' THEN t.amt ELSE 0 END) d, COUNT(*) c FROM vt t JOIN vp p ON p.id=t.product_id GROUP BY p.categ', 'k,d,c'],
    ['SELECT g AS k, SUM(amt) s, COUNT(*) c FROM vt GROUP BY g HAVING SUM(amt) > 1500', 'k,s,c'],
    ['SELECT g AS k, SUM(amt) s, COUNT(*) c FROM vt GROUP BY g HAVING COUNT(*) > 60', 'k,s,c'],
    -- two same-OID sums, HAVING on the SECOND: guards the argument-aware bind
    -- (an OID-only match would filter on SUM(amt) and diverge from REFRESH)
    ['SELECT g AS k, SUM(amt) sa, SUM(qty) sb, COUNT(*) c FROM vt GROUP BY g HAVING SUM(qty) > 3000', 'k,sa,sb,c'],
    -- expression-arg aggregate + HAVING on it: delta + deparse failing-group backfill
    ['SELECT g AS k, SUM(CASE WHEN st=''done'' THEN amt ELSE 0 END) d, COUNT(*) c FROM vt GROUP BY g HAVING SUM(CASE WHEN st=''done'' THEN amt ELSE 0 END) > 2000', 'k,d,c'],
    -- INNER JOIN + HAVING (plain) and JOIN + SUM(CASE) + HAVING (deparse backfill over the join)
    ['SELECT p.categ k, SUM(t.amt) s, COUNT(*) c FROM vt t JOIN vp p ON p.id=t.product_id GROUP BY p.categ HAVING SUM(t.amt) > 3000', 'k,s,c'],
    ['SELECT p.categ k, SUM(CASE WHEN t.st=''done'' THEN t.amt ELSE 0 END) d, COUNT(*) c FROM vt t JOIN vp p ON p.id=t.product_id GROUP BY p.categ HAVING SUM(CASE WHEN t.st=''done'' THEN t.amt ELSE 0 END) > 1500', 'k,d,c']
  ];
  i int; n int; bad int := 0;
BEGIN
  FOR i IN 1 .. array_length(shapes,1) LOOP
    n := _vs(shapes[i][1], shapes[i][2]);
    IF n <> 0 THEN
      RAISE WARNING '[%] shape % diverges from full REFRESH: % rows', tag, i, n;
      bad := bad + 1;
    END IF;
  END LOOP;
  IF bad = 0 THEN RAISE NOTICE 'incremental == full REFRESH for all % shapes [%]: PASS',
                              array_length(shapes,1), tag;
  ELSE RAISE EXCEPTION 'incremental diverged from full REFRESH in % shape(s) [%]: FAIL', bad, tag; END IF;
END $$;

SET dbblue_ivm_deparse_delta = off;
SELECT _vs_all('hand path');
SET dbblue_ivm_deparse_delta = on;
SELECT _vs_all('deparse path');
RESET dbblue_ivm_deparse_delta;

DROP FUNCTION _vs_all(text);
DROP FUNCTION _vs(text,text);
DROP TABLE vt, vp CASCADE;
\echo ''
\echo '=== gold-standard oracle test complete ==='
