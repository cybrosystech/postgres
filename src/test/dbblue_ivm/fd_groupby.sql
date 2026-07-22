-- DBblue IVM — functional-dependency GROUP BY (bare non-key columns).
--
-- A grouped view may SELECT bare, non-aggregated columns of a table that are
-- NOT group keys, provided the GROUP BY includes that table's PRIMARY KEY:
-- Postgres proves the functional dependency (parseCheckAggregates ->
-- check_functional_grouping) and admits the columns.  This is pervasive in
-- reporting views (e.g. SELECT t.id, t.name, t.date, sum(x) FROM t GROUP BY t.id).
--
-- Two hazards this test guards:
--   1. The incremental delta reads a NamedTuplestore transition table that has
--      NO primary key, so the FD proof evaporates on re-parse ("column t.name
--      must appear in the GROUP BY clause").  The engine must add the FD columns
--      to the delta GROUP BY (they are functionally dependent, so grouping by
--      them never subdivides a group) — proven per relation before augmenting.
--   2. The additive delta path cannot maintain a bare column's VALUE: its
--      ON CONFLICT touches only aggregate columns, so UPDATING a bare column
--      (key unchanged) would leave the matview stale.  Such views must route to
--      the recompute engine, which re-derives every column of each affected group.
--
-- Oracle: incremental == full REFRESH after DML that specifically UPDATES a bare
-- FD column.  Run on both the hand path and the deparse path.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: functional-dependency GROUP BY == full REFRESH ==='
\echo ''

DROP TABLE IF EXISTS fdt, fdd, fdf CASCADE;
-- single fact table, id is PK -> every other column is FD on id
CREATE TABLE fdt(id int PRIMARY KEY, d date, cat text, note text, v numeric);
INSERT INTO fdt SELECT g, DATE '2024-01-01' + g, 'c'||(g%4), 'note'||g, g*10 FROM generate_series(1,80) g;
-- dimension (id PK) + fact, for the join shapes: dim columns are FD on dim PK
CREATE TABLE fdd(id int PRIMARY KEY, name text, region text);
CREATE TABLE fdf(fid int PRIMARY KEY, dim_id int REFERENCES fdd(id), amt numeric);
INSERT INTO fdd SELECT g, 'name'||g, 'r'||(g%5) FROM generate_series(1,25) g;
INSERT INTO fdf SELECT g, (g%25)+1, g*3 FROM generate_series(1,300) g;

-- Build incremental + plain twin from one definition, apply DML (INCLUDING an
-- update of a BARE FD column and a primary-key migration), REFRESH the twin,
-- return the symmetric difference over `cols`.
CREATE FUNCTION _fd(qdef text, cols text) RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int; kind text;
BEGIN
  EXECUTE 'CREATE MATERIALIZED VIEW _inc WITH (incremental_refresh=true) AS '||qdef;
  EXECUTE 'CREATE MATERIALIZED VIEW _norm AS '||qdef;

  -- identical DML on both base tables (idempotent across repeated calls)
  INSERT INTO fdt SELECT g, DATE '2024-06-01'+g, 'cX', 'noteX', g FROM generate_series(81,120) g
    ON CONFLICT (id) DO NOTHING;
  UPDATE fdt SET v = v + 7 WHERE id % 9 = 0;                       -- measure change
  UPDATE fdt SET cat = 'ZZ', d = DATE '2030-01-01' WHERE id % 8 = 0; -- BARE FD columns
  UPDATE fdt SET note = NULL WHERE id % 11 = 0;                    -- bare col -> NULL
  UPDATE fdt SET id = id + 100000 WHERE id = 3;                    -- PRIMARY KEY migration
  DELETE FROM fdt WHERE id % 13 = 0;                               -- some deletes

  INSERT INTO fdf SELECT g, (g%25)+1, g FROM generate_series(301,360) g ON CONFLICT (fid) DO NOTHING;
  UPDATE fdf SET amt = amt + 1 WHERE fid % 10 = 0;                 -- measure on fact
  UPDATE fdd SET name = 'RENAMED', region = 'rX' WHERE id = 7;     -- BARE FD col on the WHOLE group
  DELETE FROM fdf WHERE fid % 17 = 0;

  EXECUTE 'REFRESH MATERIALIZED VIEW _norm';
  EXECUTE format('SELECT (SELECT count(*) FROM (SELECT %1$s FROM _inc EXCEPT SELECT %1$s FROM _norm) a)
                       + (SELECT count(*) FROM (SELECT %1$s FROM _norm EXCEPT SELECT %1$s FROM _inc) b)',
                 cols) INTO n;

  SELECT relkind::text INTO kind FROM pg_class WHERE relname = '_inc';
  IF kind = 'v' THEN EXECUTE 'DROP VIEW _inc'; ELSE EXECUTE 'DROP MATERIALIZED VIEW _inc'; END IF;
  EXECUTE 'DROP MATERIALIZED VIEW _norm';
  RETURN n;
END $$;

CREATE FUNCTION _fd_all(tag text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  shapes text[][] := ARRAY[
    -- single table, bare FD columns d/cat/note alongside additive aggregates
    ['SELECT id, d, cat, note, sum(v) s, count(*) c FROM fdt GROUP BY id', 'id,d,cat,note,s,c'],
    -- AVG + bare FD
    ['SELECT id, cat, avg(v) a, count(*) c FROM fdt GROUP BY id', 'id,cat,a,c'],
    -- MIN/MAX + bare FD
    ['SELECT id, cat, min(v) mn, max(v) mx FROM fdt GROUP BY id', 'id,cat,mn,mx'],
    -- HAVING + bare FD
    ['SELECT id, cat, sum(v) s, count(*) c FROM fdt GROUP BY id HAVING sum(v) > 100', 'id,cat,s,c'],
    -- buried-expression FD (columns appear only inside an expression -> resjunk path)
    ['SELECT id, (cat || note) AS cn, sum(v) s FROM fdt GROUP BY id', 'id,cn,s'],
    -- INNER JOIN, GROUP BY dim PK, bare FD columns from the dimension (multi-row groups)
    ['SELECT d.id, d.name, d.region, sum(f.amt) s, count(*) c FROM fdd d JOIN fdf f ON f.dim_id=d.id GROUP BY d.id', 'id,name,region,s,c']
  ];
  i int; n int;
BEGIN
  FOR i IN 1 .. array_length(shapes,1) LOOP
    n := _fd(shapes[i][1], shapes[i][2]);
    IF n <> 0 THEN
      RAISE EXCEPTION 'FD-groupby [%] shape % DIVERGES from REFRESH by % rows: %', tag, i, n, shapes[i][1];
    END IF;
  END LOOP;
  RAISE NOTICE 'FD-groupby [%]: all % shapes == REFRESH', tag, array_length(shapes,1);
END $$;

-- hand path (default) and forced deparse path
SET dbblue_ivm_deparse_delta = off;
SELECT _fd_all('hand');
SET dbblue_ivm_deparse_delta = on;
SELECT _fd_all('deparse');
RESET dbblue_ivm_deparse_delta;

DROP FUNCTION _fd(text,text);
DROP FUNCTION _fd_all(text);
DROP TABLE fdt, fdd, fdf CASCADE;
\echo 'PASS: functional-dependency GROUP BY maintained byte-identically to REFRESH'
