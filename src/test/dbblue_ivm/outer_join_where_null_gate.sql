-- outer_join_where_null_gate.sql
--
-- Safety gate: an "IS NULL" test in the WHERE on a column that an OUTER JOIN can
-- NULL-extend (an optional/dimension-side column) admits orphan (NULL-extended)
-- rows into the result.  The recompute path applies the view WHERE to the MATCHED
-- (pre-orphan) image of a delta row, so a row that transitions INTO the orphan
-- set — e.g. its dimension row is deleted — is dropped from the affected set and
-- its NULL-extended group is never recomputed.  The matview then silently
-- diverges from a full REFRESH (a CONFIRMED wrong-result before this gate).
--
-- The gate rejects ONLY orphan-admitting IS NULL on an outer-join optional-side
-- column.  Null-REJECTING predicates on the same column (IS NOT NULL, equality,
-- comparison) are FALSE for the orphan image, so they exclude orphans and remain
-- supported.  A plain nullable column that no outer join extends (single table,
-- or the preserved/anchor side of the join) is also unaffected.

\set ON_ERROR_STOP on

DROP TABLE IF EXISTS ojw_d, ojw_f CASCADE;
CREATE TABLE ojw_d(id int primary key, k text, extra text);
CREATE TABLE ojw_f(id int primary key, did int, v numeric);
INSERT INTO ojw_d VALUES (1,'K1','x'),(2,'K2','x');
INSERT INTO ojw_f VALUES (100,1,10),(101,1,20),(200,2,7),(300,999,5);

-- ------------------------------------------------------------------ rejection
\echo '--- the orphan-admitting shape must be rejected (was a silent wrong result) ---'
DO $$
DECLARE rejected int := 0;
BEGIN
  -- optional-side column IS NULL, directly
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ojw_x WITH (incremental_refresh=true) AS
             SELECT d.k, count(*) c, sum(f.v) s FROM ojw_f f LEFT JOIN ojw_d d ON f.did=d.id
             WHERE d.extra IS NULL GROUP BY d.k';
    EXECUTE 'DROP MATERIALIZED VIEW ojw_x';
  EXCEPTION WHEN feature_not_supported THEN rejected := rejected + 1; END;

  -- buried inside an OR: the orphan image (all optional cols NULL) still passes
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ojw_x WITH (incremental_refresh=true) AS
             SELECT d.k, count(*) c FROM ojw_f f LEFT JOIN ojw_d d ON f.did=d.id
             WHERE f.v > 0 OR d.extra IS NULL GROUP BY d.k';
    EXECUTE 'DROP MATERIALIZED VIEW ojw_x';
  EXCEPTION WHEN feature_not_supported THEN rejected := rejected + 1; END;

  -- RIGHT JOIN: the optional side is the left table here
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ojw_x WITH (incremental_refresh=true) AS
             SELECT d.k, count(*) c FROM ojw_f f RIGHT JOIN ojw_d d ON f.did=d.id
             WHERE f.v IS NULL GROUP BY d.k';
    EXECUTE 'DROP MATERIALIZED VIEW ojw_x';
  EXCEPTION WHEN feature_not_supported THEN rejected := rejected + 1; END;

  IF rejected = 3 THEN RAISE NOTICE 'orphan-admitting IS NULL on optional side (direct / OR / RIGHT) rejected: PASS';
  ELSE RAISE EXCEPTION 'expected 3 rejections, got %', rejected; END IF;
END $$;

-- ------------------------------------------------------------------ acceptance
\echo '--- null-rejecting / non-extended shapes must still be accepted ---'
DO $$
DECLARE accepted int := 0;
BEGIN
  -- single table, plain nullable column IS NULL (no outer join -> safe)
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ojw_x WITH (incremental_refresh=true) AS
             SELECT k, count(*) c FROM ojw_d WHERE extra IS NULL GROUP BY k WITH NO DATA';
    EXECUTE 'DROP MATERIALIZED VIEW ojw_x'; accepted := accepted + 1;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;

  -- outer join, optional column IS NOT NULL (null-rejecting -> safe)
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ojw_x WITH (incremental_refresh=true) AS
             SELECT d.k, count(*) c FROM ojw_f f LEFT JOIN ojw_d d ON f.did=d.id
             WHERE d.extra IS NOT NULL GROUP BY d.k WITH NO DATA';
    EXECUTE 'DROP MATERIALIZED VIEW ojw_x'; accepted := accepted + 1;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;

  -- outer join, preserved-anchor column IS NULL (never NULL-extended -> safe)
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ojw_x WITH (incremental_refresh=true) AS
             SELECT d.k, count(*) c FROM ojw_f f LEFT JOIN ojw_d d ON f.did=d.id
             WHERE f.v IS NULL GROUP BY d.k WITH NO DATA';
    EXECUTE 'DROP MATERIALIZED VIEW ojw_x'; accepted := accepted + 1;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;

  -- outer join, optional column equality (null-rejecting -> safe)
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ojw_x WITH (incremental_refresh=true) AS
             SELECT d.k, count(*) c FROM ojw_f f LEFT JOIN ojw_d d ON f.did=d.id
             WHERE d.extra = ''x'' GROUP BY d.k WITH NO DATA';
    EXECUTE 'DROP MATERIALIZED VIEW ojw_x'; accepted := accepted + 1;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;

  IF accepted = 4 THEN RAISE NOTICE 'single-table IS NULL / optional IS NOT NULL / anchor IS NULL / optional = const accepted: PASS';
  ELSE RAISE EXCEPTION 'expected 4 acceptances, got %', accepted; END IF;
END $$;

-- ------------------------------------------------------ differential (accepted)
\echo '--- an accepted optional-side WHERE stays == REFRESH under the DML that broke the buggy shape ---'
CREATE MATERIALIZED VIEW ojw_inc WITH (incremental_refresh=true) AS
  SELECT d.k, count(*) c, sum(f.v) s FROM ojw_f f LEFT JOIN ojw_d d ON f.did=d.id
  WHERE d.extra IS NOT NULL GROUP BY d.k;
CREATE MATERIALIZED VIEW ojw_ref AS
  SELECT d.k, count(*) c, sum(f.v) s FROM ojw_f f LEFT JOIN ojw_d d ON f.did=d.id
  WHERE d.extra IS NOT NULL GROUP BY d.k;

DELETE FROM ojw_d WHERE id=1;          -- facts 100,101 become orphans
INSERT INTO ojw_f VALUES (400,2,3);    -- add a matched fact
UPDATE ojw_d SET extra=NULL WHERE id=2;-- flip K2 rows OUT of the filter
INSERT INTO ojw_f VALUES (500,777,9);  -- add an orphan fact
DELETE FROM ojw_f WHERE id=300;        -- remove an orphan fact
REFRESH MATERIALIZED VIEW ojw_ref;

DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT k,c,s FROM ojw_inc EXCEPT SELECT k,c,s FROM ojw_ref)
    UNION ALL (SELECT k,c,s FROM ojw_ref EXCEPT SELECT k,c,s FROM ojw_inc)) z;
  IF d = 0 THEN RAISE NOTICE 'accepted optional-side WHERE == REFRESH after mixed DML: PASS';
  ELSE RAISE EXCEPTION 'incremental diverged from REFRESH by % row(s)', d; END IF;
END $$;

DROP TABLE ojw_d, ojw_f CASCADE;
\echo ''
