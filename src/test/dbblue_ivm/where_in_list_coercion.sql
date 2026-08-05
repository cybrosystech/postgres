-- where_in_list_coercion.sql
--
-- Odoo IN-lists (col = ANY(ARRAY['a','b'])) parse to a ScalarArrayOpExpr whose
-- array argument is an ArrayCoerceExpr (ARRAY[...]::varchar[]::text[]).  The WHERE
-- validator handled ScalarArrayOpExpr + ArrayExpr but not the ArrayCoerceExpr in
-- between, so these common filters were rejected.  Accepting the coerced array is
-- byte-identical to a full REFRESH — an IN-list is a deterministic per-row
-- predicate — PROVIDED the per-element coercion is IMMUTABLE (a STABLE/VOLATILE
-- element cast in a membership position would drift, so it stays rejected).

\set ON_ERROR_STOP on

DROP TABLE IF EXISTS inl CASCADE;
CREATE TABLE inl(id int primary key, s varchar, v numeric);
INSERT INTO inl SELECT g, (ARRAY['a','b','c','d'])[1+g%4], g FROM generate_series(1,40) g;

\echo '--- ArrayCoerceExpr IN-list accepted + == REFRESH through filter-crossing DML ---'
CREATE MATERIALIZED VIEW inl_inc WITH (incremental_refresh=true) AS
  SELECT s, count(*) c, sum(v) sm FROM inl
  WHERE s = ANY(ARRAY['a','b']::varchar[]) GROUP BY s;
CREATE MATERIALIZED VIEW inl_ref AS
  SELECT s, count(*) c, sum(v) sm FROM inl
  WHERE s = ANY(ARRAY['a','b']::varchar[]) GROUP BY s;

DO $$
DECLARE d int;
BEGIN
  INSERT INTO inl VALUES (100,'a',5),(101,'z',9);   -- one inside, one outside the list
  UPDATE inl SET s='b' WHERE id=3;                  -- stays inside
  UPDATE inl SET s='x' WHERE id=5;                  -- leaves the list
  UPDATE inl SET s='a' WHERE id=6;                  -- enters the list (was 'c')
  DELETE FROM inl WHERE id=9;
  REFRESH MATERIALIZED VIEW inl_ref;
  SELECT count(*) INTO d FROM (
    (SELECT s,c,sm FROM inl_inc EXCEPT SELECT s,c,sm FROM inl_ref)
    UNION ALL (SELECT s,c,sm FROM inl_ref EXCEPT SELECT s,c,sm FROM inl_inc)) z;
  IF d = 0 THEN RAISE NOTICE 'ArrayCoerceExpr IN-list == REFRESH through filter-crossing DML: PASS';
  ELSE RAISE EXCEPTION 'IN-list diverged from REFRESH by % row(s)', d; END IF;
END $$;
DROP MATERIALIZED VIEW inl_inc, inl_ref;
DROP TABLE inl CASCADE;
\echo ''
