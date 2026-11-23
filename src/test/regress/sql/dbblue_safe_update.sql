--
-- DBblue safe mode (dbblue_safe_mode): refuses UPDATE/DELETE that would change
-- every row.  Two fixes are exercised here:
--
--   Bug 1: an UPDATE on an INSTEAD OF trigger view WITH a real WHERE is allowed
--          (it used to be wrongly refused), while a no-WHERE one is still
--          refused.
--   Bug 2: a DO INSTEAD rule that rewrites a restricted statement into a
--          whole-table change is refused even after AutoPrepare caches the
--          plan (the post-rewrite guard used to be skipped once cached).
--
-- Self-contained: dsu_ objects.

SET client_min_messages = warning;
\set VERBOSITY terse

-- ---------- Bug 1: INSTEAD OF trigger view ----------
CREATE TABLE dsu_acct (id int PRIMARY KEY, bal int);
INSERT INTO dsu_acct VALUES (1, 100), (2, 200), (3, 300);
CREATE VIEW dsu_vacct AS SELECT id, bal FROM dsu_acct;
CREATE FUNCTION dsu_vacct_upd() RETURNS trigger LANGUAGE plpgsql AS
  $$ BEGIN UPDATE dsu_acct SET bal = NEW.bal WHERE id = OLD.id; RETURN NEW; END $$;
CREATE TRIGGER dsu_vacct_io INSTEAD OF UPDATE ON dsu_vacct
  FOR EACH ROW EXECUTE FUNCTION dsu_vacct_upd();

SET dbblue_safe_mode = on;

-- restricted view update: ALLOWED (Bug 1 fix), changes only id = 2
UPDATE dsu_vacct SET bal = 999 WHERE id = 2;
SELECT id, bal FROM dsu_acct ORDER BY id;

-- no-WHERE view update: still REFUSED
UPDATE dsu_vacct SET bal = 0;

-- plain table: WHERE allowed, no-WHERE refused, always-true refused
UPDATE dsu_acct SET bal = 111 WHERE id = 1;
UPDATE dsu_acct SET bal = 0;
UPDATE dsu_acct SET bal = 0 WHERE true;

RESET dbblue_safe_mode;

-- ---------- Bug 2: DO INSTEAD rule + AutoPrepare ----------
CREATE TABLE dsu_t (id int, flag bool);
INSERT INTO dsu_t SELECT g, false FROM generate_series(1, 5) g;
CREATE VIEW dsu_vt AS SELECT id, flag FROM dsu_t;
-- a rule that turns any update on the view into a whole-table update
CREATE RULE dsu_vt_upd AS ON UPDATE TO dsu_vt DO INSTEAD UPDATE dsu_t SET flag = true;

SET dbblue_safe_mode = on;
SET compute_query_id = on;
SET dbblue_autoprepare_enabled = on;
SET dbblue_autoprepare_threshold = 2;

-- Same statement repeated: must be refused every time, whether or not
-- AutoPrepare has cached a plan by now.
UPDATE dsu_vt SET flag = true WHERE id = 3;
UPDATE dsu_vt SET flag = true WHERE id = 3;
UPDATE dsu_vt SET flag = true WHERE id = 3;
UPDATE dsu_vt SET flag = true WHERE id = 3;

-- No row of dsu_t may have changed.
SELECT count(*) FILTER (WHERE flag) AS changed, count(*) AS total FROM dsu_t;

RESET dbblue_autoprepare_enabled;
RESET compute_query_id;

-- ---------- Rule rewriting on every route ----------
-- The same rule-widened statement reached through EXPLAIN ANALYZE (which runs
-- it), a data-modifying WITH, and CREATE TABLE AS: each command rewrites the
-- statement itself, and WITH hides it below a SELECT.  All must be refused.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  UPDATE dsu_vt SET flag = true WHERE id = 3;
WITH u AS (UPDATE dsu_vt SET flag = true WHERE id = 3) SELECT 1 AS x;
CREATE TABLE dsu_ctas AS
  WITH u AS (UPDATE dsu_vt SET flag = true WHERE id = 3) SELECT 1 AS x;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  WITH u AS (UPDATE dsu_vt SET flag = true WHERE id = 3) SELECT 1 AS x;

-- Still no row of dsu_t may have changed.
SELECT count(*) FILTER (WHERE flag) AS changed, count(*) AS total FROM dsu_t;

-- A restricted statement on the table itself still runs on every route.
WITH u AS (UPDATE dsu_t SET flag = false WHERE id = 1) SELECT 'ran' AS restricted_with;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  UPDATE dsu_t SET flag = false WHERE id = 1;

RESET dbblue_safe_mode;

-- ---------- Plans cached before safe mode was turned on ----------
-- Turning safe mode on invalidates the session's cached plans, so a statement
-- prepared, planned by PL/pgSQL, or cached by AutoPrepare while it was off is
-- parsed and rewritten -- and so checked -- again on its next use.
SET dbblue_safe_mode = off;
PREPARE dsu_wipe AS UPDATE dsu_t SET flag = true;
CREATE FUNCTION dsu_wipe_fn() RETURNS void LANGUAGE plpgsql AS
  $$ BEGIN UPDATE dsu_t SET flag = true; END $$;
SELECT dsu_wipe_fn();	-- plans its UPDATE, and runs it
UPDATE dsu_t SET flag = false WHERE flag;
SET compute_query_id = on;
SET dbblue_autoprepare_enabled = on;
SET dbblue_autoprepare_threshold = 2;
UPDATE dsu_vt SET flag = true WHERE id = 4 AND flag IS NOT NULL;
UPDATE dsu_vt SET flag = true WHERE id = 4 AND flag IS NOT NULL;
UPDATE dsu_vt SET flag = true WHERE id = 4 AND flag IS NOT NULL;
UPDATE dsu_vt SET flag = true WHERE id = 4 AND flag IS NOT NULL;
SELECT state FROM dbblue_autoprepare_shapes(pg_backend_pid())
  WHERE query LIKE 'UPDATE dsu_vt%IS NOT NULL%';
UPDATE dsu_t SET flag = false WHERE flag;

SET dbblue_safe_mode = on;
EXECUTE dsu_wipe;
SELECT dsu_wipe_fn();
UPDATE dsu_vt SET flag = true WHERE id = 4 AND flag IS NOT NULL;
SELECT count(*) FILTER (WHERE flag) AS changed, count(*) AS total FROM dsu_t;

-- Switching it off again needs no re-check: the same statements run.
SET dbblue_safe_mode = off;
EXECUTE dsu_wipe;
SELECT count(*) FILTER (WHERE flag) AS changed FROM dsu_t;
UPDATE dsu_t SET flag = false WHERE flag;
DEALLOCATE dsu_wipe;
RESET dbblue_autoprepare_enabled;
RESET compute_query_id;
RESET dbblue_safe_mode;

-- ---------- Storing a statement is not running it ----------
-- A rule action or a function body is only stored when it is created, so
-- safe mode lets it be created (and restored by pg_restore) and checks it
-- when it runs instead.
SET dbblue_safe_mode = on;
CREATE TABLE dsu_s (id int, v int);
INSERT INTO dsu_s VALUES (1, 0), (2, 0);
CREATE VIEW dsu_vs AS SELECT * FROM dsu_s;
CREATE RULE dsu_vs_del AS ON DELETE TO dsu_vs DO INSTEAD DELETE FROM dsu_s;
CREATE FUNCTION dsu_atomic() RETURNS void LANGUAGE sql
  BEGIN ATOMIC UPDATE dsu_s SET v = 1; END;
-- a string body, validated now because check_function_bodies is on
CREATE FUNCTION dsu_string() RETURNS void LANGUAGE sql AS 'UPDATE dsu_s SET v = 1';
-- ... but none of them can change every row
DELETE FROM dsu_vs WHERE id = 1;
SELECT dsu_atomic();
SELECT dsu_string();
SELECT count(*) AS rows_left, count(*) FILTER (WHERE v <> 0) AS changed FROM dsu_s;
-- a failure while storing a body does not leave safe mode switched off
CREATE FUNCTION dsu_broken() RETURNS void LANGUAGE sql
  BEGIN ATOMIC UPDATE dsu_no_such_table SET v = 1; END;
UPDATE dsu_s SET v = 1;
RESET dbblue_safe_mode;

-- ---------- The error code ----------
-- Every refusal reports SQLSTATE 0A000 (feature_not_supported), never an
-- integrity-constraint code: psycopg2 turns class 23 into IntegrityError,
-- which Odoo shows as a validation error and logs without a traceback.
CREATE FUNCTION dsu_sqlstate(stmt text) RETURNS text LANGUAGE plpgsql AS
  $$ BEGIN EXECUTE stmt; RETURN 'no error'; EXCEPTION WHEN OTHERS THEN RETURN SQLSTATE; END $$;
SET dbblue_safe_mode = on;
SELECT dsu_sqlstate('UPDATE dsu_acct SET bal = 0') AS no_where,
       dsu_sqlstate('UPDATE dsu_acct SET bal = 0 WHERE true') AS always_true,
       dsu_sqlstate('DELETE FROM dsu_acct WHERE false') AS not_referring,
       dsu_sqlstate('MERGE INTO dsu_acct USING (VALUES (1)) v(id) ON true WHEN MATCHED THEN DELETE') AS merge,
       dsu_sqlstate('TRUNCATE dsu_acct') AS truncate,
       dsu_sqlstate('UPDATE dsu_acct SET bal = bal WHERE id = 1') AS restricted;
RESET dbblue_safe_mode;

-- cleanup
DROP VIEW dsu_vs;
DROP FUNCTION dsu_atomic(), dsu_string();
DROP TABLE dsu_s;
DROP FUNCTION dsu_wipe_fn();
DROP FUNCTION dsu_sqlstate(text);
DROP VIEW dsu_vt;
DROP TABLE dsu_t;
DROP VIEW dsu_vacct;
DROP TABLE dsu_acct;
DROP FUNCTION dsu_vacct_upd();
