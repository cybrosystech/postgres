-- =============================================================================
-- Phase 7: DBblue offset-flip measurement matrix
-- account_move_line (1.95M rows), three sort shapes, ON vs OFF
--
-- Shapes:
--   B1 = ORDER BY id DESC                          (pkey)
--   B2 = ORDER BY date DESC, id DESC               (multi-col, partial index)
--   B4 = ORDER BY date DESC, move_name DESC, id ASC (Odoo _order, exact index)
--
-- Method: COUNT query seeds the cache, then paginated SELECT at K≈N-80
-- (jump-to-last, K > N/2) is run 5x.  Repeated with GUC off for baseline.
-- =============================================================================

\timing on
\set ON_ERROR_STOP on
\pset format aligned

-- Odoo-style predicate (mirrors web_search_read for Journal Items view)
\set pred 'company_id = 1 AND (display_type NOT IN (''line_section'',''line_note'') OR display_type IS NULL)'

-- ─────────────────────────────────────────────────────────────────────────────
-- 0. Warm shared buffers (table + indexes in cache; discard timing)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '===== Warming shared buffers ====='
\o /dev/null
SELECT id, date, move_name FROM account_move_line
WHERE company_id = 1 LIMIT 1000000;
\o

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Establish N (we use this for OFFSET K = N - 80 in all three shapes)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '===== 1. COUNT under predicate (establishes N) ====='

SELECT count(*) AS n FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
\gset

\echo 'N = ' :n

\set k :n - 80

-- ─────────────────────────────────────────────────────────────────────────────
-- SHAPE B1: ORDER BY id DESC  (pkey backward → pkey forward after flip)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '╔══════════════════════════════════════════════════════════════════╗'
\echo '║  SHAPE B1: ORDER BY id DESC                                     ║'
\echo '╚══════════════════════════════════════════════════════════════════╝'

SET dbblue_count_cache = on;
SET dbblue_offset_flip = on;

-- Seed the cache
\echo '-- Seeding COUNT cache --'
SELECT count(*) FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL);

\echo ''
\echo '--- B1 ON (flip should fire): plan ---'
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC
OFFSET :k LIMIT 80;

\echo ''
\echo '--- B1 ON: 5 timing runs ---'
\o /dev/null
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC OFFSET :k LIMIT 80;
\o

SET dbblue_count_cache = off;
SET dbblue_offset_flip = off;

\echo ''
\echo '--- B1 OFF (baseline): plan ---'
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC
OFFSET :k LIMIT 80;

\echo ''
\echo '--- B1 OFF: 5 timing runs ---'
\o /dev/null
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC OFFSET :k LIMIT 80;
\o

-- ─────────────────────────────────────────────────────────────────────────────
-- SHAPE B2: ORDER BY date DESC, id DESC  (multi-col, no perfect index match)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '╔══════════════════════════════════════════════════════════════════╗'
\echo '║  SHAPE B2: ORDER BY date DESC, id DESC                          ║'
\echo '╚══════════════════════════════════════════════════════════════════╝'

SET dbblue_count_cache = on;
SET dbblue_offset_flip = on;

\echo '-- Seeding COUNT cache --'
SELECT count(*) FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL);

\echo ''
\echo '--- B2 ON (flip should fire): plan ---'
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, id DESC
OFFSET :k LIMIT 80;

\echo ''
\echo '--- B2 ON: 5 timing runs ---'
\o /dev/null
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, id DESC OFFSET :k LIMIT 80;
\o

SET dbblue_count_cache = off;
SET dbblue_offset_flip = off;

\echo ''
\echo '--- B2 OFF (baseline): plan ---'
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, id DESC
OFFSET :k LIMIT 80;

\echo ''
\echo '--- B2 OFF: 5 timing runs ---'
\o /dev/null
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, id DESC OFFSET :k LIMIT 80;
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, id DESC OFFSET :k LIMIT 80;
\o

-- ─────────────────────────────────────────────────────────────────────────────
-- SHAPE B4: ORDER BY date DESC, move_name DESC, id ASC  (Odoo _order, exact index)
-- The index account_move_line_date_name_id_idx is (date DESC, move_name DESC, id ASC).
-- After flip: (date ASC, move_name ASC, id DESC) = backward scan of that index.
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '╔══════════════════════════════════════════════════════════════════╗'
\echo '║  SHAPE B4: ORDER BY date DESC, move_name DESC, id ASC (Odoo)   ║'
\echo '╚══════════════════════════════════════════════════════════════════╝'

SET dbblue_count_cache = on;
SET dbblue_offset_flip = on;

\echo '-- Seeding COUNT cache --'
SELECT count(*) FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL);

\echo ''
\echo '--- B4 ON (flip should fire): plan ---'
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, date, move_name, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, move_name DESC, id ASC
OFFSET :k LIMIT 80;

\echo ''
\echo '--- B4 ON: 5 timing runs ---'
\o /dev/null
SELECT id, date, move_name, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, move_name DESC, id ASC OFFSET :k LIMIT 80;
SELECT id, date, move_name, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, move_name DESC, id ASC OFFSET :k LIMIT 80;
SELECT id, date, move_name, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, move_name DESC, id ASC OFFSET :k LIMIT 80;
SELECT id, date, move_name, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, move_name DESC, id ASC OFFSET :k LIMIT 80;
SELECT id, date, move_name, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, move_name DESC, id ASC OFFSET :k LIMIT 80;
\o

SET dbblue_count_cache = off;
SET dbblue_offset_flip = off;

\echo ''
\echo '--- B4 OFF (baseline): plan ---'
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, date, move_name, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, move_name DESC, id ASC
OFFSET :k LIMIT 80;

\echo ''
\echo '--- B4 OFF: 5 timing runs ---'
\o /dev/null
SELECT id, date, move_name, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, move_name DESC, id ASC OFFSET :k LIMIT 80;
SELECT id, date, move_name, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, move_name DESC, id ASC OFFSET :k LIMIT 80;
SELECT id, date, move_name, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, move_name DESC, id ASC OFFSET :k LIMIT 80;
SELECT id, date, move_name, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, move_name DESC, id ASC OFFSET :k LIMIT 80;
SELECT id, date, move_name, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY date DESC, move_name DESC, id ASC OFFSET :k LIMIT 80;
\o

-- ─────────────────────────────────────────────────────────────────────────────
-- REGRESSION: first-page query (K=0, should NEVER flip)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '╔══════════════════════════════════════════════════════════════════╗'
\echo '║  REGRESSION: first-page (K=0, must not flip)                   ║'
\echo '╚══════════════════════════════════════════════════════════════════╝'

SET dbblue_count_cache = on;
SET dbblue_offset_flip = on;

\echo '-- Seeding COUNT cache --'
SELECT count(*) FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL);

\echo ''
\echo '--- First page ON (must NOT flip, K=0 <= N/2): plan ---'
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC
OFFSET 0 LIMIT 80;

-- ─────────────────────────────────────────────────────────────────────────────
-- REGRESSION: mid-page query (K=N/2, boundary — must NOT flip)
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '--- Mid-page ON (K=N/2 exactly, boundary, must NOT flip): plan ---'

\set k_mid :n / 2

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, date, balance FROM account_move_line
WHERE company_id = 1
  AND (display_type NOT IN ('line_section','line_note') OR display_type IS NULL)
ORDER BY id DESC
OFFSET :k_mid LIMIT 80;

\echo ''
\echo '===== Phase 7 complete ====='

-- Reset GUCs
SET dbblue_count_cache = on;
SET dbblue_offset_flip = on;
