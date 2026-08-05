#!/usr/bin/env bash
#
# DBblue IVM — pg_dump / restore round-trip consistency test.
#
# Verifies that an incremental materialized view survives a dump/restore cycle
# with incremental maintenance fully re-armed:
#   * the engine-managed unique index is NOT dumped twice (clean restore),
#   * delta triggers + catalog rows are re-established on restore,
#   * incremental maintenance (INSERT/DELETE) is correct after restore for
#     SUM/COUNT, AVG, MIN/MAX, JOIN and HAVING matviews,
#   * HAVING failing-group seeding is rebuilt (a group crossing the threshold
#     after restore yields the correct full aggregate, not just the delta),
#   * TRUNCATE re-seeding works after restore.
#
# Usage:  src/test/dbblue_ivm/dump_restore_consistency.sh
# Env:    PGPORT (5432), PGUSER (cybrosys), BINDIR (./bin)
set -u

BINDIR="${BINDIR:-./bin}"
PORT="${PGPORT:-5432}"
USER="${PGUSER:-cybrosys}"
PSQL="$BINDIR/psql -p $PORT -U $USER"
PGDUMP="$BINDIR/pg_dump -p $PORT -U $USER"
SRC=ivm_dr_src
DST=ivm_dr_dst
fail=0

note() { printf '%s\n' "$*"; }
check() { # check <label> <actual> <expected>
  if [ "$2" = "$3" ]; then note "  PASS: $1 ($2)"; else note "  FAIL: $1 (got '$2', want '$3')"; fail=1; fi
}

$PSQL -d postgres -q -c "DROP DATABASE IF EXISTS $SRC;" -c "DROP DATABASE IF EXISTS $DST;"
$PSQL -d postgres -q -c "CREATE DATABASE $SRC;" -c "CREATE DATABASE $DST;"

note "=== building source matviews (SUM/COUNT, AVG, MIN/MAX, JOIN, HAVING, expr) ==="
$PSQL -d $SRC -q <<'SQL'
\set ON_ERROR_STOP on
CREATE TABLE prod (id int PRIMARY KEY, categ int);
INSERT INTO prod SELECT g, g % 4 FROM generate_series(1,20) g;
CREATE TABLE sales (id serial PRIMARY KEY, product_id int, amount numeric);
INSERT INTO sales (product_id, amount)
  SELECT (g % 20) + 1, (g % 97 + 1)::numeric FROM generate_series(1, 800) g;

CREATE MATERIALIZED VIEW mv_sum WITH (incremental_refresh=true) AS
  SELECT product_id, SUM(amount) AS rev, COUNT(*) AS cnt FROM sales GROUP BY product_id WITH DATA;
CREATE MATERIALIZED VIEW mv_avg WITH (incremental_refresh=true) AS
  SELECT product_id, SUM(amount) AS rev, COUNT(*) AS cnt, AVG(amount) AS a FROM sales GROUP BY product_id WITH DATA;
CREATE MATERIALIZED VIEW mv_minmax WITH (incremental_refresh=true) AS
  SELECT product_id, MIN(amount) AS mn, MAX(amount) AS mx, COUNT(*) AS cnt FROM sales GROUP BY product_id WITH DATA;
CREATE MATERIALIZED VIEW mv_join WITH (incremental_refresh=true) AS
  SELECT p.categ, SUM(s.amount) AS rev, COUNT(*) AS cnt
  FROM sales s JOIN prod p ON p.id = s.product_id GROUP BY p.categ WITH DATA;
CREATE MATERIALIZED VIEW mv_having WITH (incremental_refresh=true) AS
  SELECT product_id, SUM(amount) AS rev, COUNT(*) AS cnt FROM sales GROUP BY product_id HAVING SUM(amount) > 100 WITH DATA;

-- Expression aggregates (SUM(CASE...), AVG(COALESCE...)) are auto-routed to the
-- deparse delta core regardless of the GUC; this matview proves such a shape is
-- restorable under DEFAULT settings (the restore path re-runs setup, which must
-- route it the same way — otherwise restore would fail).
CREATE MATERIALIZED VIEW mv_expr WITH (incremental_refresh=true) AS
  SELECT product_id,
         SUM(CASE WHEN amount > 50 THEN amount ELSE 0 END) AS hi_rev,
         AVG(COALESCE(amount, 0)) AS avg_amt,
         COUNT(*) AS cnt
  FROM sales GROUP BY product_id WITH DATA;

-- INNER JOIN with an expression aggregate arg: auto-routed to deparse, one delta
-- per source table.  Proves the join-expression shape is restorable by default.
CREATE MATERIALIZED VIEW mv_join_expr WITH (incremental_refresh=true) AS
  SELECT p.categ,
         SUM(CASE WHEN s.amount > 50 THEN s.amount ELSE 0 END) AS hi_rev,
         COUNT(*) AS cnt
  FROM sales s JOIN prod p ON p.id = s.product_id GROUP BY p.categ WITH DATA;

-- Expression-arg aggregate + HAVING on it: exercises the deparse failing-group
-- backfill across dump/restore (the backfill SQL is regenerated at restore).
CREATE MATERIALIZED VIEW mv_having_expr WITH (incremental_refresh=true) AS
  SELECT product_id,
         SUM(CASE WHEN amount > 50 THEN amount ELSE 0 END) AS hi_rev,
         COUNT(*) AS cnt
  FROM sales GROUP BY product_id
  HAVING SUM(CASE WHEN amount > 50 THEN amount ELSE 0 END) > 200 WITH DATA;

-- INNER JOIN + SUM(CASE) + HAVING: exercises the deparse failing-group backfill
-- over a join, on both the create and restore paths.
CREATE MATERIALIZED VIEW mv_join_having WITH (incremental_refresh=true) AS
  SELECT p.categ,
         SUM(CASE WHEN s.amount > 50 THEN s.amount ELSE 0 END) AS hi_rev,
         COUNT(*) AS cnt
  FROM sales s JOIN prod p ON p.id = s.product_id GROUP BY p.categ
  HAVING SUM(CASE WHEN s.amount > 50 THEN s.amount ELSE 0 END) > 500 WITH DATA;

-- Row-level UNION ALL with cross-branch duplicates (exercises dedup + count).
CREATE TABLE tag_a (id int PRIMARY KEY, tag text);
CREATE TABLE tag_b (id int PRIMARY KEY, tag text);
INSERT INTO tag_a SELECT g, 'T'||(g%5) FROM generate_series(1,50) g;
INSERT INTO tag_b SELECT g, 'T'||(g%5) FROM generate_series(1,50) g;
CREATE MATERIALIZED VIEW mv_union WITH (incremental_refresh=true) AS
  SELECT tag FROM tag_a UNION ALL SELECT tag FROM tag_b WITH DATA;
SQL

note "=== dump ==="
$PGDUMP -d $SRC -f /tmp/ivm_dr_dump.sql
n_idx=$(grep -c "CREATE UNIQUE INDEX" /tmp/ivm_dr_dump.sql)
check "engine unique indexes not dumped" "$n_idx" "0"

note "=== restore (ON_ERROR_STOP=1 must succeed cleanly) ==="
$PSQL -d $DST -v ON_ERROR_STOP=1 -q -f /tmp/ivm_dr_dump.sql >/tmp/ivm_dr_restore.log 2>&1
check "restore exit status" "$?" "0"
n_err=$(grep -ci "error" /tmp/ivm_dr_restore.log)
check "restore had no errors" "$n_err" "0"
n_warn=$(grep -ci "not re-established\|not enabled" /tmp/ivm_dr_restore.log)
check "restore had no re-establish warnings" "$n_warn" "0"

note "=== triggers + catalog re-established (incl. HAVING base) ==="
# Single-source matviews have one catalog row; the JOIN matview has one per
# source table (sales + prod = 2).
for mv in mv_sum mv_avg mv_minmax mv_expr; do
  cat=$($PSQL -d $DST -tAc "SELECT count(*) FROM pg_dbblue_matview WHERE mvrelid='$mv'::regclass;")
  check "$mv catalog rows" "$cat" "1"
done
# mv_expr must have been auto-routed to the deparse core (CASE rendered natively).
ec=$($PSQL -d $DST -tAc "SELECT (ins_sql LIKE '%CASE%')::int FROM pg_dbblue_matview WHERE mvrelid='mv_expr'::regclass;")
check "mv_expr auto-routed to deparse core (CASE in delta SQL)" "$ec" "1"
catj=$($PSQL -d $DST -tAc "SELECT count(*) FROM pg_dbblue_matview WHERE mvrelid='mv_join'::regclass;")
check "mv_join catalog rows (one per source table)" "$catj" "2"
catje=$($PSQL -d $DST -tAc "SELECT count(*) FROM pg_dbblue_matview WHERE mvrelid='mv_join_expr'::regclass;")
check "mv_join_expr catalog rows (one per source table)" "$catje" "2"
ecj=$($PSQL -d $DST -tAc "SELECT (bool_and(ins_sql LIKE '%CASE%' AND ins_sql LIKE '%__mv_newtable%'))::int FROM pg_dbblue_matview WHERE mvrelid='mv_join_expr'::regclass;")
check "mv_join_expr auto-routed to deparse core (CASE in delta SQL)" "$ecj" "1"
# Three HAVING matviews: mv_having, mv_having_expr, mv_join_having.
hbase=$($PSQL -d $DST -tAc "SELECT count(DISTINCT p.mvrelid) FROM pg_dbblue_matview p JOIN pg_class c ON c.oid=p.mvrelid WHERE c.relname LIKE '\_dbblue\_%\_base';")
check "HAVING base catalog rows" "$hbase" "3"

note "=== incremental correctness after restore (INSERT + DELETE) ==="
$PSQL -d $DST -q -c "INSERT INTO sales(product_id, amount) VALUES (1, 50),(2, 200),(1, 7);"
$PSQL -d $DST -q -c "DELETE FROM sales WHERE amount=7 AND product_id=1;"
mm=$($PSQL -d $DST -tAc "
WITH live AS (SELECT product_id, SUM(amount) rev, COUNT(*) cnt, AVG(amount) a FROM sales GROUP BY product_id)
SELECT
  (SELECT count(*) FROM live JOIN mv_sum m USING(product_id) WHERE abs(live.rev-m.rev)>0.001 OR live.cnt<>m.cnt)
+ (SELECT count(*) FROM live JOIN mv_avg m USING(product_id) WHERE abs(live.rev-m.rev)>0.001 OR live.cnt<>m.cnt OR abs(live.a-m.a)>0.001)
+ (SELECT count(*) FROM (SELECT product_id, MIN(amount) mn, MAX(amount) mx, COUNT(*) c FROM sales GROUP BY product_id) l
     JOIN mv_minmax m USING(product_id) WHERE abs(l.mn-m.mn)>0.001 OR abs(l.mx-m.mx)>0.001 OR l.c<>m.cnt)
+ (SELECT count(*) FROM (SELECT p.categ, SUM(s.amount) rev, COUNT(*) c FROM sales s JOIN prod p ON p.id=s.product_id GROUP BY p.categ) l
     JOIN mv_join m USING(categ) WHERE abs(l.rev-m.rev)>0.001 OR l.c<>m.cnt);
")
check "mismatches (sum+avg+minmax+join)" "$mm" "0"
me=$($PSQL -d $DST -tAc "
WITH live AS (SELECT product_id,
                     SUM(CASE WHEN amount>50 THEN amount ELSE 0 END) hi,
                     AVG(COALESCE(amount,0)) a, COUNT(*) c
              FROM sales GROUP BY product_id)
SELECT count(*) FROM live JOIN mv_expr m USING(product_id)
  WHERE abs(live.hi-m.hi_rev)>0.001 OR abs(live.a-m.avg_amt)>0.001 OR live.c<>m.cnt;")
check "mv_expr (SUM(CASE)+AVG(COALESCE)) mismatches after restore" "$me" "0"
mje=$($PSQL -d $DST -tAc "
WITH live AS (SELECT p.categ, SUM(CASE WHEN s.amount>50 THEN s.amount ELSE 0 END) hi, COUNT(*) c
              FROM sales s JOIN prod p ON p.id=s.product_id GROUP BY p.categ)
SELECT count(*) FROM live JOIN mv_join_expr m USING(categ)
  WHERE abs(live.hi-m.hi_rev)>0.001 OR live.c<>m.cnt;")
check "mv_join_expr (SUM(CASE) over JOIN) mismatches after restore" "$mje" "0"
mhe=$($PSQL -d $DST -tAc "
WITH live AS (SELECT product_id, SUM(CASE WHEN amount>50 THEN amount ELSE 0 END) hi, COUNT(*) c
              FROM sales GROUP BY product_id
              HAVING SUM(CASE WHEN amount>50 THEN amount ELSE 0 END) > 200)
SELECT count(*) FROM live FULL JOIN mv_having_expr m USING(product_id)
  WHERE live.product_id IS DISTINCT FROM m.product_id
     OR abs(live.hi-m.hi_rev)>0.001 OR live.c<>m.cnt;")
check "mv_having_expr (SUM(CASE)+HAVING) mismatches after restore" "$mhe" "0"
mjh=$($PSQL -d $DST -tAc "
WITH live AS (SELECT p.categ, SUM(CASE WHEN s.amount>50 THEN s.amount ELSE 0 END) hi, COUNT(*) c
              FROM sales s JOIN prod p ON p.id=s.product_id GROUP BY p.categ
              HAVING SUM(CASE WHEN s.amount>50 THEN s.amount ELSE 0 END) > 500)
SELECT count(*) FROM live FULL JOIN mv_join_having m USING(categ)
  WHERE live.categ IS DISTINCT FROM m.categ
     OR abs(live.hi-m.hi_rev)>0.001 OR live.c<>m.cnt;")
check "mv_join_having (JOIN+SUM(CASE)+HAVING) mismatches after restore" "$mjh" "0"

note "=== HAVING: failing group crosses threshold after restore (needs rebuilt seed) ==="
# pick a currently-failing product and push it well over the threshold
$PSQL -d $DST -q -c "INSERT INTO sales(product_id, amount)
  SELECT product_id, 100000 FROM (SELECT product_id FROM sales GROUP BY product_id HAVING SUM(amount)<100 LIMIT 1) x;"
hmm=$($PSQL -d $DST -tAc "
SELECT count(*) FROM
  (SELECT product_id, SUM(amount) rev, COUNT(*) cnt FROM sales GROUP BY product_id HAVING SUM(amount)>100) live
  FULL JOIN mv_having m USING(product_id)
  WHERE live.product_id IS DISTINCT FROM m.product_id OR abs(live.rev-m.rev)>0.001 OR live.cnt<>m.cnt;")
check "HAVING mismatches after threshold cross" "$hmm" "0"

note "=== UNION ALL: duplicates kept (multiset), incremental correct on restore ==="
# UNION ALL keeps every row: 100 source rows -> 100 matview rows (no dedup, no __mv_count__).
ut=$($PSQL -d $DST -tAc "SELECT count(*) FROM mv_union;")
check "mv_union total rows (duplicates kept)" "$ut" "100"
$PSQL -d $DST -q -c "INSERT INTO tag_a VALUES (9000,'T0'); DELETE FROM tag_b WHERE id=1;"
# multiset compare vs a live recompute of the identical UNION ALL
umm=$($PSQL -d $DST -tAc "
SELECT count(*) FROM (
  (SELECT tag FROM mv_union EXCEPT ALL (SELECT tag FROM tag_a UNION ALL SELECT tag FROM tag_b))
  UNION ALL
  ((SELECT tag FROM tag_a UNION ALL SELECT tag FROM tag_b) EXCEPT ALL SELECT tag FROM mv_union)
) d;")
check "mv_union multiset mismatches after restore+DML" "$umm" "0"

note "=== TRUNCATE re-seeding after restore ==="
$PSQL -d $DST -q -c "TRUNCATE sales CASCADE;"
rows=$($PSQL -d $DST -tAc "SELECT (SELECT count(*) FROM mv_sum)+(SELECT count(*) FROM mv_avg)+(SELECT count(*) FROM mv_minmax)+(SELECT count(*) FROM mv_join)+(SELECT count(*) FROM mv_having)+(SELECT count(*) FROM mv_expr)+(SELECT count(*) FROM mv_join_expr)+(SELECT count(*) FROM mv_having_expr)+(SELECT count(*) FROM mv_join_having);")
check "all aggregate matviews empty after TRUNCATE" "$rows" "0"

$PSQL -d postgres -q -c "DROP DATABASE IF EXISTS $SRC;" -c "DROP DATABASE IF EXISTS $DST;"

note ""
if [ "$fail" = "0" ]; then note "=== ALL DUMP/RESTORE CHECKS PASSED ==="; exit 0
else note "=== DUMP/RESTORE TEST FAILED ==="; exit 1; fi
