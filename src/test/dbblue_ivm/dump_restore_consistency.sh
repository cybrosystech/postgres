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

note "=== building source matviews (SUM/COUNT, AVG, MIN/MAX, JOIN, HAVING) ==="
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
for mv in mv_sum mv_avg mv_minmax; do
  cat=$($PSQL -d $DST -tAc "SELECT count(*) FROM pg_dbblue_matview WHERE mvrelid='$mv'::regclass;")
  check "$mv catalog rows" "$cat" "1"
done
catj=$($PSQL -d $DST -tAc "SELECT count(*) FROM pg_dbblue_matview WHERE mvrelid='mv_join'::regclass;")
check "mv_join catalog rows (one per source table)" "$catj" "2"
hbase=$($PSQL -d $DST -tAc "SELECT count(*) FROM pg_dbblue_matview p JOIN pg_class c ON c.oid=p.mvrelid WHERE c.relname LIKE '\_dbblue\_%\_base';")
check "HAVING base catalog rows" "$hbase" "1"

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

note "=== UNION ALL: dedup + count rebuilt on restore, incremental correct ==="
# source totals are 100 rows across 5 tags = 20 each, collapsed to 5 distinct
ud=$($PSQL -d $DST -tAc "SELECT count(*) FROM mv_union;")
check "mv_union distinct rows" "$ud" "5"
ut=$($PSQL -d $DST -tAc "SELECT sum(__mv_count__) FROM mv_union;")
check "mv_union total count" "$ut" "100"
$PSQL -d $DST -q -c "INSERT INTO tag_a VALUES (9000,'T0'); DELETE FROM tag_b WHERE id=1;"
umm=$($PSQL -d $DST -tAc "
WITH live AS (SELECT tag FROM tag_a UNION ALL SELECT tag FROM tag_b)
SELECT count(*) FROM (SELECT tag, count(*) c FROM live GROUP BY tag) l
  FULL JOIN mv_union m USING(tag) WHERE l.c IS DISTINCT FROM m.__mv_count__;")
check "mv_union mismatches after restore+DML" "$umm" "0"

note "=== TRUNCATE re-seeding after restore ==="
$PSQL -d $DST -q -c "TRUNCATE sales CASCADE;"
rows=$($PSQL -d $DST -tAc "SELECT (SELECT count(*) FROM mv_sum)+(SELECT count(*) FROM mv_avg)+(SELECT count(*) FROM mv_minmax)+(SELECT count(*) FROM mv_join)+(SELECT count(*) FROM mv_having);")
check "all aggregate matviews empty after TRUNCATE" "$rows" "0"

$PSQL -d postgres -q -c "DROP DATABASE IF EXISTS $SRC;" -c "DROP DATABASE IF EXISTS $DST;"

note ""
if [ "$fail" = "0" ]; then note "=== ALL DUMP/RESTORE CHECKS PASSED ==="; exit 0
else note "=== DUMP/RESTORE TEST FAILED ==="; exit 1; fi
