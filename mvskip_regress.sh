#!/bin/bash
# Regression suite for the matview auto_skip_unchanged feature.
#
# Each case drives the feature to a state where it believes nothing has
# changed, applies a mutation, REFRESHes, then compares the matview against
# ground truth.  A mismatch means REFRESH reported success while leaving the
# matview stale -- a silent wrong answer, which is the whole risk of the
# feature.  T13 is the inverse check: the feature must still actually skip.
#
# Usage: mvskip_regress.sh [<install-prefix>] [<port>]
set -u
PREFIX="${1:-/home/cybrosys/dbblue-matview-skip/inst}"
PORT="${2:-55999}"
DB=mvskip_regress

PSQL="$PREFIX/bin/psql -p $PORT -U postgres -q -t -A -X"

pass=0; fail=0; declare -a FAILED=()

# Fresh database per run: watermarks are per matview OID and are never evicted,
# so reusing a database could let earlier state mask a regression.
$PSQL -d postgres -c "DROP DATABASE IF EXISTS $DB" >/dev/null 2>&1
$PSQL -d postgres -c "CREATE DATABASE $DB" >/dev/null 2>&1
Q="$PSQL -d $DB"

# check <name> <actual-sql> <expected-sql>
check() {
  local name="$1" got exp
  got=$($Q -c "$2" 2>&1 | tr -d '[:space:]')
  exp=$($Q -c "$3" 2>&1 | tr -d '[:space:]')
  if [ "$got" = "$exp" ]; then
    printf '  PASS  %-52s (%s)\n' "$name" "$got"; pass=$((pass+1))
  else
    printf '  FAIL  %-52s got=%s want=%s\n' "$name" "$got" "$exp"; fail=$((fail+1)); FAILED+=("$name")
  fi
}

echo "=============================================================================="
echo " matview auto_skip_unchanged regression suite"
echo "=============================================================================="

# ---------------------------------------------------------------- T1 happy path
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t1(id int, v int);
INSERT INTO t1 SELECT g,g FROM generate_series(1,1000) g;
CREATE MATERIALIZED VIEW mv1 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t1;
REFRESH MATERIALIZED VIEW mv1;
INSERT INTO t1 VALUES (1001,1001);
REFRESH MATERIALIZED VIEW mv1;
EOF
check "T1  plain table insert is tracked" \
      "SELECT c FROM mv1" "SELECT count(*) FROM t1"

# ------------------------------------------------------- T2 FROM-subquery source
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t2(id int, v int);
INSERT INTO t2 SELECT g,g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv2 WITH (auto_skip_unchanged=true) AS
  SELECT count(*) c FROM (SELECT * FROM t2 WHERE v > 0) x;
REFRESH MATERIALIZED VIEW mv2;
INSERT INTO t2 SELECT g,g FROM generate_series(11,20) g;
REFRESH MATERIALIZED VIEW mv2;
EOF
check "T2  source nested in a FROM-subquery" \
      "SELECT c FROM mv2" "SELECT count(*) FROM (SELECT * FROM t2 WHERE v>0) x"

# ---------------------------------------------------------------- T2b CTE source
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t2b(id int);
INSERT INTO t2b SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv2b WITH (auto_skip_unchanged=true) AS
  WITH cte AS (SELECT * FROM t2b) SELECT count(*) c FROM cte;
REFRESH MATERIALIZED VIEW mv2b;
INSERT INTO t2b SELECT g FROM generate_series(11,25) g;
REFRESH MATERIALIZED VIEW mv2b;
EOF
check "T2b source nested in a CTE" \
      "SELECT c FROM mv2b" "SELECT count(*) FROM t2b"

# ------------------------------------------------------- T2c source only in SubLink
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t2c(id int);
CREATE TABLE t2c_probe(id int);
INSERT INTO t2c SELECT g FROM generate_series(1,10) g;
INSERT INTO t2c_probe VALUES (1);
CREATE MATERIALIZED VIEW mv2c WITH (auto_skip_unchanged=true) AS
  SELECT (SELECT count(*) FROM t2c) AS c FROM t2c_probe;
REFRESH MATERIALIZED VIEW mv2c;
INSERT INTO t2c SELECT g FROM generate_series(11,30) g;
REFRESH MATERIALIZED VIEW mv2c;
EOF
check "T2c source reachable only via a SubLink" \
      "SELECT c FROM mv2c" "SELECT count(*) FROM t2c"

# -------------------------------------------------------------- T2d view source
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t2d(id int);
INSERT INTO t2d SELECT g FROM generate_series(1,10) g;
CREATE VIEW v2d AS SELECT * FROM t2d;
CREATE MATERIALIZED VIEW mv2d WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM v2d;
REFRESH MATERIALIZED VIEW mv2d;
INSERT INTO t2d SELECT g FROM generate_series(11,30) g;
REFRESH MATERIALIZED VIEW mv2d;
EOF
check "T2d source is a VIEW over a table" \
      "SELECT c FROM mv2d" "SELECT count(*) FROM v2d"

# ------------------------------------------------------ T2e view REDEFINED in place
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t2e(id int, v int);
INSERT INTO t2e SELECT g,g FROM generate_series(1,20) g;
CREATE VIEW v2e AS SELECT * FROM t2e WHERE v > 0;
CREATE MATERIALIZED VIEW mv2e WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM v2e;
REFRESH MATERIALIZED VIEW mv2e;
INSERT INTO t2e VALUES (21,21);
REFRESH MATERIALIZED VIEW mv2e;
CREATE OR REPLACE VIEW v2e AS SELECT * FROM t2e WHERE v > 10;
REFRESH MATERIALIZED VIEW mv2e;
EOF
check "T2e CREATE OR REPLACE VIEW changes the definition" \
      "SELECT c FROM mv2e" "SELECT count(*) FROM v2e"

# -------------------------------------------------------------------- T3 TRUNCATE
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t3(id int);
INSERT INTO t3 SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv3 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t3;
REFRESH MATERIALIZED VIEW mv3;
INSERT INTO t3 VALUES (99);
REFRESH MATERIALIZED VIEW mv3;
TRUNCATE t3;
REFRESH MATERIALIZED VIEW mv3;
EOF
check "T3  TRUNCATE of the source" \
      "SELECT c FROM mv3" "SELECT count(*) FROM t3"

# -------------------------------------------------- T4 partitioned source via parent
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t4(id int) PARTITION BY RANGE (id);
CREATE TABLE t4_a PARTITION OF t4 FOR VALUES FROM (0) TO (100);
CREATE TABLE t4_b PARTITION OF t4 FOR VALUES FROM (100) TO (1000);
INSERT INTO t4 SELECT g FROM generate_series(1,50) g;
CREATE MATERIALIZED VIEW mv4 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t4;
REFRESH MATERIALIZED VIEW mv4;
INSERT INTO t4 VALUES (60);
REFRESH MATERIALIZED VIEW mv4;
INSERT INTO t4 SELECT g FROM generate_series(200,209) g;
REFRESH MATERIALIZED VIEW mv4;
EOF
check "T4  INSERT through a partitioned parent" \
      "SELECT c FROM mv4" "SELECT count(*) FROM t4"

# ------------------------------------------------------- T4b DML direct on the leaf
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t4b(id int) PARTITION BY RANGE (id);
CREATE TABLE t4b_a PARTITION OF t4b FOR VALUES FROM (0) TO (1000);
INSERT INTO t4b SELECT g FROM generate_series(1,50) g;
CREATE MATERIALIZED VIEW mv4b WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t4b;
REFRESH MATERIALIZED VIEW mv4b;
INSERT INTO t4b VALUES (60);
REFRESH MATERIALIZED VIEW mv4b;
INSERT INTO t4b_a SELECT g FROM generate_series(200,209) g;
REFRESH MATERIALIZED VIEW mv4b;
EOF
check "T4b INSERT direct into a leaf partition" \
      "SELECT c FROM mv4b" "SELECT count(*) FROM t4b"

# ------------------------------------------------------------ T4c DETACH PARTITION
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t4c(id int) PARTITION BY RANGE (id);
CREATE TABLE t4c_a PARTITION OF t4c FOR VALUES FROM (0) TO (100);
CREATE TABLE t4c_b PARTITION OF t4c FOR VALUES FROM (100) TO (1000);
INSERT INTO t4c SELECT g FROM generate_series(1,50) g;
INSERT INTO t4c SELECT g FROM generate_series(100,149) g;
CREATE MATERIALIZED VIEW mv4c WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t4c;
REFRESH MATERIALIZED VIEW mv4c;
INSERT INTO t4c VALUES (60);
REFRESH MATERIALIZED VIEW mv4c;
ALTER TABLE t4c DETACH PARTITION t4c_b;
REFRESH MATERIALIZED VIEW mv4c;
EOF
check "T4c DETACH PARTITION removes rows" \
      "SELECT c FROM mv4c" "SELECT count(*) FROM t4c"

# ------------------------------------------------------------ T4d ATTACH PARTITION
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t4d(id int) PARTITION BY RANGE (id);
CREATE TABLE t4d_a PARTITION OF t4d FOR VALUES FROM (0) TO (100);
CREATE TABLE t4d_new(id int);
INSERT INTO t4d SELECT g FROM generate_series(1,50) g;
INSERT INTO t4d_new SELECT g FROM generate_series(100,120) g;
CREATE MATERIALIZED VIEW mv4d WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t4d;
REFRESH MATERIALIZED VIEW mv4d;
INSERT INTO t4d VALUES (60);
REFRESH MATERIALIZED VIEW mv4d;
ALTER TABLE t4d ATTACH PARTITION t4d_new FOR VALUES FROM (100) TO (1000);
REFRESH MATERIALIZED VIEW mv4d;
EOF
check "T4d ATTACH PARTITION adds rows" \
      "SELECT c FROM mv4d" "SELECT count(*) FROM t4d"

# --------------------------------------------------------- T4e DROP a leaf partition
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t4e(id int) PARTITION BY RANGE (id);
CREATE TABLE t4e_a PARTITION OF t4e FOR VALUES FROM (0) TO (100);
CREATE TABLE t4e_b PARTITION OF t4e FOR VALUES FROM (100) TO (1000);
INSERT INTO t4e SELECT g FROM generate_series(1,50) g;
INSERT INTO t4e SELECT g FROM generate_series(100,149) g;
CREATE MATERIALIZED VIEW mv4e WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t4e;
REFRESH MATERIALIZED VIEW mv4e;
INSERT INTO t4e VALUES (60);
REFRESH MATERIALIZED VIEW mv4e;
DROP TABLE t4e_b;
REFRESH MATERIALIZED VIEW mv4e;
EOF
check "T4e DROP of a leaf partition" \
      "SELECT c FROM mv4e" "SELECT count(*) FROM t4e"

# --------------------------------------------------------- T5 rolled-back REFRESH
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t5(id int);
INSERT INTO t5 SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv5 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t5;
REFRESH MATERIALIZED VIEW mv5;
INSERT INTO t5 SELECT g FROM generate_series(11,30) g;
BEGIN; REFRESH MATERIALIZED VIEW mv5; ROLLBACK;
REFRESH MATERIALIZED VIEW mv5;
EOF
check "T5  REFRESH rolled back must not mark clean" \
      "SELECT c FROM mv5" "SELECT count(*) FROM t5"

# ------------------------------------------------------ T5b REFRESH then ERROR in txn
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t5b(id int);
INSERT INTO t5b SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv5b WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t5b;
REFRESH MATERIALIZED VIEW mv5b;
INSERT INTO t5b SELECT g FROM generate_series(11,30) g;
BEGIN; REFRESH MATERIALIZED VIEW mv5b; SELECT 1/0; COMMIT;
REFRESH MATERIALIZED VIEW mv5b;
EOF
check "T5b REFRESH then error-abort must not mark clean" \
      "SELECT c FROM mv5b" "SELECT count(*) FROM t5b"

# --------------------------------------------------- T5c REFRESH in aborted subxact
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t5c(id int);
INSERT INTO t5c SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv5c WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t5c;
REFRESH MATERIALIZED VIEW mv5c;
INSERT INTO t5c SELECT g FROM generate_series(11,30) g;
BEGIN;
  SAVEPOINT sp;
  REFRESH MATERIALIZED VIEW mv5c;
  ROLLBACK TO sp;
COMMIT;
REFRESH MATERIALIZED VIEW mv5c;
EOF
check "T5c REFRESH rolled back to savepoint" \
      "SELECT c FROM mv5c" "SELECT count(*) FROM t5c"

# ------------------------------------------------------ T7 WITH NO DATA then REFRESH
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t7(id int);
INSERT INTO t7 SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv7 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t7;
REFRESH MATERIALIZED VIEW mv7;
INSERT INTO t7 VALUES (11);
REFRESH MATERIALIZED VIEW mv7;
REFRESH MATERIALIZED VIEW mv7 WITH NO DATA;
REFRESH MATERIALIZED VIEW mv7;
EOF
check "T7  plain REFRESH must repopulate after WITH NO DATA" \
      "SELECT relispopulated FROM pg_class WHERE relname='mv7'" "SELECT true"

# ---------------------------------------------------- T8 write + REFRESH, same txn
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t8(id int);
INSERT INTO t8 SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv8 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t8;
REFRESH MATERIALIZED VIEW mv8;
INSERT INTO t8 VALUES (11);
REFRESH MATERIALIZED VIEW mv8;
BEGIN;
  INSERT INTO t8 SELECT g FROM generate_series(12,20) g;
  REFRESH MATERIALIZED VIEW mv8;
COMMIT;
EOF
check "T8  write then REFRESH in the same transaction" \
      "SELECT c FROM mv8" "SELECT count(*) FROM t8"

# ---------------------------------------------------------- T9 matview over matview
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t9(id int);
INSERT INTO t9 SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv9_inner AS SELECT id FROM t9;
CREATE MATERIALIZED VIEW mv9_outer WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM mv9_inner;
REFRESH MATERIALIZED VIEW mv9_outer;
INSERT INTO t9 VALUES (11);
REFRESH MATERIALIZED VIEW mv9_outer;
REFRESH MATERIALIZED VIEW mv9_inner;
REFRESH MATERIALIZED VIEW mv9_outer;
EOF
check "T9  source is another matview that was refreshed" \
      "SELECT c FROM mv9_outer" "SELECT count(*) FROM mv9_inner"

# ------------------------------------------------------------------------ T10 COPY
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t10(id int);
INSERT INTO t10 SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv10 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t10;
REFRESH MATERIALIZED VIEW mv10;
INSERT INTO t10 VALUES (11);
REFRESH MATERIALIZED VIEW mv10;
COPY t10 FROM PROGRAM 'seq 100 109';
REFRESH MATERIALIZED VIEW mv10;
EOF
check "T10 COPY into the source" \
      "SELECT c FROM mv10" "SELECT count(*) FROM t10"

# ------------------------------------------------------- T11 ALTER TABLE type rewrite
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t11(id int, v text);
INSERT INTO t11 SELECT g, g::text FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv11 WITH (auto_skip_unchanged=true) AS SELECT sum(length(v)) c FROM t11;
REFRESH MATERIALIZED VIEW mv11;
INSERT INTO t11 VALUES (11,'11');
REFRESH MATERIALIZED VIEW mv11;
ALTER TABLE t11 ALTER COLUMN v TYPE text USING v || 'xyz';
REFRESH MATERIALIZED VIEW mv11;
EOF
check "T11 ALTER COLUMN TYPE rewrite changes content" \
      "SELECT c FROM mv11" "SELECT sum(length(v)) FROM t11"

# ------------------------------------------------------------------ T12 DELETE/UPDATE
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t12(id int, v int);
INSERT INTO t12 SELECT g,g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv12 WITH (auto_skip_unchanged=true) AS SELECT sum(v) c FROM t12;
REFRESH MATERIALIZED VIEW mv12;
INSERT INTO t12 VALUES (11,11);
REFRESH MATERIALIZED VIEW mv12;
UPDATE t12 SET v = v * 10 WHERE id <= 3;
DELETE FROM t12 WHERE id = 11;
REFRESH MATERIALIZED VIEW mv12;
EOF
check "T12 UPDATE and DELETE are tracked" \
      "SELECT c FROM mv12" "SELECT sum(v) FROM t12"

# -------------------------------------------------------- T13 skip really does skip
# The inverse check: an untouched source must NOT be rescanned, or the feature
# does nothing.  A 300k-row count(DISTINCT) takes far longer than 50ms to rebuild.
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t13(id int, v int);
INSERT INTO t13 SELECT g,g FROM generate_series(1,300000) g;
CREATE MATERIALIZED VIEW mv13 WITH (auto_skip_unchanged=true) AS
  SELECT count(DISTINCT v) c FROM t13;
REFRESH MATERIALIZED VIEW mv13;
INSERT INTO t13 VALUES (300001,300001);
REFRESH MATERIALIZED VIEW mv13;
EOF
T_START=$(date +%s%N)
$Q -c "REFRESH MATERIALIZED VIEW mv13;" >/dev/null 2>&1
T_END=$(date +%s%N)
MS=$(( (T_END - T_START) / 1000000 ))
if [ "$MS" -lt 50 ]; then
  printf '  PASS  %-52s (%s ms, skipped)\n' "T13 clean source is actually skipped" "$MS"; pass=$((pass+1))
else
  printf '  FAIL  %-52s (%s ms -- not skipping)\n' "T13 clean source is actually skipped" "$MS"; fail=$((fail+1)); FAILED+=("T13")
fi
check "T13b and the skipped result is still correct" \
      "SELECT c FROM mv13" "SELECT count(DISTINCT v) FROM t13"

# -------------------------------------------- T14 concurrent write during the refresh
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t14(id int);
INSERT INTO t14 SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv14 WITH (auto_skip_unchanged=true) AS
  SELECT count(*) c FROM t14
  CROSS JOIN (SELECT pg_sleep(coalesce(current_setting('my.nap',true),'0')::float)) s;
REFRESH MATERIALIZED VIEW mv14;
INSERT INTO t14 VALUES (100);
EOF
( $Q -c "SET my.nap='6'; REFRESH MATERIALIZED VIEW mv14;" >/dev/null 2>&1 ) &
RPID=$!
sleep 2
$Q -c "INSERT INTO t14 VALUES (777);" >/dev/null 2>&1
wait $RPID
$Q -c "REFRESH MATERIALIZED VIEW mv14;" >/dev/null 2>&1
check "T14 row committed mid-refresh is not lost" \
      "SELECT c FROM mv14" "SELECT count(*) FROM t14"

# ----------------------------------------------------- T15 VACUUM FULL / CLUSTER
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t15(id int);
INSERT INTO t15 SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv15 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t15;
REFRESH MATERIALIZED VIEW mv15;
INSERT INTO t15 VALUES (11);
REFRESH MATERIALIZED VIEW mv15;
VACUUM FULL t15;
INSERT INTO t15 VALUES (12);
REFRESH MATERIALIZED VIEW mv15;
EOF
check "T15 writes after VACUUM FULL still tracked" \
      "SELECT c FROM mv15" "SELECT count(*) FROM t15"

# ------------------------------------------- T16 time-dependent matview definition
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t16(id int, ts timestamptz);
INSERT INTO t16 SELECT g, now() - (g || ' seconds')::interval FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv16 WITH (auto_skip_unchanged=true) AS
  SELECT count(*) c FROM t16 WHERE ts > now() - interval '4 seconds';
REFRESH MATERIALIZED VIEW mv16;
INSERT INTO t16 VALUES (99, now());
REFRESH MATERIALIZED VIEW mv16;
EOF
sleep 5
$Q -c "REFRESH MATERIALIZED VIEW mv16;" >/dev/null 2>&1
check "T16 now()-dependent matview must not be skipped" \
      "SELECT c FROM mv16" "SELECT count(*) FROM t16 WHERE ts > now() - interval '4 seconds'"

# --------------------------------------------------- T17 UNLOGGED source is tracked
$Q >/dev/null 2>&1 <<'EOF'
CREATE UNLOGGED TABLE t17(id int);
INSERT INTO t17 SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv17 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t17;
REFRESH MATERIALIZED VIEW mv17;
INSERT INTO t17 VALUES (11);
REFRESH MATERIALIZED VIEW mv17;
INSERT INTO t17 SELECT g FROM generate_series(12,20) g;
REFRESH MATERIALIZED VIEW mv17;
EOF
check "T17 UNLOGGED source is tracked" \
      "SELECT c FROM mv17" "SELECT count(*) FROM t17"

# ------------------------------------------------- T18 two matviews, shared source
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t18(id int, v int);
INSERT INTO t18 SELECT g,g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv18_a WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t18;
CREATE MATERIALIZED VIEW mv18_b WITH (auto_skip_unchanged=true) AS SELECT sum(v) c FROM t18;
REFRESH MATERIALIZED VIEW mv18_a;
REFRESH MATERIALIZED VIEW mv18_b;
INSERT INTO t18 VALUES (11,11);
REFRESH MATERIALIZED VIEW mv18_a;
INSERT INTO t18 VALUES (12,12);
REFRESH MATERIALIZED VIEW mv18_b;
REFRESH MATERIALIZED VIEW mv18_a;
EOF
check "T18a shared source: first matview stays correct" \
      "SELECT c FROM mv18_a" "SELECT count(*) FROM t18"
check "T18b shared source: second matview stays correct" \
      "SELECT c FROM mv18_b" "SELECT sum(v) FROM t18"

# ---------------------------------- T18c refresh-all loop over N shared-source mvs
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t18c(id int);
INSERT INTO t18c SELECT g FROM generate_series(1,100) g;
CREATE MATERIALIZED VIEW r1 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t18c;
CREATE MATERIALIZED VIEW r2 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t18c;
CREATE MATERIALIZED VIEW r3 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t18c;
REFRESH MATERIALIZED VIEW r1; REFRESH MATERIALIZED VIEW r2; REFRESH MATERIALIZED VIEW r3;
INSERT INTO t18c SELECT g FROM generate_series(101,150) g;
REFRESH MATERIALIZED VIEW r1; REFRESH MATERIALIZED VIEW r2; REFRESH MATERIALIZED VIEW r3;
EOF
check "T18c refresh-all loop: none starved" \
      "SELECT (SELECT c FROM r1)||','||(SELECT c FROM r2)||','||(SELECT c FROM r3)" \
      "SELECT count(*)||','||count(*)||','||count(*) FROM t18c"

# ---------------------------------------------------------------- T19 sequence source
$Q >/dev/null 2>&1 <<'EOF'
CREATE SEQUENCE sq19;
CREATE MATERIALIZED VIEW mv19 WITH (auto_skip_unchanged=true) AS SELECT last_value c FROM sq19;
REFRESH MATERIALIZED VIEW mv19;
SELECT nextval('sq19'); SELECT nextval('sq19');
REFRESH MATERIALIZED VIEW mv19;
EOF
check "T19 sequence source must not be skipped" \
      "SELECT c FROM mv19" "SELECT last_value FROM sq19"

# ----------------------------------------------------------------- T20 catalog source
# Counts a deterministic subset: a bare count(*) over pg_class also counts the
# transient heap the rebuild itself creates, which stock PostgreSQL does too.
$Q >/dev/null 2>&1 <<'EOF'
CREATE MATERIALIZED VIEW mv20 WITH (auto_skip_unchanged=true) AS
  SELECT count(*) c FROM pg_class WHERE relname LIKE 'catalog\_bump\_%';
REFRESH MATERIALIZED VIEW mv20;
CREATE TABLE catalog_bump_a(id int);
CREATE TABLE catalog_bump_b(id int);
REFRESH MATERIALIZED VIEW mv20;
EOF
check "T20 system catalog source must not be skipped" \
      "SELECT c FROM mv20" \
      "SELECT count(*) FROM pg_class WHERE relname LIKE 'catalog\_bump\_%'"

# ------------------------------------------------- T21 virtual generated column source
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t21(id int, base int, vg int GENERATED ALWAYS AS (base * 2) VIRTUAL);
INSERT INTO t21(id, base) SELECT g, g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv21 WITH (auto_skip_unchanged=true) AS SELECT sum(vg) c FROM t21;
REFRESH MATERIALIZED VIEW mv21;
INSERT INTO t21(id, base) VALUES (11,11);
REFRESH MATERIALIZED VIEW mv21;
ALTER TABLE t21 ALTER COLUMN vg SET EXPRESSION AS (base * 100);
REFRESH MATERIALIZED VIEW mv21;
EOF
check "T21 virtual generated column redefined" \
      "SELECT c FROM mv21" "SELECT sum(vg) FROM t21"

# ---------------------------------------------------------------------- T22 TABLESAMPLE
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t22(id int);
INSERT INTO t22 SELECT g FROM generate_series(1,10000) g;
CREATE MATERIALIZED VIEW mv22 WITH (auto_skip_unchanged=true) AS
  SELECT count(*) c FROM t22 TABLESAMPLE BERNOULLI (10);
REFRESH MATERIALIZED VIEW mv22;
INSERT INTO t22 VALUES (99999);
REFRESH MATERIALIZED VIEW mv22;
EOF
# A skipped TABLESAMPLE matview would return the identical count every time.
$Q -c "CREATE TABLE t22_probe AS SELECT c FROM mv22;" >/dev/null 2>&1
$Q -c "REFRESH MATERIALIZED VIEW mv22;" >/dev/null 2>&1
RESAMPLED=$($Q -c "SELECT count(*) FROM (SELECT c FROM mv22 EXCEPT SELECT c FROM t22_probe) x" 2>&1 | tr -d '[:space:]')
if [ "$RESAMPLED" != "" ]; then
  printf '  PASS  %-52s (re-sampled, not skipped)\n' "T22 TABLESAMPLE matview is refreshed"; pass=$((pass+1))
else
  printf '  FAIL  %-52s\n' "T22 TABLESAMPLE matview is refreshed"; fail=$((fail+1)); FAILED+=("T22")
fi

# ------------------------------------------------------------------------ T23 RLS
# The matview must be owned by a NON-superuser, because a superuser bypasses RLS
# outright and the policy would never apply.  REFRESH runs the data-fill as the
# matview's owner, so with FORCE ROW LEVEL SECURITY the policy does apply.
$Q >/dev/null 2>&1 <<'EOF'
DROP ROLE IF EXISTS t23owner;
CREATE ROLE t23owner NOLOGIN;
GRANT CREATE, USAGE ON SCHEMA public TO t23owner;
SET ROLE t23owner;
CREATE TABLE t23(id int, tenant int);
INSERT INTO t23 SELECT g, g % 2 FROM generate_series(1,20) g;
ALTER TABLE t23 ENABLE ROW LEVEL SECURITY;
ALTER TABLE t23 FORCE ROW LEVEL SECURITY;
CREATE POLICY p23 ON t23 USING (tenant = 0);
CREATE MATERIALIZED VIEW mv23 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t23;
REFRESH MATERIALIZED VIEW mv23;
INSERT INTO t23 VALUES (21, 0);
REFRESH MATERIALIZED VIEW mv23;
-- Changing the policy alters the contents with no write to any source.
ALTER POLICY p23 ON t23 USING (tenant = 1);
REFRESH MATERIALIZED VIEW mv23;
RESET ROLE;
EOF
check "T23 RLS policy change is reflected" \
      "SELECT c FROM mv23" "SELECT count(*) FROM t23 WHERE tenant = 1"

# ------------------------------------------------------- T24 GUC turns it fully off
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t24(id int);
INSERT INTO t24 SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv24 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t24;
REFRESH MATERIALIZED VIEW mv24;
INSERT INTO t24 VALUES (11);
REFRESH MATERIALIZED VIEW mv24;
EOF
$Q -c "SET dbblue_matview_skip_unchanged = off; REFRESH MATERIALIZED VIEW mv24;" >/dev/null 2>&1
check "T24 GUC off still refreshes correctly" \
      "SELECT c FROM mv24" "SELECT count(*) FROM t24"

# ----------------------------------------------- T25 REPEATABLE READ must not skip
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t25(id int);
INSERT INTO t25 SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv25 WITH (auto_skip_unchanged=true) AS SELECT count(*) c FROM t25;
REFRESH MATERIALIZED VIEW mv25;
INSERT INTO t25 VALUES (11);
REFRESH MATERIALIZED VIEW mv25;
INSERT INTO t25 SELECT g FROM generate_series(12,20) g;
BEGIN ISOLATION LEVEL REPEATABLE READ;
  REFRESH MATERIALIZED VIEW mv25;
COMMIT;
EOF
check "T25 REPEATABLE READ refresh is correct" \
      "SELECT c FROM mv25" "SELECT count(*) FROM t25"

# ------------------------------------------------------------- T26 CONCURRENTLY path
$Q >/dev/null 2>&1 <<'EOF'
CREATE TABLE t26(id int);
INSERT INTO t26 SELECT g FROM generate_series(1,10) g;
CREATE MATERIALIZED VIEW mv26 WITH (auto_skip_unchanged=true) AS SELECT id FROM t26;
CREATE UNIQUE INDEX mv26_idx ON mv26(id);
REFRESH MATERIALIZED VIEW mv26;
INSERT INTO t26 VALUES (11);
REFRESH MATERIALIZED VIEW CONCURRENTLY mv26;
INSERT INTO t26 VALUES (12);
REFRESH MATERIALIZED VIEW CONCURRENTLY mv26;
EOF
check "T26 REFRESH CONCURRENTLY stays correct" \
      "SELECT count(*) FROM mv26" "SELECT count(*) FROM t26"

echo "=============================================================================="
printf ' %d passed, %d failed\n' "$pass" "$fail"
if [ "$fail" -gt 0 ]; then
  printf ' failing: %s\n' "${FAILED[*]}"
fi
echo "=============================================================================="
[ "$fail" -eq 0 ]
