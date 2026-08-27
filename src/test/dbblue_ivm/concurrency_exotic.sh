#!/usr/bin/env bash
#
# DBblue IVM — concurrency stress for the "exotic" shapes that carry the
# "not yet certified under concurrent writes" warning: INNER JOIN aggregate,
# UNION ALL (multiset), self-join aggregate, and LEFT OUTER JOIN aggregate.
#
# For each shape: run concurrent pgbench writers (insert + delete [+ update])
# against the source table(s) with --max-tries retries, then assert the matview
# equals a full live recompute (== REFRESH) under a stable snapshot.
#
# Tested at READ COMMITTED (the realistic default — and the hard case for
# recompute/absolute-overwrite deltas, which can lose updates without a lock)
# and REPEATABLE READ.  Per-shape, per-isolation PASS/FAIL is reported so a
# concurrency limitation is pinpointed rather than hidden.
#
# Usage:  src/test/dbblue_ivm/concurrency_exotic.sh [duration_secs]
set -u
PSQL="/home/cybrosys/postgres/bin/psql"
PGBENCH="/home/cybrosys/postgres/bin/pgbench"
CONN="-U cybrosys -p 5432 postgres"
DURATION="${1:-8}"
fail=0
iso_pgopt() { printf '%s' "$1" | sed 's/ /\\ /g'; }

# ---- per-shape pgbench write scripts ----
cat > /tmp/cx_sj_ins.sql <<'EOF'
\set k random(1,40)
\set m random(1,40)
INSERT INTO cx_emp(mgr,sal) VALUES(:m, (random()*100)::int+1);
EOF
cat > /tmp/cx_sj_del.sql <<'EOF'
DELETE FROM cx_emp WHERE id=(SELECT id FROM cx_emp ORDER BY random() LIMIT 1);
EOF
cat > /tmp/cx_ua_ins.sql <<'EOF'
\set v random(1,8)
INSERT INTO cx_ua(val) VALUES(:v);
EOF
cat > /tmp/cx_ub_ins.sql <<'EOF'
\set v random(1,8)
INSERT INTO cx_ub(val) VALUES(:v);
EOF
cat > /tmp/cx_ua_del.sql <<'EOF'
DELETE FROM cx_ua WHERE ctid=(SELECT ctid FROM cx_ua ORDER BY random() LIMIT 1);
EOF
cat > /tmp/cx_lj_ins.sql <<'EOF'
\set p random(1,20)
INSERT INTO cx_child(pid,v) VALUES(:p, (random()*100)::int+1);
EOF
cat > /tmp/cx_lj_del.sql <<'EOF'
DELETE FROM cx_child WHERE id=(SELECT id FROM cx_child ORDER BY random() LIMIT 1);
EOF
cat > /tmp/cx_ij_ins.sql <<'EOF'
\set o random(1,20)
INSERT INTO cx_line(oid,amt) VALUES(:o, (random()*100)::int+1);
EOF
cat > /tmp/cx_ij_del.sql <<'EOF'
DELETE FROM cx_line WHERE id=(SELECT id FROM cx_line ORDER BY random() LIMIT 1);
EOF

# run_shape <name> <iso> <gate:1|0> <setup-sql> <ins-script> <del-script> <consistency-sql>
# gate=1: a divergence fails the test (a level/shape that MUST be consistent).
#         Every shape is gated at every level now that the recompute/multiset
#         shapes serialize maintenance on a matview-level advisory lock.
# gate=0: informational only — report a divergence without failing (kept for
#         characterizing any future shape whose consistency is not yet a
#         guarantee at a given isolation level).
run_shape() {
    local name="$1" iso="$2" gate="$3" setup="$4" insf="$5" delf="$6" check="$7"
    local opt; opt="-c default_transaction_isolation=$(iso_pgopt "$iso")"
    $PSQL $CONN -q -c "$setup" 2>/dev/null
    PGOPTIONS="$opt" $PGBENCH -n -T "$DURATION" -c 3 -j 3 --max-tries=50 -f "$insf" $CONN >/tmp/cx_a.log 2>&1 &
    local P1=$!
    PGOPTIONS="$opt" $PGBENCH -n -T "$DURATION" -c 2 -j 2 --max-tries=50 -f "$delf" $CONN >/tmp/cx_b.log 2>&1 &
    local P2=$!
    wait $P1 $P2
    local proc; proc=$(grep -h "transactions actually processed" /tmp/cx_a.log /tmp/cx_b.log | grep -oE '[0-9]+' | paste -sd+ | bc 2>/dev/null || echo 0)
    local disc; disc=$($PSQL $CONN -tAc "$check" 2>/dev/null | grep -E '^[0-9]+$' | head -1); disc=${disc:-99}
    if [ "${proc:-0}" = "0" ]; then echo "  [$name @ $iso] SETUP FAIL (no txns)"; fail=1
    elif [ "$disc" = "0" ]; then echo "  [$name @ $iso] PASS (processed=$proc, matview == live)"
    elif [ "$gate" = "1" ]; then echo "  [$name @ $iso] FAIL ($disc discrepant; processed=$proc)"; fail=1
    else echo "  [$name @ $iso] info: diverges at $iso ($disc groups) — needs REPEATABLE READ+ (known)"; fi
}

SJ_SETUP="DROP TABLE IF EXISTS cx_emp CASCADE;
  CREATE TABLE cx_emp(id serial PRIMARY KEY, mgr int, sal int);
  INSERT INTO cx_emp(mgr,sal) SELECT (g%40)+1, (g%100)+1 FROM generate_series(1,200) g;
  CREATE MATERIALIZED VIEW cx_sj WITH (incremental_refresh=true) AS
    SELECT m.id mgrid, COUNT(*) c, SUM(e.sal) s FROM cx_emp e JOIN cx_emp m ON e.mgr=m.id GROUP BY m.id;"
SJ_CHECK="BEGIN; LOCK TABLE cx_emp IN SHARE MODE;
  WITH live AS (SELECT m.id mgrid, COUNT(*) c, SUM(e.sal) s FROM cx_emp e JOIN cx_emp m ON e.mgr=m.id GROUP BY m.id)
  SELECT COUNT(*) FROM live FULL JOIN cx_sj i USING(mgrid)
    WHERE live.c IS DISTINCT FROM i.c OR live.s IS DISTINCT FROM i.s; COMMIT;"

UA_SETUP="DROP TABLE IF EXISTS cx_ua CASCADE; DROP TABLE IF EXISTS cx_ub CASCADE;
  CREATE TABLE cx_ua(val int); CREATE TABLE cx_ub(val int);
  INSERT INTO cx_ua SELECT g%8 FROM generate_series(1,100) g;
  INSERT INTO cx_ub SELECT g%8 FROM generate_series(1,100) g;
  CREATE MATERIALIZED VIEW cx_un WITH (incremental_refresh=true) AS
    SELECT val FROM cx_ua UNION ALL SELECT val FROM cx_ub;"
UA_CHECK="BEGIN; LOCK TABLE cx_ua IN SHARE MODE; LOCK TABLE cx_ub IN SHARE MODE;
  SELECT (SELECT count(*) FROM ((SELECT val FROM cx_un) EXCEPT ALL (SELECT val FROM cx_ua UNION ALL SELECT val FROM cx_ub)) a)
       + (SELECT count(*) FROM ((SELECT val FROM cx_ua UNION ALL SELECT val FROM cx_ub) EXCEPT ALL (SELECT val FROM cx_un)) b); COMMIT;"

LJ_SETUP="DROP TABLE IF EXISTS cx_child CASCADE; DROP TABLE IF EXISTS cx_par CASCADE;
  CREATE TABLE cx_par(id int PRIMARY KEY, nm text);
  CREATE TABLE cx_child(id serial PRIMARY KEY, pid int, v int);
  INSERT INTO cx_par SELECT g, 'p'||g FROM generate_series(1,20) g;
  INSERT INTO cx_child(pid,v) SELECT (g%20)+1, (g%100)+1 FROM generate_series(1,100) g;
  CREATE MATERIALIZED VIEW cx_lj WITH (incremental_refresh=true) AS
    SELECT p.id pid, COUNT(c.v) c, SUM(c.v) s FROM cx_par p LEFT JOIN cx_child c ON c.pid=p.id GROUP BY p.id;"
LJ_CHECK="BEGIN; LOCK TABLE cx_child IN SHARE MODE;
  WITH live AS (SELECT p.id pid, COUNT(c.v) c, SUM(c.v) s FROM cx_par p LEFT JOIN cx_child c ON c.pid=p.id GROUP BY p.id)
  SELECT COUNT(*) FROM live FULL JOIN cx_lj i USING(pid)
    WHERE live.c IS DISTINCT FROM i.c OR live.s IS DISTINCT FROM i.s; COMMIT;"

IJ_SETUP="DROP TABLE IF EXISTS cx_line CASCADE; DROP TABLE IF EXISTS cx_ord CASCADE;
  CREATE TABLE cx_ord(id int PRIMARY KEY, cat int);
  CREATE TABLE cx_line(id serial PRIMARY KEY, oid int, amt int);
  INSERT INTO cx_ord SELECT g, g%5 FROM generate_series(1,20) g;
  INSERT INTO cx_line(oid,amt) SELECT (g%20)+1, (g%100)+1 FROM generate_series(1,100) g;
  CREATE MATERIALIZED VIEW cx_ij WITH (incremental_refresh=true) AS
    SELECT o.cat, COUNT(*) c, SUM(l.amt) s FROM cx_line l JOIN cx_ord o ON o.id=l.oid GROUP BY o.cat;"
IJ_CHECK="BEGIN; LOCK TABLE cx_line IN SHARE MODE;
  WITH live AS (SELECT o.cat, COUNT(*) c, SUM(l.amt) s FROM cx_line l JOIN cx_ord o ON o.id=l.oid GROUP BY o.cat)
  SELECT COUNT(*) FROM live FULL JOIN cx_ij i USING(cat)
    WHERE live.c IS DISTINCT FROM i.c OR live.s IS DISTINCT FROM i.s; COMMIT;"

# INNER JOIN aggregates are additive (ON CONFLICT serializes on the matview row).
# UNION ALL / self-join / LEFT JOIN are recompute/multiset shapes: they take a
# matview-level advisory lock that serializes their maintenance, so they are now
# consistent with a full REFRESH at every isolation level — READ COMMITTED
# included.  Every shape is therefore gated (gate=1) at every level.
echo "════ isolation = read committed ════"
run_shape "INNER JOIN" "read committed" 1 "$IJ_SETUP" /tmp/cx_ij_ins.sql /tmp/cx_ij_del.sql "$IJ_CHECK"
run_shape "UNION ALL"  "read committed" 1 "$UA_SETUP" /tmp/cx_ua_ins.sql /tmp/cx_ua_del.sql "$UA_CHECK"
run_shape "self-join"  "read committed" 1 "$SJ_SETUP" /tmp/cx_sj_ins.sql /tmp/cx_sj_del.sql "$SJ_CHECK"
run_shape "LEFT JOIN"  "read committed" 1 "$LJ_SETUP" /tmp/cx_lj_ins.sql /tmp/cx_lj_del.sql "$LJ_CHECK"
echo "════ isolation = repeatable read ════"
run_shape "INNER JOIN" "repeatable read" 1 "$IJ_SETUP" /tmp/cx_ij_ins.sql /tmp/cx_ij_del.sql "$IJ_CHECK"
run_shape "UNION ALL"  "repeatable read" 1 "$UA_SETUP" /tmp/cx_ua_ins.sql /tmp/cx_ua_del.sql "$UA_CHECK"
run_shape "self-join"  "repeatable read" 1 "$SJ_SETUP" /tmp/cx_sj_ins.sql /tmp/cx_sj_del.sql "$SJ_CHECK"
run_shape "LEFT JOIN"  "repeatable read" 1 "$LJ_SETUP" /tmp/cx_lj_ins.sql /tmp/cx_lj_del.sql "$LJ_CHECK"

$PSQL $CONN -q -c "DROP TABLE IF EXISTS cx_emp,cx_ua,cx_ub,cx_child,cx_par,cx_line,cx_ord CASCADE;" 2>/dev/null
echo ""
if [ "$fail" = "0" ]; then echo "=== EXOTIC CONCURRENCY: ALL SHAPES CONSISTENT ==="; exit 0
else echo "=== EXOTIC CONCURRENCY: SOME SHAPES DIVERGED (see above) ==="; exit 1; fi
