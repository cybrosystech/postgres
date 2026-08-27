#!/usr/bin/env bash
# DBblue IVM — Phase-4 single-writer delta benchmark.
#
# For each "action" (a DML statement standing in for a real Odoo action — post
# invoice, confirm SO, receive PO, a stock move, a bulk import), measures:
#   * incremental latency — time to run the DML with trigger maintenance (which
#     runs INSIDE the writer's transaction, so this is the user-facing commit
#     cost); measured as BEGIN; <dml>; ROLLBACK so the run is repeatable.
#   * REFRESH latency — time to REFRESH the equivalent plain matview.
#   * speedup — refresh / incremental.
# Reports the warm median of N iterations, then asserts the incremental matview
# == a fresh REFRESH (correctness alongside the timing).
#
# To benchmark REAL reports: set SETUP_SQL to create your incremental matview
# (MV_INCR) and an identically-defined plain twin (MV_PLAIN), and fill ACTIONS
# with your real DML.  With no config it self-tests on a synthetic matview.
#
# Usage: benchmark_delta.sh [iterations]      (env: PSQL, CONN, SETUP_SQL, MV_PLAIN, ACTIONS)
set -u
PSQL="${PSQL:-/home/cybrosys/postgres/bin/psql}"
CONN="${CONN:--p 5432 postgres}"
ITERS="${1:-7}"
q() { $PSQL $CONN -qtA -c "$1" 2>/dev/null; }

# ---- configuration (defaults = synthetic self-test) -------------------------
MV_PLAIN="${MV_PLAIN:-bd_plain}"           # plain twin to time REFRESH against
KEYCOLS="${KEYCOLS:-k,s,c}"                # columns to compare for correctness
: "${SETUP_SQL:=}"
if [ -z "$SETUP_SQL" ]; then
  SETUP_SQL="
    DROP TABLE IF EXISTS bd_dim, bd_fact CASCADE;
    CREATE TABLE bd_dim(id int primary key, code text);
    CREATE TABLE bd_fact(id bigserial primary key, did int, a numeric);
    INSERT INTO bd_dim SELECT g,'C'||g FROM generate_series(1,500) g;
    INSERT INTO bd_fact(did,a) SELECT 1+(random()*499)::int,(random()*10)::numeric(6,2) FROM generate_series(1,200000);
    CREATE INDEX ON bd_fact(did); ANALYZE;
    CREATE MATERIALIZED VIEW bd_incr WITH (incremental_refresh=true) AS
      SELECT d.code AS k, sum(f.a) s, count(*) c FROM bd_fact f LEFT JOIN bd_dim d ON f.did=d.id GROUP BY d.code;
    CREATE MATERIALIZED VIEW bd_plain AS
      SELECT d.code AS k, sum(f.a) s, count(*) c FROM bd_fact f LEFT JOIN bd_dim d ON f.did=d.id GROUP BY d.code;"
  # label:::dml   — synthetic stand-ins for Odoo actions
  ACTIONS=(
    "fact insert (1 row)      :::INSERT INTO bd_fact(did,a) VALUES (7,5.5)"
    "fact update (1 row)      :::UPDATE bd_fact SET a=a+1 WHERE id=100"
    "dimension rename (1 row) :::UPDATE bd_dim SET code=code||'x' WHERE id=7"
    "bulk import (5000 rows)  :::INSERT INTO bd_fact(did,a) SELECT 1+(random()*499)::int,1 FROM generate_series(1,5000)"
  )
fi
MV_INCR="${MV_INCR:-bd_incr}"

echo "== DBblue IVM delta benchmark (median of ${ITERS}) =="
$PSQL $CONN -q -c "$SETUP_SQL" >/dev/null 2>&1

median() { sort -n | awk '{v[NR]=$1} END{ if(NR%2){print v[(NR+1)/2]} else {printf "%.3f",(v[NR/2]+v[NR/2+1])/2} }'; }
timeit() {  # $1=sql  -> ms for that statement inside BEGIN/ROLLBACK
  $PSQL $CONN -qAt -c "\timing on" -c "BEGIN;" -c "$1;" -c "ROLLBACK;" 2>/dev/null \
    | awk -F'[ :]+' '/^Time/{print $2}' | sed -n '2p'
}

printf '\n%-28s | %-11s | %-11s | %-8s\n' "action" "incr (ms)" "REFRESH(ms)" "speedup"
printf -- '-----------------------------+-------------+-------------+---------\n'
for entry in "${ACTIONS[@]}"; do
  label="${entry%%:::*}"; dml="${entry#*:::}"
  # warm plan caches
  timeit "$dml" >/dev/null; timeit "REFRESH MATERIALIZED VIEW $MV_PLAIN" >/dev/null
  inc=(); ref=()
  for _ in $(seq 1 "$ITERS"); do inc+=("$(timeit "$dml")"); done
  for _ in $(seq 1 "$ITERS"); do ref+=("$(timeit "REFRESH MATERIALIZED VIEW $MV_PLAIN")"); done
  mi=$(printf '%s\n' "${inc[@]}" | median); mr=$(printf '%s\n' "${ref[@]}" | median)
  sp=$(awk -v a="$mr" -v b="$mi" 'BEGIN{ if(b>0) printf "%.1fx", a/b; else print "n/a" }')
  printf '%-28s | %-11s | %-11s | %-8s\n' "$label" "$mi" "$mr" "$sp"
done

# ---- correctness: apply each action for real, compare to a REFRESH ----------
echo ""
allok=1
for entry in "${ACTIONS[@]}"; do
  dml="${entry#*:::}"; label="${entry%%:::*}"
  $PSQL $CONN -q -c "$dml" >/dev/null 2>&1
  $PSQL $CONN -q -c "REFRESH MATERIALIZED VIEW $MV_PLAIN" >/dev/null 2>&1
  d=$(q "SELECT count(*) FROM ((SELECT $KEYCOLS FROM $MV_INCR EXCEPT SELECT $KEYCOLS FROM $MV_PLAIN) UNION ALL (SELECT $KEYCOLS FROM $MV_PLAIN EXCEPT SELECT $KEYCOLS FROM $MV_INCR)) z;")
  [ "${d:-1}" = "0" ] || { echo "== REFRESH after '$label': MISMATCH ($d rows)"; allok=0; }
done
[ "$allok" = "1" ] && echo "incremental == REFRESH after every action: PASS"

[ -z "${SETUP_SQL_KEEP:-}" ] && $PSQL $CONN -q -c "DROP TABLE IF EXISTS bd_dim, bd_fact CASCADE;" >/dev/null 2>&1
