#!/usr/bin/env bash
# DBblue IVM — concurrency THROUGHPUT / scaling characterization (production
# gate #2).  concurrency_exotic.sh already proves CORRECTNESS under concurrent
# writers (== REFRESH); this measures the SCALING PROFILE: how per-writer commit
# latency and aggregate throughput move as the number of concurrent writers to a
# matview's sources rises (1, 10, 25, 50, 100 by default).
#
# It contrasts the two maintenance classes, because they behave oppositely:
#   * ADDITIVE  (single-table SUM/COUNT/AVG) — per-group write concurrency; TPS
#     should scale with clients (as long as they touch different group keys).
#   * RECOMPUTE (outer join) — a matview-level advisory lock serializes
#     maintenance; TPS should PLATEAU, per-writer latency should climb.
# The gap between the two curves is the serialization-lock cost you are sizing.
#
# Maintenance runs inside the writer's transaction (AFTER-STATEMENT triggers), so
# "latency" here is the user-facing commit latency of the DML (posting a row),
# not a background cost.
#
# Each level ends with a "== REFRESH" assertion so a scaling run can never hide a
# concurrency correctness bug.
#
# Usage: concurrency_scaling.sh [duration_secs_per_level] [client_levels...]
#   e.g. concurrency_scaling.sh 10 1 10 25 50 100
# Point PSQL/PGBENCH/CONN at a real instance to characterize a production copy.
set -u
PSQL="${PSQL:-/home/cybrosys/postgres/bin/psql}"
PGBENCH="${PGBENCH:-/home/cybrosys/postgres/bin/pgbench}"
CONN="${CONN:--p 5432 postgres}"
DUR="${1:-10}"; shift || true
LEVELS=("$@"); [ "${#LEVELS[@]}" -eq 0 ] && LEVELS=(1 10 25 50)
TMP="${TMPDIR:-/tmp}/dbblue_cs.$$"; mkdir -p "$TMP"
q() { $PSQL $CONN -qtA -c "$1"; }

echo "== DBblue IVM concurrency scaling =="
echo "duration/level=${DUR}s  levels=${LEVELS[*]}"

# ---- schema: shared dimension + two fact tables, one matview each ----
$PSQL $CONN -q >/dev/null 2>&1 <<'SQL'
DROP TABLE IF EXISTS cs_add, cs_fact, cs_dim CASCADE;
CREATE TABLE cs_dim(id int primary key, code text);
CREATE TABLE cs_add(id bigserial primary key, k int, a numeric);            -- additive mv source
CREATE TABLE cs_fact(id bigserial primary key, did int, a numeric);         -- recompute mv source
INSERT INTO cs_dim SELECT g,'C'||g FROM generate_series(1,200) g;
INSERT INTO cs_add(k,a)   SELECT 1+(random()*199)::int,(random()*10)::numeric(6,2) FROM generate_series(1,20000);
INSERT INTO cs_fact(did,a) SELECT 1+(random()*199)::int,(random()*10)::numeric(6,2) FROM generate_series(1,20000);
CREATE INDEX ON cs_fact(did);
ANALYZE;
-- additive: single-table GROUP BY (per-group concurrency)
CREATE MATERIALIZED VIEW cs_add_mv WITH (incremental_refresh=true) AS
  SELECT k, sum(a) s, count(*) c FROM cs_add GROUP BY k;
-- recompute: outer join (matview-level serialization lock)
CREATE MATERIALIZED VIEW cs_rec_mv WITH (incremental_refresh=true) AS
  SELECT d.code, sum(f.a) s, count(*) c FROM cs_fact f LEFT JOIN cs_dim d ON f.did=d.id GROUP BY d.code;
-- plain twins for the correctness assertion
CREATE MATERIALIZED VIEW cs_add_ref AS SELECT k, sum(a) s, count(*) c FROM cs_add GROUP BY k;
CREATE MATERIALIZED VIEW cs_rec_ref AS SELECT d.code, sum(f.a) s, count(*) c FROM cs_fact f LEFT JOIN cs_dim d ON f.did=d.id GROUP BY d.code;
SQL

# ---- writer scripts: a mix of insert/update/delete on each source ----
cat > "$TMP/add.sql" <<'EOF'
\set k random(1,200)
\set r random(1,3)
\if :r = 1
  INSERT INTO cs_add(k,a) VALUES(:k,(random()*10)::numeric(6,2));
\elif :r = 2
  UPDATE cs_add SET a=(random()*10)::numeric(6,2) WHERE id=(SELECT id FROM cs_add WHERE k=:k LIMIT 1);
\else
  DELETE FROM cs_add WHERE id=(SELECT id FROM cs_add WHERE k=:k LIMIT 1);
\endif
EOF
cat > "$TMP/rec.sql" <<'EOF'
\set d random(1,200)
\set r random(1,3)
\if :r = 1
  INSERT INTO cs_fact(did,a) VALUES(:d,(random()*10)::numeric(6,2));
\elif :r = 2
  UPDATE cs_fact SET a=(random()*10)::numeric(6,2) WHERE id=(SELECT id FROM cs_fact WHERE did=:d LIMIT 1);
\else
  DELETE FROM cs_fact WHERE id=(SELECT id FROM cs_fact WHERE did=:d LIMIT 1);
\endif
EOF

run_level() {   # $1=label $2=script $3=clients  -> "tps mean_ms"
  local out
  out=$($PGBENCH $CONN -n -f "$2" -c "$3" -j "$(( $3<8 ? $3 : 8 ))" -T "$DUR" --max-tries=20 2>&1)
  local tps mean
  tps=$(printf '%s' "$out"  | awk '/^tps =/{print $3; exit}')
  mean=$(printf '%s' "$out" | awk '/^latency average/{print $4; exit}')
  printf '%s %s' "${tps:-0}" "${mean:-0}"
}

printf '\n%-10s | %-28s | %-28s\n' "clients" "ADDITIVE (per-group)" "RECOMPUTE (serialized)"
printf '%-10s | %-13s %-14s | %-13s %-14s\n' "" "tps" "lat(ms)" "tps" "lat(ms)"
base_add=""; base_rec=""
for c in "${LEVELS[@]}"; do
  read a_tps a_lat < <(run_level add "$TMP/add.sql" "$c")
  read r_tps r_lat < <(run_level rec "$TMP/rec.sql" "$c")
  [ -z "$base_add" ] && base_add="$a_tps" && base_rec="$r_tps"
  printf '%-10s | %-13s %-14s | %-13s %-14s\n' "$c" "$a_tps" "$a_lat" "$r_tps" "$r_lat"
done

echo ""
echo "Read: ADDITIVE tps should rise with clients; RECOMPUTE tps should flatten"
echo "(matview-level lock).  Rising RECOMPUTE latency = queueing on that lock."

# ---- correctness under the concurrent load ----
$PSQL $CONN -q >/dev/null 2>&1 -c "REFRESH MATERIALIZED VIEW cs_add_ref;" -c "REFRESH MATERIALIZED VIEW cs_rec_ref;"
ad=$(q "SELECT count(*) FROM ((SELECT k,s,c FROM cs_add_mv EXCEPT SELECT k,s,c FROM cs_add_ref) UNION ALL (SELECT k,s,c FROM cs_add_ref EXCEPT SELECT k,s,c FROM cs_add_mv)) z;")
rd=$(q "SELECT count(*) FROM ((SELECT code,s,c FROM cs_rec_mv EXCEPT SELECT code,s,c FROM cs_rec_ref) UNION ALL (SELECT code,s,c FROM cs_rec_ref EXCEPT SELECT code,s,c FROM cs_rec_mv)) z;")
echo ""
[ "${ad:-1}" = "0" ] && echo "ADDITIVE  == REFRESH after concurrent load: PASS" || echo "ADDITIVE  MISMATCH ($ad rows): FAIL"
[ "${rd:-1}" = "0" ] && echo "RECOMPUTE == REFRESH after concurrent load: PASS" || echo "RECOMPUTE MISMATCH ($rd rows): FAIL"

$PSQL $CONN -q -c "DROP TABLE cs_add, cs_fact, cs_dim CASCADE;" >/dev/null 2>&1
rm -rf "$TMP"
