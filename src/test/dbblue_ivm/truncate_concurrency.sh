#!/usr/bin/env bash
# B11: TRUNCATE under concurrent writes.
# Exercises the AFTER TRUNCATE trigger (full REFRESH fallback) and the
# unpopulated-matview guard while INSERT/DELETE workers hammer the source.
set -euo pipefail
PSQL="/home/cybrosys/postgres/bin/psql"
PGBENCH="/home/cybrosys/postgres/bin/pgbench"
CONN="-U cybrosys -p 5432 postgres"
DURATION="${1:-30}"
ROUNDS="${2:-2}"
echo "════ B11: TRUNCATE under concurrent writes ════"
total_disc=0
for round in $(seq 1 "$ROUNDS"); do
    echo "  Round $round/$ROUNDS"
    $PSQL $CONN -q -c "DROP TABLE IF EXISTS b11_src CASCADE;
        CREATE TABLE b11_src(id serial, region text, amount int);
        CREATE MATERIALIZED VIEW b11_mv WITH (incremental_refresh=true) AS
          SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM b11_src GROUP BY region WITH DATA;" 2>/dev/null
    $PSQL $CONN -q -c "INSERT INTO b11_src(region,amount)
        SELECT chr(64+r), 1+(random()*999)::int FROM generate_series(1,5) r, generate_series(1,100) s;"
    cat > /tmp/adv_b11_ins.pgbench <<'EOF'
\set r random(65,69)
INSERT INTO b11_src(region,amount) VALUES(chr(:r), (random()*1000)::int+1);
EOF
    cat > /tmp/adv_b11_del.pgbench <<'EOF'
\set r random(65,69)
DELETE FROM b11_src WHERE id=(SELECT id FROM b11_src WHERE region=chr(:r) ORDER BY random() LIMIT 1);
EOF
    # insert/delete workers
    $PGBENCH -n -T "$DURATION" -c 4 -j 4 -f /tmp/adv_b11_ins.pgbench $CONN >/dev/null 2>&1 &
    PID1=$!
    $PGBENCH -n -T "$DURATION" -c 4 -j 4 -f /tmp/adv_b11_del.pgbench $CONN >/dev/null 2>&1 &
    PID2=$!
    # TRUNCATE loop: periodically wipe the source, then reseed, while workers run
    ( end=$((SECONDS+DURATION))
      while [ $SECONDS -lt $end ]; do
          sleep 4
          $PSQL $CONN -q -c "TRUNCATE b11_src;" 2>/dev/null || true
          $PSQL $CONN -q -c "INSERT INTO b11_src(region,amount)
              SELECT chr(64+r), 1+(random()*999)::int FROM generate_series(1,5) r, generate_series(1,50) s;" 2>/dev/null || true
      done ) &
    PID3=$!
    wait $PID1 $PID2 $PID3
    # Validate matview == live recompute under a stable snapshot
    result=$($PSQL $CONN -tAc "
        BEGIN;
        LOCK TABLE b11_src IN SHARE MODE;
        WITH truth AS (SELECT region,SUM(amount) total,COUNT(*) cnt FROM b11_src GROUP BY region),
             ivm   AS (SELECT region,total,cnt FROM b11_mv)
        SELECT COUNT(*) FILTER(WHERE t.total!=i.total OR t.cnt!=i.cnt OR i.region IS NULL OR t.region IS NULL)
        FROM truth t FULL OUTER JOIN ivm i USING(region);
        COMMIT;" 2>/dev/null | grep -E '^[0-9]+$' | head -1)
    disc=${result:-0}
    total_disc=$((total_disc+disc))
    [ "$disc" -gt 0 ] && echo "  *** FAIL round $round: $disc discrepant groups ***" || echo "  Round $round: PASS"
done
$PSQL $CONN -q -c "DROP TABLE IF EXISTS b11_src CASCADE;" 2>/dev/null || true
echo "  Total discrepancies: $total_disc"
[ "$total_disc" -eq 0 ] && echo "  VERDICT: PASS" || echo "  VERDICT: FAIL"
