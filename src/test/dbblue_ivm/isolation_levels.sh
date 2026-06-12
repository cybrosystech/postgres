#!/usr/bin/env bash
#
# DBblue IVM — isolation-level stress test.
#
# Validates incremental maintenance under REPEATABLE READ and SERIALIZABLE.
# The matview delta runs in the same transaction as the source DML, so a
# serialization failure must roll back BOTH (no partial state); the matview
# must equal a live recompute afterward.  Serialization/deadlock failures are
# EXPECTED at these isolation levels and are retried via pgbench --max-tries;
# the test asserts only on final consistency, not on zero retries.
#
# Covers SUM/COUNT and MIN/MAX (the latter exercises the two-phase advisory
# lock, whose per-statement-snapshot assumption changes under RR/SER).
#
# Usage:  src/test/dbblue_ivm/isolation_levels.sh [duration_secs] [rounds]
set -u
PSQL="/home/cybrosys/postgres/bin/psql"
PGBENCH="/home/cybrosys/postgres/bin/pgbench"
CONN="-U cybrosys -p 5432 postgres"
DURATION="${1:-20}"
ROUNDS="${2:-1}"
fail=0

cat > /tmp/iso_ins.pgbench <<'EOF'
\set r random(65,69)
INSERT INTO iso_src(region,amount) VALUES(chr(:r), (random()*1000)::int+1);
EOF
cat > /tmp/iso_del.pgbench <<'EOF'
\set r random(65,69)
DELETE FROM iso_src WHERE id=(SELECT id FROM iso_src WHERE region=chr(:r) ORDER BY random() LIMIT 1);
EOF

# Escape spaces so the value survives PGOPTIONS' whitespace splitting
# ("repeatable read" -> "repeatable\ read").
iso_pgopt() { printf '%s' "$1" | sed 's/ /\\ /g'; }

run_iso() {
    local iso="$1"
    local opt; opt="-c default_transaction_isolation=$(iso_pgopt "$iso")"
    echo "════ isolation = $iso ════"
    for round in $(seq 1 "$ROUNDS"); do
        $PSQL $CONN -q -c "
            DROP TABLE IF EXISTS iso_src CASCADE;
            CREATE TABLE iso_src(id serial, region text, amount int);
            CREATE MATERIALIZED VIEW iso_sum WITH (incremental_refresh=true) AS
              SELECT region, SUM(amount) total, COUNT(*) cnt FROM iso_src GROUP BY region WITH DATA;
            CREATE MATERIALIZED VIEW iso_mm WITH (incremental_refresh=true) AS
              SELECT region, MIN(amount) mn, MAX(amount) mx, COUNT(*) cnt FROM iso_src GROUP BY region WITH DATA;" 2>/dev/null
        $PSQL $CONN -q -c "INSERT INTO iso_src(region,amount)
            SELECT chr(64+r), 1+(random()*999)::int FROM generate_series(1,5) r, generate_series(1,100) s;"

        PGOPTIONS="$opt" \
          $PGBENCH -n -T "$DURATION" -c 4 -j 4 --max-tries=50 -f /tmp/iso_ins.pgbench $CONN >/tmp/iso_ins.log 2>&1 &
        P1=$!
        PGOPTIONS="$opt" \
          $PGBENCH -n -T "$DURATION" -c 4 -j 4 --max-tries=50 -f /tmp/iso_del.pgbench $CONN >/tmp/iso_del.log 2>&1 &
        P2=$!
        wait $P1 $P2

        # report processed/retry/failure counts; FAIL if no work happened
        # (guards against a misconfigured run silently "passing")
        local processed retried failed
        processed=$(grep -h "transactions actually processed" /tmp/iso_ins.log /tmp/iso_del.log | grep -oE '[0-9]+' | paste -sd+ | bc 2>/dev/null || echo 0)
        retried=$(grep -h "transactions retried" /tmp/iso_ins.log /tmp/iso_del.log | grep -oE '[0-9]+ ' | head -2 | paste -sd+ | bc 2>/dev/null || echo 0)
        failed=$(grep -h "number of failed transactions" /tmp/iso_ins.log /tmp/iso_del.log | grep -oE '[0-9]+ ' | head -2 | paste -sd+ | bc 2>/dev/null || echo 0)
        echo "  round $round: processed=${processed:-0} retried=${retried:-0} failed=${failed:-0} (retries/failures expected at $iso)"
        if ! grep -q "transactions actually processed" /tmp/iso_ins.log || [ "${processed:-0}" = "0" ]; then
            echo "  round $round: SETUP FAIL — no transactions ran (check PGOPTIONS / connection)"
            sed -n '1,3p' /tmp/iso_ins.log
            fail=1
        fi

        # consistency: matview == live recompute under a stable snapshot
        local disc
        disc=$($PSQL $CONN -tAc "
            BEGIN; LOCK TABLE iso_src IN SHARE MODE;
            WITH ts AS (SELECT region,SUM(amount) total,COUNT(*) cnt,MIN(amount) mn,MAX(amount) mx FROM iso_src GROUP BY region)
            SELECT
              (SELECT COUNT(*) FROM ts t FULL JOIN iso_sum i USING(region)
                 WHERE t.total IS DISTINCT FROM i.total OR t.cnt IS DISTINCT FROM i.cnt)
            + (SELECT COUNT(*) FROM ts t FULL JOIN iso_mm i USING(region)
                 WHERE t.mn IS DISTINCT FROM i.mn OR t.mx IS DISTINCT FROM i.mx OR t.cnt IS DISTINCT FROM i.cnt);
            COMMIT;" 2>/dev/null | grep -E '^[0-9]+$' | head -1)
        disc=${disc:-99}
        if [ "$disc" = "0" ]; then echo "  round $round: CONSISTENCY PASS (sum+minmax match live)"
        else echo "  round $round: CONSISTENCY FAIL ($disc discrepant groups)"; fail=1; fi
    done
}

run_iso "repeatable read"
run_iso "serializable"

$PSQL $CONN -q -c "DROP TABLE IF EXISTS iso_src CASCADE;" 2>/dev/null
echo ""
if [ "$fail" = "0" ]; then echo "=== ISOLATION TEST PASSED (RR + SERIALIZABLE consistent) ==="; exit 0
else echo "=== ISOLATION TEST FAILED ==="; exit 1; fi
