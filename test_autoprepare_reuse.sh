#!/bin/bash
# ---------------------------------------------------------------------------
# Differential reuse fuzz for the dbblue autoprepare plan cache.
#
# For each (BASE, VARIANT) pair that COLLIDE on queryId, this promotes BASE
# under autoprepare (so a plan gets cached), then runs VARIANT (which reuses
# that plan) and compares the result to the ground truth: the SAME VARIANT run
# with autoprepare OFF.  Any mismatch means autoprepare reused a plan it should
# not have -> a correctness bug.
#
# This is the test class the pg_regress suite lacks: it never does
# "promote a shape, then reuse a *variant* of it".  That gap let the
# LIMIT/OFFSET-collision bug ship undetected.  Keep this test.
#
# Usage:  PSQL=/path/to/psql DBOPTS="-p 5432 -d yourdb" ./test_autoprepare_reuse.sh
# ---------------------------------------------------------------------------
set -u
PSQL="${PSQL:-psql}"
DBOPTS="${DBOPTS:--d postgres}"

run_off(){ $PSQL $DBOPTS -q -t -A -P footer=off -c "SET autoprepare.enabled=off; $1" 2>&1; }
run_on(){  $PSQL $DBOPTS -q -t -A -P footer=off <<EOF 2>&1
SET autoprepare.enabled=on;
SET autoprepare.threshold=2;
\o /dev/null
$1
$1
$1
$1
\o
$2
EOF
}
fail=0; n=0
check(){ n=$((n+1)); local t r; t=$(run_off "$3"); r=$(run_on "$2" "$3")
  if [ "$t" == "$r" ]; then printf "  ok   %-26s\n" "$1"
  else fail=$((fail+1)); printf "  FAIL %-26s\n        OFF: %s\n        ON : %s\n" \
       "$1" "$(echo "$t"|tr '\n' ' '|cut -c1-70)" "$(echo "$r"|tr '\n' ' '|cut -c1-70)"; fi; }

echo "=== autoprepare reuse-with-variant differential fuzz ==="

# --- data types in the target list (parameterize + bind correctness per type) ---
TYPES=(
 "int|7|99" "bigint|7000000000|123" "smallint|3::smallint|8::smallint"
 "numeric|1.5|99.99" "numeric_big|123456789.987654321|0.001" "float8|2.5::float8|7.25::float8"
 "text|'hello'|'world'" "varchar|'aa'::varchar|'bbbb'::varchar" "char|'ab'::char(5)|'xyz'::char(5)"
 "name|'foo'::name|'barbaz'::name" "bool|true|false" "oid|1234::oid|5678::oid"
 "date|DATE '2020-01-01'|DATE '2021-06-15'" "time|TIME '10:00:00'|TIME '23:59:59'"
 "timestamp|TIMESTAMP '2020-01-01 10:00'|TIMESTAMP '2022-05-05 03:00'"
 "timestamptz|TIMESTAMPTZ '2020-01-01 10:00+00'|TIMESTAMPTZ '2022-05-05 03:00+05'"
 "interval|INTERVAL '1 day'|INTERVAL '3 mon 2 day'"
 "uuid|'00000000-0000-0000-0000-000000000001'::uuid|'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid"
 "jsonb|'{\"a\":1}'::jsonb|'{\"b\":2,\"c\":[3,4]}'::jsonb" "json|'[1,2]'::json|'{\"k\":\"v\"}'::json"
 "intarray|ARRAY[1,2,3]|ARRAY[9,8]" "textarray|ARRAY['x','y']|ARRAY['z']"
 "bytea|'\\xdeadbeef'::bytea|'\\x0102'::bytea" "inet|'10.0.0.1'::inet|'192.168.1.1'::inet"
 "cidr|'10.0.0.0/8'::cidr|'192.168.0.0/16'::cidr"
)
for spec in "${TYPES[@]}"; do IFS='|' read -r tn v1 v2 <<< "$spec"
  check "tgt:$tn" "SELECT $v1 AS c, g FROM generate_series(1,2) g ORDER BY g;" "SELECT $v2 AS c, g FROM generate_series(1,2) g ORDER BY g;"
done

# --- constant positions & structural constants ---
check "where:int"     "SELECT g FROM generate_series(1,20) g WHERE g=5 ORDER BY g;" "SELECT g FROM generate_series(1,20) g WHERE g=12 ORDER BY g;"
check "in:intlist"    "SELECT g FROM generate_series(1,50) g WHERE g IN (1,2,3) ORDER BY g;" "SELECT g FROM generate_series(1,50) g WHERE g IN (10,20,30,40) ORDER BY g;"
check "between"       "SELECT g FROM generate_series(1,50) g WHERE g BETWEEN 5 AND 10 ORDER BY g;" "SELECT g FROM generate_series(1,50) g WHERE g BETWEEN 30 AND 45 ORDER BY g;"
check "limit"         "SELECT g FROM generate_series(1,100) g ORDER BY g LIMIT 20;" "SELECT g FROM generate_series(1,100) g ORDER BY g LIMIT 3;"
check "offset"        "SELECT g FROM generate_series(1,100) g ORDER BY g OFFSET 5 LIMIT 5;" "SELECT g FROM generate_series(1,100) g ORDER BY g OFFSET 60 LIMIT 5;"
check "fetch-first"   "SELECT g FROM generate_series(1,100) g ORDER BY g FETCH FIRST 20 ROWS ONLY;" "SELECT g FROM generate_series(1,100) g ORDER BY g FETCH FIRST 3 ROWS ONLY;"
check "window-frame"  "SELECT g,sum(g) OVER (ORDER BY g ROWS 2 PRECEDING) s FROM generate_series(1,6) g ORDER BY g;" "SELECT g,sum(g) OVER (ORDER BY g ROWS 4 PRECEDING) s FROM generate_series(1,6) g ORDER BY g;"
check "having"        "SELECT g%3 k,count(*) FROM generate_series(1,30) g GROUP BY 1 HAVING count(*)>100 ORDER BY 1;" "SELECT g%3 k,count(*) FROM generate_series(1,30) g GROUP BY 1 HAVING count(*)>5 ORDER BY 1;"
check "positional-ob" "SELECT g,-g n FROM generate_series(1,10) g ORDER BY 1 LIMIT 100;" "SELECT g,-g n FROM generate_series(1,10) g ORDER BY 2 LIMIT 100;"
check "null-vs-int"   "SELECT g,7 AS c FROM generate_series(1,3) g ORDER BY g;" "SELECT g,NULL::int AS c FROM generate_series(1,3) g ORDER BY g;"
check "case-result"   "SELECT g,CASE WHEN g>1 THEN 100 ELSE 0 END c FROM generate_series(1,3) g ORDER BY g;" "SELECT g,CASE WHEN g>1 THEN 222 ELSE 9 END c FROM generate_series(1,3) g ORDER BY g;"
check "coalesce"      "SELECT coalesce(nullif(g,2),99) c FROM generate_series(1,3) g ORDER BY g;" "SELECT coalesce(nullif(g,2),55) c FROM generate_series(1,3) g ORDER BY g;"
check "substring"     "SELECT substring('abcdefgh' FROM 2 FOR 3) s;" "SELECT substring('abcdefgh' FROM 5 FOR 2) s;"
check "arr-subscript" "SELECT (ARRAY[10,20,30])[g] v FROM generate_series(1,3) g ORDER BY g;" "SELECT (ARRAY[40,50,60])[g] v FROM generate_series(1,3) g ORDER BY g;"
check "jsonb-key"     "SELECT ('{\"en\":\"A\",\"fr\":\"B\"}'::jsonb)->>'en' t;" "SELECT ('{\"en\":\"A\",\"fr\":\"B\"}'::jsonb)->>'fr' t;"
check "like"          "SELECT datname FROM pg_database WHERE datname LIKE 'p%' ORDER BY 1;" "SELECT datname FROM pg_database WHERE datname LIKE 't%' ORDER BY 1;"

# --- complex structures ---
check "alias"         "SELECT g AS aaa FROM generate_series(1,3) g ORDER BY g;" "SELECT g AS bbb FROM generate_series(1,3) g ORDER BY g;"
check "cte"           "WITH c AS (SELECT g FROM generate_series(1,10) g WHERE g>7) SELECT * FROM c ORDER BY g;" "WITH c AS (SELECT g FROM generate_series(1,10) g WHERE g>3) SELECT * FROM c ORDER BY g;"
check "recursive-cte" "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5) SELECT n FROM r ORDER BY n;" "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<9) SELECT n FROM r ORDER BY n;"
check "union-arm"     "SELECT 1 s,g FROM generate_series(1,2) g UNION ALL SELECT 2,g FROM generate_series(1,2) g ORDER BY 1,2;" "SELECT 7 s,g FROM generate_series(1,2) g UNION ALL SELECT 9,g FROM generate_series(1,2) g ORDER BY 1,2;"
check "join"          "SELECT a.g FROM generate_series(1,10) AS a(g) JOIN generate_series(1,10) AS b(g) USING(g) WHERE a.g>7 ORDER BY a.g;" "SELECT a.g FROM generate_series(1,10) AS a(g) JOIN generate_series(1,10) AS b(g) USING(g) WHERE a.g>2 ORDER BY a.g;"
check "lateral"       "SELECT g,x FROM generate_series(1,4) g CROSS JOIN LATERAL (SELECT g*10 AS x) s WHERE g>1 ORDER BY g;" "SELECT g,x FROM generate_series(1,4) g CROSS JOIN LATERAL (SELECT g*10 AS x) s WHERE g>3 ORDER BY g;"
check "correlated"    "SELECT g,(SELECT count(*) FROM generate_series(1,20) x WHERE x<g AND x>2) c FROM generate_series(1,5) g ORDER BY g;" "SELECT g,(SELECT count(*) FROM generate_series(1,20) x WHERE x<g AND x>8) c FROM generate_series(1,5) g ORDER BY g;"
check "agg-filter"    "SELECT count(*) FILTER (WHERE g>2) FROM generate_series(1,6) g;" "SELECT count(*) FILTER (WHERE g>4) FROM generate_series(1,6) g;"
check "grouping-sets" "SELECT g%2,g%3,count(*) FROM generate_series(1,12) g WHERE g>1 GROUP BY GROUPING SETS ((g%2),(g%3)) ORDER BY 1,2;" "SELECT g%2,g%3,count(*) FROM generate_series(1,12) g WHERE g>6 GROUP BY GROUPING SETS ((g%2),(g%3)) ORDER BY 1,2;"
check "distinct-on"   "SELECT DISTINCT ON (g%2) g%2 k,g FROM generate_series(1,10) g ORDER BY g%2,g;" "SELECT DISTINCT ON (g%3) g%3 k,g FROM generate_series(1,10) g ORDER BY g%3,g;"
check "scalar-subq"   "SELECT (SELECT g FROM generate_series(1,100) g ORDER BY g LIMIT 1 OFFSET 5) v;" "SELECT (SELECT g FROM generate_series(1,100) g ORDER BY g LIMIT 1 OFFSET 40) v;"

# --- reuse a plan PROMOTED BY A LONGER STATEMENT with a SHORTER one ---
# Regression for the stmt_location/stmt_len crash: the cached plan kept the
# promoting statement's source-text bounds; reusing it for a shorter statement
# made CleanQuerytext() read past end-of-string -> "query_len <= strlen(query)"
# assert (SIGABRT) on assert builds / OOB read otherwise.  Trigger needs the
# REUSING statement to be shorter than the PROMOTER, and it surfaced on
# view/SRF-expanded queries (pg_settings is a view over pg_show_all_settings()).
# BASE (promoter) is long; VARIANT (reuse) is short.
check "len-shrink-view"  "SELECT count(*) FROM pg_catalog.pg_settings WHERE pg_catalog.lower(name) LIKE pg_catalog.lower('autoprepare.thre%');" "SELECT count(*) FROM pg_catalog.pg_settings WHERE pg_catalog.lower(name) LIKE pg_catalog.lower('a%');"
check "len-shrink-in"    "SELECT g FROM generate_series(1,50) g WHERE g IN (11,12,13,14,15,16,17,18,19,20) ORDER BY g;" "SELECT g FROM generate_series(1,50) g WHERE g IN (1) ORDER BY g;"
check "len-shrink-like"  "SELECT datname FROM pg_database WHERE datname LIKE 'template_does_not_exist%' ORDER BY 1;" "SELECT datname FROM pg_database WHERE datname LIKE 't%' ORDER BY 1;"

echo "=== $fail / $n failed ==="
exit $(( fail > 0 ? 1 : 0 ))
