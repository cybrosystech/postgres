WITH cand AS (
    SELECT a.*, format('%I.%I', a.schemaname, a.tablename)::regclass AS reloid
    FROM dbblue_stats_advisor a
), sized AS (
    SELECT c.*, cl.reltuples FROM cand c JOIN pg_class cl ON cl.oid = c.reloid
), cols AS (
    SELECT s.*, u.attname,
           CASE WHEN st.n_distinct < 0 THEN (-st.n_distinct) * s.reltuples
                ELSE st.n_distinct END AS nd
    FROM sized s
    CROSS JOIN LATERAL unnest(s.attnames) u(attname)
    LEFT JOIN pg_stats st ON st.schemaname = s.schemaname
                         AND st.tablename  = s.tablename
                         AND st.attname    = u.attname
)
SELECT tablename, attnames, plan_count,
       max(reltuples)::bigint          AS table_rows,
       min(nd)::bigint                 AS least_distinct_col
FROM cols
GROUP BY tablename, attnames, plan_count
HAVING max(reltuples) >= 100000          -- big enough to matter
   AND min(coalesce(nd, 0)) >= 2         -- no near-constant column
ORDER BY max(reltuples) DESC, plan_count DESC;
