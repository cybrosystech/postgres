--
-- dbblue repartition aggregate: parallel final merge of hash aggregates
--
-- With dbblue_enable_repartition_agg, the partial aggregate rows are hash-
-- exchanged among the workers by group key, so the final aggregate runs in
-- parallel (Parallel Repartition node) instead of in the leader.
--

-- The table must not be temporary: parallel workers cannot see temp tables.
CREATE TABLE repart_t (a int, b int, c int, d text);
INSERT INTO repart_t
  SELECT i, i % 190000, i % 7, 'x' || (i % 100) FROM generate_series(1, 200000) i;
-- fix the group count so that the plans below do not depend on ANALYZE sampling
ALTER TABLE repart_t ALTER COLUMN b SET (n_distinct = 190000);
ANALYZE repart_t;

-- make the planner willing to use parallel plans on a small table
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
SET parallel_leader_participation = on;

-- helper: does the plan of a query contain a Repartition node?
CREATE FUNCTION repart_used(q text) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE ln text;
BEGIN
  FOR ln IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
    IF ln LIKE '%Repartition%' THEN RETURN true; END IF;
  END LOOP;
  RETURN false;
END $$;

-- off: the plain parallel plan
SET dbblue_enable_repartition_agg = off;
SELECT repart_used('SELECT b, count(*), sum(a) FROM repart_t GROUP BY b');

-- plan shape when enabled
SET dbblue_enable_repartition_agg = on;
EXPLAIN (COSTS OFF)
SELECT b, count(*), sum(a) FROM repart_t GROUP BY b;

-- results are the same with and without the node
CREATE TABLE repart_on AS
  SELECT b, count(*) AS n, sum(a) AS s, min(d) AS mn, max(c) AS mx, avg(a) AS av
  FROM repart_t GROUP BY b;
SET dbblue_enable_repartition_agg = off;
CREATE TABLE repart_off AS
  SELECT b, count(*) AS n, sum(a) AS s, min(d) AS mn, max(c) AS mx, avg(a) AS av
  FROM repart_t GROUP BY b;
SELECT count(*) FROM repart_on;
SELECT count(*) FROM (SELECT * FROM repart_on EXCEPT SELECT * FROM repart_off) x;
SELECT count(*) FROM (SELECT * FROM repart_off EXCEPT SELECT * FROM repart_on) x;
DROP TABLE repart_on, repart_off;

-- two grouping keys, HAVING and ORDER BY on top
SET dbblue_enable_repartition_agg = on;
SELECT repart_used('SELECT b, d, count(*) FROM repart_t GROUP BY b, d HAVING count(*) > 1 ORDER BY 1, 2');
SELECT b, d, count(*) FROM repart_t GROUP BY b, d HAVING count(*) > 1 ORDER BY 1, 2 LIMIT 5;
SET dbblue_enable_repartition_agg = off;
SELECT b, d, count(*) FROM repart_t GROUP BY b, d HAVING count(*) > 1 ORDER BY 1, 2 LIMIT 5;

-- queries that cannot aggregate partially never get the node
SET dbblue_enable_repartition_agg = on;
SELECT repart_used('SELECT b, count(DISTINCT a) FROM repart_t GROUP BY b');
SELECT repart_used('SELECT b, array_agg(a ORDER BY a) FROM repart_t GROUP BY b');
SELECT repart_used('SELECT b, c, count(*) FROM repart_t GROUP BY GROUPING SETS ((b), (c))');
SELECT repart_used('SELECT count(*) FROM repart_t');

-- no workers, no node
SET max_parallel_workers_per_gather = 0;
SELECT repart_used('SELECT b, count(*) FROM repart_t GROUP BY b');
SET max_parallel_workers_per_gather = 2;

-- the leader may stay out of the aggregation
SET parallel_leader_participation = off;
SELECT repart_used('SELECT count(*), sum(n), sum(s) FROM (SELECT b, count(*) AS n, sum(a) AS s FROM repart_t GROUP BY b) x');
SELECT count(*), sum(n), sum(s) FROM
  (SELECT b, count(*) AS n, sum(a) AS s FROM repart_t GROUP BY b) x;
SET parallel_leader_participation = on;

-- tiny work_mem: the final hash aggregates spill, results stay the same.  The
-- plan is made with the normal work_mem (a tiny one would steer the planner
-- away from hashing) and then run with a tiny one.
SET enable_sort = off;
SET plan_cache_mode = force_generic_plan;
PREPARE repart_q AS
  SELECT count(*), sum(n), sum(s) FROM
    (SELECT b, count(*) AS n, sum(a) AS s FROM repart_t GROUP BY b) x;
SELECT repart_used('EXECUTE repart_q');
EXECUTE repart_q;
SET work_mem = '64kB';
SET hash_mem_multiplier = 1;
EXECUTE repart_q;
RESET work_mem;
RESET hash_mem_multiplier;
DEALLOCATE repart_q;
RESET plan_cache_mode;

-- on the inner side of a nested loop, so the node is rescanned
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SELECT repart_used('SELECT g, count(*) FROM generate_series(1, 3) g JOIN (SELECT b, count(*) AS n FROM repart_t GROUP BY b) a ON a.n >= g GROUP BY g ORDER BY g');
SELECT g, count(*) FROM generate_series(1, 3) g
  JOIN (SELECT b, count(*) AS n FROM repart_t GROUP BY b) a ON a.n >= g
GROUP BY g ORDER BY g;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_sort;

-- as a subquery of a larger query
SELECT repart_used('SELECT count(*) FROM repart_t t JOIN (SELECT b, count(*) AS n FROM repart_t GROUP BY b) x ON x.b = t.b WHERE x.n = 2');
SELECT count(*) FROM repart_t t
  JOIN (SELECT b, count(*) AS n FROM repart_t GROUP BY b) x ON x.b = t.b
 WHERE x.n = 2;

-- enable_partitionwise_aggregate must not keep the node out of plans on
-- tables that are not partitioned
SET enable_partitionwise_aggregate = on;
SELECT repart_used('SELECT b, count(*), sum(a) FROM repart_t GROUP BY b');
RESET enable_partitionwise_aggregate;

-- a partitioned table with partitionwise aggregation keeps its own plan
CREATE TABLE repart_p (a int, b int) PARTITION BY HASH (b);
CREATE TABLE repart_p0 PARTITION OF repart_p FOR VALUES WITH (modulus 2, remainder 0);
CREATE TABLE repart_p1 PARTITION OF repart_p FOR VALUES WITH (modulus 2, remainder 1);
INSERT INTO repart_p SELECT i, i % 95000 FROM generate_series(1, 100000) i;
ALTER TABLE repart_p ALTER COLUMN b SET (n_distinct = 95000);
ANALYZE repart_p;
SET enable_partitionwise_aggregate = on;
SELECT repart_used('SELECT b, count(*) FROM repart_p GROUP BY b');
SELECT count(*), sum(n) FROM (SELECT b, count(*) AS n FROM repart_p GROUP BY b) x;
RESET enable_partitionwise_aggregate;

-- cleanup
DROP FUNCTION repart_used(text);
DROP TABLE repart_p, repart_t;
