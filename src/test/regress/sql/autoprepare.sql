--
-- dbblue autoprepare: shape tracking, promotion, limit, introspection
--
-- The shape table is per-backend.  Every check turns autoprepare off first
-- so the checking query itself is not tracked as a shape.
--

CREATE TEMP TABLE aprep_t (a int, b int, c int, d int);

-- dbblue_autoprepare_limit rejects a value whose worst case (max_connections
-- backends each caching this many ~32kB shapes) would leave too little of
-- the machine's RAM for everything else.  2 billion exceeds that ceiling on
-- any real machine; the exact ceiling itself is host-dependent, so only the
-- fact of rejection is checked here, not the specific numbers in the error.
DO $$
BEGIN
	BEGIN
		EXECUTE 'SET dbblue_autoprepare_limit = 2000000000';
		RAISE EXCEPTION 'expected dbblue_autoprepare_limit to be rejected, but it was accepted';
	EXCEPTION
		WHEN OTHERS THEN
			IF SQLERRM LIKE '%exceeds the estimated safe ceiling%' THEN
				RAISE NOTICE 'rejected, as expected';
			ELSE
				RAISE;
			END IF;
	END;
END
$$;

SET dbblue_autoprepare_threshold = 2;
SET dbblue_autoprepare_limit = 4;
DISCARD PLANS;
SET dbblue_autoprepare_enabled = on;

-- shape A: seen 3 times -> promoted on the 2nd, reused on the 3rd
SELECT a FROM aprep_t WHERE a = 1;
SELECT a FROM aprep_t WHERE a = 2;
SELECT a FROM aprep_t WHERE a = 3;

-- shape B: same, with an IN list squashed into one array parameter
SELECT b FROM aprep_t WHERE b IN (1, 2);
SELECT b FROM aprep_t WHERE b IN (3, 4, 5);
SELECT b FROM aprep_t WHERE b IN (6, 7, 8, 9);

-- shape C: no constants -> declined at promotion
SELECT c FROM aprep_t;
SELECT c FROM aprep_t;

-- shape D: seen once -> still tracking
SELECT d FROM aprep_t WHERE d = 1;

-- tracking/declined entries (D, C) have no plancache counters
SET dbblue_autoprepare_enabled = off;
SELECT bool_and(num_custom_plans IS NULL AND num_generic_plans IS NULL
                AND generic_cost IS NULL AND avg_custom_cost IS NULL)
FROM dbblue_autoprepare_shapes(pg_backend_pid())
WHERE state <> 'promoted';
SET dbblue_autoprepare_enabled = on;

-- The limit is 4, of which 10% (at least one slot) is reserved for counting
-- new shapes: at most 3 promoted/declined entries (max_cached).

-- shape E: the table is full (4 entries), so E takes the slot of D, the
-- only tracking entry.  On its 2nd sighting E qualifies, but A, B and C
-- already fill the 3 cached slots, so E replaces the one worth least: C,
-- declined, whose reuse only ever saved a (cheap) failed build attempt.
SELECT a, b FROM aprep_t WHERE a = 1 AND b = 2;
SELECT a, b FROM aprep_t WHERE a = 1 AND b = 2;
SELECT a, b FROM aprep_t WHERE a = 1 AND b = 2;

SET dbblue_autoprepare_enabled = off;

SELECT entries, max_entries, max_cached, entries <= max_entries AS within_limit,
       promoted, tracking, declined,
       hits, reuse_fallbacks, promotions, declines, not_tracked,
       tracking_evicted, cached_evicted
FROM dbblue_autoprepare_stats(pg_backend_pid());

-- promoted entries carry a measured planning time and a value score, plus
-- average planning time before caching and plan-fetch time on reuse (each
-- of A, B and E has been reused once), and the plancache's own custom/
-- generic-plan counters (every reuse so far is a custom plan: < 5 of them)
SELECT state, plan_ms > 0 AS timed, score > 0 AS scored,
       avg_plan_ms_before >= plan_ms AS before_timed,
       avg_plan_ms_after >= 0 AS after_timed,
       num_custom_plans, num_generic_plans, avg_custom_cost > 0 AS costed,
       query
FROM dbblue_autoprepare_shapes(pg_backend_pid())
ORDER BY query COLLATE "C";

-- A and E are reused heavily, B is not.  A new shape F that qualifies then
-- replaces B, the cached plan with the least recent reuse.
SET dbblue_autoprepare_enabled = on;
SELECT a FROM aprep_t WHERE a = 11; SELECT a FROM aprep_t WHERE a = 12;
SELECT a FROM aprep_t WHERE a = 13; SELECT a FROM aprep_t WHERE a = 14;
SELECT a FROM aprep_t WHERE a = 15; SELECT a FROM aprep_t WHERE a = 16;
SELECT a FROM aprep_t WHERE a = 17; SELECT a FROM aprep_t WHERE a = 18;
SELECT a FROM aprep_t WHERE a = 19; SELECT a FROM aprep_t WHERE a = 20;
SELECT a FROM aprep_t WHERE a = 21; SELECT a FROM aprep_t WHERE a = 22;
SELECT a FROM aprep_t WHERE a = 23; SELECT a FROM aprep_t WHERE a = 24;
SELECT a FROM aprep_t WHERE a = 25; SELECT a FROM aprep_t WHERE a = 26;
SELECT a FROM aprep_t WHERE a = 27; SELECT a FROM aprep_t WHERE a = 28;
SELECT a FROM aprep_t WHERE a = 29; SELECT a FROM aprep_t WHERE a = 30;
SELECT a, b FROM aprep_t WHERE a = 1 AND b = 3; SELECT a, b FROM aprep_t WHERE a = 1 AND b = 4;
SELECT a, b FROM aprep_t WHERE a = 1 AND b = 5; SELECT a, b FROM aprep_t WHERE a = 1 AND b = 6;
SELECT a, b FROM aprep_t WHERE a = 1 AND b = 7; SELECT a, b FROM aprep_t WHERE a = 1 AND b = 8;
SELECT a, b FROM aprep_t WHERE a = 1 AND b = 9; SELECT a, b FROM aprep_t WHERE a = 1 AND b = 10;
SELECT a, b FROM aprep_t WHERE a = 1 AND b = 11; SELECT a, b FROM aprep_t WHERE a = 1 AND b = 12;
SELECT a, b FROM aprep_t WHERE a = 1 AND b = 13; SELECT a, b FROM aprep_t WHERE a = 1 AND b = 14;
SELECT a, b FROM aprep_t WHERE a = 1 AND b = 15; SELECT a, b FROM aprep_t WHERE a = 1 AND b = 16;
SELECT a, b FROM aprep_t WHERE a = 1 AND b = 17; SELECT a, b FROM aprep_t WHERE a = 1 AND b = 18;
SELECT a, b FROM aprep_t WHERE a = 1 AND b = 19; SELECT a, b FROM aprep_t WHERE a = 1 AND b = 20;
SELECT a, b FROM aprep_t WHERE a = 1 AND b = 21; SELECT a, b FROM aprep_t WHERE a = 1 AND b = 22;
SELECT c, d FROM aprep_t WHERE c = 1;					-- F
SELECT c, d FROM aprep_t WHERE c = 2;					-- F qualifies
SET dbblue_autoprepare_enabled = off;

SELECT state, query FROM dbblue_autoprepare_shapes(pg_backend_pid())
ORDER BY query COLLATE "C";
SELECT promoted, tracking, declined, not_tracked, tracking_evicted, cached_evicted
FROM dbblue_autoprepare_stats(pg_backend_pid());

SELECT pid = pg_backend_pid() AS own, state, seen_count, num_params, query
FROM dbblue_autoprepare_shapes(pg_backend_pid())
ORDER BY state, query COLLATE "C";

-- queryid is the core query jumble, so it is never 0 here
SELECT count(*) FROM dbblue_autoprepare_shapes(pg_backend_pid())
WHERE queryid = 0;

-- with no pid, every client backend is reported, including this one
-- (other tests' backends may be slow to answer; only our own row is checked)
SET client_min_messages = error;
SELECT count(*) FROM dbblue_autoprepare_stats() WHERE pid = pg_backend_pid();
RESET client_min_messages;

-- DISCARD PLANS empties the table but keeps the lifetime counters
DISCARD PLANS;
SELECT count(*) FROM dbblue_autoprepare_shapes(pg_backend_pid());
SELECT entries, promotions FROM dbblue_autoprepare_stats(pg_backend_pid());

-- eviction picks the tracking entry seen least recently, not the oldest:
-- G and H fill the table, G is seen again, so I evicts H and G survives
SET dbblue_autoprepare_threshold = 3;
SET dbblue_autoprepare_limit = 2;
SET dbblue_autoprepare_enabled = on;
SELECT a FROM aprep_t WHERE a = 1 AND b = 1;			-- G
SELECT a FROM aprep_t WHERE a = 1 AND c = 1;			-- H
SELECT a FROM aprep_t WHERE a = 2 AND b = 2;			-- G again
SELECT a FROM aprep_t WHERE a = 1 AND d = 1;			-- I, evicts H
SET dbblue_autoprepare_enabled = off;
SELECT state, seen_count FROM dbblue_autoprepare_shapes(pg_backend_pid())
ORDER BY seen_count DESC;
SELECT entries, tracking_evicted FROM dbblue_autoprepare_stats(pg_backend_pid());

-- lowering the limit shrinks the table by evicting tracking entries
SET dbblue_autoprepare_limit = 1;
SET dbblue_autoprepare_enabled = on;
SELECT a FROM aprep_t WHERE b = 1 AND c = 1;			-- J
SET dbblue_autoprepare_enabled = off;
SELECT entries, tracking_evicted FROM dbblue_autoprepare_stats(pg_backend_pid());

-- a shape still gets cached when its tracking entry is evicted between
-- sightings: limit 3 leaves one tracking slot once P1 and P2 are cached, K1
-- and K2 keep evicting each other from it, but the sketch remembers K1's
-- first sighting, so its second one promotes it
DISCARD PLANS;
SET dbblue_autoprepare_threshold = 2;
SET dbblue_autoprepare_limit = 3;
SET dbblue_autoprepare_enabled = on;
SELECT a FROM aprep_t WHERE b = 1; SELECT a FROM aprep_t WHERE b = 2;	-- P1
SELECT a FROM aprep_t WHERE c = 1; SELECT a FROM aprep_t WHERE c = 2;	-- P2
SELECT b FROM aprep_t WHERE d = 1;			-- K1
SELECT c FROM aprep_t WHERE d = 1;			-- K2, evicts K1
SELECT b FROM aprep_t WHERE d = 2;			-- K1 again: promoted
SET dbblue_autoprepare_enabled = off;
SELECT state, seen_count, query FROM dbblue_autoprepare_shapes(pg_backend_pid())
WHERE query LIKE '%d = %' OR state = 'tracking';
SELECT tracking_evicted, cached_evicted FROM dbblue_autoprepare_stats(pg_backend_pid());
SET dbblue_autoprepare_threshold = 2;
SET dbblue_autoprepare_limit = 4;
DISCARD PLANS;

-- a pid that is not a backend
SELECT count(*) FROM dbblue_autoprepare_shapes(0);

-- dbblue_autoprepare_reset(): applied at the start of the next statement,
-- even with autoprepare off
SET dbblue_autoprepare_enabled = on;
SELECT a FROM aprep_t WHERE a = 1;
SELECT a FROM aprep_t WHERE a = 2;
SET dbblue_autoprepare_enabled = off;
SELECT entries, promoted FROM dbblue_autoprepare_stats(pg_backend_pid());
SELECT dbblue_autoprepare_reset(pg_backend_pid());
SELECT entries, promotions FROM dbblue_autoprepare_stats(pg_backend_pid());
SELECT dbblue_autoprepare_reset(0);
SELECT dbblue_autoprepare_reset() >= 1 AS reached_this_backend;

-- DISCARD ALL must NOT clear it: poolers and psycopg2 send it on every
-- connection reuse (it resets the GUCs, so set them again)
SET dbblue_autoprepare_enabled = on;
SELECT a FROM aprep_t WHERE a = 1;
SELECT a FROM aprep_t WHERE a = 2;
DISCARD ALL;
SET dbblue_autoprepare_enabled = off;
SELECT state, num_params, query FROM dbblue_autoprepare_shapes(pg_backend_pid())
WHERE state = 'promoted';
SET dbblue_autoprepare_threshold = 2;
SET dbblue_autoprepare_limit = 4;

-- the per-backend log request
SELECT dbblue_log_autoprepare_shapes(pg_backend_pid());
SELECT dbblue_log_autoprepare_shapes(0);

-- not callable by ordinary roles by default; pg_read_all_stats may read
CREATE ROLE regress_aprep_user;
SET ROLE regress_aprep_user;
SELECT dbblue_log_autoprepare_shapes(pg_backend_pid());
SELECT count(*) FROM dbblue_autoprepare_shapes(pg_backend_pid());
SELECT count(*) FROM dbblue_autoprepare_stats(pg_backend_pid());
SELECT dbblue_autoprepare_reset(pg_backend_pid());
RESET ROLE;
GRANT pg_read_all_stats TO regress_aprep_user;
SET ROLE regress_aprep_user;
SELECT count(*) FROM dbblue_autoprepare_stats(pg_backend_pid());
RESET ROLE;
DROP ROLE regress_aprep_user;

RESET dbblue_autoprepare_enabled;
RESET dbblue_autoprepare_threshold;
RESET dbblue_autoprepare_limit;
