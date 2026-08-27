# DBblue: REFRESH MATERIALIZED VIEW skip-if-unchanged, concurrency cases.
#
# These replace a timing-dependent test that used pg_sleep and a backgrounded
# psql.  Every case here is ordered by the isolation framework instead, so there
# is nothing to race and nothing to tune.
#
# Each session reports the DEBUG1 decision line, so the cases assert which path
# the refresh took rather than only that the answer was right.

setup
{
    CREATE TABLE mvsi_src(id int);
    INSERT INTO mvsi_src SELECT g FROM generate_series(1, 10) g;

    CREATE TABLE mvsi_part(id int) PARTITION BY RANGE (id);
    CREATE TABLE mvsi_part_a PARTITION OF mvsi_part FOR VALUES FROM (0) TO (100);
    CREATE TABLE mvsi_part_b PARTITION OF mvsi_part FOR VALUES FROM (100) TO (200);
    INSERT INTO mvsi_part SELECT g FROM generate_series(1, 20) g;
    INSERT INTO mvsi_part SELECT g FROM generate_series(100, 119) g;

    CREATE MATERIALIZED VIEW mvsi_mv WITH (auto_skip_unchanged=true) AS
      SELECT count(*) AS c FROM mvsi_src;
    CREATE MATERIALIZED VIEW mvsi_mvpart WITH (auto_skip_unchanged=true) AS
      SELECT count(*) AS c FROM mvsi_part;

    -- Refresh twice so that every permutation starts from the same known state:
    -- a watermark exists and both matviews read as unchanged.  Without this the
    -- opening decision would depend on shared-memory state left by whichever
    -- permutation ran before.
    REFRESH MATERIALIZED VIEW mvsi_mv;
    REFRESH MATERIALIZED VIEW mvsi_mvpart;
    REFRESH MATERIALIZED VIEW mvsi_mv;
    REFRESH MATERIALIZED VIEW mvsi_mvpart;
}

teardown
{
    DROP MATERIALIZED VIEW mvsi_mvpart;
    DROP MATERIALIZED VIEW mvsi_mv;
    DROP TABLE mvsi_part;
    DROP TABLE mvsi_src;
}

session s1
setup           { SET client_min_messages = debug1; }
step s1_begin    { BEGIN; }
step s1_refresh  { REFRESH MATERIALIZED VIEW mvsi_mv; }
step s1_commit   { COMMIT; }
step s1_rollback { ROLLBACK; }
step s1_detach   { ALTER TABLE mvsi_part DETACH PARTITION mvsi_part_b CONCURRENTLY; }

session s2
setup           { SET client_min_messages = debug1; }
# Committed on its own, to dirty the source before the refresh under test, so
# that refresh really rebuilds and really takes a snapshot.
step s2_dirty    { INSERT INTO mvsi_src VALUES (901); }
# The row that lands while the refresh under test is still running.
step s2_during   { INSERT INTO mvsi_src VALUES (902); }
step s2_begin    { BEGIN; }
step s2_uncommitted { INSERT INTO mvsi_src VALUES (903); }
step s2_commit   { COMMIT; }
step s2_hold_b   { BEGIN; SELECT count(*) FROM mvsi_part_b; }
step s2_release  { COMMIT; }

session s3
setup             { SET client_min_messages = debug1; }
step s3_refresh    { REFRESH MATERIALIZED VIEW mvsi_mv; }
step s3_check      { SELECT c AS matview, (SELECT count(*) FROM mvsi_src) AS truth FROM mvsi_mv; }
step s3_refresh_p  { REFRESH MATERIALIZED VIEW mvsi_mvpart; }
step s3_check_p    { SELECT c AS matview, (SELECT count(*) FROM mvsi_part) AS truth FROM mvsi_mvpart; }

# The row committed while the refresh was running must not be folded into the
# watermark.  s2_dirty makes s1's refresh a real rebuild, so it genuinely takes a
# snapshot; s2_during then commits after that snapshot but before s1 does.  The
# defect this covers recorded s2_during's write as already accounted for, so the
# next refresh skipped and the row never reached the matview.
permutation s2_dirty s1_begin s1_refresh s2_during s1_commit s3_refresh s3_check

# The mirror image: the row commits before the refresh's snapshot, so the refresh
# already contains it and the following refresh may legitimately skip.
permutation s2_dirty s2_during s1_begin s1_refresh s1_commit s3_refresh s3_check

# An uncommitted writer holds RowExclusiveLock on the source, which is what the
# ShareLock probe detects.  The refresh must decline to skip rather than trust
# the counter, because that writer's rows are not accounted for yet.
permutation s2_begin s2_uncommitted s3_refresh s2_commit s3_refresh s3_check

# A refresh whose transaction rolls back must leave the matview reading as
# changed, so the next refresh rebuilds rather than trusting a watermark written
# by work that was thrown away.
permutation s2_dirty s1_begin s1_refresh s1_rollback s3_refresh s3_check

# DETACH PARTITION CONCURRENTLY marks the partition detach-pending and commits,
# then waits for lockers.  While it is in that window the descendant set is
# snapshot-dependent, so no refresh may skip.
permutation s2_hold_b s1_detach s3_refresh_p s3_check_p s2_release s3_refresh_p s3_check_p
