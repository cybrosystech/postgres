# DBblue COUNT cache: cross-backend invalidation.
#
# The cache is session-local but a count can be invalidated by any backend, so
# these are the cases the shared write stamps exist for.  Nothing here inspects
# whether a query hit the cache -- each permutation asserts the count s1 sees,
# which is the only property that has to hold.

setup
{
    CREATE TABLE dbb_iso (id int PRIMARY KEY);
    INSERT INTO dbb_iso SELECT generate_series(1, 100);
}

teardown
{
    DROP TABLE dbb_iso;
}

session s1
setup       { SET dbblue_count_cache = on; SET dbblue_offset_flip = on; }
# Warm the cache in its own transaction, so the later count is a fresh
# transaction reusing a previously cached entry -- the ERP request pattern.
step s1warm { SELECT count(*) FROM dbb_iso WHERE id > 0; }
step s1read { SELECT count(*) FROM dbb_iso WHERE id > 0; }
step s1page { SELECT id FROM dbb_iso WHERE id > 0 ORDER BY id LIMIT 5 OFFSET 90; }

session s2
setup        { SET dbblue_count_cache = on; }
step s2begin { BEGIN; }
step s2ins   { INSERT INTO dbb_iso SELECT generate_series(101, 130); }
step s2del   { DELETE FROM dbb_iso WHERE id > 90; }
step s2trunc { TRUNCATE dbb_iso; }
step s2commit{ COMMIT; }
step s2abort { ROLLBACK; }

# A committed insert by another backend must be visible to s1's next count.
permutation s1warm s2begin s2ins s2commit s1read

# ... and must not be visible while s2 is still uncommitted.
permutation s1warm s2begin s2ins s1read s2commit s1read

# An aborted insert must never affect s1.
permutation s1warm s2begin s2ins s2abort s1read

# Deletes and TRUNCATE by another backend likewise.
permutation s1warm s2begin s2del s2commit s1read
permutation s1warm s2begin s2trunc s2commit s1read

# The write-before-capture / commit-after-capture ordering: s2 writes before s1
# ever caches a count, and commits afterwards.  The write-time stamp bump alone
# would not catch this, which is why commit bumps again.
permutation s2begin s2ins s1warm s2commit s1read

# The OFFSET-flip consumes the same cached N, so a concurrent commit must leave
# the returned page correct rather than shifted.
permutation s1warm s2begin s2ins s2commit s1page
permutation s1warm s2begin s2del s2commit s1page
permutation s2begin s2ins s1warm s2commit s1page
