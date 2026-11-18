# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# DBblue COUNT cache: the paths where rows become visible without the tableam
# write hooks having run for that visibility event.
#
# 1. WAL redo on a hot standby.  Redo does not go through the tableam wrappers
#    where per-relation write stamps are bumped, so a count cached on a standby
#    would otherwise stay "valid" while being wrong -- and because a cache hit
#    does not renew the TTL, it would never age out.  The cache is therefore
#    inert whenever RecoveryInProgress().
#
# 2. COMMIT PREPARED.  Stamps are bumped when the transaction PREPAREs, which is
#    too early: the rows only become visible at COMMIT PREPARED, so a count
#    captured in between would survive with an unchanged stamp.
#
# The cache is session-local, so the capture and the re-read must happen in one
# session -- a fresh connection would recompute and pass vacuously.  That is why
# these use background_psql() rather than safe_psql().

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->append_conf(
	'postgresql.conf', q{
dbblue_track_relation_writes = on
dbblue_count_cache = on
dbblue_offset_flip = on
max_prepared_transactions = 10
});
$primary->start;

$primary->safe_psql('postgres',
	q{CREATE TABLE t (id int PRIMARY KEY);
	  INSERT INTO t SELECT generate_series(1, 100);});

$primary->backup('bkp');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'bkp', has_streaming => 1);
$standby->start;

# ----------------------------------------------------------------------
# 1. a hot standby must not serve a count that redo has invalidated
# ----------------------------------------------------------------------

my $sby = $standby->background_psql('postgres');

is($sby->query_safe('SELECT count(*) FROM t WHERE id > 0;'),
	'100', 'standby caches the pre-write count');

# The primary writes from a different connection entirely, so the standby
# session above keeps its cached entry.
$primary->safe_psql('postgres',
	q{INSERT INTO t SELECT generate_series(101, 150);});
$primary->wait_for_catchup($standby, 'replay');

is($sby->query_safe('SELECT count(*) FROM t WHERE id > 0;'),
	'150',
	'standby does not serve a stale cached count after replay');

# Again, to show it is not a one-off: the entry must not become authoritative
# after being refreshed either.
$primary->safe_psql('postgres',
	q{INSERT INTO t SELECT generate_series(151, 200);});
$primary->wait_for_catchup($standby, 'replay');

is($sby->query_safe('SELECT count(*) FROM t WHERE id > 0;'),
	'200', 'standby stays correct across repeated replays');

$sby->quit;

# ----------------------------------------------------------------------
# 2. COMMIT PREPARED makes rows visible long after the PREPARE-time bump
# ----------------------------------------------------------------------

$primary->safe_psql('postgres',
	q{CREATE TABLE p (id int PRIMARY KEY);
	  INSERT INTO p SELECT generate_series(1, 100);});

$primary->safe_psql('postgres',
	q{BEGIN;
	  INSERT INTO p SELECT generate_series(101, 150);
	  PREPARE TRANSACTION 'dbb_tap';});

is($primary->safe_psql('postgres', 'SELECT count(*) FROM p;'),
	'100', 'prepared rows are not yet visible');

my $pri = $primary->background_psql('postgres');

is($pri->query_safe('SELECT count(*) FROM p WHERE id > 0;'),
	'100', 'count before COMMIT PREPARED excludes the prepared rows');

# Committed from a different connection, so the session above still holds its
# cached entry.
$primary->safe_psql('postgres', q{COMMIT PREPARED 'dbb_tap';});

is($pri->query_safe('SELECT count(*) FROM p WHERE id > 0;'),
	'150',
	'count after COMMIT PREPARED is not served stale from cache');

# ROLLBACK PREPARED must leave the count alone.
$primary->safe_psql('postgres',
	q{BEGIN;
	  INSERT INTO p SELECT generate_series(200, 260);
	  PREPARE TRANSACTION 'dbb_tap2';});
$primary->safe_psql('postgres', q{ROLLBACK PREPARED 'dbb_tap2';});

is($pri->query_safe('SELECT count(*) FROM p WHERE id > 0;'),
	'150', 'count after ROLLBACK PREPARED is unchanged');

$pri->quit;

# ----------------------------------------------------------------------
# 3. a promoted standby must not inherit anything cached during recovery
# ----------------------------------------------------------------------

$standby->promote;
$standby->safe_psql('postgres', 'SELECT 1;');

my $prom = $standby->background_psql('postgres');

is($prom->query_safe('SELECT count(*) FROM t WHERE id > 0;'),
	'200', 'promoted standby reads the correct count');

$prom->query_safe('INSERT INTO t SELECT generate_series(300, 309);');

is($prom->query_safe('SELECT count(*) FROM t WHERE id > 0;'),
	'210', 'promoted standby reflects its own writes');

$prom->quit;

$primary->stop;
$standby->stop;

done_testing();
