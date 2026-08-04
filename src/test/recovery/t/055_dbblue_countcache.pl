# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# DBblue COUNT cache: the two paths where rows become visible without the
# tableam write hooks running.
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
# Both checks run the capture and the re-read in a single psql session, because
# the cache is session-local -- a fresh connection would recompute and pass
# vacuously.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->append_conf(
	'postgresql.conf', q{
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
# 1. hot standby must not serve a count that redo has invalidated
# ----------------------------------------------------------------------

# One session: cache a count, let the primary write and the standby replay,
# then re-read.  \! runs the primary's insert from inside that same session.
my $primary_conn = $primary->installed_command('psql')
  . ' -X -q -A -t -d '
  . $primary->connstr('postgres');

my $out = $standby->safe_psql(
	'postgres', q{SELECT count(*) FROM t WHERE id > 0;});
is($out, '100', 'standby sees the pre-write count');

$primary->safe_psql('postgres',
	q{INSERT INTO t SELECT generate_series(101, 150);});
$primary->wait_for_catchup($standby, 'replay');

is( $standby->safe_psql('postgres', q{SELECT count(*) FROM t WHERE id > 0;}),
	'150',
	'standby replayed the primary insert');

# The real check: capture and re-read inside one standby session, with the
# primary's write landing in between.
$out = $standby->safe_psql(
	'postgres', qq{
		SELECT count(*) FROM t WHERE id > 0;
		\\! $primary_conn -c "INSERT INTO t SELECT generate_series(151, 200);" >/dev/null
		SELECT pg_sleep(1);
		SELECT count(*) FROM t WHERE id > 0;
	});
my @lines = grep { /^\d+$/ } split /\n/, $out;
is($lines[-1], '200',
	'standby does not serve a stale cached count after replay');

# ----------------------------------------------------------------------
# 2. COMMIT PREPARED makes rows visible after the PREPARE-time stamp bump
# ----------------------------------------------------------------------

$primary->safe_psql('postgres',
	q{CREATE TABLE p (id int PRIMARY KEY);
	  INSERT INTO p SELECT generate_series(1, 100);});

$primary->safe_psql('postgres',
	q{BEGIN;
	  INSERT INTO p SELECT generate_series(101, 150);
	  PREPARE TRANSACTION 'dbb_tap';});

is( $primary->safe_psql('postgres', q{SELECT count(*) FROM p;}),
	'100',
	'prepared rows are not yet visible');

$out = $primary->safe_psql(
	'postgres', qq{
		SELECT count(*) FROM p WHERE id > 0;
		\\! $primary_conn -c "COMMIT PREPARED 'dbb_tap';" >/dev/null
		SELECT count(*) FROM p WHERE id > 0;
	});
@lines = grep { /^\d+$/ } split /\n/, $out;
is($lines[0], '100', 'count before COMMIT PREPARED excludes prepared rows');
is($lines[-1], '150',
	'count after COMMIT PREPARED is not served stale from cache');

# ----------------------------------------------------------------------
# 3. a promoted standby must not inherit anything cached during recovery
# ----------------------------------------------------------------------

$standby->promote;
$standby->safe_psql('postgres', q{SELECT 1;});

is( $standby->safe_psql(
		'postgres', q{
			SELECT count(*) FROM t WHERE id > 0;
			INSERT INTO t SELECT generate_series(300, 309);
			SELECT count(*) FROM t WHERE id > 0;
		}
	),
	"200\n210",
	'promoted standby caches correctly and reflects its own writes');

$primary->stop;
$standby->stop;

done_testing();
