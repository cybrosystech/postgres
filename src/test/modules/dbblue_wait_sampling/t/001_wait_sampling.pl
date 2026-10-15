# Copyright (c) 2026, Cybrosys Technologies
#
# Tests for the dbblue wait sampler's connection handling.
#
#   Bug 1: with the feature off, a dbblue_wait_sampling_database that does not
#          exist must NOT send the worker into a FATAL restart loop (it used to
#          connect eagerly at startup regardless of the enabled flag).
#
#   Bug 2: while the sampler is attached to its database, DROP DATABASE on it is
#          blocked; turning sampling off must release that connection so the
#          database can then be dropped.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('wait_sampling');
$node->init;

# Point the sampler at a database that does not exist, with the feature OFF.
$node->append_conf('postgresql.conf', q(
dbblue_wait_sampling_database = 'ws_nonexistent'
dbblue_wait_sampling_enabled = off
));
$node->start;

my $sampler_conns = q{
	SELECT count(*) FROM pg_stat_activity
	WHERE backend_type = 'dbblue wait sampler'};

# ---------------------------------------------------------------------------
# Bug 1: disabled + missing database -> no restart loop, no connection held.
# ---------------------------------------------------------------------------
sleep 3;    # old bug would have logged several FATALs by now
ok( !$node->log_contains(qr/FATAL:  database "ws_nonexistent" does not exist/),
	'disabled sampler with a missing database does not FATAL-loop');
is( $node->safe_psql('postgres', $sampler_conns),
	'0', 'disabled sampler holds no database connection');

# ---------------------------------------------------------------------------
# Bug 2: an attached sampler blocks DROP DATABASE; disabling releases it.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', 'CREATE DATABASE ws_sampledb');

# Hardening (search_path pin): the worker connects to the sampling database as a
# superuser.  Give that database a hostile default search_path and a relation
# that shadows the worker's table name.  Because the worker pins its path to
# pg_catalog,pg_temp and names every object as dbblue_catalog.*, it must keep
# writing only to its own catalog and never touch the attacker-planted object.
$node->safe_psql('ws_sampledb', q(
	CREATE SCHEMA hostile;
	CREATE TABLE hostile.wait_history(evil int);
	ALTER DATABASE ws_sampledb SET search_path = 'hostile, public';
));

# dbblue_wait_sampling_database is PGC_POSTMASTER, so set it and restart.
$node->safe_psql('postgres',
	"ALTER SYSTEM SET dbblue_wait_sampling_database = 'ws_sampledb'");
$node->safe_psql('postgres', 'ALTER SYSTEM SET dbblue_wait_sampling_enabled = on');
$node->restart;

$node->poll_query_until('postgres', q{
	SELECT count(*) FROM pg_stat_activity
	WHERE backend_type = 'dbblue wait sampler' AND datname = 'ws_sampledb'}, '1')
  or die "timed out waiting for the sampler to attach to ws_sampledb";

# Even under the hostile default search_path, the worker must build its own
# (fully schema-qualified) catalog -- proof the pin does not break qualified
# access to dbblue_catalog.*.
$node->poll_query_until('ws_sampledb', q{
	SELECT count(*) FROM pg_tables
	WHERE schemaname = 'dbblue_catalog' AND tablename = 'wait_history'}, '1')
  or die "sampler did not create dbblue_catalog.wait_history under a hostile search_path";
pass('sampler builds its qualified catalog despite a hostile default search_path');

# ... and it must never have written to the search_path-shadowing relation.
is($node->safe_psql('ws_sampledb', 'SELECT count(*) FROM hostile.wait_history'),
	'0', 'worker ignores a search_path-shadowing wait_history relation');

# DROP must be blocked while the sampler is attached.
my ($err1, $err2) = ('', '');
my $rc1 = $node->psql('postgres', 'DROP DATABASE ws_sampledb',
	stderr => \$err1, on_error_die => 0);
ok( $rc1 != 0 && $err1 =~ /being accessed by other users/,
	'DROP DATABASE is blocked while the sampler is attached');

# Turn sampling off; the worker exits and restarts idle, releasing the DB.
$node->safe_psql('postgres', 'ALTER SYSTEM SET dbblue_wait_sampling_enabled = off');
$node->safe_psql('postgres', 'SELECT pg_reload_conf()');

$node->poll_query_until('postgres', q{
	SELECT count(*) FROM pg_stat_activity
	WHERE backend_type = 'dbblue wait sampler' AND datname = 'ws_sampledb'}, '0')
  or die "timed out waiting for the sampler to release ws_sampledb";

# Now the DROP succeeds.
my $rc2 = $node->psql('postgres', 'DROP DATABASE ws_sampledb',
	stderr => \$err2, on_error_die => 0);
is($rc2, 0, 'DROP DATABASE succeeds once sampling is disabled');

$node->stop;
done_testing();
