# Copyright (c) 2026, Cybrosys Technologies
#
# Tests for the automatic standby worker (dbblue_create_standby).
#
# With dbblue_create_standby = on, a background worker builds a standby of the
# server with pg_basebackup in "<datadir>_standby", starts it on a free port
# above the server's own, reports that port in <datadir>/dbblue_standby.port,
# and looks after it.  Each section below covers one behaviour, most of them
# fixes made after an audit of the feature:
#
#   * the build itself: standby started, port reported, data replicated;
#   * a port a stopped cluster beside the primary is configured for is not
#     taken (M4);
#   * hot_standby = off in the shared postgresql.conf does not make the
#     standby refuse connections (L5);
#   * a reload of the primary reaches the standby, without a restart (M2);
#   * the standby is stopped along with the primary, and keeps its port (H1);
#   * a standby that stops replaying is fenced -- restarted to refuse
#     connections, its slot no longer holding back VACUUM, its port no longer
#     reported -- and let back in once it has caught up (H3, H4);
#   * a slot invalidated by idle_replication_slot_timeout is reported as such,
#     not blamed on max_slot_wal_keep_size, and the standby rebuilt (L8);
#   * with max_slot_wal_keep_size = -1, a standby whose slot is gone is kept,
#     not deleted with no rebuild to follow, and rebuilt once a limit is set
#     (M6);
#   * a promoted standby is no longer taken for ours, and its slot is dropped
#     (H4).
#
# Not covered here, as they need a large database, minutes of waiting, or
# pg_upgrade: cancelling a build in progress (M3), leaving a partial copy at
# shutdown (L7), the retry backoff (M5), the pg_upgrade guard (L1), and a
# port taken at a listen address other than 127.0.0.1 (L6).
#
# The worker checks on the standby every 10 seconds, and fences one only
# after it has not replayed for 60, so this test takes a few minutes.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);

my $pgdata = $node->data_dir;
my $sbdir = "${pgdata}_standby";
my $portfile = "$pgdata/dbblue_standby.port";
my $port = $node->port;

# The worker stops the standby along with the server, but not on an
# immediate stop, which is how a failed test tears the server down.  Stop
# both here, the server first, so its worker cannot start the standby again.
END
{
	local $?;
	eval { $node->stop('immediate') if defined $node; };
	system('pg_ctl', '-D', $sbdir, '-m', 'immediate', 'stop')
	  if defined $sbdir && -f "$sbdir/postmaster.pid";
}

# A stopped cluster beside ours, configured for the port just above ours.
# Nothing listens on that port, so only its configuration tells that it is
# taken (M4).
my $other = $node->basedir . '/stopped_cluster';
mkdir $other or die "could not create $other: $!";
append_to_file("$other/PG_VERSION", slurp_file("$pgdata/PG_VERSION"));
append_to_file("$other/postgresql.conf", 'port = ' . ($port + 1) . "\n");

# hot_standby does nothing on a primary, and the standby shares this file;
# the standby must accept connections all the same (L5).  A limit on the WAL
# the slot keeps is required for a build; set it here, whatever initdb's
# auto-tune chose.
$node->append_conf(
	'postgresql.conf', qq(
dbblue_create_standby = on
max_slot_wal_keep_size = '256MB'
hot_standby = off
));

$node->start;

# One PostgreSQL::Test::Cluster object for each port the standby is reported
# on, only to connect to it; this test never starts or stops it that way.
my %standby_nodes;
my $standby_count = 0;

sub standby_port
{
	return undef unless -f $portfile;
	my $p = slurp_file($portfile);
	$p =~ s/\s+//g;
	return $p;
}

sub standby_pid
{
	return undef unless -f "$sbdir/postmaster.pid";
	my ($pid) = split /\n/, slurp_file("$sbdir/postmaster.pid");
	return $pid;
}

# Wait for the standby to be reported, and to accept queries in recovery.
sub wait_for_standby
{
	my ($what) = @_;
	my $deadline = time + $PostgreSQL::Test::Utils::timeout_default;

	until (defined standby_port())
	{
		die "timed out waiting for the standby to be reported ($what)"
		  if time > $deadline;
		usleep(200_000);
	}

	my $p = standby_port();
	$standby_nodes{$p} //= PostgreSQL::Test::Cluster->new(
		'standby' . ++$standby_count,
		port => $p);
	my $sb = $standby_nodes{$p};
	$sb->poll_query_until('postgres', 'SELECT pg_is_in_recovery()')
	  or die "timed out waiting for the standby to accept queries ($what)";
	return $sb;
}

# Wait for a condition on the file system.
sub wait_until
{
	my ($what, $cond) = @_;
	my $deadline = time + $PostgreSQL::Test::Utils::timeout_default;

	until ($cond->())
	{
		die "timed out waiting until $what" if time > $deadline;
		usleep(200_000);
	}
	return 1;
}

sub slot_count
{
	return $node->safe_psql('postgres',
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'dbblue_standby'"
	);
}

# Set parameters with ALTER SYSTEM, each in its own statement, and reload.
sub alter_system
{
	my (@settings) = @_;
	$node->safe_psql('postgres', "ALTER SYSTEM $_") foreach @settings;
	$node->reload;
	return;
}

# ---------------------------------------------------------------------------
# The build: a standby is made, started, reported, and replicates.
# ---------------------------------------------------------------------------
my $sb = wait_for_standby('initial build');

ok(-f "$sbdir/standby.signal", 'standby built in <datadir>_standby');
is( $node->safe_psql(
		'postgres',
		"SELECT active FROM pg_replication_slots WHERE slot_name = 'dbblue_standby'"
	),
	't',
	'standby streams through replication slot dbblue_standby');
ok($node->log_contains(qr/dbblue create standby: standby is running on port \d+/),
	'standby port is reported in the log');

isnt($sb->port, $port + 1,
	'standby skips the port a stopped cluster beside the primary is configured for'
);

like(
	slurp_file("$sbdir/postgresql.auto.conf"),
	qr/^hot_standby = on$/m,
	'standby is given hot_standby = on, whatever the shared postgresql.conf says'
);
is($sb->safe_psql('postgres', 'SELECT 1'),
	'1',
	'standby accepts connections though the shared postgresql.conf has hot_standby = off'
);

$node->safe_psql('postgres',
	'CREATE TABLE t (i int); INSERT INTO t SELECT generate_series(1, 10)');
ok($sb->poll_query_until('postgres', 'SELECT count(*) FROM t', '10'),
	'rows written on the primary reach the standby');

# ---------------------------------------------------------------------------
# M2: a reload of the primary reaches the standby, without a restart.
# ---------------------------------------------------------------------------
my $pid = standby_pid();
$node->append_conf('postgresql.conf', "statement_timeout = '42s'");
$node->reload;
ok($sb->poll_query_until('postgres', 'SHOW statement_timeout', '42s'),
	'a reload of the primary reaches the standby');
is(standby_pid(), $pid, 'the standby reloaded without being restarted');

# ---------------------------------------------------------------------------
# H1: the standby stops along with the primary, and keeps its port.
# ---------------------------------------------------------------------------
my $sbport = $sb->port;
$node->stop;
ok(!-f "$sbdir/postmaster.pid", 'standby stopped along with the primary');
ok($node->log_contains(qr/shutting down the standby in .* along with this server/),
	'the worker shut the standby down cleanly');

$node->start;
$sb = wait_for_standby('after a restart of the primary');
is($sb->port, $sbport,
	'standby is started again on the same port after a restart of the primary');

# ---------------------------------------------------------------------------
# H3, H4: a standby that stops replaying is fenced, then let back in.
# ---------------------------------------------------------------------------
my $offset = -s $node->logfile;
$sb->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');

# Keep the primary moving while the standby does not replay.
my $deadline = time + $PostgreSQL::Test::Utils::timeout_default;
my $next = 100;
until ($node->log_contains(qr/restarting it to refuse connections/, $offset))
{
	die "timed out waiting for the standby to be fenced" if time > $deadline;
	$node->safe_psql('postgres', 'INSERT INTO t VALUES (' . $next++ . ')');
	sleep 2;
}
pass('a standby that stopped replaying is fenced');

$node->wait_for_log(
	qr/replication slot "dbblue_standby" no longer holds back VACUUM on this server, as its standby refuses connections/,
	$offset);

# The fence lasts until the next check at the earliest, ten seconds away.
ok(!-f $portfile, 'the port of a fenced standby is not reported');
is( $node->safe_psql(
		'postgres',
		"SELECT xmin IS NULL AND catalog_xmin IS NULL FROM pg_replication_slots WHERE slot_name = 'dbblue_standby'"
	),
	't',
	'the slot of a fenced standby no longer holds back VACUUM');
my ($ret, $stdout, $stderr) = $sb->psql('postgres', 'SELECT 1');
like($stderr, qr/not accepting connections|not yet accepting connections/,
	'a fenced standby refuses connections');

# The fenced restart ends the paused replay, so it catches up.
$node->wait_for_log(
	qr/has caught up with this server, restarting it to accept connections again/,
	$offset);
$sb = wait_for_standby('after the fence is lifted');
my $rows = $node->safe_psql('postgres', 'SELECT count(*) FROM t');
ok($sb->poll_query_until('postgres', 'SELECT count(*) FROM t', $rows),
	'once let back in, the standby has every row written meanwhile');

# ---------------------------------------------------------------------------
# L8: an idle_replication_slot_timeout invalidation is reported as such.
# ---------------------------------------------------------------------------
alter_system("SET idle_replication_slot_timeout = '1s'",
	'SET dbblue_create_standby = paused');
wait_until('the paused standby has stopped',
	sub { !-f "$sbdir/postmaster.pid" });
$node->poll_query_until('postgres',
	"SELECT NOT active FROM pg_replication_slots WHERE slot_name = 'dbblue_standby'"
) or die "timed out waiting for the slot to be released";

sleep 3;
$node->safe_psql('postgres', 'CHECKPOINT');
is( $node->safe_psql(
		'postgres',
		"SELECT invalidation_reason FROM pg_replication_slots WHERE slot_name = 'dbblue_standby'"
	),
	'idle_timeout',
	'the idle slot of a paused standby is invalidated by idle_replication_slot_timeout'
);

$offset = -s $node->logfile;
alter_system('RESET idle_replication_slot_timeout',
	'SET dbblue_create_standby = on');
$node->wait_for_log(qr/has been invalidated, rebuilding the standby/, $offset);
ok( $node->log_contains(
		qr/The slot went unused for longer than idle_replication_slot_timeout/,
		$offset),
	'the invalidation is put down to idle_replication_slot_timeout');
ok( !$node->log_contains(qr/fell more than max_slot_wal_keep_size behind/,
		$offset),
	'the invalidation is not blamed on max_slot_wal_keep_size');

$sb = wait_for_standby('after the rebuild of an invalidated slot');
$rows = $node->safe_psql('postgres', 'SELECT count(*) FROM t');
ok($sb->poll_query_until('postgres', 'SELECT count(*) FROM t', $rows),
	'the rebuilt standby has every row');

# ---------------------------------------------------------------------------
# M6: with max_slot_wal_keep_size = -1, a standby is not deleted with no
# rebuild to follow.
# ---------------------------------------------------------------------------
alter_system('SET max_slot_wal_keep_size = -1',
	'SET dbblue_create_standby = off');
$node->poll_query_until('postgres',
	"SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'dbblue_standby'",
	'0')
  or die "timed out waiting for the slot to be dropped";
wait_until('the standby has stopped', sub { !-f "$sbdir/postmaster.pid" });

$offset = -s $node->logfile;
alter_system('SET dbblue_create_standby = on');
$node->wait_for_log(
	qr/not rebuilding the standby in .*, as max_slot_wal_keep_size is -1/,
	$offset);
ok(-f "$sbdir/PG_VERSION",
	'with max_slot_wal_keep_size = -1, the standby is kept, not deleted');
ok(!-f "$sbdir/postmaster.pid", 'and it is not running');
ok(!-f $portfile, 'and its port is not reported');
ok(!$node->log_contains(qr/does not exist, rebuilding/, $offset),
	'and no rebuild is announced');

$offset = -s $node->logfile;
alter_system('RESET max_slot_wal_keep_size');
$node->wait_for_log(qr/does not exist, rebuilding the standby/, $offset);
$sb = wait_for_standby('after a limit is set again');
$rows = $node->safe_psql('postgres', 'SELECT count(*) FROM t');
ok($sb->poll_query_until('postgres', 'SELECT count(*) FROM t', $rows),
	'once a limit is set, the standby is rebuilt');

# ---------------------------------------------------------------------------
# H4: a promoted standby is no longer ours, and its slot is dropped.
# ---------------------------------------------------------------------------
$offset = -s $node->logfile;
command_ok([ 'pg_ctl', '-D', $sbdir, '-w', 'promote' ],
	'promote the standby');
$node->wait_for_log(qr/is no longer a standby of this server/, $offset);
ok( $node->poll_query_until(
		'postgres',
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'dbblue_standby'",
		'0'),
	'the slot of a promoted standby is dropped');
ok(!-f $portfile, 'the port of a promoted standby is not reported');

# A promoted standby is somebody's primary now; ours is not to stop it.
command_ok([ 'pg_ctl', '-D', $sbdir, '-m', 'fast', '-w', 'stop' ],
	'stop the promoted standby');

$node->stop;
done_testing();
