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
#   * a standby whose replay has stopped is fenced as soon as it is more than
#     dbblue_standby_max_lag behind, not only by the 60-second stall timer;
#   * a slot invalidated by idle_replication_slot_timeout is reported as such,
#     not blamed on max_slot_wal_keep_size, and the standby rebuilt (L8);
#   * with max_slot_wal_keep_size = -1, a standby whose slot is gone is kept,
#     not deleted with no rebuild to follow, and rebuilt once a limit is set
#     (M6);
#   * pointed back at a directory whose standby was set aside for one built
#     in another, the standby there is rebuilt, not started on its old data
#     and left fenced for good, as the slot it streamed through was made anew
#     for the other one;
#   * CREATE TABLESPACE is refused while the feature is on or paused, as the
#     standby on this host would share the tablespace's directory and write
#     over the primary's files, and allowed once it is off (R1);
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
my $sbdir_b = $node->basedir . '/standby_b';
my $portfile = "$pgdata/dbblue_standby.port";
my $port = $node->port;

# The worker stops the standby along with the server, but not on an
# immediate stop, which is how a failed test tears the server down.  Stop
# both here, the server first, so its worker cannot start the standby again.
END
{
	local $?;
	eval { $node->stop('immediate') if defined $node; };
	foreach my $dir ($sbdir, $sbdir_b)
	{
		system('pg_ctl', '-D', $dir, '-m', 'immediate', 'stop')
		  if defined $dir && -f "$dir/postmaster.pid";
	}
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
ok($node->log_contains(qr/shutting down the standby in .*, as this worker is stopping/),
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
# A standby whose replay has stopped is fenced as soon as it is more than
# dbblue_standby_max_lag behind, not only once it has not replayed for 60
# seconds: a stopped replay falls behind at the full rate of our writes.
# ---------------------------------------------------------------------------
alter_system("SET dbblue_standby_max_lag = '1MB'");
$offset = -s $node->logfile;
my $paused_at = time;
$sb->safe_psql('postgres', 'SELECT pg_wal_replay_pause()');

# Several MB of WAL at once, far more than the 1MB allowed.
$node->safe_psql('postgres',
	"CREATE TABLE lag_test AS SELECT g, repeat('x', 100) AS pad FROM generate_series(1, 50000) g"
);
$node->wait_for_log(
	qr/MB behind this server, more than dbblue_standby_max_lag, restarting it to refuse connections/,
	$offset);
my $fenced_after = time - $paused_at;
cmp_ok($fenced_after, '<', 55,
	"a stalled standby over dbblue_standby_max_lag is fenced at once ($fenced_after s), not after the 60 s stall timer"
);
ok($node->log_contains(qr/Its replay has stopped/, $offset),
	'the fence says its replay has stopped');
ok(!$node->log_contains(qr/has not replayed past/, $offset),
	'it was fenced for its lag, not by the stall timer');

$node->wait_for_log(
	qr/has caught up with this server, restarting it to accept connections again/,
	$offset);
alter_system('RESET dbblue_standby_max_lag');
$sb = wait_for_standby('after the lag fence of a stalled standby is lifted');
is($sb->safe_psql('postgres', 'SELECT count(*) FROM lag_test'),
	'50000', 'once let back in, the standby has the rows it fell behind on');
$node->safe_psql('postgres', 'DROP TABLE lag_test');

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
# A standby set aside for one built in another directory is rebuilt, not
# started on its old data, once dbblue_standby_directory points back at it:
# the build in the other directory dropped the slot it streamed through, and
# made one of the same name, for itself.
# ---------------------------------------------------------------------------
mkdir $sbdir_b or die "could not create $sbdir_b: $!";
chmod 0700, $sbdir_b;

alter_system("SET dbblue_standby_directory = '$sbdir_b'");
wait_until('the standby is running in the second directory',
	sub { -f "$sbdir_b/standby.signal" && -f "$sbdir_b/postmaster.pid" });
ok(!-f "$sbdir/postmaster.pid",
	'the standby in the first directory is stopped');
like(
	slurp_file("$pgdata/dbblue_standby.slot"),
	qr/standby_b$/m,
	'the slot is recorded as made for the second directory');
$sb = wait_for_standby('in the second directory');

# Rows the set-aside standby never received.
$node->safe_psql('postgres',
	'INSERT INTO t SELECT generate_series(1001, 1100)');
$rows = $node->safe_psql('postgres', 'SELECT count(*) FROM t');
ok($sb->poll_query_until('postgres', 'SELECT count(*) FROM t', $rows),
	'the standby in the second directory replicates');

$offset = -s $node->logfile;
alter_system('RESET dbblue_standby_directory');
$node->wait_for_log(
	qr/replication slot "dbblue_standby" was made for the standby in ".*standby_b", not for this one, rebuilding the standby in/,
	$offset);
pass('pointed back at, the set-aside standby is rebuilt');
wait_until('the standby in the second directory has stopped',
	sub { !-f "$sbdir_b/postmaster.pid" });

$sb = wait_for_standby('back in the first directory');
ok($sb->poll_query_until('postgres', 'SELECT count(*) FROM t', $rows),
	'the rebuilt standby has the rows written while it was set aside');

my $log = substr(slurp_file($node->logfile), $offset);
my $rebuilt_at = index($log, 'not for this one, rebuilding the standby');
my $reported_at = index($log, 'standby is running on port');
ok($rebuilt_at >= 0 && $reported_at > $rebuilt_at,
	'the set-aside standby is not started on its old data before the rebuild');
ok(!$node->log_contains(qr/has not streamed from this server/, $offset),
	'and is never left fenced for not streaming');
like(
	slurp_file("$pgdata/dbblue_standby.slot"),
	qr/_standby$/m,
	'the slot is recorded as made for the first directory again');

# ---------------------------------------------------------------------------
# Tablespaces: the standby on this host would replay CREATE TABLESPACE with
# the same location and write over the primary's files, so it is refused
# while the feature is on or paused, and allowed once it is off.
# ---------------------------------------------------------------------------
my $tbsdir = $node->basedir . '/tbs';
mkdir $tbsdir or die "could not create $tbsdir: $!";

my ($tbs_ret, $tbs_out, $tbs_err) = $node->psql('postgres',
	"CREATE TABLESPACE tbs LOCATION '$tbsdir'");
ok( $tbs_ret != 0
	  && $tbs_err =~ /cannot create a tablespace while dbblue_create_standby is "on"/,
	'CREATE TABLESPACE is refused while dbblue_create_standby is on');

alter_system('SET dbblue_create_standby = paused');
($tbs_ret, $tbs_out, $tbs_err) = $node->psql('postgres',
	"CREATE TABLESPACE tbs LOCATION '$tbsdir'");
ok( $tbs_ret != 0
	  && $tbs_err =~ /cannot create a tablespace while dbblue_create_standby is "paused"/,
	'CREATE TABLESPACE is refused while dbblue_create_standby is paused');

alter_system('SET dbblue_create_standby = off');
$node->safe_psql('postgres', "CREATE TABLESPACE tbs LOCATION '$tbsdir'");
pass('CREATE TABLESPACE is allowed once dbblue_create_standby is off');
$node->safe_psql('postgres', 'DROP TABLESPACE tbs');

alter_system('SET dbblue_create_standby = on');
$sb = wait_for_standby('after the tablespace test');

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
