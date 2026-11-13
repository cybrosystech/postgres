# Copyright (c) 2026, Cybrosys Technologies
#
# The cooldown (dbblue_backup_interval) runs from the last successful backup,
# so a database whose backups kept failing used to be retried every naptime,
# forever -- for a dump that always hit dbblue_backup_timeout, an hour-long
# pg_dump every five minutes.  Now, after the N-th failure in a row, the next
# attempt waits naptime * 2^(N-1), capped at dbblue_backup_interval:
#
#   * the wait doubles after each failure (naptime = 10 s: 10 s, 20 s, 40 s)
#     and is capped at the interval (60 s);
#   * a configuration reload ends the current wait, so a fixed problem is
#     retried at the next cycle -- but keeps the failure count, so if the
#     problem is not fixed the waits go on growing rather than start over;
#   * the backup that finally succeeds is logged with the failures before it.
#
# Backups are made to fail fast by making the database's backup directory
# read-only, so pg_dump cannot create its output file.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('failure_backoff');
$node->init;

my $backupdir = $node->basedir . '/dbblue_backups';
my $dbdir = "$backupdir/bktest";
mkdir $backupdir or die "could not create $backupdir: $!";
mkdir $dbdir or die "could not create $dbdir: $!";
chmod 0500, $dbdir or die "could not chmod $dbdir: $!";

# retention_count is high so that every failed attempt keeps its row.
$node->append_conf(
	'postgresql.conf', qq(
dbblue_backup_enabled = on
dbblue_backup_database = 'postgres'
dbblue_backup_databases = 'bktest'
dbblue_backup_directory = '$backupdir'
dbblue_backup_interval = 60
dbblue_backup_naptime = 10
dbblue_backup_timeout = 0
dbblue_backup_retention_count = 100
));
$node->start;

$node->safe_psql('postgres', 'CREATE DATABASE bktest');

my $offset = 0;

sub wait_for_failure
{
	my ($n, $delay) = @_;
	my $word = $n == 1 ? 'time' : 'times';
	$offset = $node->wait_for_log(
		qr/backup of database "bktest" has failed $n $word in a row, next attempt in $delay s/,
		$offset);
	pass("failure $n is followed by a wait of $delay s");
}

sub reload_now
{
	return $node->safe_psql('postgres',
		'SELECT extract(epoch FROM clock_timestamp()) FROM pg_reload_conf()');
}

sub failed_starts
{
	return split /\n/,
	  $node->safe_psql(
		'postgres', q{
		SELECT extract(epoch FROM backup_started_at)
		FROM dbblue.dbblue_backup_history
		WHERE database_name = 'bktest' AND status = 'failed'
		ORDER BY backup_started_at});
}

# ---------------------------------------------------------------------------
# The wait doubles after each failure.
# ---------------------------------------------------------------------------
wait_for_failure(1, 10);
wait_for_failure(2, 20);

my @started = failed_starts();
my $gap = $started[1] - $started[0];
ok($gap >= 9 && $gap <= 22,
	sprintf('attempt 2 started %.1f s after attempt 1 (wait 10 s)', $gap));

# ---------------------------------------------------------------------------
# A reload ends the 20 s wait, but keeps the count: the unfixed problem fails
# a 3rd time (not a 1st), and the wait after it is 40 s.
# ---------------------------------------------------------------------------
my $reloaded_at = reload_now();
$offset = $node->wait_for_log(
	qr/configuration reloaded, retrying 1 failing database at the next cycle/,
	$offset);

wait_for_failure(3, 40);

@started = failed_starts();
my $after = $started[2] - $reloaded_at;
ok($after < 15,
	sprintf('after a reload the next attempt came %.1f s later, not after the 20 s wait',
		$after));

# ---------------------------------------------------------------------------
# The wait is capped at the interval (60 s), and is otherwise waited out.
# ---------------------------------------------------------------------------
wait_for_failure(4, 60);

@started = failed_starts();
is(scalar(@started), 4, 'no attempt was made during a wait');
$gap = $started[3] - $started[2];
ok($gap >= 39 && $gap <= 52,
	sprintf('attempt 4 started %.1f s after attempt 3 (wait 40 s)', $gap));

# ---------------------------------------------------------------------------
# Fix the cause and reload: backed up at the next cycle, not after 60 s.
# ---------------------------------------------------------------------------
chmod 0700, $dbdir or die "could not chmod $dbdir: $!";
$reloaded_at = reload_now();

$node->poll_query_until(
	'postgres', q{
	SELECT count(*) FROM dbblue.dbblue_backup_history
	WHERE database_name = 'bktest' AND status = 'success'},
	'1')
  or die "timed out waiting for the backup after the reload";

my $success_started = $node->safe_psql(
	'postgres', q{
	SELECT extract(epoch FROM backup_started_at)
	FROM dbblue.dbblue_backup_history
	WHERE database_name = 'bktest' AND status = 'success'});
$after = $success_started - $reloaded_at;
ok($after < 15,
	sprintf('once fixed and reloaded, the backup ran %.1f s later, not after the 60 s wait',
		$after));

ok( $node->log_contains(
		qr/backed up database "bktest" after 4 failed attempts/, $offset),
	'the success is logged with the failures before it');

$node->stop;
done_testing();
