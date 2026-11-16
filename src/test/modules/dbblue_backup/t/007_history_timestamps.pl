# Copyright (c) 2026, Cybrosys Technologies
#
# backup_finished_at is now(), the start time of the transaction that records
# the row, which a background worker's transaction copies from its statement
# start time.  The launcher set that only at the start of each cycle, so every
# row of a cycle got the cycle's start as its finish time: before the backup
# had even started, and the same for every database of the cycle.
#
#   * every row, success or failure, finishes no earlier than it started;
#   * a success finishes no earlier than its dump file was last written;
#   * the second database of a cycle finishes after the first one.
#
# A third database is made to fail by making its backup directory read-only.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(stat);

my $node = PostgreSQL::Test::Cluster->new('history_timestamps');
$node->init;

my $backupdir = $node->basedir . '/dbblue_backups';
mkdir $backupdir or die "could not create $backupdir: $!";
mkdir "$backupdir/db_fail" or die "could not create $backupdir/db_fail: $!";
chmod 0500, "$backupdir/db_fail" or die "could not chmod: $!";

# Disabled until the databases exist, so that all three are in one cycle.
$node->append_conf(
	'postgresql.conf', qq(
dbblue_backup_enabled = off
dbblue_backup_database = 'postgres'
dbblue_backup_databases = 'db_one, db_two, db_fail'
dbblue_backup_directory = '$backupdir'
dbblue_backup_interval = 3600
dbblue_backup_naptime = 10
));
$node->start;

foreach my $db ('db_one', 'db_two', 'db_fail')
{
	$node->safe_psql('postgres', "CREATE DATABASE $db");
	$node->safe_psql($db,
		'CREATE TABLE x AS SELECT g FROM generate_series(1, 100000) g');
}

$node->safe_psql('postgres',
	'ALTER SYSTEM SET dbblue_backup_enabled = on; SELECT pg_reload_conf()');

$node->poll_query_until(
	'postgres', q{
	SELECT count(*) FROM dbblue.dbblue_backup_history
	WHERE database_name IN ('db_one', 'db_two', 'db_fail')},
	'3')
  or die "timed out waiting for the three backups";

my %row;
foreach my $line (
	split /\n/,
	$node->safe_psql(
		'postgres', q{
		SELECT database_name, status,
		       extract(epoch FROM backup_started_at),
		       extract(epoch FROM backup_finished_at),
		       coalesce(file_path, '')
		FROM dbblue.dbblue_backup_history}))
{
	my ($db, $status, $started, $finished, $path) = split /\|/, $line;
	$row{$db} = {
		status => $status,
		started => $started,
		finished => $finished,
		path => $path
	};
}

is($row{db_one}{status}, 'success', 'db_one was backed up');
is($row{db_two}{status}, 'success', 'db_two was backed up');
is($row{db_fail}{status}, 'failed', 'db_fail failed');

foreach my $db ('db_one', 'db_two', 'db_fail')
{
	my $r = $row{$db};
	ok( $r->{finished} >= $r->{started},
		sprintf('%s: finished %.3f s after it started (not before)',
			$db, $r->{finished} - $r->{started}));
}

foreach my $db ('db_one', 'db_two')
{
	my $mtime = (stat($row{$db}{path}))[9];
	ok( $row{$db}{finished} >= $mtime - 0.01,
		sprintf('%s: finished %.3f s after its dump file was last written',
			$db, $row{$db}{finished} - $mtime));
}

# Backups run in list order, one at a time.
ok( $row{db_two}{finished} > $row{db_one}{finished},
	'the second database of the cycle finished after the first');

$node->stop;
done_testing();
