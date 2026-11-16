# Copyright (c) 2026, Cybrosys Technologies
#
# Nothing checked free disk space: pg_dump wrote until it finished or the disk
# was full.  With the backup directory on the data directory's file system,
# that stopped the database's own writes too, and could shut the server down.
#
# dbblue_backup_min_free_space (MB, 0 = off) is now kept free:
#
#   * a backup is not started unless the free space covers the size of the
#     database's last backup plus the margin;
#   * a running backup is stopped, and its .partial removed, as soon as free
#     space falls below the margin;
#   * a backup directory sharing the data directory's file system is warned
#     about when the margin is 0, and logged otherwise.
#
# Free space is made to fall mid-dump by fallocate'ing a file while pg_dump is
# held by a lock on the database's one table.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('free_space');
$node->init;

my $basedir = $node->basedir;
my $backupdir = "$basedir/dbblue_backups";
mkdir $backupdir or die "could not create $backupdir: $!";

$node->append_conf(
	'postgresql.conf', qq(
dbblue_backup_enabled = on
dbblue_backup_database = 'postgres'
dbblue_backup_databases = 'db_a'
dbblue_backup_directory = '$backupdir'
dbblue_backup_interval = 3600
dbblue_backup_naptime = 10
dbblue_backup_timeout = 0
dbblue_backup_min_free_space = 0
));
$node->start;

foreach my $db ('db_a', 'db_b')
{
	$node->safe_psql('postgres', "CREATE DATABASE $db");
	$node->safe_psql($db,
		'CREATE TABLE x AS SELECT g FROM generate_series(1, 1000) g');
}

sub set_and_reload
{
	my (%settings) = @_;
	foreach my $name (sort keys %settings)
	{
		$node->safe_psql('postgres',
			"ALTER SYSTEM SET $name = $settings{$name}");
	}
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
}

sub free_bytes
{
	my $out = `df -B1 --output=avail '$backupdir'`;
	my ($avail) = $out =~ /(\d+)\s*$/
	  or die "could not read free space from df: $out";
	return $avail;
}

sub history
{
	my ($db, $col) = @_;
	return $node->safe_psql(
		'postgres', qq{
		SELECT $col FROM dbblue.dbblue_backup_history
		WHERE database_name = '$db' ORDER BY id DESC LIMIT 1});
}

# ---------------------------------------------------------------------------
# A shared file system with the margin off is warned about.
# ---------------------------------------------------------------------------
$node->wait_for_log(
	qr/dbblue_backup_directory ".*" is on the same file system as the data directory, and dbblue_backup_min_free_space is 0/
);
pass('a backup directory on the data directory\'s file system is warned about');

$node->poll_query_until('postgres',
	q{SELECT count(*) FROM dbblue.dbblue_backup_history
	  WHERE database_name = 'db_a' AND status = 'success'}, '1')
  or die "timed out waiting for the first backup of db_a";
my $size = history('db_a', 'file_size_bytes');

# ---------------------------------------------------------------------------
# Not started without room for its last size plus the margin.
# ---------------------------------------------------------------------------
my $margin_mb = 1_000_000_000;    # about 1 PB: never available
my $needed = $margin_mb * 1024 * 1024 + $size;

# Expire db_a's cooldown so it is due again.
$node->safe_psql('postgres', q{
	UPDATE dbblue.dbblue_backup_history
	SET backup_started_at = backup_started_at - interval '2 hours',
	    backup_finished_at = backup_finished_at - interval '2 hours'
	WHERE database_name = 'db_a'});

my $offset = -s $node->logfile;
set_and_reload(dbblue_backup_min_free_space => $margin_mb);

$node->wait_for_log(
	qr/not enough free disk space to back up database "db_a": \d+ bytes free on the file system of ".*", $needed needed/,
	$offset);
pass('a backup is not started without room for its last size plus the margin');

$node->poll_query_until('postgres',
	q{SELECT count(*) FROM dbblue.dbblue_backup_history
	  WHERE database_name = 'db_a' AND status = 'failed'}, '1')
  or die "timed out waiting for the refusal to be recorded";
like(history('db_a', 'error_message'),
	qr/^not enough free disk space: \d+ bytes free, $needed needed$/,
	'the refusal is recorded with the space needed');
ok(!glob("'$backupdir/db_a/*.partial'"), 'no dump was started');

# ---------------------------------------------------------------------------
# Stopped as soon as free space falls below the margin.
# ---------------------------------------------------------------------------
my $lock = $node->background_psql('db_b');
$lock->query_safe('BEGIN; LOCK TABLE x IN ACCESS EXCLUSIVE MODE');

# Margin 300 MB below what is free now: enough for the dump to start.
$margin_mb = int((free_bytes() - 300 * 1024 * 1024) / (1024 * 1024));
$offset = -s $node->logfile;
set_and_reload(
	dbblue_backup_databases => "'db_b'",
	dbblue_backup_min_free_space => $margin_mb);

my $partial;
for (1 .. 10 * $PostgreSQL::Test::Utils::timeout_default)
{
	($partial) = glob("'$backupdir/db_b/db_b_*.dump.partial'");
	last if defined $partial;
	usleep(100_000);
}
ok(defined $partial, 'the dump starts while enough space is free');

# Use up 600 MB, taking free space 300 MB below the margin.
my $filler = "$basedir/filler";
system('fallocate', '-l', '600M', $filler) == 0
  or die "fallocate failed: $?";

$node->wait_for_log(
	qr/free space on the file system of ".*db_b" fell to \d+ bytes, below dbblue_backup_min_free_space \($margin_mb MB\), stopping pg_dump/,
	$offset);
pass('the running dump is stopped once free space falls below the margin');

$node->poll_query_until('postgres',
	q{SELECT count(*) FROM dbblue.dbblue_backup_history
	  WHERE database_name = 'db_b' AND status = 'failed'}, '1')
  or die "timed out waiting for the stopped dump to be recorded";
is( history('db_b', 'error_message'),
	'stopped: free disk space fell below dbblue_backup_min_free_space',
	'the stop is recorded');
ok(!-e $partial, 'the stopped dump\'s .partial file is removed');

unlink $filler or die "could not remove $filler: $!";
eval { $lock->quit; };

# ---------------------------------------------------------------------------
# With the margin on, the shared file system is only logged.
# ---------------------------------------------------------------------------
set_and_reload(dbblue_backup_min_free_space => 2048);
$offset = -s $node->logfile;
$node->restart;
$node->wait_for_log(
	qr/LOG:.*dbblue_backup_directory ".*" is on the same file system as the data directory; backups keep 2048 MB of it free/,
	$offset);
pass('with the margin on, the shared file system is only logged');

$node->stop;
done_testing();
