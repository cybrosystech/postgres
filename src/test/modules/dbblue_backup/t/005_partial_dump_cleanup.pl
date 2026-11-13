# Copyright (c) 2026, Cybrosys Technologies
#
# pg_dump used to write straight to the final <db>_<time>.dump name.  When the
# worker was killed mid-dump (immediate shutdown, crash-restart, kill -9,
# postmaster death, power loss) the partial file stayed, with no history row,
# so retention never removed it -- and it looked like any other backup.
#
# Now the dump is written to <db>_<time>.dump.partial and renamed to .dump only
# once complete and fsync'd, and leftover .partial files are removed before
# the database is dumped again:
#
#   * while pg_dump runs, its output is named .partial, never .dump;
#   * a dump that fails while the worker is alive (here: dbblue_backup_timeout)
#     leaves no file behind;
#   * a dump cut off by a crash leaves a .partial, which is removed before the
#     next backup of that database; other files are left alone;
#   * a completed backup is a .dump that matches its history row.
#
# pg_dump is held mid-dump by a lock on the database's one table: it creates
# its output file before it locks the tables it dumps.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('partial_dump');
$node->init;

my $backupdir = $node->basedir . '/dbblue_backups';
my $dbdir = "$backupdir/bktest";
mkdir $backupdir or die "could not create $backupdir: $!";

# Disabled at first, so the table can be locked before the first dump starts.
$node->append_conf(
	'postgresql.conf', qq(
dbblue_backup_enabled = off
dbblue_backup_database = 'postgres'
dbblue_backup_databases = 'bktest'
dbblue_backup_directory = '$backupdir'
dbblue_backup_interval = 60
dbblue_backup_naptime = 10
dbblue_backup_timeout = 2
));
$node->start;

$node->safe_psql('postgres', 'CREATE DATABASE bktest');
$node->safe_psql('bktest',
	'CREATE TABLE x AS SELECT g FROM generate_series(1, 100) g');

# Files in the database's backup directory, sorted.
sub dir_files
{
	opendir(my $dh, $dbdir) or return ();
	my @f = sort grep { !/^\.\.?$/ } readdir($dh);
	closedir($dh);
	return @f;
}

# Wait until a file matching $re appears in the backup directory.
sub wait_for_backup_file
{
	my ($re) = @_;
	my $max = 10 * $PostgreSQL::Test::Utils::timeout_default;
	for (1 .. $max)
	{
		my @hit = grep { /$re/ } dir_files();
		return $hit[0] if @hit;
		usleep(100_000);
	}
	die "timed out waiting for a file matching $re in $dbdir";
}

my $lock = $node->background_psql('bktest');
$lock->query_safe('BEGIN; LOCK TABLE x IN ACCESS EXCLUSIVE MODE');

$node->safe_psql('postgres',
	'ALTER SYSTEM SET dbblue_backup_enabled = on; SELECT pg_reload_conf()');

# ---------------------------------------------------------------------------
# A dump in progress is named .partial.
# ---------------------------------------------------------------------------
my $first = wait_for_backup_file(qr/^bktest_\d{8}_\d{6}\.dump\.partial$/);
ok(!grep({ /\.dump$/ } dir_files()),
	'while pg_dump runs, its output is .partial and no .dump exists');

# ---------------------------------------------------------------------------
# A failure the worker survives (the 2 s timeout) leaves no file behind.
# ---------------------------------------------------------------------------
$node->poll_query_until(
	'postgres', q{
	SELECT count(*) > 0 FROM dbblue.dbblue_backup_history
	WHERE database_name = 'bktest' AND status = 'failed'})
  or die "timed out waiting for the timed-out backup to be recorded";

ok(!-e "$dbdir/$first",
	'a dump that times out has its .partial file removed');

# ---------------------------------------------------------------------------
# A dump cut off by a crash leaves a .partial behind.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres',
	'ALTER SYSTEM SET dbblue_backup_timeout = 0; SELECT pg_reload_conf()');

# The next cycle retries (a failure sets no cooldown) and blocks on the lock.
my $crashed = wait_for_backup_file(qr/^bktest_\d{8}_\d{6}\.dump\.partial$/);

# Files that must survive the cleanup, and an older leftover that must not.
my @keep = (
	'other_20000101_000000.dump.partial',    # not this database's prefix
	'bktest_notes.txt',                      # not a .partial
	'bktest_20000101_000000.dump.orig');     # not a .partial
my $old_leftover = 'bktest_20000101_000000.dump.partial';
foreach my $f (@keep, $old_leftover)
{
	open(my $fh, '>', "$dbdir/$f") or die "could not create $dbdir/$f: $!";
	print $fh "x";
	close($fh);
}

$node->stop('immediate');
eval { $lock->quit; };

ok(-e "$dbdir/$crashed",
	'after a crash mid-dump the unfinished .partial file is still there');

# ---------------------------------------------------------------------------
# The next backup removes it, and produces a complete .dump.
# ---------------------------------------------------------------------------
$node->start;

$node->poll_query_until(
	'postgres', q{
	SELECT count(*) FROM dbblue.dbblue_backup_history
	WHERE database_name = 'bktest' AND status = 'success'},
	'1')
  or die "timed out waiting for a successful backup after the crash";

ok(!-e "$dbdir/$crashed", 'the .partial left by the crash is removed');
ok(!-e "$dbdir/$old_leftover", 'an older leftover .partial is removed too');
ok( $node->log_contains(
		qr/removed ".*\Q$crashed\E", left by a backup of database "bktest" that did not finish/
	),
	'the removal is logged');

foreach my $f (@keep)
{
	ok(-e "$dbdir/$f", "unrelated file $f is left alone");
}

my @dumps = grep { /^bktest_\d{8}_\d{6}\.dump$/ } dir_files();
is(scalar(@dumps), 1, 'exactly one completed .dump exists');
ok(!grep({ /^bktest_.*\.dump\.partial$/ } dir_files()),
	'no .partial file of this database remains');

my ($path, $size) = split /\|/,
  $node->safe_psql(
	'postgres', q{
	SELECT file_path, file_size_bytes FROM dbblue.dbblue_backup_history
	WHERE database_name = 'bktest' AND status = 'success'});
is($path, "$dbdir/$dumps[0]", 'the success row names the .dump file');
is(-s $path, $size, 'the .dump file has the size the success row records');

command_ok([ 'pg_restore', '--list', $path ],
	'the completed .dump is a readable archive');

$node->stop;
done_testing();
