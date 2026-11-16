# Copyright (c) 2026, Cybrosys Technologies
#
# dbblue_backup_directory was not checked at all, so dumps could be written
# into the data directory: set to the data directory or a directory inside it,
# set to a relative path (interpreted against the data directory), or reached
# through a symlink.  There every base backup copied them -- including the one
# the standby is built from -- and a database named after one of the server's
# own subdirectories (pg_wal, base, ...) would be dumped into it.
#
# Now the launcher does not back up into such a directory, and says why; the
# server still starts (a rejected value in postgresql.conf would stop it):
#
#   * the data directory itself, a directory inside it, a symlink into it, a
#     relative path, and a missing directory are each refused;
#   * with the data directory inside the backup directory, a database named
#     after it is refused, while other databases are backed up normally;
#   * ALTER SYSTEM warns about such a value, but accepts it.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('backup_location');
$node->init;

my $basedir = $node->basedir;
my $datadir = $node->data_dir;
my ($datadir_name) = $datadir =~ m{([^/]+)$};

$node->append_conf(
	'postgresql.conf', qq(
dbblue_backup_enabled = on
dbblue_backup_database = 'postgres'
dbblue_backup_databases = 'bktest'
dbblue_backup_directory = ''
dbblue_backup_interval = 3600
dbblue_backup_naptime = 10
));
$node->start;

$node->safe_psql('postgres', 'CREATE DATABASE bktest');
$node->safe_psql('postgres', qq(CREATE DATABASE "$datadir_name"));

# Point dbblue_backup_directory at $dir, restart, and wait for the launcher to
# log a message matching $re.
sub use_backup_directory
{
	my ($dir, $re) = @_;

	$node->adjust_conf('postgresql.conf', 'dbblue_backup_directory', "'$dir'");
	my $offset = -s $node->logfile;
	$node->restart;
	pass("the server starts with dbblue_backup_directory = '$dir'");
	$node->wait_for_log($re, $offset);
}

# ---------------------------------------------------------------------------
# Directories in the data directory are refused.
# ---------------------------------------------------------------------------
my $inside = qr/dbblue_backup_directory ".*" is the data directory or inside it, not backing up/;

use_backup_directory($datadir, $inside);
ok(!-e "$datadir/bktest", 'the data directory itself is refused');

mkdir "$datadir/sub" or die "could not create $datadir/sub: $!";
use_backup_directory("$datadir/sub", $inside);
ok(!-e "$datadir/sub/bktest", 'a directory inside the data directory is refused');

mkdir "$datadir/sub2" or die "could not create $datadir/sub2: $!";
symlink("$datadir/sub2", "$basedir/link_into_data")
  or die "could not create symlink: $!";
use_backup_directory("$basedir/link_into_data", $inside);
ok(!-e "$datadir/sub2/bktest", 'a symlink into the data directory is refused');

use_backup_directory('backups',
	qr/dbblue_backup_directory "backups" is not an absolute path, not backing up/);
ok(!-e "$datadir/backups", 'a relative path is refused');

use_backup_directory("$basedir/missing",
	qr/cannot use dbblue_backup_directory ".*missing": No such file or directory, not backing up/
);
ok(!-e "$basedir/missing", 'a missing directory is refused, not created');

# ---------------------------------------------------------------------------
# The data directory inside the backup directory: a database named after it
# would be dumped into it, so that one database is refused, and only that one.
# ---------------------------------------------------------------------------
$node->adjust_conf('postgresql.conf', 'dbblue_backup_databases',
	"'bktest, \"$datadir_name\"'");
use_backup_directory($basedir,
	qr/backup directory ".*" of database "\Q$datadir_name\E" is the data directory or inside it, skipping/
);
pass('a database whose directory would be the data directory is refused');

$node->poll_query_until(
	'postgres', q{
	SELECT count(*) FROM dbblue.dbblue_backup_history
	WHERE database_name = 'bktest' AND status = 'success'},
	'1')
  or die "timed out waiting for the backup of bktest";

my @dumps = glob("'$basedir/bktest/bktest_*.dump'");
is(scalar(@dumps), 1, 'another database is still backed up normally');

is( $node->safe_psql(
		'postgres', qq{
		SELECT error_message FROM dbblue.dbblue_backup_history
		WHERE database_name = '$datadir_name'}),
	'backup directory is inside the data directory',
	'the refusal is recorded in the backup history');

ok(!glob("'$datadir/${datadir_name}_*'"),
	'no dump was written into the data directory');

# ---------------------------------------------------------------------------
# ALTER SYSTEM warns, but accepts the value.
# ---------------------------------------------------------------------------
foreach my $case (
	[ $datadir, qr/is the data directory or inside it/ ],
	[ 'backups', qr/is not an absolute path/ ])
{
	my ($value, $re) = @$case;
	my $err = '';
	my $rc = $node->psql(
		'postgres',
		"ALTER SYSTEM SET dbblue_backup_directory = '$value'",
		stderr => \$err,
		on_error_die => 0);
	ok($rc == 0 && $err =~ /WARNING:.*$re/,
		"ALTER SYSTEM SET dbblue_backup_directory = '$value' warns but is accepted");
}
$node->safe_psql('postgres', 'ALTER SYSTEM RESET dbblue_backup_directory');

$node->stop;
done_testing();
