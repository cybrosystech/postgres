# Copyright (c) 2026, Cybrosys Technologies
#
# Tests for the automatic pg_dump backup worker (dbblue_backup_*).
#
# Two fixes are exercised:
#
#   * dbblue_backup_retention_count = 0 must keep the newest backup (as its
#     documentation promises), not delete every backup -- the retention prune
#     used OFFSET <count>, so 0 matched and removed even the dump just taken,
#     leaving nothing to restore from;
#
#   * pg_dump is invoked with an explicit -U (the bootstrap superuser), rather
#     than being left to default to the operating-system user name, which need
#     not be a valid or privileged database role.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('auto_backup');
$node->init;

my $backupdir = $node->basedir . '/dbblue_backups';
mkdir $backupdir or die "could not create $backupdir: $!";

# debug3 so the worker's "executing \"<pg_dump ...>\"" line is logged, which is
# how the -U is asserted below (it is logged at DEBUG3).
$node->append_conf(
	'postgresql.conf', qq(
dbblue_backup_enabled = on
dbblue_backup_database = 'postgres'
dbblue_backup_databases = 'bktest'
dbblue_backup_directory = '$backupdir'
dbblue_backup_interval = 60
dbblue_backup_naptime = 10
dbblue_backup_retention_count = 0
log_min_messages = debug3
));

$node->start;

$node->safe_psql('postgres', 'CREATE DATABASE bktest');
$node->safe_psql('bktest',
	'CREATE TABLE x AS SELECT g FROM generate_series(1, 100) g');

# Wait for the worker to take its first successful backup of bktest.
my $success_count = qq{
	SELECT count(*) FROM public.dbblue_backup_history
	WHERE database_name = 'bktest' AND status = 'success'};

$node->poll_query_until('postgres', $success_count, '1')
  or die "timed out waiting for a successful backup of bktest";

# ---------------------------------------------------------------------------
# Bug 1: retention_count = 0 keeps exactly the newest backup, never zero.
# ---------------------------------------------------------------------------
is( $node->safe_psql('postgres', $success_count),
	'1', 'retention_count = 0 keeps the newest backup, not zero');

my @dumps = glob("$backupdir/bktest/*.dump");
is(scalar(@dumps), 1,
	'retention_count = 0 leaves exactly one dump file on disk');
ok(-s $dumps[0], 'the retained dump file is non-empty');

# ---------------------------------------------------------------------------
# Bug 2: pg_dump is invoked with an explicit -U, not left to the OS user.
# ---------------------------------------------------------------------------
ok( $node->log_contains(
		qr/dbblue backup launcher: executing ".*pg_dump.* -U /),
	'pg_dump is invoked with an explicit -U role');

# ---------------------------------------------------------------------------
# End-of-options marker: the database name is passed after "--" so a name
# beginning with "-" is taken as the dbname, not parsed as a pg_dump option.
# ---------------------------------------------------------------------------
ok( $node->log_contains(
		qr/dbblue backup launcher: executing ".*pg_dump.* -- /),
	'pg_dump receives the database name after a "--" end-of-options marker');

# ---------------------------------------------------------------------------
# Read-only grants: the history table is SELECT-able by any role but writable
# by none.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', 'CREATE ROLE dbblue_reader LOGIN');

is( $node->safe_psql(
		'postgres',
		'SELECT 1 FROM public.dbblue_backup_history LIMIT 1',
		extra_params => [ '-U', 'dbblue_reader' ]),
	'1', 'a non-superuser can SELECT the backup history');

my $err = '';
my $rc = $node->psql(
	'postgres',
	"INSERT INTO public.dbblue_backup_history (database_name, backup_started_at, status) "
	  . "VALUES ('x', now(), 'y')",
	extra_params => [ '-U', 'dbblue_reader' ],
	stderr => \$err,
	on_error_die => 0);
ok( $rc != 0 && $err =~ /permission denied/,
	'a non-superuser cannot INSERT into the backup history');

$node->stop;
done_testing();
