# Copyright (c) 2026, Cybrosys Technologies
#
# Retention unlinks the dump file named in each pruned history row, running as
# the server's OS user.  The table's ACL keeps ordinary roles out, but members
# of pg_write_all_data bypass ACLs and can rewrite file_path.  Retention must
# therefore remove only files it could have written itself:
# <dbblue_backup_directory>/<dbname>/<dbname>_*.dump.
#
#   * a path outside the backup directory is not removed;
#   * a ".." path that starts inside the backup directory is not removed;
#   * a file in the backup directory that is not one of our dumps is not removed;
#   * ordinary pruning of a real dump still works.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('retention_path');
$node->init;

my $backupdir = $node->basedir . '/dbblue_backups';
mkdir $backupdir or die "could not create $backupdir: $!";

# retention_count = 0 keeps only the newest success, so every older row is
# pruned on the next successful backup.
$node->append_conf(
	'postgresql.conf', qq(
dbblue_backup_enabled = on
dbblue_backup_database = 'appdb'
dbblue_backup_databases = 'appdb'
dbblue_backup_directory = '$backupdir'
dbblue_backup_interval = 60
dbblue_backup_naptime = 10
dbblue_backup_retention_count = 0
));
$node->start;

$node->safe_psql('postgres', 'CREATE DATABASE appdb');

my $success_count = q{
	SELECT count(*) FROM dbblue.dbblue_backup_history
	WHERE database_name = 'appdb' AND status = 'success'};

$node->poll_query_until('appdb', $success_count, '1')
  or die "timed out waiting for the first backup of appdb";

# Files the attacker wants removed.
my $outside = $node->basedir . '/victim_outside';
my $dotdot = $node->basedir . '/victim_dotdot';
my $stray = "$backupdir/appdb/not_a_dump.conf";
for my $f ($outside, $dotdot, $stray)
{
	append_to_file($f, "do not delete\n");
}

# A role that is not a superuser, holding only pg_write_all_data.
$node->safe_psql('postgres',
	'CREATE ROLE writer LOGIN; GRANT pg_write_all_data TO writer');

# The attacker repoints the existing row, adds two more, and backdates them
# all so the cooldown has expired and they are the oldest, i.e. pruned next.
$node->safe_psql(
	'appdb', qq{
	UPDATE dbblue.dbblue_backup_history
	   SET file_path = '$outside',
	       backup_finished_at = now() - interval '3 days'
	 WHERE database_name = 'appdb' AND status = 'success';
	INSERT INTO dbblue.dbblue_backup_history
	       (database_name, backup_started_at, backup_finished_at, status, file_path)
	VALUES ('appdb', now() - interval '3 days', now() - interval '3 days',
	        'success', '$backupdir/appdb/../../victim_dotdot'),
	       ('appdb', now() - interval '3 days', now() - interval '3 days',
	        'success', '$stray');
	},
	extra_params => [ '-U', 'writer' ]);
pass('pg_write_all_data member rewrote file_path in the history table');

# Next backup runs retention over the three tampered rows.
my $tampered = q{
	SELECT count(*) FROM dbblue.dbblue_backup_history
	WHERE file_path NOT LIKE '%/appdb/appdb\_%.dump'};
$node->poll_query_until('appdb', $tampered, '0')
  or die "timed out waiting for retention to prune the tampered rows";

ok(-e $outside, 'retention did not remove a file outside the backup directory');
ok(-e $dotdot, 'retention did not follow ".." out of the backup directory');
ok(-e $stray,
	'retention did not remove a non-dump file inside the backup directory');

# Normal pruning still works: expire the cooldown, wait for the next backup,
# and check the previous real dump is gone.
my $old_dump = $node->safe_psql('appdb',
	"SELECT file_path FROM dbblue.dbblue_backup_history WHERE status = 'success'"
);
ok(-e $old_dump, 'current dump exists before the next backup');

$node->safe_psql(
	'appdb', q{
	UPDATE dbblue.dbblue_backup_history
	   SET backup_finished_at = now() - interval '1 day'
	 WHERE status = 'success'});
$node->poll_query_until(
	'appdb', qq{
	SELECT count(*) FROM dbblue.dbblue_backup_history
	WHERE status = 'success' AND file_path <> '$old_dump'}, '1')
  or die "timed out waiting for the next backup of appdb";

ok(!-e $old_dump, 'retention still removes a real dump it wrote');

$node->stop;
done_testing();
