# Copyright (c) 2026, Cybrosys Technologies
#
# Retention used to run only right after a backup of the same database, so:
#
#   * a lowered dbblue_backup_retention_count took effect only at the
#     database's next successful backup -- never, for one that kept failing;
#   * failed attempts a database had since recovered from stayed forever;
#   * a dump whose "success" row could not be recorded stayed on disk, listed
#     nowhere, never pruned.
#
# Now retention runs every cycle for every configured database that exists,
# failed attempts older than the last success are cleared, and a dump whose
# row cannot be recorded is removed again and the attempt counted as failed.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('retention_cycle');
$node->init;

my $backupdir = $node->basedir . '/dbblue_backups';
mkdir $backupdir or die "could not create $backupdir: $!";

$node->append_conf(
	'postgresql.conf', qq(
dbblue_backup_enabled = on
dbblue_backup_database = 'postgres'
dbblue_backup_databases = 'db_a'
dbblue_backup_directory = '$backupdir'
dbblue_backup_interval = 3600
dbblue_backup_naptime = 10
dbblue_backup_retention_count = 7
));
$node->start;

$node->safe_psql('postgres', 'CREATE DATABASE db_a');
$node->safe_psql('postgres', 'CREATE DATABASE db_c');

sub count_rows
{
	my ($where) = @_;
	return $node->safe_psql('postgres',
		"SELECT count(*) FROM dbblue.dbblue_backup_history WHERE $where");
}

$node->poll_query_until('postgres',
	q{SELECT count(*) FROM dbblue.dbblue_backup_history
	  WHERE database_name = 'db_a' AND status = 'success'}, '1')
  or die "timed out waiting for the backup of db_a";

my ($real_id, $real_path) = split /\|/,
  $node->safe_psql('postgres',
	q{SELECT id, file_path FROM dbblue.dbblue_backup_history
	  WHERE database_name = 'db_a' AND status = 'success'});

# Three older successful backups, with their files, and failed attempts: two
# before the real backup (resolved since) and one after it (still current).
my @old_files;
foreach my $day (1 .. 3)
{
	my $file = "$backupdir/db_a/db_a_2000010${day}_000000.dump";
	open(my $fh, '>', $file) or die "could not create $file: $!";
	print $fh "old dump";
	close($fh);
	push @old_files, $file;
	$node->safe_psql('postgres', qq{
		INSERT INTO dbblue.dbblue_backup_history
		  (database_name, backup_started_at, backup_finished_at, status,
		   file_path, file_size_bytes)
		VALUES ('db_a', '2000-01-0$day', '2000-01-0$day', 'success',
		        '$file', 8)});
}
$node->safe_psql('postgres', q{
	INSERT INTO dbblue.dbblue_backup_history
	  (database_name, backup_started_at, backup_finished_at, status, error_message)
	VALUES ('db_a', '2000-01-04', '2000-01-04', 'failed', 'resolved 1'),
	       ('db_a', '2000-01-05', '2000-01-05', 'failed', 'resolved 2'),
	       ('db_a', now() + interval '1 minute', now() + interval '1 minute',
	        'failed', 'current')});

# ---------------------------------------------------------------------------
# Failed attempts older than the last success are cleared at the next cycle,
# with no backup taken.
# ---------------------------------------------------------------------------
$node->poll_query_until('postgres',
	q{SELECT count(*) FROM dbblue.dbblue_backup_history
	  WHERE database_name = 'db_a' AND status = 'failed'}, '1')
  or die "timed out waiting for resolved failures to be cleared";
is(count_rows(q{database_name = 'db_a' AND status = 'failed'
	AND error_message = 'current'}),
	'1', 'resolved failed attempts are cleared, a later one is kept');
is(count_rows(q{database_name = 'db_a' AND status = 'success'}),
	'4', 'successful backups within the retention count are kept');

# ---------------------------------------------------------------------------
# A lowered retention count takes effect at the next cycle, not at the next
# successful backup.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres',
	'ALTER SYSTEM SET dbblue_backup_retention_count = 1; SELECT pg_reload_conf()'
);
$node->poll_query_until('postgres',
	q{SELECT count(*) FROM dbblue.dbblue_backup_history
	  WHERE database_name = 'db_a' AND status = 'success'}, '1')
  or die "timed out waiting for the lowered retention count to apply";

is( $node->safe_psql('postgres',
		q{SELECT id FROM dbblue.dbblue_backup_history
		  WHERE database_name = 'db_a' AND status = 'success'}),
	$real_id,
	'a lowered retention count applies without a new backup, keeping the newest');
ok(!grep({ -e $_ } @old_files), 'the pruned backups\' files are removed');
ok(-e $real_path, 'the newest backup\'s file is kept');

# ---------------------------------------------------------------------------
# A dump whose "success" row cannot be recorded is removed again.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', q{
	CREATE FUNCTION public.reject_db_c_success() RETURNS trigger
	LANGUAGE plpgsql AS $$
	BEGIN
	  IF NEW.database_name = 'db_c' AND NEW.status = 'success' THEN
	    RAISE EXCEPTION 'simulated failure to record db_c';
	  END IF;
	  RETURN NEW;
	END $$;
	CREATE TRIGGER reject_db_c_success
	  BEFORE INSERT ON dbblue.dbblue_backup_history
	  FOR EACH ROW EXECUTE FUNCTION public.reject_db_c_success();
});

my $offset = -s $node->logfile;
$node->safe_psql('postgres',
	q{ALTER SYSTEM SET dbblue_backup_databases = 'db_a, db_c'; SELECT pg_reload_conf()}
);

$node->poll_query_until('postgres',
	q{SELECT count(*) FROM dbblue.dbblue_backup_history
	  WHERE database_name = 'db_c' AND status = 'failed'}, '1')
  or die "timed out waiting for the attempt on db_c";

is( $node->safe_psql('postgres',
		q{SELECT error_message FROM dbblue.dbblue_backup_history
		  WHERE database_name = 'db_c'}),
	'could not record the backup in dbblue_backup_history',
	'a backup whose row cannot be recorded counts as failed');
ok( $node->log_contains(
		qr/removing backup ".*db_c_\d{8}T\d{6}Z\.dump" of database "db_c", which could not be recorded/,
		$offset),
	'its removal is logged');
ok(!glob("'$backupdir/db_c/*.dump*'"), 'and no file of it is left');

$node->safe_psql('postgres',
	'DROP TRIGGER reject_db_c_success ON dbblue.dbblue_backup_history');

$node->stop;
done_testing();
