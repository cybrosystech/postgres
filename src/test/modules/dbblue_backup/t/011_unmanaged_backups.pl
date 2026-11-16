# Copyright (c) 2026, Cybrosys Technologies
#
# Some dumps are kept but never removed by the launcher, on purpose: the last
# backups of a dropped database may be the only copy left, and retention only
# removes files in the current dbblue_backup_directory.  They are now listed
# in the log -- after every start, and daily -- for deletion by hand:
#
#   * dumps of a database dropped, or removed from dbblue_backup_databases;
#   * dumps in an earlier dbblue_backup_directory, whose rows retention now
#     keeps (the only record of where they are) until the file is deleted.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('unmanaged');
$node->init;

my $bkA = $node->basedir . '/bkA';
my $bkB = $node->basedir . '/bkB';
mkdir $bkA or die "could not create $bkA: $!";
mkdir $bkB or die "could not create $bkB: $!";

$node->append_conf(
	'postgresql.conf', qq(
dbblue_backup_enabled = on
dbblue_backup_database = 'postgres'
dbblue_backup_databases = 'db_keep, db_gone, db_drop'
dbblue_backup_directory = '$bkA'
dbblue_backup_interval = 3600
dbblue_backup_naptime = 10
));
$node->start;

$node->safe_psql('postgres', "CREATE DATABASE $_")
  foreach ('db_keep', 'db_gone', 'db_drop');

$node->poll_query_until('postgres',
	q{SELECT count(DISTINCT database_name) FROM dbblue.dbblue_backup_history
	  WHERE status = 'success'}, '3')
  or die "timed out waiting for the three backups";

ok(!$node->log_contains(qr/that it no longer manages/),
	'nothing is reported while every backup is managed');

sub dump_of
{
	my ($db) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT file_path FROM dbblue.dbblue_backup_history
		   WHERE database_name = '$db' AND status = 'success'});
}
my %dump = map { $_ => dump_of($_) } ('db_keep', 'db_gone', 'db_drop');

# ---------------------------------------------------------------------------
# A database removed from the list, and a dropped one.
# ---------------------------------------------------------------------------
my $offset = -s $node->logfile;
$node->safe_psql('postgres', 'DROP DATABASE db_drop');
$node->safe_psql('postgres',
	q{ALTER SYSTEM SET dbblue_backup_databases = 'db_keep, db_drop'; SELECT pg_reload_conf()}
);
$node->wait_for_log(qr/keeping 2 backups \(\d+ bytes\) that it no longer manages/,
	$offset);
pass('the unmanaged backups are reported at the next cycle, without a restart');

foreach my $db ('db_gone', 'db_drop')
{
	ok( $node->log_contains(
			qr/\Q$dump{$db}\E \(\d+ bytes\): database no longer backed up/,
			$offset),
		"$db is listed as no longer backed up");
}
ok(!$node->log_contains(qr/\Q$dump{db_keep}\E/, $offset),
	'a managed database\'s backup is not listed');
ok(-e $dump{db_gone} && -e $dump{db_drop}, 'neither is removed');

# Nothing changes over the next cycle (naptime 10 s), so it is not repeated.
sleep(12);
my @reports = slurp_file($node->logfile, $offset) =~ /that it no longer manages/g;
is(scalar(@reports), 1, 'an unchanged list is not logged again next cycle');

# ---------------------------------------------------------------------------
# dbblue_backup_directory changes: the dumps in the earlier one are listed.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres',
	"ALTER SYSTEM SET dbblue_backup_directory = '$bkB'");
$offset = -s $node->logfile;
$node->restart;
$node->wait_for_log(qr/keeping 3 backups \(\d+ bytes\) that it no longer manages/,
	$offset);
ok( $node->log_contains(
		qr/\Q$dump{db_keep}\E \(\d+ bytes\): in an earlier dbblue_backup_directory/,
		$offset),
	'a dump in the earlier backup directory is listed');

# A new backup in the new directory, with retention down to one: the old row
# is past the count, but its dump is in the earlier directory, so both stay.
$node->safe_psql('postgres', q{
	UPDATE dbblue.dbblue_backup_history
	SET backup_started_at = backup_started_at - interval '2 hours'
	WHERE database_name = 'db_keep'});
$node->safe_psql('postgres',
	'ALTER SYSTEM SET dbblue_backup_retention_count = 0; SELECT pg_reload_conf()');

$node->poll_query_until('postgres',
	q{SELECT count(*) FROM dbblue.dbblue_backup_history
	  WHERE database_name = 'db_keep' AND file_path LIKE '%/bkB/%'}, '1')
  or die "timed out waiting for the backup in the new directory";

# Let a whole cycle of retention (naptime 10 s) pass over it.
$offset = -s $node->logfile;
sleep(12);

my $old_row = qq{SELECT count(*) FROM dbblue.dbblue_backup_history
				 WHERE file_path = '$dump{db_keep}'};
is($node->safe_psql('postgres', $old_row),
	'1', 'the earlier directory\'s row is kept past the retention count');
ok(-e $dump{db_keep}, 'and so is its file');
ok(!$node->log_contains(qr/not removing/, $offset),
	'without a "not removing" warning');

# Once the file is deleted by hand, its row goes at the next cycle.
unlink $dump{db_keep} or die "could not remove $dump{db_keep}: $!";
$node->poll_query_until('postgres', $old_row, '0')
  or die "timed out waiting for the row of the deleted dump to go";
pass('the row goes once its file has been deleted by hand');

$node->stop;
done_testing();
