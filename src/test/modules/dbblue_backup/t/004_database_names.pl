# Copyright (c) 2026, Cybrosys Technologies
#
# dbblue_backup_databases must accept every database name Odoo can create
# (^[a-zA-Z0-9][a-zA-Z0-9_.-]+$), with its case kept as written:
#
#   * a name containing "." was refused by the check hook -- in
#     postgresql.conf that stopped the server from starting at all;
#   * an unquoted mixed-case name was downcased, so "MyCompany" was looked up
#     as "mycompany", reported missing, and never backed up.
#
# Only names that cannot be a path component of the backup directory are
# refused: ".", "..", and anything containing "/".

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('database_names');
$node->init;

my $backupdir = $node->basedir . '/dbblue_backups';
mkdir $backupdir or die "could not create $backupdir: $!";

# The list is in postgresql.conf from the start: a dotted name there used to
# make the server refuse to start.
$node->append_conf(
	'postgresql.conf', qq(
dbblue_backup_enabled = on
dbblue_backup_database = 'postgres'
dbblue_backup_databases = 'acme.prod, MyCompany, "Sales, EU"'
dbblue_backup_directory = '$backupdir'
dbblue_backup_interval = 60
dbblue_backup_naptime = 10
));

ok($node->start(fail_ok => 1),
	'server starts with a dotted database name in dbblue_backup_databases');

is( $node->safe_psql('postgres', 'SHOW dbblue_backup_databases'),
	'acme.prod, MyCompany, "Sales, EU"',
	'the configured list is kept as written');

$node->safe_psql('postgres', 'CREATE DATABASE "acme.prod"');
$node->safe_psql('postgres', 'CREATE DATABASE "MyCompany"');
$node->safe_psql('postgres', 'CREATE DATABASE "Sales, EU"');

$node->poll_query_until(
	'postgres', q{
	SELECT count(DISTINCT database_name) FROM dbblue.dbblue_backup_history
	WHERE status = 'success'
	  AND database_name IN ('acme.prod', 'MyCompany', 'Sales, EU')},
	'3')
  or die "timed out waiting for backups of all three databases";

my @dotted = glob("'$backupdir/acme.prod/acme.prod_*.dump'");
is(scalar(@dotted), 1, 'a database whose name contains "." is backed up');

my @mixed = glob("'$backupdir/MyCompany/MyCompany_*.dump'");
is(scalar(@mixed), 1,
	'an unquoted mixed-case name is backed up under its exact name');

my @quoted = glob("'$backupdir/Sales, EU/Sales, EU_*.dump'");
is(scalar(@quoted), 1,
	'a double-quoted name containing a comma and a space is backed up');

ok( !$node->log_contains(qr/configured database "mycompany" does not exist/),
	'the mixed-case name is not downcased');

# Names that cannot be a path component are refused, quoted or not.
foreach my $bad ('a/b', '"a/b"', '..', '"."', 'acme.prod, ../etc')
{
	my $err = '';
	my $rc = $node->psql(
		'postgres',
		"ALTER SYSTEM SET dbblue_backup_databases = '$bad'",
		stderr => \$err,
		on_error_die => 0);
	ok($rc != 0 && $err =~ /cannot be used as a backup directory name/,
		"dbblue_backup_databases = '$bad' is refused");
}

$node->stop;
done_testing();
