# Copyright (c) 2026, Cybrosys Technologies
#
# The backup launcher writes to its history table as the bootstrap superuser,
# so that table must be one no other role controls.  It lives in the
# dedicated "dbblue" schema (not public, which the database owner owns and so
# could drop and replace the table in), and the launcher verifies ownership
# of both the schema and the table instead of adopting whatever
# CREATE ... IF NOT EXISTS finds.
#
#   * a "dbblue" schema created first by a non-superuser database owner is
#     refused, and accepted once its ownership is fixed;
#   * the database owner cannot drop the launcher's table;
#   * a history table left in public by an earlier version is moved into
#     dbblue with its rows, so cooldown and retention carry over.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('history_owner');
$node->init;

my $backupdir = $node->basedir . '/dbblue_backups';
mkdir $backupdir or die "could not create $backupdir: $!";

# Start disabled, so the database owner gets to create "dbblue" first.
$node->append_conf(
	'postgresql.conf', qq(
dbblue_backup_enabled = off
dbblue_backup_database = 'appdb'
dbblue_backup_databases = 'appdb'
dbblue_backup_directory = '$backupdir'
dbblue_backup_interval = 60
dbblue_backup_naptime = 10
));
$node->start;

my $superuser = $node->safe_psql('postgres', 'SELECT current_user');

$node->safe_psql('postgres', 'CREATE ROLE app LOGIN');
$node->safe_psql('postgres', 'CREATE DATABASE appdb OWNER app');

# ---------------------------------------------------------------------------
# A "dbblue" schema squatted by the (non-superuser) database owner is refused.
# ---------------------------------------------------------------------------
$node->safe_psql('appdb', 'CREATE SCHEMA dbblue',
	extra_params => [ '-U', 'app' ]);

my $offset = -s $node->logfile;
$node->safe_psql('postgres',
	'ALTER SYSTEM SET dbblue_backup_enabled = on; SELECT pg_reload_conf()');

$node->wait_for_log(
	qr/schema "dbblue" in database "appdb" is not owned by the bootstrap superuser, refusing to use it/,
	$offset);
pass('launcher refuses a dbblue schema owned by another role');

is( $node->safe_psql(
		'appdb', "SELECT to_regclass('dbblue.dbblue_backup_history') IS NULL"),
	't',
	'no history table is created in a refused schema');

# ---------------------------------------------------------------------------
# Once the schema belongs to the bootstrap superuser, the launcher proceeds.
# ---------------------------------------------------------------------------
$node->safe_psql('appdb', "ALTER SCHEMA dbblue OWNER TO \"$superuser\"");

$node->poll_query_until(
	'appdb', q{
	SELECT count(*) FROM dbblue.dbblue_backup_history
	WHERE database_name = 'appdb' AND status = 'success'}, '1')
  or die "timed out waiting for a successful backup of appdb";
pass('launcher uses the schema once the bootstrap superuser owns it');

is( $node->safe_psql(
		'appdb', q{
	SELECT c.relkind::text || ':' || pg_get_userbyid(c.relowner)
	FROM pg_class c WHERE c.oid = 'dbblue.dbblue_backup_history'::regclass}),
	"r:$superuser",
	'history table is an ordinary table owned by the bootstrap superuser');

# The database owner can no longer drop (and so replace) the table.
my $err = '';
my $rc = $node->psql(
	'appdb', 'DROP TABLE dbblue.dbblue_backup_history',
	extra_params => [ '-U', 'app' ],
	stderr => \$err,
	on_error_die => 0);
ok($rc != 0 && $err =~ /must be owner of table dbblue_backup_history/,
	'database owner cannot drop the history table');

# A non-superuser can still read it (needed for a non-superuser pg_dump).
is( $node->safe_psql(
		'appdb',
		'SELECT count(*) > 0 FROM dbblue.dbblue_backup_history',
		extra_params => [ '-U', 'app' ]),
	't',
	'non-superuser can read the history table through schema USAGE');

# ---------------------------------------------------------------------------
# Migration: a table left in public by an earlier version is moved to dbblue.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres',
	'ALTER SYSTEM SET dbblue_backup_enabled = off; SELECT pg_reload_conf()');
$node->safe_psql('postgres', 'CREATE DATABASE legacydb');
$node->safe_psql(
	'legacydb', q{
	CREATE TABLE public.dbblue_backup_history (
	  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	  database_name text NOT NULL,
	  backup_started_at timestamptz NOT NULL,
	  backup_finished_at timestamptz,
	  status text NOT NULL,
	  file_path text,
	  file_size_bytes bigint,
	  error_message text);
	CREATE INDEX dbblue_backup_history_db_status_idx
	  ON public.dbblue_backup_history (database_name, status, backup_finished_at);
	INSERT INTO public.dbblue_backup_history
	  (database_name, backup_started_at, backup_finished_at, status)
	VALUES ('legacydb', now(), now(), 'success');
});

# dbblue_backup_database is PGC_POSTMASTER, so pointing it at legacydb needs a
# restart.
$node->append_conf('postgresql.conf',
	"dbblue_backup_database = 'legacydb'\ndbblue_backup_databases = 'legacydb'\n");
$node->safe_psql('postgres',
	'ALTER SYSTEM SET dbblue_backup_enabled = on');
$offset = -s $node->logfile;
$node->restart;

$node->wait_for_log(
	qr/moved public\.dbblue_backup_history to schema "dbblue"/, $offset);
pass('legacy public.dbblue_backup_history is migrated');

is( $node->safe_psql(
		'legacydb', q{
	SELECT to_regclass('public.dbblue_backup_history') IS NULL,
	       (SELECT count(*) FROM dbblue.dbblue_backup_history
	        WHERE database_name = 'legacydb' AND status = 'success')}),
	't|1',
	'migrated table keeps its history rows (so the cooldown carries over)');

$node->stop;
done_testing();
