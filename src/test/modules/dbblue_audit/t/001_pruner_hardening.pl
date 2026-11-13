# Copyright (c) 2026, Cybrosys Technologies
#
# The dbblue audit pruner deletes expired audit rows as the bootstrap
# superuser, in every database named in dbblue_audit_database.  Two ways a
# database owner (an ordinary role) could make it run their code as superuser
# are closed:
#
#   * a planted audit log: before auditing has created the dbblue schema in a
#     database, its owner can create dbblue.dbblue_audit_log with a DELETE
#     trigger.  The pruner must refuse a log the bootstrap superuser does not
#     own, rather than delete from it;
#
#   * a planted function: the sweep calls make_interval(); a
#     public.make_interval(integer, integer, numeric) -- plus ALTER DATABASE
#     ... SET search_path putting public first -- must not be picked over
#     pg_catalog's.  The pruner pins search_path and the sweep is
#     schema-qualified.
#
# In both, the planted code records current_user into a table; it staying
# empty is the proof that the code never ran.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('audit_pruner');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
dbblue_audit_database = 'appdb,appdb2'
dbblue_audit_retention = '1 hour'
dbblue_audit_prune_naptime = 1
));
$node->start;

$node->safe_psql('postgres', 'CREATE ROLE app LOGIN');
$node->safe_psql('postgres', 'CREATE DATABASE appdb OWNER app');
$node->safe_psql('postgres', 'CREATE DATABASE appdb2 OWNER app');

# ---------------------------------------------------------------------------
# A-2: an audit log planted by the database owner is not swept.
# ---------------------------------------------------------------------------
my $offset = -s $node->logfile;
$node->safe_psql(
	'appdb', q{
	CREATE TABLE public.proof (who name);
	CREATE FUNCTION public.on_delete() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN
	  INSERT INTO public.proof VALUES (current_user);
	  RETURN OLD;
	END $$;
	CREATE SCHEMA dbblue;
	CREATE TABLE dbblue.dbblue_audit_log (id bigint, logged_at timestamptz);
	CREATE TRIGGER t BEFORE DELETE ON dbblue.dbblue_audit_log
	  FOR EACH ROW EXECUTE FUNCTION public.on_delete();
	INSERT INTO dbblue.dbblue_audit_log VALUES (1, now() - interval '1 day');
},
	extra_params => [ '-U', 'app' ]);

$node->wait_for_log(
	qr/dbblue audit pruner: not sweeping database "appdb": "dbblue\.dbblue_audit_log" or its schema is not owned by the bootstrap superuser/,
	$offset);
pass('pruner refuses an audit log owned by another role');

is($node->safe_psql('appdb', 'SELECT count(*) FROM public.proof'),
	'0', 'the planted DELETE trigger never ran');
is($node->safe_psql('appdb', 'SELECT count(*) FROM dbblue.dbblue_audit_log'),
	'1', 'nothing was deleted from the planted table');

# ---------------------------------------------------------------------------
# A-3: a planted make_interval() is not called by the sweep.
# ---------------------------------------------------------------------------

# A genuine audit log, created by the audit code itself (as the bootstrap
# superuser), holding one row that is past retention.
$node->safe_psql(
	'appdb2', q{
	CREATE TABLE public.t (id int PRIMARY KEY);
	SET dbblue_audit_operations = 'all';
	SET dbblue_audit_tables = 'public.t';
	INSERT INTO public.t VALUES (1);
});
is( $node->safe_psql(
		'appdb2', q{
	SELECT pg_get_userbyid(c.relowner) = current_user
	FROM pg_class c WHERE c.oid = 'dbblue.dbblue_audit_log'::regclass}),
	't', 'audit log in appdb2 was created by the bootstrap superuser');

# The database owner plants the function, and puts public ahead of
# pg_catalog for every new session in the database.
$node->safe_psql(
	'appdb2', q{
	CREATE TABLE public.proof (who name);
	CREATE FUNCTION public.make_interval(months integer, days integer,
	                                     secs numeric)
	  RETURNS interval LANGUAGE plpgsql AS $$
	BEGIN
	  INSERT INTO public.proof VALUES (current_user);
	  RETURN interval '1 hour';
	END $$;
	ALTER DATABASE appdb2 SET search_path = public, pg_catalog;
},
	extra_params => [ '-U', 'app' ]);

# Age the row past retention, so the next sweep deletes it.
$node->safe_psql('appdb2',
	"UPDATE dbblue.dbblue_audit_log SET logged_at = now() - interval '1 day'");

$node->poll_query_until('appdb2',
	'SELECT count(*) FROM dbblue.dbblue_audit_log', '0')
  or die "timed out waiting for the pruner to sweep appdb2";
pass('pruner still sweeps an audit log the bootstrap superuser owns');

is($node->safe_psql('appdb2', 'SELECT count(*) FROM public.proof'),
	'0', 'the planted make_interval() was never called');

$node->stop;
done_testing();
