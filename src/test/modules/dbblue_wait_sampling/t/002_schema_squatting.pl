# Copyright (c) 2026, Cybrosys Technologies
#
# The wait sampler connects to its sampling database as the bootstrap superuser
# and creates dbblue_catalog + wait_history/wait_profile there with
# CREATE ... IF NOT EXISTS, which matches on name only.  In an Odoo deployment
# the sampling database is owned by a non-superuser, so someone could pre-create
# ("squat") those objects and have the superuser worker adopt them -- at worst a
# view with an INSTEAD OF trigger running attacker code as the superuser on
# INSERT.  The worker must instead verify it owns the objects (schema + each
# relation) and refuse anything it did not create, recovering on its own once the
# conflicting object is removed.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# ---------------------------------------------------------------------------
# Scenario 1: a NON-superuser squats the whole dbblue_catalog schema.
# ---------------------------------------------------------------------------
my $node = PostgreSQL::Test::Cluster->new('ws_squat');
$node->init;
$node->append_conf('postgresql.conf', q(
dbblue_wait_sampling_database = 'ws_target'
dbblue_wait_sampling_enabled = off
));
$node->start;

$node->safe_psql('postgres', 'CREATE DATABASE ws_target');

# A non-superuser (mirrors the Odoo role) plants dbblue_catalog + a fake table.
$node->safe_psql('ws_target', q(
	CREATE ROLE ws_attacker NOSUPERUSER;
	CREATE SCHEMA dbblue_catalog AUTHORIZATION ws_attacker;
	SET ROLE ws_attacker;
	CREATE TABLE dbblue_catalog.wait_history(evil text);
	RESET ROLE;
));

# dbblue_wait_sampling_database is PGC_POSTMASTER, so point at it and restart.
$node->safe_psql('postgres',
	"ALTER SYSTEM SET dbblue_wait_sampling_database = 'ws_target'");
$node->safe_psql('postgres', 'ALTER SYSTEM SET dbblue_wait_sampling_enabled = on');
my $off = -s $node->logfile;
$node->restart;

# The worker must refuse the foreign schema rather than grant on / write to it.
$node->wait_for_log(
	qr/schema "dbblue_catalog" exists but is not owned by the sampler/, $off);
pass('worker refuses a non-superuser-squatted dbblue_catalog schema');

# It is still running (backing off, not crash-looping) and wrote nothing.
is( $node->safe_psql('postgres', q{
		SELECT count(*) FROM pg_stat_activity
		WHERE backend_type = 'dbblue wait sampler'}),
	'1', 'worker survives the refusal (no crash loop)');
is($node->safe_psql('ws_target', 'SELECT count(*) FROM dbblue_catalog.wait_history'),
	'0', 'worker never writes into the squatted relation');

# Once the squatted schema is gone, the worker must self-heal: build its own
# (owned by the role it runs as) and start sampling.
$node->safe_psql('ws_target', 'DROP SCHEMA dbblue_catalog CASCADE');
$node->poll_query_until('ws_target', q{
	SELECT count(*) FROM pg_tables
	WHERE schemaname = 'dbblue_catalog' AND tablename = 'wait_history'}, '1')
  or die "worker did not rebuild dbblue_catalog after the squatter was removed";
is( $node->safe_psql('ws_target', q{
		SELECT c.relkind = 'r'
		   AND c.relowner = (SELECT oid FROM pg_roles WHERE rolname = current_user)
		FROM pg_class c
		WHERE c.relname = 'wait_history'
		  AND c.relnamespace = 'dbblue_catalog'::regnamespace}),
	't', 'self-healed wait_history is an ordinary table owned by the sampler');

$node->stop;

# ---------------------------------------------------------------------------
# Scenario 2: the schema owner looks right, but wait_history is a VIEW (the
# INSTEAD OF trigger RCE vector).  The per-relation relkind check must refuse.
# ---------------------------------------------------------------------------
my $node2 = PostgreSQL::Test::Cluster->new('ws_view');
$node2->init;
$node2->append_conf('postgresql.conf', q(
dbblue_wait_sampling_database = 'ws_target2'
dbblue_wait_sampling_enabled = off
));
$node2->start;

$node2->safe_psql('postgres', 'CREATE DATABASE ws_target2');
# Superuser-owned schema (so the schema-owner check passes), but wait_history is
# a view, not a table.
$node2->safe_psql('ws_target2', q(
	CREATE SCHEMA dbblue_catalog;
	CREATE VIEW dbblue_catalog.wait_history AS SELECT 1 AS pid;
));

$node2->safe_psql('postgres',
	"ALTER SYSTEM SET dbblue_wait_sampling_database = 'ws_target2'");
$node2->safe_psql('postgres', 'ALTER SYSTEM SET dbblue_wait_sampling_enabled = on');
my $off2 = -s $node2->logfile;
$node2->restart;

$node2->wait_for_log(
	qr/"dbblue_catalog\.wait_history" is not a table owned by the sampler/, $off2);
pass('worker refuses a wait_history squatted as a view');

is( $node2->safe_psql('postgres', q{
		SELECT count(*) FROM pg_stat_activity
		WHERE backend_type = 'dbblue wait sampler'}),
	'1', 'worker survives the view refusal (no crash loop)');

$node2->stop;

done_testing();
