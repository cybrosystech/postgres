# Copyright (c) 2026, Cybrosys Technologies
#
# Tests for the automatic repack worker (dbblue_repack_enabled).
#
# A launcher starts one worker per enabled database each cycle; the worker
# measures the bloat of every table in dbblue_repack_tables and runs
# REPACK (CONCURRENTLY) on those over dbblue_repack_threshold.  This covers:
#
#   * the happy path: a bloated table is repacked, shrinks, and gets a row in
#     public.dbblue_repack_history;
#   * a partitioned table in the list is expanded to its leaf partitions;
#   * the failure path: a table REPACK rejects is reported as a WARNING,
#     recorded with last_failed_at, and the worker goes on to the next table
#     (it used to crash here, in CopyErrorData());
#   * a repack yields to a session that waits for its lock, as autovacuum
#     does, so an ALTER TABLE (odoo -u) does not wait for the whole repack.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time);

my $node = PostgreSQL::Test::Cluster->new('auto_repack');
# REPACK (CONCURRENTLY) decodes the table's changes: it needs wal_level=logical.
$node->init(allows_streaming => 'logical');
# The naptime is long on purpose: each cycle below is started by a reload,
# so the test decides when the worker runs.
$node->append_conf('postgresql.conf', <<'EOF');
dbblue_repack_enabled = on
dbblue_repack_naptime = 3600
dbblue_repack_min_interval = 60
EOF
$node->start;

# A reload makes the launcher start a cycle at once instead of waiting out its
# naptime; the worker it starts reads the database's settings afresh.
sub run_cycle
{
	my ($tables) = @_;
	$node->safe_psql('postgres',
		"ALTER DATABASE postgres SET dbblue_repack_tables = '$tables'");
	$node->reload;
}

# A table of $rows rows with three quarters deleted and vacuumed: the free
# space stays in the file, so its bloat ratio is about 4.
sub make_bloated
{
	my ($name, $extra) = @_;
	$extra //= '';
	$node->safe_psql('postgres', <<"EOF");
INSERT INTO $name (id, pad) SELECT g, repeat('x', 200) FROM generate_series(1, 20000) g;
DELETE FROM $name WHERE id % 4 <> 0;
EOF
	$node->safe_psql('postgres', "VACUUM ANALYZE $name");
}

sub history
{
	my ($table, $cols) = @_;
	return $node->safe_psql('postgres',
		"SELECT $cols FROM public.dbblue_repack_history WHERE table_name = '$table'"
	);
}

my $history_exists =
  "SELECT to_regclass('public.dbblue_repack_history') IS NOT NULL";

# ---------------------------------------------------------------------------
# Happy path: a bloated table is repacked, shrinks and is recorded.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres',
	'CREATE TABLE sale_order (id int PRIMARY KEY, pad text)');
make_bloated('sale_order');

my $size_before =
  $node->safe_psql('postgres', "SELECT pg_table_size('sale_order')");

my $offset = -s $node->logfile;
run_cycle('public.sale_order');
$node->wait_for_log(
	qr/dbblue repack launcher: repacked "public\.sale_order" \(bloat ratio [\d.]+\)/,
	$offset);
pass('a bloated table is repacked');

my $size_after =
  $node->safe_psql('postgres', "SELECT pg_table_size('sale_order')");
cmp_ok($size_after, '<', $size_before / 2, 'the repacked table shrank');

is(history('sale_order', 'last_repack_at IS NOT NULL, last_failed_at IS NULL, repack_count'),
	't|t|1', 'the repack is recorded in dbblue_repack_history');

# The indexes were rebuilt with the table, not left empty.
cmp_ok($node->safe_psql('postgres', "SELECT pg_relation_size('sale_order_pkey')"),
	'>', 0, 'the primary key index of the repacked table is not empty');
is( $node->safe_psql('postgres', <<'EOF'),
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), sum(id) FROM sale_order WHERE id > 0;
EOF
	'5000|50010000',
	'an index scan on the repacked table finds every row');

# ---------------------------------------------------------------------------
# A partitioned table in the list is expanded to its leaf partitions, and each
# bloated leaf is repacked on its own.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', <<'EOF');
CREATE TABLE account_move (id int PRIMARY KEY, pad text) PARTITION BY RANGE (id);
CREATE TABLE account_move_p1 PARTITION OF account_move FOR VALUES FROM (0) TO (100000);
CREATE TABLE account_move_p2 PARTITION OF account_move FOR VALUES FROM (100000) TO (200000);
EOF
make_bloated('account_move');

$offset = -s $node->logfile;
run_cycle('public.account_move');
$node->wait_for_log(
	qr/dbblue repack launcher: repacked "public\.account_move_p1"/, $offset);
pass('a bloated leaf partition of a listed partitioned table is repacked');

is( $node->safe_psql('postgres',
		"SELECT count(*) FROM public.dbblue_repack_history WHERE table_name LIKE 'account_move%'"
	),
	'1',
	'only the bloated leaf is recorded, not the parent or the empty leaf');

# ---------------------------------------------------------------------------
# Failure path: REPACK (CONCURRENTLY) refuses a table with no primary key or
# replica identity.  The error must be caught, logged, recorded, and must not
# stop the cycle: the next table in the list is still repacked.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', <<'EOF');
CREATE TABLE no_pkey (id int, pad text);
CREATE TABLE stock_move (id int PRIMARY KEY, pad text);
EOF
make_bloated('no_pkey');
make_bloated('stock_move');

$offset = -s $node->logfile;
run_cycle('public.no_pkey, public.stock_move');
$node->wait_for_log(
	qr/WARNING:  dbblue repack launcher: repacking "public\.no_pkey" failed: /,
	$offset);
pass('a failed repack is reported as a warning');

$node->wait_for_log(
	qr/dbblue repack launcher: repacked "public\.stock_move"/, $offset);
pass('the worker goes on to the next table after a failure');

my $log = slurp_file($node->logfile, $offset);
unlike(
	$log,
	qr/terminated by signal|server process .* exited with exit code [1-9]|TRAP:/,
	'the failure does not crash the worker or the server');
like(
	$log,
	qr/repacking "public\.no_pkey" failed: .*\n.*DETAIL:  It will be tried again in 60 minutes at the earliest\./,
	'the warning says when the table will be retried');

is(history('no_pkey', 'last_failed_at IS NOT NULL, last_repack_at IS NULL'),
	't|t', 'the failure is recorded with last_failed_at');

# The next cycle leaves the failed table alone: it waits out the retry delay.
# A fresh bloated table in the same list shows the cycle really ran.
$node->safe_psql('postgres',
	'CREATE TABLE purchase_order (id int PRIMARY KEY, pad text)');
make_bloated('purchase_order');

$offset = -s $node->logfile;
run_cycle('public.no_pkey, public.purchase_order');
$node->wait_for_log(
	qr/dbblue repack launcher: repacked "public\.purchase_order"/, $offset);
$node->poll_query_until('postgres',
	"SELECT count(*) = 0 FROM pg_stat_activity WHERE backend_type = 'dbblue repack worker'"
) or die "timed out waiting for the repack worker to finish";
$log = slurp_file($node->logfile, $offset);
unlike($log, qr/repacking "public\.no_pkey"/,
	'a failed table is not retried before the retry delay');

# ---------------------------------------------------------------------------
# Rows changed while the repack runs are carried over to the new table and
# its indexes.  An index on a deliberately slow expression (1ms a row, about
# 5s for the 5000 live rows) keeps the repack running while they happen.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', <<'EOF');
CREATE FUNCTION slow_id(int) RETURNS int LANGUAGE plpgsql IMMUTABLE
  AS $$ BEGIN PERFORM pg_sleep(0.001); RETURN $1; END $$;
CREATE TABLE crm_lead (id int PRIMARY KEY, pad text);
EOF
make_bloated('crm_lead');
$node->safe_psql('postgres',
	'CREATE INDEX crm_lead_slow_idx ON crm_lead (slow_id(id))');

$offset = -s $node->logfile;
run_cycle('public.crm_lead');
$node->poll_query_until('postgres',
	"SELECT count(*) > 0 FROM pg_stat_activity WHERE query = 'dbblue repack launcher: repacking public.crm_lead'"
) or die "timed out waiting for the repack of crm_lead to start";

$node->safe_psql('postgres', "UPDATE crm_lead SET pad = 'changed' WHERE id <= 400");
$node->safe_psql('postgres',
	"INSERT INTO crm_lead SELECT g, 'new' FROM generate_series(100001, 100100) g");
$node->safe_psql('postgres', 'DELETE FROM crm_lead WHERE id BETWEEN 401 AND 800');

$node->wait_for_log(
	qr/dbblue repack launcher: (repacked|repacking) "public\.crm_lead"/, $offset);
like(slurp_file($node->logfile, $offset),
	qr/dbblue repack launcher: repacked "public\.crm_lead"/,
	'a table changed during its repack is repacked');

is( $node->safe_psql('postgres', <<'EOF'),
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), count(*) FILTER (WHERE pad = 'changed'),
       count(*) FILTER (WHERE pad = 'new')
FROM crm_lead WHERE id > 0;
EOF
	'5000|100|100',
	'the changes made during the repack are in the new table and its primary key');
is( $node->safe_psql('postgres', <<'EOF'),
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM crm_lead WHERE slow_id(id) > 0;
EOF
	'5000', 'and in its expression index');

# ---------------------------------------------------------------------------
# A repack yields to a session waiting for its lock.  REPACK (CONCURRENTLY)
# holds ShareUpdateExclusiveLock on the table throughout, which ALTER TABLE
# must wait for.  The slow expression index again keeps the repack running;
# meanwhile the ALTER must cancel it after deadlock_timeout and go through,
# instead of waiting for it to finish.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres',
	'CREATE TABLE res_partner (id int PRIMARY KEY, pad text)');
make_bloated('res_partner');
$node->safe_psql('postgres',
	'CREATE INDEX res_partner_slow_idx ON res_partner (slow_id(id))');

$offset = -s $node->logfile;
run_cycle('public.res_partner');
$node->poll_query_until('postgres',
	"SELECT count(*) > 0 FROM pg_stat_activity WHERE query = 'dbblue repack launcher: repacking public.res_partner'"
) or die "timed out waiting for the repack of res_partner to start";
# Make sure it holds its lock on the table before the ALTER queues up.
$node->poll_query_until('postgres',
	"SELECT count(*) > 0 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
	 WHERE l.relation = 'res_partner'::regclass AND l.granted
	   AND a.backend_type = 'dbblue repack worker'")
  or die "timed out waiting for the repack worker to lock res_partner";

my $start = time();
my ($ret, $stdout, $stderr) = $node->psql('postgres',
	'ALTER TABLE res_partner ADD COLUMN x int',
	timeout => $PostgreSQL::Test::Utils::timeout_default);
my $elapsed = time() - $start;
is($ret, 0, 'ALTER TABLE succeeds while a repack of the table is running');
cmp_ok($elapsed, '<', 30,
	'ALTER TABLE does not wait for the repack to finish');

$node->wait_for_log(
	qr/LOG:  dbblue repack launcher: repacking "public\.res_partner" cancelled: another session was waiting for a lock on it/,
	$offset);
pass('the repack is cancelled and says why');

is(history('res_partner', 'last_failed_at IS NOT NULL, last_repack_at IS NULL'),
	't|t', 'the cancelled repack is retried later, like a failure');

$node->stop;
done_testing();
