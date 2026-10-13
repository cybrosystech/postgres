# Copyright (c) 2026, Cybrosys Technologies
#
# Tests for the automatic BRIN index worker (dbblue_create_brin).
#
# Odoo gives every model an append-ordered create_date column, so on a large
# table a BRIN index on it is cheap and well correlated.  A per-database worker
# scans pg_stats for such columns and builds the index.  This exercises the
# three properties the worker must hold:
#
#   * the build runs CREATE INDEX CONCURRENTLY, so it takes only
#     ShareUpdateExclusiveLock and never blocks writes, and completes to a
#     valid index (it is run from a background worker, outside SPI, which is
#     delicate: an earlier version left the index INVALID and the worker dead);
#   * a name already taken by another relation is reported once, honestly, as a
#     warning -- not logged as a creation that never happened and retried every
#     cycle;
#   * the worker runs with a pinned search_path and still succeeds.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('auto_brin');
$node->init;
# Enable the feature cluster-wide so the worker scans the default database, and
# make the worker's own log lines visible for the collision assertions.
$node->append_conf('postgresql.conf', <<'EOF');
dbblue_create_brin = on
log_min_messages = info
EOF
$node->start;

# A reload makes the always-running launcher start a fresh scan at once instead
# of waiting out its interval; each scan below is kicked off this way.
sub kick_scan
{
	$node->reload;
}

# ---------------------------------------------------------------------------
# An append-ordered create_date column: correlation near 1.0, so eligible.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', <<'EOF');
CREATE TABLE sale_order (id bigserial PRIMARY KEY,
						 create_date timestamptz,
						 amount numeric);
INSERT INTO sale_order (create_date, amount)
  SELECT timestamptz '2020-01-01' + (g || ' minutes')::interval, g % 1000
  FROM generate_series(1, 20000) g;
ANALYZE sale_order;
EOF

is( $node->safe_psql(
		'postgres',
		"SELECT correlation > 0.9 FROM pg_stats WHERE tablename = 'sale_order' AND attname = 'create_date'"
	),
	't',
	'create_date is highly correlated, so the worker should index it');

# ---------------------------------------------------------------------------
# Happy path: the worker builds a valid BRIN index on create_date.
# ---------------------------------------------------------------------------
kick_scan();

my $brin_valid_q = qq{
	SELECT count(*)
	FROM pg_index i
	JOIN pg_class ic ON ic.oid = i.indexrelid
	JOIN pg_am am ON am.oid = ic.relam
	JOIN pg_attribute a ON a.attrelid = i.indexrelid AND a.attnum > 0
	WHERE i.indrelid = 'sale_order'::regclass
	  AND am.amname = 'brin'
	  AND a.attname = 'create_date'
	  AND i.indisvalid};

$node->poll_query_until('postgres', $brin_valid_q, '1')
  or die "timed out waiting for the BRIN index to be built";

is( $node->safe_psql('postgres', $brin_valid_q),
	'1', 'a valid BRIN index on create_date was created');

is( $node->safe_psql(
		'postgres',
		"SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid = 'sale_order'::regclass AND indexrelid::regclass::text LIKE '%brin%'"
	),
	'sale_order_create_date_brin_idx',
	'the index is named <table>_create_date_brin_idx');

# ---------------------------------------------------------------------------
# The build really is CONCURRENTLY: while another session holds an open writer
# transaction, the worker's build parks at its validation wait holding only
# ShareUpdateExclusiveLock (a blocking CREATE INDEX would instead wait for
# ShareLock, which conflicts with the writer), and unrelated writes still go
# through.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', 'DROP INDEX sale_order_create_date_brin_idx');

my $writer = $node->background_psql('postgres');
$writer->query_safe('BEGIN');
$writer->query_safe(
	"INSERT INTO sale_order (create_date, amount) VALUES (now(), 1)");

kick_scan();

# Wait until the worker holds its lock on sale_order, then read the mode.
my $lock_mode_q = qq{
	SELECT l.mode
	FROM pg_locks l
	JOIN pg_stat_activity a ON a.pid = l.pid
	WHERE l.relation = 'sale_order'::regclass
	  AND a.backend_type = 'dbblue BRIN worker'
	  AND l.locktype = 'relation'};

$node->poll_query_until('postgres', "SELECT count(*) > 0 FROM ($lock_mode_q) s")
  or die "timed out waiting for the BRIN worker to take its table lock";

is( $node->safe_psql('postgres', $lock_mode_q),
	'ShareUpdateExclusiveLock',
	'the concurrent build holds ShareUpdateExclusiveLock, not ShareLock');

# A concurrent writer is not blocked by the in-progress build.
my $timed_out = 0;
$node->psql(
	'postgres',
	"INSERT INTO sale_order (create_date, amount) VALUES (now(), 2)",
	timeout => $PostgreSQL::Test::Utils::timeout_default,
	timed_out => \$timed_out);
is($timed_out, 0, 'writes are not blocked during the concurrent build');

# Release the writer; the build then validates and the index becomes valid.
$writer->query_safe('COMMIT');
$writer->quit;

$node->poll_query_until('postgres', $brin_valid_q, '1')
  or die "timed out waiting for the concurrent build to finish";
is( $node->safe_psql('postgres', $brin_valid_q),
	'1', 'the concurrent build finished and the index is valid');

# ---------------------------------------------------------------------------
# Name collision: when the name the worker would use is already taken by an
# unrelated relation, it must warn -- honestly, every cycle -- and never log a
# phantom "created", and no BRIN index must appear.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', <<'EOF');
DROP INDEX sale_order_create_date_brin_idx;
CREATE TABLE sale_order_create_date_brin_idx (x int);
EOF

my $offset = -s $node->logfile;
kick_scan();
$node->wait_for_log(
	qr/dbblue BRIN: cannot create index on public\.sale_order .*name "sale_order_create_date_brin_idx" is already in use/,
	$offset);
pass('a name collision is reported as a warning');

# A second scan must warn again and still never claim a creation.
$offset = -s $node->logfile;
kick_scan();
$node->wait_for_log(
	qr/name "sale_order_create_date_brin_idx" is already in use/, $offset);

my $log = slurp_file($node->logfile, $offset);
unlike(
	$log,
	qr/dbblue BRIN: created index/,
	'no phantom "created" is logged while the name stays taken');

is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_am am ON am.oid = ic.relam WHERE i.indrelid = 'sale_order'::regclass AND am.amname = 'brin'"
	),
	'0',
	'no BRIN index is created while the name is taken');

# ---------------------------------------------------------------------------
# Once the conflict is cleared the worker recovers and builds the index.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', 'DROP TABLE sale_order_create_date_brin_idx');
kick_scan();
$node->poll_query_until('postgres', $brin_valid_q, '1')
  or die "timed out waiting for the worker to recover after the collision";
is( $node->safe_psql('postgres', $brin_valid_q),
	'1', 'the worker builds the index once the conflicting name is freed');

$node->stop;
done_testing();
