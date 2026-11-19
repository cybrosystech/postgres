
# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# DBblue raw bulk insert (dbblue_enable_tableam_multi_insert): durability.
#
# The fast path that populates a brand-new relation for CREATE TABLE AS and
# CREATE / REFRESH [CONCURRENTLY] MATERIALIZED VIEW bypasses the buffer manager
# and hands whole pages to bulk_write.c.  What makes those rows survive is
# therefore not the ordinary heap_insert() machinery but:
#
#   wal_level = replica   one full-page image per page (log_newpages()), and
#                         an fsync -- registered with the checkpointer, or done
#                         immediately if a checkpoint began during the load
#   wal_level = minimal   no WAL at all; smgrDoPendingSyncs() at commit either
#                         fsyncs the file or, below wal_skip_threshold,
#                         WAL-logs it whole
#
# None of that is visible to the regression suite, which never crashes, never
# changes wal_level and has no standby.  This test loads a set of relations
# through the fast path on a node at each wal_level and checks their contents
# after:
#
#   - streaming replay on a standby built from a backup taken BEFORE the loads,
#     so everything it has came from WAL alone (replica only);
#   - an immediate shutdown (crash) and crash recovery;
#   - amcheck's verify_heapam() and an offline pg_checksums --check.
#
# Every load is also checked to have really taken the fast path, from the
# backend's own I/O statistics: the buffer-managed path extends through a
# BAS_BULKWRITE ring (or local buffers, for a temp relation), the fast path
# through neither.  Without that, a fast path that had silently been disabled
# would make every check here pass vacuously.
#
# Nothing reads a loaded relation on the primary before the standby has been
# checked.  With data checksums on, the first read of a page after a checkpoint
# sets hint bits and WAL-logs the whole page (XLOG_FPI_FOR_HINT), which would
# hand the standby exactly the images a broken fast path had failed to log.
# The expected contents are therefore computed from the source tables, with
# the query each load ran, never from the loaded relations themselves.
#
# Limitation: an immediate shutdown kills processes but keeps the OS page
# cache, so a missing fsync of data that is not in WAL cannot be detected by a
# crash on the same machine.  The standby is what proves the WAL is complete;
# the crash cases prove that recovery leaves the loaded relations intact.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Relations whose contents must survive.
my @durable = qw(
  dbi_ctas dbi_small dbi_toast dbi_ckpt_during dbi_ckpt_after
  dbi_then_insert dbi_same_xact dbi_mv dbi_mvc);

# Count and checksum of the rows of $from_item (a relation or a parenthesized
# query), independent of physical order.
sub digest
{
	my ($node, $from_item) = @_;
	return $node->safe_psql('postgres',
		"SELECT count(*) || ':' || coalesce(md5(string_agg(r::text, ',' ORDER BY r::text)), '')
		   FROM $from_item r");
}

# Run $sql in one session, then check this backend's buffer-managed extends
# (see the header comment).  pg_stat_force_next_flush() makes the stats visible
# as soon as that statement's transaction ends.
sub load_fast
{
	my ($node, $name, $sql) = @_;
	my $out = $node->safe_psql(
		'postgres', qq{
$sql;
SELECT pg_stat_force_next_flush();
SELECT coalesce(sum(extends), 0)
  FROM pg_stat_get_backend_io(pg_backend_pid())
 WHERE (object = 'relation' AND context = 'bulkwrite')
    OR object = 'temp relation';
});
	my @lines = split /\n/, $out;
	is($lines[-1], '0', "$name: loaded through the raw bulk insert fast path");
	return;
}

sub setup_sources
{
	my ($node) = @_;
	$node->safe_psql(
		'postgres', q{
CREATE EXTENSION amcheck;

-- ~300 pages: many bulk-write flushes (dbblue_bulk_write_maxpages = 32) and
-- above the default wal_skip_threshold (2MB).
CREATE TABLE dbi_src AS
  SELECT g AS id,
         CASE WHEN g % 7 = 0 THEN NULL ELSE 'row ' || g END AS txt,
         (g * 1.5)::numeric AS num,
         repeat(chr(65 + g % 26), 60) AS pad
    FROM generate_series(1, 20000) g;

-- Out-of-line values: the toast rows go through the ordinary insert path
-- while the main fork is written raw.
CREATE TABLE dbi_toast_src AS
  SELECT g AS id,
         (SELECT string_agg(md5(g::text || ':' || i::text), '')
            FROM generate_series(1, 300) i) AS big
    FROM generate_series(1, 200) g;

-- Called from a CTAS's SELECT: checkpoints once, when the load is well under
-- way, so some pages are WAL-logged before the new redo pointer and some
-- after.  CHECKPOINT writes shared buffers only, which the fast path does not
-- use, so it is not a second writer.
CREATE FUNCTION dbi_checkpoint_at(id int, at int) RETURNS bool
  LANGUAGE plpgsql AS
  $$ BEGIN IF id = at THEN EXECUTE 'CHECKPOINT'; END IF; RETURN true; END $$;
});
	return;
}

# Load every case through the fast path.  Returns the expected digest of each
# durable relation, computed from the sources only (see the header comment).
sub run_loads
{
	my ($node, $label) = @_;
	my %expected;

	# What dbi_then_insert and dbi_same_xact hold after their extra writes.
	my $src_after_writes = sub {
		my ($txt_insert, $txt_update) = @_;
		return "(SELECT id, CASE WHEN id = 19999 THEN '$txt_update' ELSE txt END AS txt, num, pad
		           FROM dbi_src WHERE id <> 5
		         UNION ALL
		         SELECT -1, '$txt_insert', 0::numeric, '')";
	};

	load_fast($node, "$label CTAS, multi-flush",
		'CREATE TABLE dbi_ctas AS SELECT * FROM dbi_src');
	$expected{dbi_ctas} = digest($node, 'dbi_src');

	load_fast($node, "$label CTAS, small",
		'CREATE TABLE dbi_small AS SELECT * FROM dbi_src WHERE id <= 300');
	$expected{dbi_small} =
	  digest($node, '(SELECT * FROM dbi_src WHERE id <= 300)');

	load_fast($node, "$label CTAS, TOAST",
		'CREATE TABLE dbi_toast AS SELECT * FROM dbi_toast_src');
	$expected{dbi_toast} = digest($node, 'dbi_toast_src');

	my $redo_before = $node->safe_psql('postgres',
		'SELECT redo_lsn FROM pg_control_checkpoint()');
	load_fast($node, "$label CTAS, checkpoint during the load",
		'CREATE TABLE dbi_ckpt_during AS SELECT * FROM dbi_src WHERE dbi_checkpoint_at(id, 10000)'
	);
	isnt(
		$node->safe_psql('postgres',
			'SELECT redo_lsn FROM pg_control_checkpoint()'),
		$redo_before,
		"$label checkpoint really happened during the load");
	$expected{dbi_ckpt_during} = digest($node, 'dbi_src');

	load_fast(
		$node,
		"$label CTAS, checkpoint between load and commit",
		q{BEGIN;
		  CREATE TABLE dbi_ckpt_after AS SELECT * FROM dbi_src;
		  CHECKPOINT;
		  COMMIT});
	$expected{dbi_ckpt_after} = digest($node, 'dbi_src');

	# A later, ordinary (WAL-logged) write to a fast-path relation: its redo
	# must find the page the load left behind.  (The UPDATE's scan WAL-logs
	# every page for hint bits, so this relation -- like dbi_mvc, which
	# REFRESH CONCURRENTLY scans -- cannot detect a missing load-time image;
	# the other relations do.)
	load_fast($node, "$label CTAS, then writes in a later transaction",
		'CREATE TABLE dbi_then_insert AS SELECT * FROM dbi_src');
	$node->safe_psql(
		'postgres', q{
INSERT INTO dbi_then_insert VALUES (-1, 'later', 0, '');
UPDATE dbi_then_insert SET txt = 'later update' WHERE id = 19999;
DELETE FROM dbi_then_insert WHERE id = 5;
});
	$expected{dbi_then_insert} =
	  digest($node, $src_after_writes->('later', 'later update'));

	# Ordinary writes to the new relation in the loading transaction itself
	# (not WAL-logged at all under wal_level = minimal).
	load_fast(
		$node,
		"$label CTAS, then writes in the same transaction",
		q{BEGIN;
		  CREATE TABLE dbi_same_xact AS SELECT * FROM dbi_src;
		  INSERT INTO dbi_same_xact VALUES (-1, 'same xact', 0, '');
		  UPDATE dbi_same_xact SET txt = 'same xact update' WHERE id = 19999;
		  DELETE FROM dbi_same_xact WHERE id = 5;
		  COMMIT});
	$expected{dbi_same_xact} =
	  digest($node, $src_after_writes->('same xact', 'same xact update'));

	# A rolled-back load must leave nothing behind.
	load_fast($node, "$label CTAS, rolled back",
		'BEGIN; CREATE TABLE dbi_rolled_back AS SELECT * FROM dbi_src; ROLLBACK'
	);

	# Unlogged: the contents must be reset by crash recovery, not kept.  (An
	# unlogged relation's hint bits are never WAL-logged, so reading it here
	# hides nothing.)
	load_fast($node, "$label CTAS, unlogged",
		'CREATE UNLOGGED TABLE dbi_unlogged AS SELECT * FROM dbi_src');
	is($node->safe_psql('postgres', 'SELECT count(*) FROM dbi_unlogged'),
		'20000', "$label unlogged table loaded");

	# Materialized views: create, then refresh after the source changed (the
	# new contents are loaded into a transient table that is swapped in).
	load_fast($node, "$label CREATE MATERIALIZED VIEW",
		'CREATE MATERIALIZED VIEW dbi_mv AS SELECT * FROM dbi_src');
	load_fast($node, "$label CREATE MATERIALIZED VIEW, second",
		'CREATE MATERIALIZED VIEW dbi_mvc AS SELECT * FROM dbi_src');
	$node->safe_psql(
		'postgres', q{
CREATE UNIQUE INDEX dbi_mv_id ON dbi_mv (id);
CREATE UNIQUE INDEX dbi_mvc_id ON dbi_mvc (id);
UPDATE dbi_src SET txt = 'changed' WHERE id % 10 = 1;
DELETE FROM dbi_src WHERE id % 10 = 0;
INSERT INTO dbi_src SELECT g, 'new', 0, '' FROM generate_series(20001, 21000) g;
});
	load_fast($node, "$label REFRESH MATERIALIZED VIEW",
		'REFRESH MATERIALIZED VIEW dbi_mv');

	# CONCURRENTLY loads a temp transient table, then merges the differences
	# into the materialized view with ordinary WAL-logged writes.  The diff
	# table it fills is itself a buffer-managed temp table, so the I/O probe
	# cannot be used here; the regression test dbblue_bulk_insert checks that
	# this path is taken.
	$node->safe_psql('postgres',
		'REFRESH MATERIALIZED VIEW CONCURRENTLY dbi_mvc');

	$expected{dbi_mv} = digest($node, 'dbi_src');
	$expected{dbi_mvc} = $expected{dbi_mv};

	return \%expected;
}

# The expected contents must be found in every durable relation.
sub check_digests
{
	my ($node, $expected, $label) = @_;
	foreach my $rel (@durable)
	{
		is(digest($node, $rel), $expected->{$rel},
			"$label: $rel contents intact");
	}
	is($node->safe_psql('postgres', "SELECT to_regclass('dbi_rolled_back')"),
		'', "$label: rolled-back load left no relation");
	return;
}

# Heap structure (tuple headers, xmin, line pointers, toast pointers) on every
# loaded relation.
sub check_heap
{
	my ($node, $label) = @_;
	foreach my $rel (@durable)
	{
		is( $node->safe_psql(
				'postgres',
				"SELECT count(*) FROM verify_heapam('$rel', check_toast => true)"
			),
			'0',
			"$label: verify_heapam finds no corruption in $rel");
	}
	return;
}

# Every page of the stopped cluster carries a valid checksum.
sub check_checksums
{
	my ($node, $checksums, $label) = @_;
  SKIP:
	{
		skip "$label: data checksums are off", 1 unless $checksums eq 'on';
		command_ok(
			[ 'pg_checksums', '--check', '--pgdata' => $node->data_dir ],
			"$label: pg_checksums --check passes");
	}
	return;
}

my $common_conf = q{
dbblue_enable_tableam_multi_insert = on
fsync = on
autovacuum = off
};

# ----------------------------------------------------------------------
# wal_level = replica: standby replay, then crash recovery
# ----------------------------------------------------------------------

my $primary = PostgreSQL::Test::Cluster->new('replica_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', $common_conf);
$primary->start;
my $checksums = $primary->safe_psql('postgres', 'SHOW data_checksums');

setup_sources($primary);

# The backup is taken before any load: whatever the standby ends up with for
# the loaded relations it can only have got from WAL.
$primary->backup('before_loads');
my $standby = PostgreSQL::Test::Cluster->new('replica_standby');
$standby->init_from_backup($primary, 'before_loads', has_streaming => 1);
$standby->start;

my $expected = run_loads($primary, 'replica:');

# Standby first: nothing has read the loaded relations on the primary yet.
$primary->wait_for_catchup($standby, 'replay');
check_digests($standby, $expected, 'replica, standby replay');
check_heap($standby, 'replica, standby replay');

check_digests($primary, $expected, 'replica, primary');

$primary->stop('immediate');
$primary->start;
check_digests($primary, $expected, 'replica, after crash');
check_heap($primary, 'replica, after crash');
is($primary->safe_psql('postgres', 'SELECT count(*) FROM dbi_unlogged'),
	'0', 'replica, after crash: unlogged table was reset');

# The standby follows the restarted primary and still agrees with it.
$primary->safe_psql('postgres',
	"INSERT INTO dbi_then_insert VALUES (-2, 'after restart', 0, '')");
$expected->{dbi_then_insert} = digest($primary, 'dbi_then_insert');
$primary->wait_for_catchup($standby, 'replay');
check_digests($standby, $expected, 'replica, standby after primary crash');

$standby->stop;
check_checksums($standby, $checksums, 'replica, standby');
$primary->stop;
check_checksums($primary, $checksums, 'replica, primary');

# ----------------------------------------------------------------------
# wal_level = minimal: the loaded pages are never WAL-logged as they are
# written; crash recovery must still find them
# ----------------------------------------------------------------------

my $minimal = PostgreSQL::Test::Cluster->new('minimal');
$minimal->init;
$minimal->append_conf(
	'postgresql.conf', $common_conf . q{
wal_level = minimal
max_wal_senders = 0
});
$minimal->start;
$checksums = $minimal->safe_psql('postgres', 'SHOW data_checksums');

setup_sources($minimal);
$expected = run_loads($minimal, 'minimal:');

$minimal->stop('immediate');
$minimal->start;
check_digests($minimal, $expected, 'minimal, after crash');
check_heap($minimal, 'minimal, after crash');
is($minimal->safe_psql('postgres', 'SELECT count(*) FROM dbi_unlogged'),
	'0', 'minimal, after crash: unlogged table was reset');

# A second crash, after a checkpoint has made the loads part of the base
# image: replay from the new redo pointer must not disturb them either.
$minimal->safe_psql('postgres', 'CHECKPOINT');
$minimal->safe_psql('postgres',
	"INSERT INTO dbi_then_insert VALUES (-2, 'after restart', 0, '')");
$expected->{dbi_then_insert} = digest($minimal, 'dbi_then_insert');
$minimal->stop('immediate');
$minimal->start;
check_digests($minimal, $expected, 'minimal, after second crash');

$minimal->stop;
check_checksums($minimal, $checksums, 'minimal');

done_testing();
