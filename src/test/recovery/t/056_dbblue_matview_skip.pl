# Copyright (c) 2026, PostgreSQL Global Development Group

# DBblue: restart and crash behaviour of REFRESH MATERIALIZED VIEW
# skip-if-unchanged (auto_skip_unchanged).
#
# The watermark that decides whether a refresh can be skipped lives in shared
# memory, so it does not survive a restart.  That has to fail safe: after a
# restart, or after a crash, the next refresh must rebuild rather than trust a
# watermark it no longer has.  A regression here is invisible in ordinary
# testing -- the matview would simply stop being refreshed -- so it is checked
# here rather than left to reasoning.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->start;

# Run a REFRESH and return the decision the feature logged: 'skipped', or the
# reason it gave for not skipping.  Reading it from the client's DEBUG output is
# more precise than timing the statement and quieter than raising
# log_min_messages for the whole server.
sub refresh_decision
{
	my ($matview) = @_;
	my ($ret, $stdout, $stderr) = $node->psql('postgres',
		"SET client_min_messages = debug1; REFRESH MATERIALIZED VIEW $matview;");

	die "REFRESH of $matview failed: $stderr" if $ret != 0;

	return 'skipped' if $stderr =~ /skipped, nothing it depends on has changed/;
	return $1       if $stderr =~ /not skipped: ([^\n]*)/;
	return 'no decision logged';
}

sub matview_matches_source
{
	return $node->safe_psql('postgres',
		'SELECT (SELECT c FROM mvr_mv) = (SELECT count(*) FROM mvr_src)');
}

$node->safe_psql('postgres', q{
	CREATE TABLE mvr_src(id int);
	INSERT INTO mvr_src SELECT g FROM generate_series(1, 100) g;
	CREATE MATERIALIZED VIEW mvr_mv WITH (auto_skip_unchanged=true) AS
	  SELECT count(*) AS c FROM mvr_src;
});

# Baseline: nothing has changed since CREATE recorded a watermark, so this is
# the case the feature exists for.
is(refresh_decision('mvr_mv'), 'skipped',
	'refresh is skipped when nothing has changed');
is(matview_matches_source(), 't', 'contents correct after a skipped refresh');

# ---------------------------------------------------------------- clean restart
$node->restart;

is(refresh_decision('mvr_mv'), 'no watermark from an earlier refresh',
	'after a clean restart the watermark is gone, so the refresh rebuilds');
is(matview_matches_source(), 't', 'contents correct after the post-restart rebuild');

# Having rebuilt once, the feature is armed again.
is(refresh_decision('mvr_mv'), 'skipped',
	'a watermark is re-established after the post-restart rebuild');

# A write is noticed again, which proves the write-path tracking resumed rather
# than staying disabled after the restart.
$node->safe_psql('postgres', 'INSERT INTO mvr_src VALUES (101)');
is(refresh_decision('mvr_mv'), 'a source was written',
	'writes are tracked again after a restart');
is(matview_matches_source(), 't', 'contents correct after a tracked write');
is(refresh_decision('mvr_mv'), 'skipped', 'and it settles back to skipping');

# ---------------------------------------------------------------------- crash
# An immediate stop leaves shared memory to be reinitialised from scratch, the
# same as any unplanned outage.
$node->stop('immediate');
$node->start;

is(refresh_decision('mvr_mv'), 'no watermark from an earlier refresh',
	'after a crash the watermark is gone, so the refresh rebuilds');
is(matview_matches_source(), 't', 'contents correct after the post-crash rebuild');

# The case worth being careful about: immediately after a restart no source is
# registered yet, and the write path short-circuits on exactly that condition.
# A write landing in that window must still not be lost -- here the refresh has
# no watermark to trust, so it rebuilds and picks the row up.
$node->stop('immediate');
$node->start;

$node->safe_psql('postgres', 'INSERT INTO mvr_src VALUES (102)');
isnt(refresh_decision('mvr_mv'), 'skipped',
	'a write made before anything was registered does not get skipped over');
is(matview_matches_source(), 't',
	'contents correct for a write made in the unregistered window');

# And tracking is properly armed afterwards.
is(refresh_decision('mvr_mv'), 'skipped', 'settles back to skipping');
$node->safe_psql('postgres', 'INSERT INTO mvr_src VALUES (103)');
is(refresh_decision('mvr_mv'), 'a source was written',
	'a later write is tracked normally');
is(matview_matches_source(), 't', 'contents correct at the end');

# --------------------------------------------- crash mid-refresh, uncommitted
# A refresh that is still open when the server dies must not leave anything
# behind that a later refresh would trust.  The watermark is written eagerly,
# inside the refreshing transaction, so this is the case that would expose it if
# the matview's post-swap relfilenumber were not acting as the commit witness.
$node->safe_psql('postgres', 'INSERT INTO mvr_src VALUES (104)');
my $before = $node->safe_psql('postgres', 'SELECT c FROM mvr_mv');

my $bg = $node->background_psql('postgres');
$bg->query_safe('BEGIN');
$bg->query_safe('REFRESH MATERIALIZED VIEW mvr_mv');
# Deliberately not committed: kill the server with the refresh still open.
$node->stop('immediate');
eval { $bg->quit; };

$node->start;

is($node->safe_psql('postgres', 'SELECT c FROM mvr_mv'), $before,
	'a refresh still open at crash time did not reach the matview');
isnt(refresh_decision('mvr_mv'), 'skipped',
	'and the next refresh rebuilds rather than trusting its watermark');
is(matview_matches_source(), 't', 'contents correct after that rebuild');

$node->stop;
done_testing();
