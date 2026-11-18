# contrib/pgvector/meson.build

# pgvector is vendored third-party code; upstream pgvector only ships a
# Makefile/PGXS build, with no meson.build of its own (true as of pgvector
# 0.8.7, the version vendored here). This file mirrors contrib/pgvector/
# Makefile for the meson build, the same way contrib/pg_partman/meson.build
# does for pg_partman.

vector_sources = files(
  'src/bitutils.c',
  'src/bitvec.c',
  'src/halfutils.c',
  'src/halfvec.c',
  'src/hnsw.c',
  'src/hnswbuild.c',
  'src/hnswinsert.c',
  'src/hnswscan.c',
  'src/hnswutils.c',
  'src/hnswvacuum.c',
  'src/ivfbuild.c',
  'src/ivfflat.c',
  'src/ivfinsert.c',
  'src/ivfkmeans.c',
  'src/ivfscan.c',
  'src/ivfutils.c',
  'src/ivfvacuum.c',
  'src/sparsevec.c',
  'src/vector.c',
)

if host_system == 'windows'
  vector_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'vector',
    '--FILEDESC', 'vector - vector data type and ivfflat/hnsw access methods',])
endif

# DBblue: -march=native is deliberately NOT used.  It ties the binary to the
# build host's CPU, so a package built on a new machine dies with SIGILL on
# older ones.
vector_cflags = []

# These are plain auto-vectorization/fast-math hints; unlike -march=native
# they're safe to add wherever the compiler accepts the flag at all (they
# simply won't apply, e.g. under MSVC, which is what has_argument() tests).
foreach arg : ['-ftree-vectorize', '-fassociative-math', '-fno-signed-zeros',
               '-fno-trapping-math', '-ffp-contract=fast']
  if cc.has_argument(arg)
    vector_cflags += [arg]
  endif
endforeach

vector = shared_module('vector',
  vector_sources,
  c_pch: pch_postgres_h,
  c_args: vector_cflags,
  kwargs: contrib_mod_args,
)
contrib_targets += vector

# The Makefile generates sql/vector--0.8.7.sql by copying sql/vector.sql at
# build time (DATA_built); do the same thing here instead of installing
# vector.sql directly, so the installed script is named the way PostgreSQL's
# extension loader (CREATE EXTENSION vector) expects.
vector_base_sql = custom_target('vector--0.8.7.sql',
  input: 'sql/vector.sql',
  output: 'vector--0.8.7.sql',
  command: [python, '-c', 'import sys,shutil; shutil.copyfile(sys.argv[1], sys.argv[2])', '@INPUT@', '@OUTPUT@'],
  install: true,
  install_dir: contrib_data_dir,
)
contrib_targets += vector_base_sql

install_data(
  'vector.control',
  'sql/vector--0.1.0--0.1.1.sql',
  'sql/vector--0.1.1--0.1.3.sql',
  'sql/vector--0.1.3--0.1.4.sql',
  'sql/vector--0.1.4--0.1.5.sql',
  'sql/vector--0.1.5--0.1.6.sql',
  'sql/vector--0.1.6--0.1.7.sql',
  'sql/vector--0.1.7--0.1.8.sql',
  'sql/vector--0.1.8--0.2.0.sql',
  'sql/vector--0.2.0--0.2.1.sql',
  'sql/vector--0.2.1--0.2.2.sql',
  'sql/vector--0.2.2--0.2.3.sql',
  'sql/vector--0.2.3--0.2.4.sql',
  'sql/vector--0.2.4--0.2.5.sql',
  'sql/vector--0.2.5--0.2.6.sql',
  'sql/vector--0.2.6--0.2.7.sql',
  'sql/vector--0.2.7--0.3.0.sql',
  'sql/vector--0.3.0--0.3.1.sql',
  'sql/vector--0.3.1--0.3.2.sql',
  'sql/vector--0.3.2--0.4.0.sql',
  'sql/vector--0.4.0--0.4.1.sql',
  'sql/vector--0.4.1--0.4.2.sql',
  'sql/vector--0.4.2--0.4.3.sql',
  'sql/vector--0.4.3--0.4.4.sql',
  'sql/vector--0.4.4--0.5.0.sql',
  'sql/vector--0.5.0--0.5.1.sql',
  'sql/vector--0.5.1--0.6.0.sql',
  'sql/vector--0.6.0--0.6.1.sql',
  'sql/vector--0.6.1--0.6.2.sql',
  'sql/vector--0.6.2--0.7.0.sql',
  'sql/vector--0.7.0--0.7.1.sql',
  'sql/vector--0.7.1--0.7.2.sql',
  'sql/vector--0.7.2--0.7.3.sql',
  'sql/vector--0.7.3--0.7.4.sql',
  'sql/vector--0.7.4--0.8.0.sql',
  'sql/vector--0.8.0--0.8.1.sql',
  'sql/vector--0.8.1--0.8.2.sql',
  'sql/vector--0.8.2--0.8.3.sql',
  'sql/vector--0.8.3--0.8.4.sql',
  'sql/vector--0.8.4--0.8.5.sql',
  'sql/vector--0.8.5--0.8.6.sql',
  'sql/vector--0.8.6--0.8.7.sql',
  kwargs: contrib_data_args,
)

install_headers(
  'src/halfvec.h',
  'src/sparsevec.h',
  'src/vector.h',
  install_dir: dir_include_extension / 'vector',
)

# pgvector keeps its regression tests under test/ (test/sql, test/expected)
# rather than in sql/ and expected/ at the top, so point the harness there,
# the same way the Makefile's REGRESS_OPTS does.  Keep this list in sync with
# test/sql/*.sql (the Makefile picks them up with a wildcard).
#
# "copy" is left out on purpose: it does \copy ... TO 'results/vector.bin',
# a path relative to psql's working directory.  Under make that is the
# directory pg_regress writes results/ into; under meson, testwrap runs from
# the source directory, where no results/ exists.  It still runs under
# "make check".
tests += {
  'name': 'pgvector',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'bit',
      'btree',
      'cast',
      'halfvec',
      'hnsw_bit',
      'hnsw_halfvec',
      'hnsw_sparsevec',
      'hnsw_vector',
      'ivfflat_bit',
      'ivfflat_halfvec',
      'ivfflat_vector',
      'sparsevec',
      'vector_type',
    ],
    'inputdir': meson.current_source_dir() / 'test',
    'expecteddir': meson.current_source_dir() / 'test',
    'regress_args': ['--load-extension=vector'],
  },
}
