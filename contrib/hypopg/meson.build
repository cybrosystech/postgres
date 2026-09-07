# contrib/hypopg/meson.build

# hypopg is vendored third-party code; upstream hypopg only ships a
# Makefile/PGXS build, with no meson.build of its own (true as of hypopg
# 1.4.2, the version vendored here). This file mirrors contrib/hypopg/
# Makefile for the meson build, the same way contrib/pgvector/meson.build
# and contrib/pg_partman/meson.build do for those modules.

hypopg_sources = files(
  'hypopg.c',
  'hypopg_index.c',
  'import/hypopg_import.c',
  'import/hypopg_import_index.c',
)

if host_system == 'windows'
  hypopg_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'hypopg',
    '--FILEDESC', 'hypopg - hypothetical indexes for PostgreSQL',])
endif

# The sources -- and include/hypopg.h itself -- refer to their own headers as
# "include/hypopg.h" etc., i.e. relative to this directory rather than to the
# including file. PGXS makes that resolve by putting the module directory on
# the include path (-I. -I$(srcdir) in src/Makefile.global); meson builds set
# implicit_include_directories: false, so state it explicitly here.
hypopg = shared_module('hypopg',
  hypopg_sources,
  include_directories: include_directories('.'),
  kwargs: contrib_mod_args,
)
contrib_targets += hypopg

# Plain DATA in the Makefile (the *--*.sql wildcard), not DATA_built, so the
# scripts are installed as-is rather than generated.
install_data(
  'hypopg.control',
  'hypopg--1.3.1.sql',
  'hypopg--1.3.1--1.4.0.sql',
  'hypopg--1.4.0.sql',
  'hypopg--1.4.0--1.4.1.sql',
  'hypopg--1.4.1.sql',
  'hypopg--1.4.1--1.4.2.sql',
  'hypopg--1.4.2.sql',
  kwargs: contrib_data_args,
)

# NOTE: hypopg's regression tests use a non-standard layout -- the input
# scripts live in test/sql while the expected output stays in expected/ at
# the module root (the Makefile passes only --inputdir=test), which is not
# where pg_regress looks for expected files under meson's test harness. As
# with contrib/pgvector, run them via the classic PGXS path instead:
#   cd contrib/hypopg && make installcheck
