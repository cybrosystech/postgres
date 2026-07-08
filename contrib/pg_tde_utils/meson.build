# Copyright (c) 2026, PostgreSQL Global Development Group

pg_tde_utils_sources = files(
  'pg_tde_utils.c',
)

if host_system == 'windows'
  pg_tde_utils_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_tde_utils',
    '--FILEDESC', 'pg_tde_utils - utility functions for transparent data encryption',])
endif

pg_tde_utils = shared_module('pg_tde_utils',
  pg_tde_utils_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_tde_utils

install_data(
  'pg_tde_utils.control',
  'pg_tde_utils--1.0.sql',
  kwargs: contrib_data_args,
)
