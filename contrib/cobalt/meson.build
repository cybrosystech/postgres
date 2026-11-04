# Copyright (c) 2026, Dbblue contributors

cobalt_sources = files(
  'cobalt_build.c',
  'cobalt_insert.c',
  'cobalt_scan.c',
  'cobalt_util.c',
  'cobalt_vacuum.c',
)

if host_system == 'windows'
  cobalt_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'cobalt',
    '--FILEDESC', 'cobalt access method - bitmap-posting trigram index',])
endif

cobalt = shared_module('cobalt',
  cobalt_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += cobalt

install_data(
  'cobalt.control',
  'cobalt--1.0.sql',
  kwargs: contrib_data_args,
)
