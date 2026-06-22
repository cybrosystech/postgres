# Copyright (c) 2022-2026, Cybrosys Technologies 
# contrib/dbblue_partition/meson.build

dbblue_partition_sources = files(
  'dbblue_partition.c',
)

if host_system == 'windows'
  dbblue_partition_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'dbblue_partition',
    '--FILEDESC', 'dbblue_partition - native table partitioning for Odoo workloads',])
endif

dbblue_partition = shared_module('dbblue_partition',
  dbblue_partition_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += dbblue_partition

install_data(
  'dbblue_partition.control',
  'dbblue_partition--1.0.sql',
  kwargs: contrib_data_args,
)
