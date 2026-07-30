# contrib/dbblue_partition/meson.build

dbblue_partition_bgw_sources = files('src/dbblue_partition_bgw.c')

if host_system == 'windows'
  dbblue_partition_bgw_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'dbblue_partition_bgw',
    '--FILEDESC', 'dbblue_partition_bgw - partition maintenance worker for DBblue',])
endif

dbblue_partition_bgw = shared_module('dbblue_partition_bgw',
  dbblue_partition_bgw_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += dbblue_partition_bgw

install_data(
  'dbblue_partition.control',
  'dbblue_partition--1.0.sql',
  'dbblue_partition--1.0--1.1.sql',
  'dbblue_partition--1.1--1.2.sql',
  'dbblue_partition--1.2--1.3.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'dbblue_partition',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'dbblue_partition',
    ],
  },
}
