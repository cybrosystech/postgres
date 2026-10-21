# contrib/dbblue_columnar/meson.build

dbblue_columnar_sources = files(
  'columnar_scan.c',
  'columnar_store.c',
  'dbblue_columnar.c',
)

if host_system == 'windows'
  dbblue_columnar_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'dbblue_columnar',
    '--FILEDESC', 'dbblue_columnar - DBblue Columnar Engine (skeleton)',])
endif

dbblue_columnar = shared_module('dbblue_columnar',
  dbblue_columnar_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += dbblue_columnar

install_data(
  'dbblue_columnar.control',
  'dbblue_columnar--1.0.sql',
  'dbblue_columnar--1.1.sql',
  'dbblue_columnar--1.0--1.1.sql',
  kwargs: contrib_data_args,
)

# Differential regression test (columnar results must equal heap results).
tests += {
  'name': 'dbblue_columnar',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'dbblue_columnar',
    ],
    'regress_args': ['--temp-config', files('dbblue_columnar.conf')],
    # Disabled because the test requires
    # "shared_preload_libraries=dbblue_columnar".
    'runningcheck': false,
  },
}
