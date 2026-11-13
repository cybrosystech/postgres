# Copyright (c) 2026, Cybrosys Technologies

tests += {
  'name': 'dbblue_backup',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_auto_backup.pl',
      't/002_history_table_ownership.pl',
      't/003_retention_path_check.pl',
      't/004_database_names.pl',
      't/005_partial_dump_cleanup.pl',
    ],
  },
}
