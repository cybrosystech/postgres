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
      't/006_failure_backoff.pl',
      't/007_history_timestamps.pl',
      't/008_backup_directory_location.pl',
      't/009_free_space.pl',
      't/010_retention_every_cycle.pl',
      't/011_unmanaged_backups.pl',
    ],
  },
}
