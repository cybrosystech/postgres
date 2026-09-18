# Copyright (c) 2026, PostgreSQL Global Development Group
#
# src/common/tde/meson.build
#
# Transparent-data-encryption support shared by the frontend client utilities.
# Holds the keyring, principal-key and WAL-encryption code that pg_waldump,
# pg_basebackup, pg_rewind, pg_resetwal and pg_checksums need in order to read
# or write encrypted WAL.
#
# Only a FRONTEND variant is built.  The backend gets the same functionality
# from the pg_tde extension, so linking these objects into the server would
# only bloat it with libcrypto/libcurl for no benefit.
#
# The KMIP key provider is deliberately excluded; see pg_tde_fe.c for why.

tde_sources = files(
  'enc_aes.c',
  'enc_tde.c',
  'keyring_api.c',
  'keyring_curl.c',
  'keyring_file.c',
  'keyring_vault.c',
  'pg_tde_fe.c',
  'pg_tde_tdemap.c',
  'pg_tde_utils.c',
  'pg_tde_xlog_keys.c',
  'pg_tde_xlog_smgr.c',
  'tde_keyring.c',
  'tde_keyring_parse_opts.c',
  'tde_principal_key.c',
)

tde_inc = include_directories('../../include/tde')

# The Vault key provider talks HTTPS, so libcurl and OpenSSL are hard
# requirements here.  Note that 'libcurl' is only defined at all when the
# libcurl option is not disabled, hence the is_variable() guard: without it a
# -Dlibcurl=disabled build fails with an opaque "Unknown variable" error
# instead of the explanation below.
if not is_variable('libcurl') or not libcurl.found()
  error('''TDE client support requires libcurl (for the Vault key provider).
Configure with -Dlibcurl=enabled, or disable TDE client support.''')
endif
if not ssl.found()
  error('TDE client support requires OpenSSL. Configure with -Dssl=openssl.')
endif

libpgtde = static_library('libpgtde',
  tde_sources,
  c_args: ['-DFRONTEND'],
  include_directories: [postgres_inc, tde_inc],
  dependencies: [frontend_common_code, ssl, libcurl],
  build_by_default: false,
  kwargs: default_lib_args + {
    'install': false,
  },
)

# Client utilities that need to read or write encrypted WAL pull this in.
tde_code = declare_dependency(
  link_with: libpgtde,
  include_directories: tde_inc,
  dependencies: [ssl, libcurl],
)
