# contrib/pg_tde/meson.build

# pg_tde is vendored third-party code. Upstream ships only a STANDALONE meson
# project that builds against an already-installed server via pg_config; that
# file is preserved beside this one as meson-standalone.build. It cannot be
# pulled in with subdir() because it calls project(), which meson only allows
# at the top of a build tree -- so this file mirrors contrib/pg_tde/Makefile
# for the in-tree meson build, the same way contrib/pgvector/meson.build does
# for pgvector.
#
# To build pg_tde standalone against an installed server instead, copy
# meson-standalone.build back over meson.build in a separate checkout.

# pg_tde's KMIP key provider and all of libkmip are C++. PostgreSQL's own
# meson project declares only C, so the C++ toolchain has to be requested
# here. If it isn't available, skip pg_tde rather than failing the build.
pg_tde_have_cpp = add_languages('cpp', native: false, required: false)

if not pg_tde_have_cpp
  message('pg_tde: no C++ compiler found, skipping')
  subdir_done()
endif

if not libcurl.found()
  message('pg_tde: libcurl not found, skipping')
  subdir_done()
endif

if not ssl.found()
  message('pg_tde: OpenSSL not found, skipping')
  subdir_done()
endif

# libkmip is a git submodule (subprojects/libkmip). Upstream builds it as a
# CMake subproject, but meson resolves subprojects only from the top-level
# subprojects/ directory, so its sources are compiled directly here instead.
# These two lists match kmipcore/CMakeLists.txt and kmipclient/CMakeLists.txt,
# which both simply glob every .cpp under their src/ directory.
pg_tde_kmip_dir = 'subprojects/libkmip'

if not fs.is_dir(pg_tde_kmip_dir / 'kmipcore' / 'src')
  message('pg_tde: subprojects/libkmip not checked out, skipping')
  message('pg_tde: run "git submodule update --init --recursive" in contrib/pg_tde')
  subdir_done()
endif

pg_tde_kmip_sources = files(
  pg_tde_kmip_dir / 'kmipcore/src/attributes_parser.cpp',
  pg_tde_kmip_dir / 'kmipcore/src/key_parser.cpp',
  pg_tde_kmip_dir / 'kmipcore/src/kmip_attributes.cpp',
  pg_tde_kmip_dir / 'kmipcore/src/kmip_basics.cpp',
  pg_tde_kmip_dir / 'kmipcore/src/kmip_errors.cpp',
  pg_tde_kmip_dir / 'kmipcore/src/kmip_formatter.cpp',
  pg_tde_kmip_dir / 'kmipcore/src/kmip_payloads.cpp',
  pg_tde_kmip_dir / 'kmipcore/src/kmip_protocol.cpp',
  pg_tde_kmip_dir / 'kmipcore/src/kmip_requests.cpp',
  pg_tde_kmip_dir / 'kmipcore/src/kmip_responses.cpp',
  pg_tde_kmip_dir / 'kmipcore/src/response_parser.cpp',
  pg_tde_kmip_dir / 'kmipcore/src/serialization_buffer.cpp',
  pg_tde_kmip_dir / 'kmipclient/src/IOUtils.cpp',
  pg_tde_kmip_dir / 'kmipclient/src/Key.cpp',
  pg_tde_kmip_dir / 'kmipclient/src/KmipClient.cpp',
  pg_tde_kmip_dir / 'kmipclient/src/KmipClientPool.cpp',
  pg_tde_kmip_dir / 'kmipclient/src/NetClientOpenSSL.cpp',
  pg_tde_kmip_dir / 'kmipclient/src/PEMReader.cpp',
  pg_tde_kmip_dir / 'kmipclient/src/PrivateKey.cpp',
  pg_tde_kmip_dir / 'kmipclient/src/PublicKey.cpp',
  pg_tde_kmip_dir / 'kmipclient/src/StringUtils.cpp',
  pg_tde_kmip_dir / 'kmipclient/src/SymmetricKey.cpp',
  pg_tde_kmip_dir / 'kmipclient/src/X509Certificate.cpp',
)

# Mirrors the OBJS list in contrib/pg_tde/Makefile, which in turn mirrors the
# shared_module('pg_tde', ...) target in meson-standalone.build. The frontend
# tools defined there (pg_tde_waldump, pg_tde_basebackup, ...) are not built
# here; they are compiled from patched copies of PostgreSQL's own src/bin
# sources under fetools/ and need their own targets.
pg_tde_sources = files(
  'src/access/pg_tde_tdemap.c',
  'src/access/pg_tde_xlog.c',
  'src/access/pg_tde_xlog_keys.c',
  'src/access/pg_tde_xlog_smgr.c',
  'src/catalog/tde_keyring.c',
  'src/catalog/tde_keyring_parse_opts.c',
  'src/catalog/tde_principal_key.c',
  'src/common/pg_tde_utils.c',
  'src/encryption/enc_aes.c',
  'src/encryption/enc_tde.c',
  'src/keyring/keyring_api.c',
  'src/keyring/keyring_curl.c',
  'src/keyring/keyring_file.c',
  'src/keyring/keyring_kmip.cpp',
  'src/keyring/keyring_vault.c',
  'src/pg_tde.c',
  'src/pg_tde_event_capture.c',
  'src/pg_tde_guc.c',
  'src/smgr/pg_tde_smgr.c',
)

pg_tde_inc = include_directories(
  'src/include',
  'fetools/pg@0@/include'.format(pg_version_major),
  pg_tde_kmip_dir / 'kmipcore/include',
  pg_tde_kmip_dir / 'kmipclient/include',
)

pg_tde = shared_module('pg_tde',
  pg_tde_sources + pg_tde_kmip_sources,
  include_directories: [pg_tde_inc],
  override_options: ['cpp_std=c++20'],
  link_language: 'cpp',
  kwargs: contrib_mod_args + {
    'dependencies': [ssl, libcurl, contrib_mod_args['dependencies']],
  },
)
contrib_targets += pg_tde

install_data(
  'pg_tde.control',
  'pg_tde--1.0.sql',
  'pg_tde--1.0--2.0.sql',
  'pg_tde--2.0--2.1.sql',
  'pg_tde--2.1--2.2.sql',
  kwargs: contrib_data_args,
)

# NOTE: pg_tde's regression tests need a configured key provider and a
# principal key before any tde_heap table can be created, so they are not
# wired into meson's test harness here. Run them via the PGXS path instead:
#   cd contrib/pg_tde && make installcheck
