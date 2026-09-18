# contrib/pg_tde/Makefile
#
# Upstream pg_tde ships a standalone meson project and no Makefile at all
# (true as of pg_tde 2.2.2, the version vendored here). This file adds the
# classic PGXS / in-tree contrib build so that pg_tde is built and installed
# with the rest of the dbblue tree, the same way contrib/pgvector is.
#
# libkmip (subprojects/libkmip) is a git submodule and must be checked out
# before building:
#
#     git submodule update --init --recursive
#
# Upstream builds libkmip with CMake. We compile its sources directly instead,
# which keeps this build free of a CMake dependency; the two file lists below
# match kmipcore/CMakeLists.txt and kmipclient/CMakeLists.txt, both of which
# simply take every .cpp under their src/ directory.

MODULE_big = pg_tde
EXTENSION = pg_tde
PGFILEDESC = "pg_tde - transparent data encryption"

DATA = \
	pg_tde--1.0.sql \
	pg_tde--1.0--2.0.sql \
	pg_tde--2.0--2.1.sql \
	pg_tde--2.1--2.2.sql

# pg_tde's own backend sources. This mirrors the shared_module('pg_tde', ...)
# target in meson-standalone.build; the frontend tools defined there
# (pg_tde_waldump, pg_tde_basebackup, ...) are NOT built here, because they are
# compiled from patched copies of PostgreSQL's own src/bin sources under
# fetools/ and need their own build rules.
TDE_OBJS = \
	src/access/pg_tde_tdemap.o \
	src/access/pg_tde_xlog.o \
	src/access/pg_tde_xlog_keys.o \
	src/access/pg_tde_xlog_smgr.o \
	src/catalog/tde_keyring.o \
	src/catalog/tde_keyring_parse_opts.o \
	src/catalog/tde_principal_key.o \
	src/common/pg_tde_utils.o \
	src/encryption/enc_aes.o \
	src/encryption/enc_tde.o \
	src/keyring/keyring_api.o \
	src/keyring/keyring_curl.o \
	src/keyring/keyring_file.o \
	src/keyring/keyring_kmip.o \
	src/keyring/keyring_vault.o \
	src/pg_tde.o \
	src/pg_tde_event_capture.o \
	src/pg_tde_guc.o \
	src/smgr/pg_tde_smgr.o

# Vendored libkmip, compiled straight into the module.
KMIP = subprojects/libkmip
KMIP_OBJS = \
	$(patsubst %.cpp,%.o,$(wildcard $(KMIP)/kmipcore/src/*.cpp)) \
	$(patsubst %.cpp,%.o,$(wildcard $(KMIP)/kmipclient/src/*.cpp))

OBJS = $(TDE_OBJS) $(KMIP_OBJS)

PG_CPPFLAGS = \
	-I$(srcdir)/src/include \
	-I$(srcdir)/$(KMIP)/kmipcore/include \
	-I$(srcdir)/$(KMIP)/kmipclient/include

# The KMIP key provider (keyring_kmip.cpp) and all of libkmip are C++, so the
# module has to be linked against the C++ runtime even though every other
# object in it is C.
SHLIB_LINK = -lcrypto -lssl -lcurl -lpthread -lstdc++

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_tde
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# MAJORVERSION only exists once Makefile.global has been read, so this include
# directory has to be appended below the block above rather than set in
# PG_CPPFLAGS. fetools/pg$(MAJORVERSION) carries the version-matched copies of
# PostgreSQL's frontend headers that pg_tde's shared headers refer to.
override CPPFLAGS += -I$(srcdir)/fetools/pg$(MAJORVERSION)/include

# Makefile.global defines CXX and CXXFLAGS but no pattern rule for C++, and
# Makefile.shlib only adds CFLAGS_SL (-fPIC) to CFLAGS, not to CXXFLAGS -- so
# both have to be spelled out here.
%.o: %.cpp
	$(CXX) -std=c++20 $(CXXFLAGS) $(CFLAGS_SL) $(CPPFLAGS) -c -o $@ $<
