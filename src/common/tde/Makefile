#-------------------------------------------------------------------------
#
# Makefile
#    Makefile for src/common/tde
#
# This builds libpgtde.a, the transparent-data-encryption support shared by
# the frontend client utilities.  It holds the keyring, principal-key and
# WAL-encryption code that pg_waldump, pg_basebackup, pg_rewind, pg_resetwal
# and pg_checksums need in order to read or write encrypted WAL.
#
# Only a FRONTEND variant is built here.  The backend gets the same
# functionality from the pg_tde extension, so linking these objects into the
# server would only bloat it with libcrypto/libcurl for no benefit.
#
# The KMIP key provider is deliberately excluded; see pg_tde_fe.c for why.
#
# IDENTIFICATION
#    src/common/tde/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common/tde
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND -I$(top_srcdir)/src/include/tde $(CPPFLAGS)

OBJS = \
	enc_aes.o \
	enc_tde.o \
	keyring_api.o \
	keyring_curl.o \
	keyring_file.o \
	keyring_vault.o \
	pg_tde_fe.o \
	pg_tde_tdemap.o \
	pg_tde_utils.o \
	pg_tde_xlog_keys.o \
	pg_tde_xlog_smgr.o \
	tde_keyring.o \
	tde_keyring_parse_opts.o \
	tde_principal_key.o

all: libpgtde.a

libpgtde.a: $(OBJS)
	rm -f $@
	$(AR) $(AROPT) $@ $^

clean distclean:
	rm -f libpgtde.a $(OBJS)
