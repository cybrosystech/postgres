#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_resetwal
#
# Copyright (c) 1998-2026, PostgreSQL Global Development Group
#
# src/bin/pg_resetwal/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_resetwal - reset PostgreSQL WAL log"
PGAPPICON=win32

subdir = src/bin/pg_resetwal
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils

# External libraries libpgtde depends on.  Kept in step with src/bin/pg_waldump.
TDE_LIBS = -lssl -lcrypto -lcurl

override CPPFLAGS := -DFRONTEND \
	-I$(top_srcdir)/src/include/tde \
	$(CPPFLAGS)

# libpgtde resolves its json/base64/list references against libpgcommon and
# libpgfeutils, so those must be repeated after it on the link line.
LIBS += -L$(top_builddir)/src/common/tde -lpgtde \
	-L$(top_builddir)/src/fe_utils -lpgfeutils \
	-L$(top_builddir)/src/common -lpgcommon \
	-L$(top_builddir)/src/port -lpgport \
	$(TDE_LIBS)

OBJS = \
	$(WIN32RES) \
	pg_resetwal.o

all: pg_resetwal

pg_resetwal: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_resetwal$(X) '$(DESTDIR)$(bindir)/pg_resetwal$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_resetwal$(X)'

clean distclean:
	rm -f pg_resetwal$(X) $(OBJS)
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
