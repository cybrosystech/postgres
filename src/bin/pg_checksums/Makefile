#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_checksums
#
# Copyright (c) 1998-2026, PostgreSQL Global Development Group
#
# src/bin/pg_checksums/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_checksums - verify data checksums in an offline cluster"
PGAPPICON=win32

subdir = src/bin/pg_checksums
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# We need libpq only because fe_utils does.
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

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
	pg_checksums.o

all: pg_checksums

pg_checksums: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_checksums$(X) '$(DESTDIR)$(bindir)/pg_checksums$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_checksums$(X)'

clean distclean:
	rm -f pg_checksums$(X) $(OBJS)
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
