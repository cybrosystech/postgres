#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_rewind
#
# Portions Copyright (c) 2013-2026, PostgreSQL Global Development Group
#
# src/bin/pg_rewind/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_rewind - synchronize a data directory with another one forked from"
PGAPPICON = win32

subdir = src/bin/pg_rewind
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) -DFRONTEND \
	-I$(top_srcdir)/src/include/tde \
	$(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

# External libraries libpgtde depends on.  Kept in step with src/bin/pg_waldump.
TDE_LIBS = -lssl -lcrypto -lcurl

# libpgtde resolves its json/base64/list references against libpgcommon and
# libpgfeutils, so those must be repeated after it on the link line.
LIBS += -L$(top_builddir)/src/common/tde -lpgtde \
	-L$(top_builddir)/src/fe_utils -lpgfeutils \
	-L$(top_builddir)/src/common -lpgcommon \
	-L$(top_builddir)/src/port -lpgport \
	$(TDE_LIBS)

OBJS = \
	$(WIN32RES) \
	datapagemap.o \
	file_ops.o \
	filemap.o \
	libpq_source.o \
	local_source.o \
	parsexlog.o \
	pg_rewind.o \
	tde_ops.o \
	timeline.o \
	xlogreader.o

EXTRA_CLEAN = xlogreader.c

all: pg_rewind

pg_rewind: $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) pg_rewind$(X) '$(DESTDIR)$(bindir)/pg_rewind$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_rewind$(X)'

clean distclean:
	rm -f pg_rewind$(X) $(OBJS) xlogreader.c
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
