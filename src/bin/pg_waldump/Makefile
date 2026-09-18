# src/bin/pg_waldump/Makefile

PGFILEDESC = "pg_waldump - decode and display WAL"
PGAPPICON=win32

# make these available to TAP test scripts
export TAR

subdir = src/bin/pg_waldump
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	$(RMGRDESCOBJS) \
	$(WIN32RES) \
	archive_waldump.o \
	compat.o \
	pg_waldump.o \
	rmgrdesc.o \
	xlogreader.o \
	xlogstats.o

# External libraries libpgtde depends on.  TODO: move to Makefile.global.in
# once the remaining client utilities are converted, so it is declared once.
TDE_LIBS = -lssl -lcrypto -lcurl

override CPPFLAGS := -DFRONTEND -I$(libpq_srcdir) \
	-I$(top_srcdir)/src/include/tde \
	$(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils

# libpgtde resolves its json/base64/list references against libpgcommon and
# libpgfeutils, so those must be repeated after it on the link line.
LIBS += -L$(top_builddir)/src/common/tde -lpgtde \
	-L$(top_builddir)/src/fe_utils -lpgfeutils \
	-L$(top_builddir)/src/common -lpgcommon \
	-L$(top_builddir)/src/port -lpgport \
	$(TDE_LIBS)

RMGRDESCSOURCES = $(sort $(notdir $(wildcard $(top_srcdir)/src/backend/access/rmgrdesc/*desc*.c)))
RMGRDESCOBJS = $(patsubst %.c,%.o,$(RMGRDESCSOURCES))


all: pg_waldump

pg_waldump: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

xlogstats.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

$(RMGRDESCSOURCES): % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) pg_waldump$(X) '$(DESTDIR)$(bindir)/pg_waldump$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_waldump$(X)'

clean distclean:
	rm -f pg_waldump$(X) $(OBJS) $(RMGRDESCSOURCES) xlogreader.c xlogstats.c
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
