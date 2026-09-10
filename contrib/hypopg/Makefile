# contrib/hypopg/Makefile

MODULE_big = hypopg
OBJS = \
	$(WIN32RES) \
	hypopg.o \
	hypopg_index.o \
	import/hypopg_import.o \
	import/hypopg_import_index.o

EXTENSION = hypopg
DATA = $(wildcard *--*.sql)
PGFILEDESC = "hypopg - hypothetical indexes for PostgreSQL"

REGRESS = hypopg
REGRESS_OPTS = --inputdir=test

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/hypopg
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# MAJORVERSION is only defined after the include above
REGRESS += hypo_brin hypo_index_part hypo_include hypo_hash hypo_hide_index