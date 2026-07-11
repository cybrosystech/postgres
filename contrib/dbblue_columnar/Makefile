# contrib/dbblue_columnar/Makefile

MODULE_big = dbblue_columnar
OBJS = \
	$(WIN32RES) \
	columnar_scan.o \
	columnar_store.o \
	dbblue_columnar.o

EXTENSION = dbblue_columnar
DATA = dbblue_columnar--1.0.sql
PGFILEDESC = "dbblue_columnar - DBblue Columnar Engine (skeleton)"

# Differential regression test (columnar results must equal heap results).
# The module must be preloaded, so it runs under `make check` (a temp instance
# configured via the .conf), not `make installcheck`.
REGRESS = dbblue_columnar
REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/dbblue_columnar/dbblue_columnar.conf
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/dbblue_columnar
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
