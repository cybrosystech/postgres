# contrib/dbblue_columnar/Makefile

MODULE_big = dbblue_columnar
OBJS = \
	$(WIN32RES) \
	columnar_store.o \
	dbblue_columnar.o

EXTENSION = dbblue_columnar
DATA = dbblue_columnar--1.0.sql
PGFILEDESC = "dbblue_columnar - DBblue Columnar Engine (skeleton)"

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
