# contrib/cobalt/Makefile

MODULE_big = cobalt
OBJS = \
	$(WIN32RES) \
	cobalt_build.o \
	cobalt_insert.o \
	cobalt_scan.o \
	cobalt_util.o \
	cobalt_vacuum.o

EXTENSION = cobalt
DATA = cobalt--1.0.sql
PGFILEDESC = "cobalt access method - bitmap-posting trigram index"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/cobalt
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
