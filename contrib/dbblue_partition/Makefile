# contrib/dbblue_partition/Makefile

MODULE_big = dbblue_partition
OBJS = \
	$(WIN32RES) \
	dbblue_partition.o

EXTENSION = dbblue_partition
DATA = dbblue_partition--1.0.sql
PGFILEDESC = "dbblue_partition - native table partitioning for Odoo/dbblue workloads"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/dbblue_partition
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
