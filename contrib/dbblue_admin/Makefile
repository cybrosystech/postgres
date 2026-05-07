# contrib/dbblue_admin/Makefile

EXTENSION = dbblue_admin
DATA = dbblue_admin--1.0.sql
PGFILEDESC = "dbblue_admin - ops helpers for DBblue"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/dbblue_admin
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
