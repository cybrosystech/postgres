# contrib/pg_tde_utils/Makefile

MODULES = pg_tde_utils

EXTENSION = pg_tde_utils
DATA = pg_tde_utils--1.0.sql
PGFILEDESC = "pg_tde_utils - utility functions for transparent data encryption"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_tde_utils
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
