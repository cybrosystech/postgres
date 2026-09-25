# contrib/dbblue_toast_tools/Makefile

MODULE_big = dbblue_toast_tools
OBJS = \
	$(WIN32RES) \
	dbblue_toast_tools.o

EXTENSION = dbblue_toast_tools
DATA = dbblue_toast_tools--1.0.sql
PGFILEDESC = "dbblue_toast_tools - audit and normalize TOAST compression"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/dbblue_toast_tools
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
