# contrib/dbblue_admin/Makefile

EXTENSION = dbblue_admin
DATA = dbblue_admin--1.0.sql
PGFILEDESC = "dbblue_admin - ops helpers for DBblue"

ifdef USE_PGXS
# Out-of-tree (PGXS) builds.  PG_CONFIG defaults to whatever 'pg_config'
# resolves to via PATH — which on hosts with a system PostgreSQL package
# installed will be the wrong server.  Override explicitly when packaging
# against a specific build:
#
#   make USE_PGXS=1 PG_CONFIG=/path/to/dbblue/bin/pg_config install
#
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/dbblue_admin
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
