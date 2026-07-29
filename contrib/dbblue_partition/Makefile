# contrib/dbblue_partition/Makefile

EXTENSION = dbblue_partition
MODULES = src/dbblue_partition_bgw
DATA = dbblue_partition--1.0.sql dbblue_partition--1.0--1.1.sql \
	dbblue_partition--1.1--1.2.sql
PGFILEDESC = "dbblue_partition - convert live Odoo tables to native range partitioning"

REGRESS = dbblue_partition

# The regression database needs pg_partman available in the test install.
EXTRA_INSTALL = contrib/pg_partman

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
