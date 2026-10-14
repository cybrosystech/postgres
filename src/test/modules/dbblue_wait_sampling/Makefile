# src/test/modules/dbblue_wait_sampling/Makefile

# The default shared_preload_libraries this fork ships in a fresh cluster names
# these, so the test's initdb needs them present in the temp install.
EXTRA_INSTALL = contrib/pg_stat_statements contrib/hypopg contrib/pg_tde

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/dbblue_wait_sampling
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
