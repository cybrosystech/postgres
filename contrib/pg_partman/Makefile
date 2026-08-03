# contrib/pg_partman/Makefile
#
# DBblue: this Makefile is the only file changed from the pristine
# pg_partman 5.4.3 release, so that the extension builds as part of the
# PostgreSQL contrib tree (same pattern as other vendored extensions).
# Pass USE_PGXS=1 to build against an installed server with the stock
# upstream behaviour.  When updating the vendored copy, carry this file
# over and drop everything else in wholesale.

EXTENSION = pg_partman
EXTVERSION := $(shell grep default_version $(EXTENSION).control | \
                sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

MODULES = src/pg_partman_bgw
# If you do not want the background worker: make NO_BGW=1
ifneq ($(NO_BGW),)
MODULES =
endif

DOCS    = $(wildcard doc/*.md)
SCRIPTS = $(wildcard bin/common/*.py)

# Update scripts ship as-is from the source dir
DATA = $(wildcard updates/*--*.sql)
# The base script is concatenated at build time, so it is DATA_built
DATA_built = sql/$(EXTENSION)--$(EXTVERSION).sql

ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_partman
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Version gate works for both build modes, since MAJORVERSION comes
# from Makefile.global in either case.
PG_VER_MIN = 14
ifeq ($(shell expr "$(PG_VER_MIN)" \<= "$(MAJORVERSION)"), 0)
$(error Minimum version of PostgreSQL required is $(PG_VER_MIN) (but have $(MAJORVERSION)))
endif

sql/$(EXTENSION)--$(EXTVERSION).sql: $(wildcard sql/types/*.sql sql/tables/*.sql \
                                        sql/functions/*.sql sql/procedures/*.sql)
	cat $^ > $@
