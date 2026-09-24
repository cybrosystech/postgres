# contrib/zstd_toast_reader/Makefile

MODULE_big = zstd_toast_reader
OBJS = \
	$(WIN32RES) \
	zstd_toast_reader.o

EXTENSION = zstd_toast_reader
DATA = zstd_toast_reader--1.0.sql
PGFILEDESC = "zstd_toast_reader - read zstd-compressed TOAST data without --with-zstd"

SHLIB_LINK += -lzstd

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/zstd_toast_reader
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
