# contrib/zstd_toast_compat/Makefile

MODULE_big = zstd_toast_compat
OBJS = \
	$(WIN32RES) \
	zstd_toast_compat.o

EXTENSION = zstd_toast_compat
DATA = zstd_toast_compat--1.0.sql
PGFILEDESC = "zstd_toast_compat - read/write zstd-compressed TOAST data without --with-zstd"

SHLIB_LINK += -lzstd

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/zstd_toast_compat
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
