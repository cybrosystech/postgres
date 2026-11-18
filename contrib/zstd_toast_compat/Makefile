# contrib/zstd_toast_compat/Makefile

MODULE_big = zstd_toast_compat
OBJS = \
	$(WIN32RES) \
	zstd_toast_compat.o

EXTENSION = zstd_toast_compat
DATA = zstd_toast_compat--1.0.sql
PGFILEDESC = "zstd_toast_compat - read/write zstd-compressed TOAST data without --with-zstd"

# libzstd is needed whether or not the server was built --with-zstd -- the
# module exists for servers built without it, so configure's ZSTD_* settings
# can't be relied on.  Ask pkg-config, falling back to plain -lzstd.
ZSTD_COMPAT_CFLAGS := $(shell pkg-config --cflags libzstd 2>/dev/null)
ZSTD_COMPAT_LIBS := $(shell pkg-config --libs libzstd 2>/dev/null)
ifeq ($(strip $(ZSTD_COMPAT_LIBS)),)
ZSTD_COMPAT_LIBS = -lzstd
endif
PG_CPPFLAGS += $(ZSTD_COMPAT_CFLAGS)
SHLIB_LINK += $(ZSTD_COMPAT_LIBS)

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
