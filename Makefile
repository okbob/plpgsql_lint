# $PostgreSQL: pgsql/contrib/plpgsql_lint/Makefile,v 1.1 2008/07/29 18:31:20 tgl Exp $

MODULES = plpgsql_lint
MODULE_big = plpgsql_lint
OBJS = plpgsql_lint.o

ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif

REGRESS_OPTS = --dbname=$(PL_TESTDB) --load-language=plpgsql
REGRESS = plpgsql_lint-$(MAJORVERSION)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/plpgsql_lint
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

override CFLAGS += -I$(top_builddir)/src/pl/plpgsql/src

