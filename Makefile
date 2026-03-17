EXTENSION = branch
MODULE_big = branch
DATA = branch--0.1.sql
OBJS = src/branch.o

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

PG_CPPFLAGS = -I$(srcdir)/src

include $(PGXS)
