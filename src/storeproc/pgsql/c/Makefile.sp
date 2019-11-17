MODULE_big = sqlbench
OBJS = delivery.o new_order.o order_status.o payment.o stock_level.o

EXTENSION = sqlbench
DATA = sqlbench--1.0.0.sql
PGFILEDESC = "sqlbench c stored functions"

PG_CPPFLAGS=-g -fno-omit-frame-pointer
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
