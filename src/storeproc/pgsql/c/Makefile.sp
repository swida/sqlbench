MODULES = delivery new_order order_status payment stock_level
DATA_built = delivery.sql new_order.sql order_status.sql payment.sql \
		stock_level.sql

PG_CPPFLAGS=-g
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
