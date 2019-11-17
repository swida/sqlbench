EXTENSION = plsqlbench
DATA = plsqlbench--1.0.0.sql
PGFILEDESC = "sqlbench plpgsql stored functions"

plsqlbench--1.0.0.sql: delivery.sql new_order_2.sql new_order.sql order_status.sql \
	payment.sql stock_level.sql
	echo '\\echo Use "CREATE EXTENSION plsqlbench" to load this file. \\quit' >$@
	cat $^ >> $@
EXTRA_CLEAN = plsqlbench--1.0.0.sql
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
