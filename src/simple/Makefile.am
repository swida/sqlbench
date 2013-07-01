noinst_LIBRARIES = libsimplesql.a

libsimplesql_a_SOURCES = simple_delivery.c simple_integrity.h simple_order_status.c simple_payment.h \
	simple_common.c simple_delivery.h simple_new_order.c simple_order_status.h simple_stock_level.c \
	simple_common.h simple_integrity.c simple_new_order.h simple_payment.c simple_stock_level.h

libsimplesql_a_CPPFLAGS = -I$(PGSQL_INCLUDE_DIR) -I$(top_srcdir)/src/include
