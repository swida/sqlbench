SUBDIRS = c plpgsql

noinst_LIBRARIES = libsppgsql.a

libsppgsql_a_SOURCES = pgsql_common.c pgsql_common.h pgsql_delivery.c  pgsql_integrity.c \
	pgsql_new_order.c  pgsql_order_status.c  pgsql_payment.c  pgsql_stock_level.c

libsppgsql_a_CPPFLAGS = -I$(PGSQL_INCLUDE_DIR) -I$(top_srcdir)/src/include
