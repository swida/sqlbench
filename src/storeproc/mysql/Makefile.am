SUBDIRS = plsql
noinst_LIBRARIES = libspmysql.a

libspmysql_a_SOURCES = mysql_common.c mysql_common.h mysql_integrity.c \
	mysql_delivery.c mysql_new_order.c mysql_order_status.c mysql_payment.c mysql_stock_level.c

libspmysql_a_CPPFLAGS = -I$(MYSQL_INCLUDE_DIR) -I$(top_srcdir)/src/include

EXTRA_DIST = delivery.sql new_order_2.sql new_order.sql \
	order_status.sql payment.sql stock_level.sql
