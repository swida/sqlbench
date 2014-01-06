noinst_LIBRARIES = libextended.a

libextended_a_SOURCES = extended_delivery.c extended_common.h extended_delivery.h \
	extended_common.c extended_new_order.c extended_new_order.h extended_order_status.c \
	extended_order_status.h extended_payment.h extended_payment.c extended_stock_level.c \
	extended_stock_level.h extended_integrity.h extended_integrity.c

libextended_a_CPPFLAGS = -I$(PGSQL_INCLUDE_DIR) -I$(top_srcdir)/src/include
