noinst_LIBRARIES = libcore.a
bin_PROGRAMS = datagen sqlbench transaction_test

AM_CPPFLAGS = -I$(top_srcdir)/src/include
AM_LDFLAGS = -pthread
libcore_a_SOURCES = common.c logging.c transaction_data.c _semaphore.c dbc_manager.c db.c \
	input_data_generator.c

LDADD = libcore.a ../dbc/pgsql/libpgsql.a ../simple/libsimplesql.a ../extended/libextended.a \
	../storeproc/pgsql/libsppgsql.a -L$(PGSQL_LIB_DIR) -lpq $(KINGBASE_LIBS)
if ENABLE_KINGBASE
LDADD += ../dbc/kingbase/libkingbase.a
endif
datagen_SOURCES = datagen.c common.c
sqlbench_SOURCES = driver_main.c driver.c transaction_queue.c db_threadpool.c terminal_queue.c
transaction_test_SOURCES = transaction_test.c

