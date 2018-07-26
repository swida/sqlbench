noinst_LIBRARIES = libcore.a
bin_PROGRAMS = datagen sqlbench transaction_test

AM_CPPFLAGS = -I$(top_srcdir)/src/include
AM_LDFLAGS = -pthread
libcore_a_SOURCES = common.c logging.c transaction_data.c _semaphore.c dbc_manager.c db.c \
	input_data_generator.c

LDADD = libcore.a ../simple/libsimplesql.a ../extended/libextended.a

if ENABLE_POSTGRESQL
LDADD += ../dbc/pgsql/libdbc_pgsql.a ../storeproc/pgsql/libsppgsql.a $(PGSQL_LIBS)
AM_LDFLAGS += -Wl,-rpath=$(PGSQL_LIB_DIR)
endif

if ENABLE_KINGBASE
LDADD += ../dbc/kingbase/libdbc_kingbase.a $(KINGBASE_LIBS)
AM_LDFLAGS += -Wl,-rpath=$(KINGBASE_LIB_DIR)
endif

if ENABLE_MYSQL
LDADD += ../dbc/mysql/libdbc_mysql.a $(MYSQL_LIBS)
AM_LDFLAGS += -Wl,-rpath=$(MYSQL_LIB_DIR)
endif

if ENABLE_ODBC
LDADD += ../dbc/odbc/libdbc_odbc.a $(ODBC_LIBS)
endif

datagen_SOURCES = datagen.c
sqlbench_SOURCES = driver_main.c driver.c transaction_queue.c db_threadpool.c terminal_queue.c
transaction_test_SOURCES = transaction_test.c
