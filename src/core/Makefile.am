noinst_LIBRARIES = libcore.a
bin_PROGRAMS = datagen driver client

AM_CPPFLAGS = -I$(top_srcdir)/src/include

libcore_a_SOURCES = common.c logging.c _socket.c client_interface.c \
	transaction_data.c _semaphore.c dbc_manager.c

datagen_SOURCES = datagen.c common.c

driver_SOURCES = driver_main.c driver.c input_data_generator.c
driver_LDFLAGS = -pthread
driver_LDADD = libcore.a

client_SOURCES = client.c transaction_queue.c listener.c db_threadpool.c db.c
client_LDFLAGS = -pthread
client_LDADD = libcore.a ../dbc/pgsql/libpgsql.a ../simple/libsimplesql.a -L$(PGSQL_LIB_DIR) -lpq
