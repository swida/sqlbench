/*
 * db_threadpool.h
 *
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Lab, Inc.
 *
 * 31 june 2002
 */
#include <semaphore.h>
extern sem_t db_worker_count;
extern int db_connections;
extern int db_conn_sleep;

int db_threadpool_init();
