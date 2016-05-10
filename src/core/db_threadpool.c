/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 *
 * 31 June 2002
 */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdio.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/time.h>

#include <strings.h>

#include "common.h"
#include "logging.h"
#include "db.h"
#include "terminal_queue.h"
#include "driver.h"

/* Function Prototypes */
void *db_worker(void *data);
int startup();

/* Global Variables */
int db_connections = 0;
int db_conn_sleep = 1000; /* milliseconds */
int *worker_count;
time_t *last_txn;

/* These should probably be handled differently. */
extern int exiting;
sem_t db_worker_count;

void *db_worker(void *data)
{
	int id = *((int *) data); /* Whoa... */

	struct transaction_queue_node_t *node;
	struct db_context_t *dbc;
	char code;
	double response_time;
	struct timeval rt0, rt1;
	int local_seed;
	pid_t pid;
	pthread_t tid;
	extern unsigned int seed;
	int status;

	/* Each thread needs to seed in Linux. */
    tid = pthread_self();
    pid = getpid();
	if (seed == -1) {
		struct timeval tv;
		unsigned long junk; /* Purposely used uninitialized */

		local_seed = pid;
		gettimeofday(&tv, NULL);
		local_seed ^=  tid ^ tv.tv_sec ^ tv.tv_usec ^ junk;
	} else {
		local_seed = seed;
	}
	set_random_seed(local_seed);

	/* Open a connection to the database. */
	dbc = db_init();

	if (!exiting && connect_to_db(dbc) != OK) {
		LOG_ERROR_MESSAGE("connect_to_db() error, terminating program");
		printf("cannot connect to database(see details in error.log file, exiting...\n");
		exit(1);
	}

	while (!exiting) {
		/*
		 * I know this loop will prevent the program from exiting
		 * because of the dequeue...
		 */
		node = dequeue_transaction();
		if (node == NULL) {
			LOG_ERROR_MESSAGE("dequeue was null");
			continue;
		}

		/* Execute transaction and record the response time. */
		if (gettimeofday(&rt0, NULL) == -1) {
			perror("gettimeofday");
		}

		status =
			process_transaction(node->client_data.transaction,
								dbc, &node->client_data.transaction_data);
		if (status == ERROR) {
			LOG_ERROR_MESSAGE("process_transaction() error on %s",
							  transaction_name[
								  node->client_data.transaction]);
			/*
			 * Assume this isn't a fatal error, send the results
			 * back, and try processing the next transaction.
			 */
			while (need_reconnect_to_db(dbc))
			{
				LOG_ERROR_MESSAGE("detected need reconnect, reconnect to database");
				disconnect_from_db(dbc);

				if (connect_to_db(dbc) != OK)
				{
					LOG_ERROR_MESSAGE("reconnect connect error, try again after sleep 5 seconds");
					sleep(5);
				}
			}
		}

		if (status == OK) {
			code = 'C';
		} else if (status == STATUS_ROLLBACK) {
			code = 'R';
		} else if (status == ERROR) {
			code = 'E';
		}

		if (gettimeofday(&rt1, NULL) == -1) {
			perror("gettimeofday");
		}

		response_time = difftimeval(rt1, rt0);
		log_transaction_mix(node->client_data.transaction, code, response_time,
							mode_altered > 0 ? node->termworker_id : node->term_id);
		enqueue_terminal(node->termworker_id, node->term_id);

		/* Keep track of how many transactions this thread has done. */
		++worker_count[id];

		/* Keep track of then the last transaction was execute. */
		time(&last_txn[id]);
	}

	/* Disconnect from the database. */
	disconnect_from_db(dbc);

	free(dbc);

	sem_wait(&db_worker_count);

	return NULL;        /* keep compiler quiet */
}

int db_threadpool_init()
{
        struct timespec ts, rem;
        int i;
        extern int errno;

        if (sem_init(&db_worker_count, 0, 0) != 0) {
                LOG_ERROR_MESSAGE("cannot init db_worker_count\n");
                return ERROR;
        }

        worker_count = (int *) malloc(sizeof(int) * db_connections);
        bzero(worker_count, sizeof(int) * db_connections);

        ts.tv_sec = (time_t) db_conn_sleep / 1000;
        ts.tv_nsec = (long) (db_conn_sleep % 1000) * 1000000;

        last_txn = (time_t *) malloc(sizeof(time_t) * db_connections);

        for (i = 0; i < db_connections; i++) {
                int ret;
                pthread_t tid;
                pthread_attr_t attr;
                size_t stacksize = 262144; /* 256 kilobytes. */

                /*
                 * Initialize the last_txn array with the time right before
                 * the worker thread is started.  This looks better than
                 * initializing the array with zeros.
                 */
                time(&last_txn[i]);

                /*
                 * Is it possible for i to change before db_worker can copy it?
                 */
                if (pthread_attr_init(&attr) != 0) {
                        LOG_ERROR_MESSAGE("could not init pthread attr");
                        return ERROR;
                }
                if (pthread_attr_setstacksize(&attr, stacksize) != 0) {
                        LOG_ERROR_MESSAGE("could not set pthread stack size");
                        return ERROR;
                }
                ret = pthread_create(&tid, &attr, &db_worker, &i);
                if (ret != 0) {
                        LOG_ERROR_MESSAGE("error creating db thread");
                        if (ret == EAGAIN) {
                                LOG_ERROR_MESSAGE(
                                        "not enough system resources");
                        }
                        return ERROR;
                }

                /* Keep a count of how many DB worker threads have started. */
                sem_post(&db_worker_count);
				if ((i + 1) % 20 == 0) {
					printf("%d / %d db connections opened...\n", i + 1, db_connections);
					fflush(stdout);
				}

                /* Don't let the database connection attempts occur too fast. */
				while (nanosleep(&ts, &rem) == -1) {
                        if (errno == EINTR) {
                                memcpy(&ts, &rem, sizeof(struct timespec));
                        } else {
                                LOG_ERROR_MESSAGE(
                                        "sleep time invalid %d s %ls ns",
                                        ts.tv_sec, ts.tv_nsec);
                                break;
                        }
                }
				pthread_attr_destroy(&attr);
        }
        return OK;
}
