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
#include <pthread.h>

#include <strings.h>

#include "common.h"
#include "logging.h"
#include "db.h"
#include "terminal_queue.h"
#include "driver.h"
#include "input_data_generator.h"
struct db_worker_desc_t
{
	int id;

	/* For no think time mode */
	struct transaction_queue_node_t node;
	int start_term;
	int end_term;
	int curr_term;
};

/* Function Prototypes */
void *db_worker(void *data);
int startup();

/* Global Variables */
int db_connections = 0;
int db_conn_sleep = 1000; /* milliseconds */
struct db_worker_desc_t *db_worker_desc;

/* These should probably be handled differently. */
extern int exiting;
extern int no_thinktime;
extern int start_time;

sem_t db_worker_count;

static struct transaction_queue_node_t *db_get_transaction(
	struct db_worker_desc_t *wd)
{
	int w_id, d_id;
	struct transaction_queue_node_t *node;
	struct client_transaction_t *client_data;
	int keying_time;
	int mean_think_time; /* In milliseconds. */

	if (no_thinktime == 0) {
		if (!(node = dequeue_transaction()))
			LOG_ERROR_MESSAGE("dequeue was null");

		return node;
	}

	/* --no-thinktime mode */
	assert(no_thinktime != 0);
	node = &wd->node;
	client_data = &node->client_data;
	client_data->transaction = generate_transaction(&keying_time, &mean_think_time);
	node->term_id = wd->curr_term;
	w_id = wd->curr_term / terminals_per_warehouse + w_id_min;
	d_id = wd->curr_term % terminals_per_warehouse + 1;
	wd->curr_term++;
	if (wd->curr_term >= wd->end_term)
		wd->curr_term = wd->start_term;

	/* Generate the input data for the transaction. */
	if (client_data->transaction != STOCK_LEVEL) {
		generate_input_data(client_data->transaction,
							&client_data->transaction_data, w_id);
	} else {
		generate_input_data2(client_data->transaction,
							 &client_data->transaction_data, w_id, d_id);
	}

	return node;
}

void *db_worker(void *data)
{
	struct db_worker_desc_t *curr_wd = (struct db_worker_desc_t *) data;

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
		struct transaction_queue_node_t *node;
		if (!(node = db_get_transaction(curr_wd)))
			continue;

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
				LOG_ERROR_MESSAGE("try to reconnect to database");
				disconnect_from_db(dbc);

				if (connect_to_db(dbc) != OK)
				{
					LOG_ERROR_MESSAGE("reconnect to database error, try again after sleep 5 seconds");
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
		log_transaction_mix(node->client_data.transaction, code, response_time, node->term_id);
		if (no_thinktime == 0)
			enqueue_terminal(node->termworker_id, node->term_id);
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
		int total_terms;
		int term_per_conn;

        if (sem_init(&db_worker_count, 0, 0) != 0) {
                LOG_ERROR_MESSAGE("cannot init db_worker_count\n");
                return ERROR;
        }

		db_worker_desc = (struct db_worker_desc_t *) malloc(sizeof(struct db_worker_desc_t) * db_connections);

        ts.tv_sec = (time_t) db_conn_sleep / 1000;
        ts.tv_nsec = (long) (db_conn_sleep % 1000) * 1000000;

		if (no_thinktime != 0) {
			total_terms = (w_id_max - w_id_min + 1) * terminals_per_warehouse;
			term_per_conn = (total_terms + db_connections - 1) / db_connections;
			start_time = (int) time(NULL);
		}

        for (i = 0; i < db_connections; i++) {
                int ret;
                pthread_t tid;
                pthread_attr_t attr;
                size_t stacksize = 262144; /* 256 kilobytes. */

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
				db_worker_desc[i].id = i;
				if (no_thinktime != 0) {
					db_worker_desc[i].start_term = i * term_per_conn;
					db_worker_desc[i].curr_term = db_worker_desc[i].start_term;
					if(i == db_connections - 1)
						db_worker_desc[i].end_term = total_terms;
					else
						db_worker_desc[i].end_term = (i + 1) * term_per_conn;
				}

				ret = pthread_create(&tid, &attr, &db_worker, &db_worker_desc[i]);
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

void db_threadpool_destroy()
{
	int count;
	if (no_thinktime != 0) {
		time_t stop_time = time(NULL) + duration + duration_rampup;
		while (time(NULL) < stop_time)
			sleep(1);
	}
	printf("DB worker threads are exiting normally\n");
	do {
		/* Loop until all the DB worker threads have exited. */
		exiting = 1;
		signal_transaction_queue();
		sem_getvalue(&db_worker_count, &count);
		sleep(1);
	} while (count > 0);
}

