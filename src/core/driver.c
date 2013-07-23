/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 *
 * 7 August 2002
 */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>

#include "common.h"
#include "logging.h"
#include "driver.h"
#include "input_data_generator.h"
#include "db.h"
#include "transaction_queue.h"
#include "db_threadpool.h"

#define MIX_LOG_NAME "mix.log"

void *terminal_worker(void *data);

/* Global Variables */
pthread_t** g_tid = NULL;
struct transaction_mix_t transaction_mix;
struct key_time_t key_time;
struct think_time_t think_time;
int duration = 0;
int stop_time = 0;
int start_time = 0;
int w_id_min = 0, w_id_max = 0;
int terminals_per_warehouse = 0;
int mode_altered = 0;
unsigned int seed = -1;
int client_conn_sleep = 1000; /* milliseconds */
int spread = 1;
int threads_start_time= 0;

FILE *log_mix = NULL;
pthread_mutex_t mutex_mix_log = PTHREAD_MUTEX_INITIALIZER;

int terminal_state[3][TRANSACTION_MAX] = {
	{ 0, 0, 0, 0, 0 },
	{ 0, 0, 0, 0, 0 },
	{ 0, 0, 0, 0, 0 }
};

pthread_mutex_t mutex_terminal_state[3][TRANSACTION_MAX] = {
	{ PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER,
		PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER,
		PTHREAD_MUTEX_INITIALIZER },
	{ PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER,
		PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER,
		PTHREAD_MUTEX_INITIALIZER },
	{ PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER,
		PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER,
		PTHREAD_MUTEX_INITIALIZER }
};

int create_pid_file()
{
	FILE * fpid;
	char pid_filename[1024];

	sprintf(pid_filename, "%s%s", output_path, DRIVER_PID_FILENAME);

	fpid = fopen(pid_filename,"w");
	if (!fpid) {
		printf("cann't create pid file: %s\n", pid_filename);
		return ERROR;
	}

	fprintf(fpid,"%d", (unsigned int) getpid());
	fclose(fpid);

	return OK;
}

int init_driver()
{
	terminals_per_warehouse = table_cardinality.districts;

	transaction_mix.delivery_actual = MIX_DELIVERY;
	transaction_mix.order_status_actual = MIX_ORDER_STATUS;
	transaction_mix.payment_actual = MIX_PAYMENT;
	transaction_mix.stock_level_actual = MIX_STOCK_LEVEL;

	key_time.delivery = KEY_TIME_DELIVERY;
	key_time.new_order = KEY_TIME_NEW_ORDER;
	key_time.order_status = KEY_TIME_ORDER_STATUS;
	key_time.payment = KEY_TIME_PAYMENT;
	key_time.stock_level = KEY_TIME_STOCK_LEVEL;

	think_time.delivery = THINK_TIME_DELIVERY;
	think_time.new_order = THINK_TIME_NEW_ORDER;
	think_time.order_status = THINK_TIME_ORDER_STATUS;
	think_time.payment = THINK_TIME_PAYMENT;
	think_time.stock_level = THINK_TIME_STOCK_LEVEL;

	return OK;
}

int init_driver_logging()
{
	char log_filename[1024];

	sprintf(log_filename, "%s%s", output_path, MIX_LOG_NAME);
	log_mix = fopen(log_filename, "w");
	if (log_mix == NULL) {
		fprintf(stderr, "cannot open %s\n", log_filename);
		return ERROR;
	}

	return OK;
}

int integrity_terminal_worker()
{
	int rc;
	struct client_transaction_t *client_data;

	struct transaction_queue_node_t *node =
			(struct transaction_queue_node_t *)
			malloc(sizeof(struct transaction_queue_node_t));

	if (sem_init(&node->s, 0, 0) != 0)
	{
		LOG_ERROR_MESSAGE("cannot init tran_status");
		return ERROR;
	}

	client_data = &node->client_data;
	client_data->transaction = INTEGRITY;

	generate_input_data(client_data->transaction,
			&client_data->transaction_data, table_cardinality.warehouses);

	enqueue_transaction(node);
	sem_wait(&node->s);

#ifdef DEBUG
	printf("executing transaction %c\n"
			 transaction_short_name[client_data.transaction]);
	fflush(stdout);

	LOG_ERROR_MESSAGE("executing transaction %c",
			transaction_short_name[client_data.transaction]);
#endif /* DEBUG */
	sem_destroy(&node->s);
	free(node);
	return rc;
}

int recalculate_mix()
{
	/*
	 * Calculate the actual percentage that the New-Order transaction will
	 * be execute.
	 */
	transaction_mix.new_order_actual = 1.0 -
			(transaction_mix.delivery_actual +
			transaction_mix.order_status_actual +
			transaction_mix.payment_actual +
			transaction_mix.stock_level_actual);

	if (transaction_mix.new_order_actual < 0.0) {
		LOG_ERROR_MESSAGE(
				"invalid transaction mix. d %0.1f. o %0.1f. p %0.1f. s %0.1f. n %0.1f.\n",
				transaction_mix.delivery_actual,
				transaction_mix.order_status_actual,
				transaction_mix.payment_actual,
				transaction_mix.stock_level_actual,
				transaction_mix.new_order_actual);
		return ERROR;
	}

	/* Calculate the thresholds of each transaction. */
	transaction_mix.new_order_threshold = transaction_mix.new_order_actual;
	transaction_mix.payment_threshold =
			transaction_mix.new_order_threshold +
			transaction_mix.payment_actual;
	transaction_mix.order_status_threshold =
			transaction_mix.payment_threshold
			+ transaction_mix.order_status_actual;
	transaction_mix.delivery_threshold =
			transaction_mix.order_status_threshold
			+ transaction_mix.delivery_actual;
	transaction_mix.stock_level_threshold =
			transaction_mix.delivery_threshold +
			transaction_mix.stock_level_actual;

	return OK;
}

int set_duration(int seconds)
{
	duration = seconds;
	return OK;
}

int set_table_cardinality(int table, int cardinality)
{
	switch (table) {
	case TABLE_WAREHOUSE:
		table_cardinality.warehouses = cardinality;
		break;
	case TABLE_CUSTOMER:
		table_cardinality.customers = cardinality;
		break;
	case TABLE_ITEM:
		table_cardinality.items = cardinality;
		break;
	case TABLE_ORDER:
		table_cardinality.orders = cardinality;
		break;
	case TABLE_NEW_ORDER:
		table_cardinality.new_orders = cardinality;
		break;
	default:
		return ERROR;
	}

	return OK;
}

int set_transaction_mix(int transaction, double mix)
{
	switch (transaction) {
	case DELIVERY:
		transaction_mix.delivery_actual = mix;
		break;
	case NEW_ORDER:
		transaction_mix.new_order_actual = mix;
		break;
	case ORDER_STATUS:
		transaction_mix.order_status_actual = mix;
		break;
	case PAYMENT:
		transaction_mix.payment_actual = mix;
		break;
	case STOCK_LEVEL:
		transaction_mix.stock_level_actual = mix;
		break;
	default:
		return ERROR;
	}
	return OK;
}

int start_db_threadpool()
{
	printf("opening %d connection(s) to database...\n", db_connections);
	/* Open database connectiosn. */
	if (db_threadpool_init() != OK) {
		LOG_ERROR_MESSAGE("cannot open database connections");
		return ERROR;
	}
	printf("%d DB worker threads have started\n", db_connections);
	fflush(stdout);
	return OK;
}

int start_driver()
{
	int i, j;
	struct timespec ts, rem;

	/* Just used to count the number of threads created. */
	int count = 0;

	ts.tv_sec = (time_t) (client_conn_sleep / 1000);
	ts.tv_nsec = (long) (client_conn_sleep % 1000) * 1000000;

	/* Caulculate when the test should stop. */
	threads_start_time = (int) ((double) client_conn_sleep / 1000.0 *
			(double) terminals_per_warehouse *
			(double) (w_id_max - w_id_min));

	stop_time = time(NULL) + duration + threads_start_time;
	printf("driver is starting to ramp up at time %d\n", (int) time(NULL));
	printf("driver will ramp up in  %d seconds\n", threads_start_time);
	printf("will stop test at time %d\n", stop_time);

	/* allocate g_tid */
	g_tid = (pthread_t**) malloc(sizeof(pthread_t*) * (w_id_max+1)/spread);
	for (i = w_id_min; i < w_id_max + 1; i += spread) {
		g_tid[i] = (pthread_t*)
				malloc(sizeof(pthread_t) * terminals_per_warehouse);
	}

	pthread_mutex_lock(&mutex_mix_log);
	start_time = (int) time(NULL);
	fprintf(log_mix, "%d,RAMPUP,,,\n", start_time);
	fflush(log_mix);
	pthread_mutex_unlock(&mutex_mix_log);

	for (j = 0; j < terminals_per_warehouse; j++) {
		for (i = w_id_min; i < w_id_max + 1; i += spread) {
			int ret;
			pthread_attr_t attr;
			size_t stacksize = 131072; /* 128 kilobytes. */
			struct terminal_context_t *tc;

			tc = (struct terminal_context_t *)
					malloc(sizeof(struct terminal_context_t));
			tc->w_id = i;
			tc->d_id = j + 1;
			tc->t_id = count;

			if (pthread_attr_init(&attr) != 0) {
				LOG_ERROR_MESSAGE("could not init pthread attr: %d",
						(i + j + 1) * terminals_per_warehouse);
				return ERROR;
			}

			if (pthread_attr_setstacksize(&attr, stacksize) != 0) {
				LOG_ERROR_MESSAGE("could not set pthread stack size: %d",
						(i + j + 1) * terminals_per_warehouse);
				return ERROR;
			}
			ret = pthread_create(&g_tid[i][j], &attr, &terminal_worker,
					(void *) tc);
			if (ret != 0) {
				perror("pthread_create");
				LOG_ERROR_MESSAGE( "error creating terminal thread: %d",
						(i + j + 1) * terminals_per_warehouse);
				if (ret == EAGAIN) {
					LOG_ERROR_MESSAGE( "not enough system resources: %d",
							(i + j + 1) * terminals_per_warehouse);
				}
				return ERROR;
			}

			++count;
			if ((count % 100) == 0) {
				printf("%d / %d threads started...\n", count,
						terminals_per_warehouse *
								(w_id_max - w_id_min + 1) / spread);
				fflush(stdout);
			}

			/* Sleep for between starting terminals. */
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
			if (mode_altered > 0 && count >= mode_altered)
				break;
		}

		if (mode_altered > 0) {
			/*
			 * This effectively allows one client to touch
			 * the entire warehouse range.  The setting of
			 * w_id and d_id is moot in this case.
			 */
			printf("altered mode detected\n");
			break;
		}
	}
	printf("terminals started...\n");

	/* Note that the driver has started up all threads in the log. */
	pthread_mutex_lock(&mutex_mix_log);
	fprintf(log_mix, "%d,START,,,\n", (int) time(NULL) - start_time);
	fflush(log_mix);
	pthread_mutex_unlock(&mutex_mix_log);

	/* wait until all threads quit */
	for (i = w_id_min; i < w_id_max + 1; i += spread) {
		for (j = 0; j < terminals_per_warehouse; j++) {
			if (pthread_join(g_tid[i][j], NULL) != 0) {
				LOG_ERROR_MESSAGE("error join terminal thread");
				return ERROR;
			}
			count --;
			if (mode_altered > 0 && count <= 0)
				break;
		}

		if (mode_altered > 0) {
			/*
			 * This effectively allows one client to touch
			 * the entire warehouse range.  The setting of
			 * w_id and d_id is moot in this case.
			 */
			printf("altered mode detected\n");
			break;
		}
	}

#if 0
	do {
		/* Loop until all the DB worker threads have exited. */
		sem_getvalue(&db_worker_count, &count);
		sleep(1);
	} while (count > 0);
#endif
	printf("driver is exiting normally\n");
	return OK;
}

void *terminal_worker(void *data)
{
	struct terminal_context_t *tc;
	struct client_transaction_t *client_data;
	double threshold;
	int keying_time;
	struct timespec thinking_time, rem;
	int mean_think_time; /* In milliseconds. */
	struct timeval rt0, rt1;
	double response_time;
	extern int errno;
	int local_seed;
	pid_t pid;
	pthread_t tid;
	char code;

	struct transaction_queue_node_t *node =
			(struct transaction_queue_node_t *)
			malloc(sizeof(struct transaction_queue_node_t));

	client_data = &node->client_data;

	extern int exiting;

	if (sem_init(&node->s, 0, 0) != 0)
	{
		LOG_ERROR_MESSAGE("cannot init tran_status");
		return NULL;
	}

	tc = (struct terminal_context_t *) data;
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
	printf("seed for %d:%x : %u\n",
			(unsigned int) pid, (unsigned int) tid, local_seed);
	fflush(stdout);
	srand(local_seed);

	do {
		if (mode_altered > 0) {
			/*
			 * Determine w_id and d_id for the client per
			 * transaction.
			 */
			tc->w_id = w_id_min + get_random(w_id_max - w_id_min + 1);
			tc->d_id = get_random(table_cardinality.districts) + 1;
		}

		/*
		 * Determine which transaction to execute, minimum keying time,
		 * and mean think time.
		 */
		threshold = get_percentage();
		if (threshold < transaction_mix.new_order_threshold) {
			client_data->transaction = NEW_ORDER;
			keying_time = key_time.new_order;
			mean_think_time = think_time.new_order;
		} else if (transaction_mix.payment_actual != 0 &&
			threshold < transaction_mix.payment_threshold) {
			client_data->transaction = PAYMENT;
			keying_time = key_time.payment;
			mean_think_time = think_time.payment;
		} else if (transaction_mix.order_status_actual != 0 &&
			threshold < transaction_mix.order_status_threshold) {
			client_data->transaction = ORDER_STATUS;
			keying_time = key_time.order_status;
			mean_think_time = think_time.order_status;
		} else if (transaction_mix.delivery_actual != 0 &&
			threshold < transaction_mix.delivery_threshold) {
			client_data->transaction = DELIVERY;
			keying_time = key_time.delivery;
			mean_think_time = think_time.delivery;
		} else {
			client_data->transaction = STOCK_LEVEL;
			keying_time = key_time.stock_level;
			mean_think_time = think_time.stock_level;
		}

#ifdef DEBUG
		printf("executing transaction %c\n",
			transaction_short_name[client_data->transaction]);
		fflush(stdout);
		LOG_ERROR_MESSAGE("executing transaction %c",
			transaction_short_name[client_data->transaction]);
#endif /* DEBUG */

		/* Generate the input data for the transaction. */
		if (client_data->transaction != STOCK_LEVEL) {
			generate_input_data(client_data->transaction,
					&client_data->transaction_data, tc->w_id);
		} else {
			generate_input_data2(client_data->transaction,
					&client_data->transaction_data, tc->w_id, tc->d_id);
		}

		/* Keying time... */
		pthread_mutex_lock(
				&mutex_terminal_state[KEYING][client_data->transaction]);
		++terminal_state[KEYING][client_data->transaction];
		pthread_mutex_unlock(
				&mutex_terminal_state[KEYING][client_data->transaction]);
		if (time(NULL) < stop_time) {
			sleep(keying_time);
		} else {
			break;
		}
		pthread_mutex_lock(
				&mutex_terminal_state[KEYING][client_data->transaction]);
		--terminal_state[KEYING][client_data->transaction];
		pthread_mutex_unlock(
				&mutex_terminal_state[KEYING][client_data->transaction]);

		/* Note this thread is executing a transation. */
		pthread_mutex_lock(
				&mutex_terminal_state[EXECUTING][client_data->transaction]);
		++terminal_state[EXECUTING][client_data->transaction];
		pthread_mutex_unlock(
				&mutex_terminal_state[EXECUTING][client_data->transaction]);
		/* Execute transaction and record the response time. */
		if (gettimeofday(&rt0, NULL) == -1) {
			perror("gettimeofday");
		}

		enqueue_transaction(node);

		sem_wait(&node->s);

		if (node->client_data.status == OK) {
			code = 'C';
		} else if (node->client_data.status == STATUS_ROLLBACK) {
			code = 'R';
		} else if (node->client_data.status == ERROR) {
			code = 'E';
		}
		/* mix log record in db_threadpool */
		if (gettimeofday(&rt1, NULL) == -1) {
			perror("gettimeofday");
		}
		response_time = difftimeval(rt1, rt0);
		pthread_mutex_lock(&mutex_mix_log);
		fprintf(log_mix, "%d,%c,%c,%f,%d\n", (int) (time(NULL) - start_time),
				transaction_short_name[client_data->transaction], code, response_time, tc->t_id);
		fflush(log_mix);
		pthread_mutex_unlock(&mutex_mix_log);
		pthread_mutex_lock(&mutex_terminal_state[EXECUTING][client_data->transaction]);
		--terminal_state[EXECUTING][client_data->transaction];
		pthread_mutex_unlock(&mutex_terminal_state[EXECUTING][client_data->transaction]);

		/* Thinking time... */
		pthread_mutex_lock(&mutex_terminal_state[THINKING][client_data->transaction]);
		++terminal_state[THINKING][client_data->transaction];
		pthread_mutex_unlock(&mutex_terminal_state[THINKING][client_data->transaction]);
		if (time(NULL) < stop_time) {
			thinking_time.tv_nsec = (long) get_think_time(mean_think_time);
			thinking_time.tv_sec = (time_t) (thinking_time.tv_nsec / 1000);
			thinking_time.tv_nsec = (thinking_time.tv_nsec % 1000) * 1000000;
			while (nanosleep(&thinking_time, &rem) == -1) {
				if (errno == EINTR) {
					memcpy(&thinking_time, &rem, sizeof(struct timespec));
				} else {
					LOG_ERROR_MESSAGE(
							"sleep time invalid %d s %ls ns",
							thinking_time.tv_sec,
							thinking_time.tv_nsec);
					break;
				}
			}
		}
		pthread_mutex_lock(&mutex_terminal_state[THINKING][client_data->transaction]);
		--terminal_state[THINKING][client_data->transaction];
		pthread_mutex_unlock(&mutex_terminal_state[THINKING][client_data->transaction]);

		if (node == NULL) {
			LOG_ERROR_MESSAGE("Cannot get a transaction node.\n");
		}

	} while (time(NULL) < stop_time);

	sem_destroy(&node->s);
	free(node);

	/* Note when each thread has exited. */
	pthread_mutex_lock(&mutex_mix_log);
	fprintf(log_mix, "%d,TERMINATED,,,%d\n", (int) (time(NULL) - start_time), tc->t_id);
	fflush(log_mix);
	pthread_mutex_unlock(&mutex_mix_log);
	return NULL; /* keep the compiler quiet */
}
