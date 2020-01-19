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
#include "db_threadpool.h"
#include "terminal_queue.h"

#define MIX_LOG_NAME "mix.log"

void *terminals_worker(void *data);

/* Global Variables */
struct transaction_mix_t transaction_mix;
struct key_time_t key_time;
struct think_time_t think_time;
int duration = 0;
time_t start_time = 0;
time_t stop_time;
int w_id_min = 0, w_id_max = 0;
int terminals_per_warehouse = 0;
unsigned int seed = -1;
int duration_rampup = 0;
static FILE *log_mix = NULL;
pthread_mutex_t mutex_mix_log = PTHREAD_MUTEX_INITIALIZER;

int terminals_per_thread = 50;

extern int exiting;

pthread_t *driver_tids;
int started_driver_count = 0;

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
	struct db_context_t *dbc;
	struct client_transaction_t client_data;
	client_data.transaction = INTEGRITY;
	/* Open a connection to the database. */
	dbc = db_init();
	if (connect_to_db(dbc) != OK) {
		LOG_ERROR_MESSAGE("connect_to_db() error");
		printf("cannot connect to database(see details in error.log file, exiting...\n");
		free(dbc);
		return ERROR;
	}

	generate_input_data(client_data.transaction,
			&client_data.transaction_data, table_cardinality.warehouses);

	rc = process_transaction(client_data.transaction,
							 dbc, &client_data.transaction_data);

#ifdef DEBUG
	printf("executing transaction %c\n",
			 transaction_short_name[client_data.transaction]);
	fflush(stdout);

	LOG_ERROR_MESSAGE("executing transaction %c",
			transaction_short_name[client_data.transaction]);
#endif /* DEBUG */

	/* Disconnect from the database. */
	disconnect_from_db(dbc);

	free(dbc);

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

int start_db_threadpool(void)
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

void start_drivers()
{
	int i;
	struct timespec rem;
	char tm_disp_str[64];
	time_t now, rampup_time;
	/* Just used to count the number of threads created. */
	int thread_count = ((w_id_max - w_id_min + 1) * terminals_per_warehouse +
			    terminals_per_thread - 1) / terminals_per_thread;
	start_time = (int) time(NULL);
	rampup_time = start_time + duration_rampup;
	stop_time = rampup_time + duration;
	format_time(tm_disp_str, sizeof(tm_disp_str), start_time);
	printf("driver is starting to ramp up at time %s\n", tm_disp_str);
	printf("driver will ramp up in %d seconds\n", duration_rampup);
	format_time(tm_disp_str, sizeof(tm_disp_str), stop_time);
	printf("will stop test at time %s\n", tm_disp_str);

	/* allocate g_tid */
	driver_tids = malloc(sizeof(pthread_t) * thread_count);
	init_termworker_array(thread_count);

	log_rampup_start();
	for (i = 0; i < thread_count; i++) {
		int ret;
		pthread_attr_t attr;
		size_t stacksize = 131072; /* 128 kilobytes. */

		struct termworker_context_t *tc = init_termworker_context(i, thread_count);
		if (exiting)
			break;
		if (pthread_attr_init(&attr) != 0) {
			LOG_ERROR_MESSAGE("could not init pthread attr: %d", i);
			break;
		}

		if (pthread_attr_setstacksize(&attr, stacksize) != 0) {
			LOG_ERROR_MESSAGE("could not set pthread stack size: %d", i);
			break;
		}

		ret = pthread_create(&driver_tids[i], &attr, &terminals_worker, tc);

		if (ret != 0) {
			perror("pthread_create");
			LOG_ERROR_MESSAGE( "error creating terminal thread: %d", i);
			if (ret == EAGAIN) {
				LOG_ERROR_MESSAGE( "not enough system resources: %d", i);
			}
			break;
		}

		++started_driver_count;
		if (started_driver_count % 20 == 0) {
			printf("%d / %d threads started...\n", started_driver_count, thread_count);
			fflush(stdout);
		}

		pthread_attr_destroy(&attr);
	}
	printf("terminals started...\n");

	now = time(NULL);
	if (now < rampup_time)
		sleep(rampup_time - now);
}

void wait_drivers_finish()
{
	/* Note that the driver has started up all threads in the log. */
	log_testing_start();
	printf("steady status started...\n");

	/* wait until all threads quit */
	for (int i = 0; i < started_driver_count; i++) {
		if (pthread_join(driver_tids[i], NULL) != 0) {
			LOG_ERROR_MESSAGE("error join terminal thread");
		}
	}

	free(driver_tids);

	printf("drivers are exiting normally\n");
}

void destroy_drivers()
{
	destroy_termworker_array(started_driver_count);
}

void log_rampup_start()
{
	pthread_mutex_lock(&mutex_mix_log);
	fprintf(log_mix, "0,RAMPUP,,,%ld\n", start_time);
	pthread_mutex_unlock(&mutex_mix_log);
	fflush(log_mix);
}

void log_testing_start()
{
	pthread_mutex_lock(&mutex_mix_log);
	fprintf(log_mix, "%ld,START,,,\n", time(NULL) - start_time);
	pthread_mutex_unlock(&mutex_mix_log);
	fflush(log_mix);
}

void log_transaction_mix(int transaction, char code, double response_time, unsigned int term_id)
{
	pthread_mutex_lock(&mutex_mix_log);
	fprintf(log_mix, "%d,%c,%c,%f,%d\n", (int) (time(NULL) - start_time),
			transaction_short_name[transaction], code, response_time, term_id);
	pthread_mutex_unlock(&mutex_mix_log);
}

void *terminals_worker(void *data)
{
	struct termworker_context_t *tc;
	extern int errno;
	int local_seed;
	pid_t pid;
	pthread_t tid;
	int i;

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

	tc = (struct termworker_context_t *)data;

	for(i = tc->start_term; i < tc->end_term; i++)
	{
		struct transaction_queue_node_t *node =
			&tc->term_queue.terminals[i - tc->start_term].node;
		node->term_id = i;
		node->termworker_id = tc->id;
		enqueue_terminal(tc->id, i);
	}

	do {
		int w_id, d_id;
		struct transaction_queue_node_t *node;
		struct client_transaction_t *client_data;
		struct timespec delay, rem;
		struct terminal_t *term =
			dequeue_terminal(tc->id, (unsigned int *)&delay.tv_nsec);

		delay.tv_sec = (time_t) (delay.tv_nsec / 1000);
		delay.tv_nsec = (delay.tv_nsec % 1000) * 1000000;
		while (nanosleep(&delay, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&delay, &rem, sizeof(struct timespec));
			} else {
				LOG_ERROR_MESSAGE(
					"sleep time invalid %d s %ls ns",
					delay.tv_sec,
					delay.tv_nsec);
				break;
			}
		}

		node = &term->node;
		client_data = &node->client_data;

		w_id = term->id / terminals_per_warehouse + w_id_min;
		d_id = term->id % terminals_per_warehouse + 1;

#ifdef DEBUG
		LOG_ERROR_MESSAGE("executing transaction[w:%d, d:%d] %c",w_id, d_id,
						  transaction_short_name[client_data->transaction]);
#endif /* DEBUG */

		/* Generate the input data for the transaction. */
		if (client_data->transaction != STOCK_LEVEL) {
			generate_input_data(client_data->transaction,
								&client_data->transaction_data, w_id);
		} else {
			generate_input_data2(client_data->transaction,
								 &client_data->transaction_data, w_id, d_id);
		}

		enqueue_transaction(node);

	} while (!exiting && time(NULL) < stop_time);

	/* Note when each thread has exited. */
	pthread_mutex_lock(&mutex_mix_log);
	fprintf(log_mix, "%d,TERMINATED,,,%d-%d\n", (int) (time(NULL) - start_time), tc->start_term, tc->end_term);
	pthread_mutex_unlock(&mutex_mix_log);
	fflush(log_mix);
	return NULL; /* keep the compiler quiet */
}
