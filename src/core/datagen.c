/*
 * This file is released under the terms of the Artistic License.
 * Please see the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Open Source Development Labs, Inc.
 *               2002-2010 Mark Wong
 *
 * Based on TPC-C Standard Specification Revision 5.0.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <getopt.h>
#include <stdarg.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include <pthread.h>
#include "common.h"
#include "db.h"
#include "dbc.h"
#include "logging.h"

#define DATAFILE_EXT ".data"
#define ENV_DBCLIENT_NAME "DGEN_DBCLIENT_COMMAND"

typedef union output_stream_t
{
	FILE *file;
	struct loader_stream_t *stream;
} output_stream;

struct stream_operation_t
{
	output_stream (*open_stream)(int worker_id, char *table_name);
	int (*write_to_stream)(output_stream stream, const char *fmt, va_list ap);
	void (*close_stream)(output_stream stream);
};

static output_stream open_file_stream(int worker_id, char *table_name);
static int write_to_file_stream(output_stream stream, const char *fmt, va_list ap);
static void close_file_stream(output_stream stream);

void gen_customers(int worker_id, int start, int end);
void gen_districts(int worker_id, int start, int end);
void gen_history(int worker_id, int start, int end);
void gen_items();
void gen_new_order(int worker_id, int start, int end);
void gen_orders(int worker_id, int start, int end);
void gen_stock(int worker_id, int start, int end);
void gen_warehouses(int worker_id, int start, int end);

struct stream_operation_t stream_operation = {
	open_file_stream,
	write_to_file_stream,
	close_file_stream
};

int warehouses = 0;
int customers = CUSTOMER_CARDINALITY;
int items = ITEM_CARDINALITY;
int orders = ORDER_CARDINALITY;
int new_orders = NEW_ORDER_CARDINALITY;

int jobs = 1;

char delimiter = ',';
char null_str[16] = "\"NULL\"";
char *dbclient_command = NULL;

void escape_me(char *str)
{
	/* Shouldn't need a buffer bigger than this. */
	char buffer[4096] = "";
	int i = 0;
	int j = 0;
	int k = 0;

	strcpy(buffer, str);
	i = strlen(buffer);
	for (k = 0; k <= i; k++) {
		if (buffer[k] == '\\') {
			str[j++] = '\\';
		}
		str[j++] = buffer[k];
	}
}

static void quickdie(const char *errmsg)
{
	printf("FATAL: %s\n", errmsg);
	exit(1);
}

	
static int is_valid_stream(output_stream stream)
{
	int res = stream.stream != NULL;
	if (!res)
		quickdie("could not open output stream");

	return res;
}

static output_stream open_output_stream(int worker_id, char *table_name)
{
	return (*stream_operation.open_stream)(worker_id, table_name);
}

static int ostprintf(output_stream stream, const char *fmt, ...)
{
	int res;
	va_list args;
	va_start(args, fmt);
	res = (*stream_operation.write_to_stream)(stream, fmt, args);
	va_end(args);
	if (res)
		quickdie("could not write to stream");
	return res;
}

static void close_output_stream(output_stream stream)
{
	return (*stream_operation.close_stream)(stream);
}

output_stream open_file_stream(int worker_id, char *table_name)
{
	char buf[1024] = "\0";
	output_stream ost;

	if (dbclient_command)
		snprintf(buf, sizeof(buf), "%s %s %c %s",
				 dbclient_command, table_name, delimiter, null_str);
	else if(strlen(output_path) > 0)
	{
		strcpy(buf, output_path);
		strcat(buf, "/");
	}

	snprintf(buf, sizeof(buf), "%s%s%d%s", buf, table_name, worker_id, DATAFILE_EXT);
	ost.file = dbclient_command ? popen(buf, "w") : fopen(buf, "w");

	if (ost.file == NULL)
		printf("cannot open file or program for table %s\n", table_name);

	return ost;
}

static int write_to_file_stream(output_stream stream, const char *fmt, va_list ap)
{
	return vfprintf(stream.file, fmt, ap) < 0;
}

static void close_file_stream(output_stream stream)
{
	fclose(stream.file);
}

output_stream open_dbloader_stream(int worker_id, char *table_name)
{
	output_stream ost;
	struct db_context_t *dbc = db_init();

	ost.stream = NULL;

	if (connect_to_db(dbc) != OK) {
		printf("cannot establish a database connection\n");
		return ost;
	}

	if ((ost.stream = dbc_open_loader_stream(dbc, table_name, delimiter, null_str)) == NULL)
		LOG_ERROR_MESSAGE("could not open loader stream to database");

	return ost;
}

static int write_to_dbloader_stream(output_stream stream, const char *fmt, va_list ap)
{
	return dbc_write_to_loader_stream(stream.stream, fmt, ap);
}

static void close_dbloader_stream(output_stream stream)
{
	struct db_context_t *dbc = stream.stream->dbc;

	dbc_close_loader_stream(stream.stream);
	disconnect_from_db(dbc);
	free(dbc);
}

void print_timestamp(output_stream stream, struct tm *date)
{
	ostprintf(stream, "%04d-%02d-%02d %02d:%02d:%02d",
				date->tm_year + 1900, date->tm_mon + 1, date->tm_mday,
				date->tm_hour, date->tm_min, date->tm_sec);
}

/* Clause 4.3.3.1 */
void gen_customers(int worker_id, int start, int end)
{
	output_stream output;
	int i, j, k;
	char a_string[1024];
	struct tm *tm1;
	time_t t1;

	set_random_seed(0);
	printf("Generating customer table data...\n");

	output = open_output_stream(worker_id, "customer");
	if (!is_valid_stream(output))
		return;

	for (i = start; i <= end; i++) {
		for (j = 0; j < DISTRICT_CARDINALITY; j++) {
			for (k = 0; k < customers; k++) {
				/* c_id */
				ostprintf(output, "%d", k + 1);
				ostprintf(output, "%c", delimiter);

				/* c_d_id */
				ostprintf(output, "%d", j + 1);
				ostprintf(output, "%c", delimiter);

				/* c_w_id */
				ostprintf(output, "%d", i);
				ostprintf(output, "%c", delimiter);

				/* c_first */
				get_a_string(a_string, 8, 16);
				escape_me(a_string);
				ostprintf(output, "%s", a_string);
				ostprintf(output, "%c", delimiter);

				/* c_middle */
				ostprintf(output, "OE");
				ostprintf(output, "%c", delimiter);

				/* c_last Clause 4.3.2.7 */
				if (k < 1000) {
					get_c_last(a_string, k);
				} else {
					get_c_last(a_string, get_nurand(255, 0, 999));
				}
				escape_me(a_string);
				ostprintf(output, "%s", a_string);
				ostprintf(output, "%c", delimiter);

				/* c_street_1 */
				get_a_string(a_string, 10, 20);
				escape_me(a_string);
				ostprintf(output, "%s", a_string);
				ostprintf(output, "%c", delimiter);

				/* c_street_2 */
				get_a_string(a_string, 10, 20);
				escape_me(a_string);
				ostprintf(output, "%s", a_string);
				ostprintf(output, "%c", delimiter);

				/* c_city */
				get_a_string(a_string, 10, 20);
				escape_me(a_string);
				ostprintf(output, "%s", a_string);
				ostprintf(output, "%c", delimiter);

				/* c_state */
				get_l_string(a_string, 2, 2);
				ostprintf(output, "%s", a_string);
				ostprintf(output, "%c", delimiter);

				/* c_zip */
				get_n_string(a_string, 4, 4);
				ostprintf(output, "%s11111", a_string);
				ostprintf(output, "%c", delimiter);

				/* c_phone */
				get_n_string(a_string, 16, 16);
				ostprintf(output, "%s", a_string);
				ostprintf(output, "%c", delimiter);

				/* c_since */
				/*
				 * Milliseconds are not calculated.  This
				 * should also be the time when the data is
				 * loaded, I think.
				 */
				time(&t1);
				tm1 = localtime(&t1);
				print_timestamp(output, tm1);
				ostprintf(output, "%c", delimiter);

				/* c_credit */
				if (get_percentage() < .10) {
					ostprintf(output, "BC");
				} else {
					ostprintf(output, "GC");
				}
				ostprintf(output, "%c", delimiter);

				/* c_credit_lim */
				ostprintf(output, "50000.00");
				ostprintf(output, "%c", delimiter);

				/* c_discount */
				ostprintf(output, "0.%04d", get_random(5000));
				ostprintf(output, "%c", delimiter);

				/* c_balance */
				ostprintf(output, "-10.00");
				ostprintf(output, "%c", delimiter);

				/* c_ytd_payment */
				ostprintf(output, "10.00");
				ostprintf(output, "%c", delimiter);

				/* c_payment_cnt */
				ostprintf(output, "1");
				ostprintf(output, "%c", delimiter);

				/* c_delivery_cnt */
				ostprintf(output, "0");
				ostprintf(output, "%c", delimiter);

				/* c_data */
				get_a_string(a_string, 300, 500);
				escape_me(a_string);
				ostprintf(output, "%s", a_string);

				ostprintf(output, "\n");
			}
		}
	}

	close_output_stream(output);

	printf("Finished customer table data...\n");
	return;
}

/* Clause 4.3.3.1 */
void gen_districts(int worker_id, int start, int end)
{
	output_stream output;
	int i, j;
	char a_string[48];

	set_random_seed(0);
	printf("Generating district table data...\n");

	output = open_output_stream(worker_id, "district");
	if (!is_valid_stream(output))
		return;

	for (i = start; i <= end; i++) {
		for (j = 0; j < DISTRICT_CARDINALITY; j++) {
			/* d_id */
			ostprintf(output, "%d", j + 1);
			ostprintf(output, "%c", delimiter);

			/* d_w_id */
			ostprintf(output, "%d", i);
			ostprintf(output, "%c", delimiter);

			/* d_name */
			get_a_string(a_string, 6, 10);
			escape_me(a_string);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* d_street_1 */
			get_a_string(a_string, 10, 20);
			escape_me(a_string);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* d_street_2 */
			get_a_string(a_string, 10, 20);
			escape_me(a_string);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* d_city */
			get_a_string(a_string, 10, 20);
			escape_me(a_string);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* d_state */
			get_l_string(a_string, 2, 2);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* d_zip */
			get_n_string(a_string, 4, 4);
			ostprintf(output, "%s11111", a_string);
			ostprintf(output, "%c", delimiter);

			/* d_tax */
			ostprintf(output, "0.%04d", get_random(2000));
			ostprintf(output, "%c", delimiter);

			/* d_ytd */
			ostprintf(output, "30000.00");
			ostprintf(output, "%c", delimiter);

			/* d_next_o_id */
			ostprintf(output, "3001");

			ostprintf(output, "\n");
		}
	}

	close_output_stream(output);

	printf("Finished district table data...\n");
	return;
}

/* Clause 4.3.3.1 */
void gen_history(int worker_id, int start, int end)
{
	output_stream output;
	int i, j, k;
	char a_string[64];
	struct tm *tm1;
	time_t t1;

	set_random_seed(0);
	printf("Generating history table data...\n");

	output = open_output_stream(worker_id, "history");

	if(!is_valid_stream(output))
		return;

	for (i = start; i <= end; i++) {
		for (j = 0; j < DISTRICT_CARDINALITY; j++) {
			for (k = 0; k < customers; k++) {
				/* h_c_id */
				ostprintf(output, "%d", k + 1);
				ostprintf(output, "%c", delimiter);

				/* h_c_d_id */
				ostprintf(output, "%d", j + 1);
				ostprintf(output, "%c", delimiter);

				/* h_c_w_id */
				ostprintf(output, "%d", i);
				ostprintf(output, "%c", delimiter);

				/* h_d_id */
				ostprintf(output, "%d", j + 1);
				ostprintf(output, "%c", delimiter);

				/* h_w_id */
				ostprintf(output, "%d", i);
				ostprintf(output, "%c", delimiter);

				/* h_date */
				/*
				 * Milliseconds are not calculated.  This
				 * should also be the time when the data is
				 * loaded, I think.
				 */
				time(&t1);
				tm1 = localtime(&t1);
				print_timestamp(output, tm1);
				ostprintf(output, "%c", delimiter);

				/* h_amount */
				ostprintf(output, "10.00");
				ostprintf(output, "%c", delimiter);

				/* h_data */
				get_a_string(a_string, 12, 24);
				escape_me(a_string);
				ostprintf(output, "%s", a_string);

				ostprintf(output, "\n");
			}
		}
	}

	close_output_stream(output);

	printf("Finished history table data...\n");
	return;
}

/* Clause 4.3.3.1 */
void gen_items()
{
	output_stream output;
	int i;
	char a_string[128];
	int j;

	set_random_seed(0);
	printf("Generating item table data...\n");

	output = open_output_stream(0, "item");

	if(!is_valid_stream(output))
		return;

	for (i = 0; i < items; i++) {
		/* i_id */
		ostprintf(output, "%d", i + 1);
		ostprintf(output, "%c", delimiter);

		/* i_im_id */
		ostprintf(output, "%d", get_random(9999) + 1);
		ostprintf(output, "%c", delimiter);

		/* i_name */
		get_a_string(a_string, 14, 24);
		escape_me(a_string);
		ostprintf(output, "%s", a_string);
		ostprintf(output, "%c", delimiter);

		/* i_price */
		ostprintf(output, "%0.2f", ((double) get_random(9900) + 100.0) / 100.0);
		ostprintf(output, "%c", delimiter);

		/* i_data */
		get_a_string(a_string, 26, 50);
		if (get_percentage() < .10) {
			j = get_random(strlen(a_string) - 8);
			strncpy(a_string + j, "ORIGINAL", 8);
		}
		escape_me(a_string);
		ostprintf(output, "%s", a_string);

		ostprintf(output, "\n");
	}

	close_output_stream(output);

	printf("Finished item table data...\n");
	return;
}

/* Clause 4.3.3.1 */
void gen_new_orders(int worker_id, int start, int end)
{
	output_stream output;
	int i, j, k;

	set_random_seed(0);
	printf("Generating new-order table data...\n");

	output = open_output_stream(worker_id, "new_order");
	if(!is_valid_stream(output))
		return;

	for (i = start; i <= end; i++) {
		for (j = 0; j < DISTRICT_CARDINALITY; j++) {
			for (k = orders - new_orders; k < orders; k++) {
				/* no_o_id */
				ostprintf(output, "%d", k + 1);
				ostprintf(output, "%c", delimiter);

				/* no_d_id */
				ostprintf(output, "%d", j + 1);
				ostprintf(output, "%c", delimiter);

				/* no_w_id */
				ostprintf(output, "%d", i);

				ostprintf(output, "\n");
			}
		}
	}

	close_output_stream(output);

	printf("Finished new-order table data...\n");
	return;
}

/* Clause 4.3.3.1 */
void gen_orders(int worker_id, int start, int end)
{
	output_stream order, order_line;
	int i, j, k, l;
	char a_string[64];
	struct tm *tm1;
	time_t t1;

	struct node_t {
		int value;
		struct node_t *next;
	};
	struct node_t *head;
	struct node_t *current;
	struct node_t *prev;
	struct node_t *new_node;
	int iter;

	int o_ol_cnt;

	set_random_seed(0);
	printf("Generating order and order-line table data...\n");

	order = open_output_stream(worker_id, "orders");

	if(!is_valid_stream(order))
		return;
	order_line = open_output_stream(worker_id, "order_line");
	if(!is_valid_stream(order_line))
		return;

	for (i = start; i <= end; i++) {
		for (j = 0; j < DISTRICT_CARDINALITY; j++) {
			/*
			 * Create a random list of numbers from 1 to customers for o_c_id.
			 */
			head = (struct node_t *) malloc(sizeof(struct node_t));
			head->value = 1;
			head->next = NULL;
			for (k = 2; k <= customers; k++) {
				current = prev = head;

				/* Find a random place in the list to insert a number. */
				iter = get_random(k - 1);
				while (iter > 0) {
					prev = current;
					current = current->next;
					--iter;
				}

				/* Insert the number. */
				new_node = (struct node_t *) malloc(sizeof(struct node_t));
				if (current == prev) {
					/* Insert at the head of the list. */
					new_node->next = head;
					head = new_node;
				} else if (current == NULL) {
					/* Insert at the tail of the list. */
					prev->next = new_node;
					new_node->next = NULL;
				} else {
					/* Insert somewhere in the middle of the list. */
					prev->next = new_node;
					new_node->next = current;
				}
				new_node->value = k;
			}

			current = head;
			for (k = 0; k < orders; k++) {
				/* o_id */
				ostprintf(order, "%d", k + 1);
				ostprintf(order, "%c", delimiter);

				/* o_d_id */
				ostprintf(order, "%d", j + 1);
				ostprintf(order, "%c", delimiter);

				/* o_w_id */
				ostprintf(order, "%d", i);
				ostprintf(order, "%c", delimiter);

				/* o_c_id */
				ostprintf(order, "%d", current->value);
				ostprintf(order, "%c", delimiter);
				current = current->next;

				/* o_entry_d */
				/*
				 * Milliseconds are not calculated.  This
				 * should also be the time when the data is
				 * loaded, I think.
				 */
				time(&t1);
				tm1 = localtime(&t1);
				print_timestamp(order, tm1);
				ostprintf(order, "%c", delimiter);

				if (k < 2101) {
					ostprintf(order, "%d", get_random(9) + 1);
				} else {
					ostprintf(order, "%s", null_str);
				}
				ostprintf(order, "%c", delimiter);

				/* o_ol_cnt */
				o_ol_cnt = get_random(10) + 5;
				ostprintf(order, "%d", o_ol_cnt);
				ostprintf(order, "%c", delimiter);

				/* o_all_local */
				ostprintf(order, "1");

				ostprintf(order, "\n");

				/*
				 * Generate data in the order-line table for
				 * this order.
				 */
				for (l = 0; l < o_ol_cnt; l++) {
					/* ol_o_id */
					ostprintf(order_line, "%d", k + 1);
					ostprintf(order_line, "%c", delimiter);

					/* ol_d_id */
					ostprintf(order_line, "%d", j + 1);
					ostprintf(order_line, "%c", delimiter);

					/* ol_w_id */
					ostprintf(order_line, "%d", i);
					ostprintf(order_line, "%c", delimiter);

					/* ol_number */
					ostprintf(order_line, "%d", l + 1);
					ostprintf(order_line, "%c", delimiter);

					/* ol_i_id */
					ostprintf(order_line, "%d",
							get_random(ITEM_CARDINALITY - 1) + 1);
					ostprintf(order_line, "%c", delimiter);

					/* ol_supply_w_id */
					ostprintf(order_line, "%d", i);
					ostprintf(order_line, "%c", delimiter);

					if (k < 2101) {
						/*
						 * Milliseconds are not
						 * calculated.  This should
						 * also be the time when the
						 * data is loaded, I think.
						 */
						time(&t1);
						tm1 = localtime(&t1);
						print_timestamp(order_line, tm1);
					} else {
						ostprintf(order_line, "%s", null_str);
					}

					ostprintf(order_line, "%c", delimiter);

					/* ol_quantity */
					ostprintf(order_line, "5");
					ostprintf(order_line, "%c", delimiter);

					/* ol_amount */
					if (k < 2101) {
						ostprintf(order_line, "0.00");
					} else {
						ostprintf(order_line, "%f",
								(double) (get_random(999998) + 1) / 100.0);
					}
					ostprintf(order_line, "%c", delimiter);

					/* ol_dist_info */
					get_l_string(a_string, 24, 24);
					ostprintf(order_line, "%s", a_string);

					ostprintf(order_line, "\n");
				}
			}
			while (head != NULL) {
				current = head;
				head = head->next;
				free(current);
			}
		}
	}
	close_output_stream(order);
	close_output_stream(order_line);

	printf("Finished order and order-line table data...\n");
	return;
}

/* Clause 4.3.3.1 */
void gen_stock(int worker_id, int start, int end)
{
	output_stream output;
	int i, j, k;
	char a_string[128];

	set_random_seed(0);
	printf("Generating stock table data...\n");

	output = open_output_stream(worker_id, "stock");
	if (!is_valid_stream(output))
		return;

	for (i = start; i <= end; i++) {
		for (j = 0; j < items; j++) {
			/* s_i_id */
			ostprintf(output, "%d", j + 1);
			ostprintf(output, "%c", delimiter);

			/* s_w_id */
			ostprintf(output, "%d", i);
			ostprintf(output, "%c", delimiter);

			/* s_quantity */
			ostprintf(output, "%d", get_random(90) + 10);
			ostprintf(output, "%c", delimiter);

			/* s_dist_01 */
			get_l_string(a_string, 24, 24);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* s_dist_02 */
			get_l_string(a_string, 24, 24);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* s_dist_03 */
			get_l_string(a_string, 24, 24);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* s_dist_04 */
			get_l_string(a_string, 24, 24);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* s_dist_05 */
			get_l_string(a_string, 24, 24);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* s_dist_06 */
			get_l_string(a_string, 24, 24);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* s_dist_07 */
			get_l_string(a_string, 24, 24);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* s_dist_08 */
			get_l_string(a_string, 24, 24);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* s_dist_09 */
			get_l_string(a_string, 24, 24);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* s_dist_10 */
			get_l_string(a_string, 24, 24);
			ostprintf(output, "%s", a_string);
			ostprintf(output, "%c", delimiter);

			/* s_ytd */
			ostprintf(output, "0");
			ostprintf(output, "%c", delimiter);

			/* s_order_cnt */
			ostprintf(output, "0");
			ostprintf(output, "%c", delimiter);

			/* s_remote_cnt */
			ostprintf(output, "0");
			ostprintf(output, "%c", delimiter);

			/* s_data */
			get_a_string(a_string, 26, 50);
			if (get_percentage() < .10) {
				k = get_random(strlen(a_string) - 8);
				strncpy(a_string + k, "ORIGINAL", 8);
			}
			escape_me(a_string);
			ostprintf(output, "%s", a_string);

			ostprintf(output, "\n");
		}
	}

	close_output_stream(output);

	printf("Finished stock table data...\n");
	return;
}

/* Clause 4.3.3.1 */
void gen_warehouses(int worker_id, int start, int end)
{
	output_stream output;
	int i;
	char a_string[48];

	set_random_seed(0);
	printf("Generating warehouse table data...\n");

	output = open_output_stream(worker_id, "warehouse");
	if (!is_valid_stream(output))
		return;

	for (i = start; i <= end; i++) {
		/* w_id */
		ostprintf(output, "%d", i);
		ostprintf(output, "%c", delimiter);

		/* w_name */
		get_a_string(a_string, 6, 10);
		escape_me(a_string);
		ostprintf(output, "%s", a_string);
		ostprintf(output, "%c", delimiter);

		/* w_street_1 */
		get_a_string(a_string, 10, 20);
		escape_me(a_string);
		ostprintf(output, "%s", a_string);
		ostprintf(output, "%c", delimiter);

		/* w_street_2 */
		get_a_string(a_string, 10, 20);
		escape_me(a_string);
		ostprintf(output, "%s", a_string);
		ostprintf(output, "%c", delimiter);

		/* w_city */
		get_a_string(a_string, 10, 20);
		escape_me(a_string);
		ostprintf(output, "%s", a_string);
		ostprintf(output, "%c", delimiter);

		/* w_state */
		get_l_string(a_string, 2, 2);
		ostprintf(output, "%s", a_string);
		ostprintf(output, "%c", delimiter);

		/* w_zip */
		get_n_string(a_string, 4, 4);
		ostprintf(output, "%s11111", a_string);
		ostprintf(output, "%c", delimiter);

		/* w_tax */
		ostprintf(output, "0.%04d", get_random(2000));
		ostprintf(output, "%c", delimiter);

		/* w_ytd */
		ostprintf(output, "300000.00");

		ostprintf(output, "\n");
	}

	close_output_stream(output);

	printf("Finished warehouse table data...\n");
	return;
}

struct datagen_context_t
{
	int worker_id;
	int start_warehouse;
	int end_warehouse;
	int gen_items;				/* 1, this worker will gen items data also*/
};

typedef void (*table_loader)(int workid, int start, int end);

table_loader table_loaders[] = {
	gen_warehouses,
	gen_stock,
	gen_districts,
	gen_customers,
	gen_history,
	gen_orders,
	gen_new_orders
};

int table_loader_count = sizeof(table_loaders) / sizeof(table_loaders[0]);
void *datagen_worker(void *data)
{
	int i = 0;
	struct datagen_context_t *dc = (struct datagen_context_t *) data;
	int loader = dc->worker_id % table_loader_count;
	if(dc->gen_items == 1)
		gen_items();
	while( i < table_loader_count)
	{
		(*table_loaders[loader])(dc->worker_id, dc->start_warehouse, dc->end_warehouse);
		i++;
		loader++;
		if(loader == table_loader_count)
			loader = 0;
	}
	return NULL;
}

void usage(char *progname)
{
	fprintf(stderr, "usage: %s [-t <dbms>] -w # [-c #] [-i #] [-o #] [-s #] [-n #] [-j #] [-d <str>]\n", progname);
	fprintf(stderr, "\n");
	fprintf(stderr, "-t <dbms>\n");
	fprintf(stderr, "\tavailable:%s\n", dbc_manager_get_dbcnames());
	fprintf(stderr, "%s", dbc_manager_get_dbcusages());
	fprintf(stderr, "\n");
	fprintf(stderr, "-w #\n");
	fprintf(stderr, "\twarehouse cardinality\n");
	fprintf(stderr, "-c #\n");
	fprintf(stderr, "\tcustomer cardinality, default %d\n", CUSTOMER_CARDINALITY);
	fprintf(stderr, "-i #\n");
	fprintf(stderr, "\titem cardinality, default %d\n", ITEM_CARDINALITY);
	fprintf(stderr, "-o #\n");
	fprintf(stderr, "\torder cardinality, default %d\n", ORDER_CARDINALITY);
	fprintf(stderr, "-n #\n");
	fprintf(stderr, "\tnew-order cardinality, default %d\n", NEW_ORDER_CARDINALITY);
	fprintf(stderr, "-j #\n");
	fprintf(stderr, "\tNumber of worker threads within datagen, default is %d\n", jobs);
	fprintf(stderr, "-d <path>\n");
	fprintf(stderr, "\toutput path of data files\n");
}

int main(int argc, char *argv[])
{
	struct stat st;
	struct db_context_t *dbc;
	char dbms_name[16] = "";
	struct option *dbms_long_options = NULL;
	int c;
	pthread_t *tid;
	struct datagen_context_t *datagen_context;
	int j;
	int chunk, rem, curr_end;

	init_common();
	init_logging();
	init_dbc_manager();

	if (argc < 2) {
		usage(argv[0]);
		return 1;
	}

	/* first stage choose dbms */
	opterr = 0;
	while(1) {
		int option_index = 0;

		static struct option long_options[] = {
			{ 0, 0, 0, 0 }
		};
		c = getopt_long(argc, argv, "t:", long_options, &option_index);
		if (c == -1) {
			break;
		}
		switch (c) {
		case 't':
			strncpy(dbms_name, optarg, sizeof(dbms_name));
			goto parse_dbc_type_done;
			break;
		}
	}

parse_dbc_type_done:
	if(dbms_name[0] != '\0')
	{
		if(dbc_manager_set(dbms_name) != OK)
			return ERROR;

		stream_operation.open_stream = open_dbloader_stream;
		stream_operation.write_to_stream = write_to_dbloader_stream;
		stream_operation.close_stream = close_dbloader_stream;
		/* second stage real parse */
		dbms_long_options = dbc_manager_get_dbcoptions();
	}
	else
	{
		char *c = getenv(ENV_DBCLIENT_NAME);
		if (c)
			dbclient_command = c;
	}

	optind = 1;
	opterr = 1;
	while (1) {
		int option_index = 0;
		c = getopt_long(argc, argv, "t:c:d:i:j:n:o:t:w:?", dbms_long_options, &option_index);
		if (c == -1)
			break;
		switch (c) {
		case 0:
			if(dbc_manager_set_dbcoption(dbms_long_options[option_index].name, optarg) == ERROR)
				return 1;
			break;
		case 'c':
			customers = atoi(optarg);
			break;
		case 'd':
			strcpy(output_path, optarg);
			break;
		case 'i':
			items = atoi(optarg);
			break;
		case 'j':
			jobs = atoi(optarg);
			break;
		case 'n':
			new_orders = atoi(optarg);
			break;
		case 'o':
			orders = atoi(optarg);
			break;
		case 't':
			break;
		case 'w':
			warehouses = atoi(optarg);
			break;
		case '?':
			usage(argv[0]);
			return 0;
		default:
			fprintf(stderr, "?? getopt returned character code 0%o ??\n", c);
			usage(argv[0]);
			return 2;
		}
	}

	set_sqlapi_operation(SQLAPI_SIMPLE);

	if (warehouses == 0) {
		printf("-w must be used\n");
		return 3;
	}

	if (jobs > warehouses)
		jobs = warehouses;

	if (strlen(output_path) > 0 && ((stat(output_path, &st) < 0) ||
			(st.st_mode & S_IFMT) != S_IFDIR)) {
		printf("Output directory of data files '%s' not exists\n", output_path);
		return 3;
	}

	/* Set the correct delimiter. */
	delimiter = '\t';
	strcpy(null_str, "\\N");

	printf("warehouses = %d\n", warehouses);
	printf("districts = %d\n", DISTRICT_CARDINALITY);
	printf("customers = %d\n", customers);
	printf("items = %d\n", items);
	printf("orders = %d\n", orders);
	printf("stock = %d\n", items);
	printf("new_orders = %d\n", new_orders);
	printf("\n");
	printf("load threads: %d\n", jobs);
	if (strlen(output_path) > 0) {
		printf("Output directory of data files: %s\n",output_path);
	} else {
		printf("Output directory of data files: current directory\n");
	}
	printf("\n");
	if(dbms_name[0] != '\0')
		printf("Loading data to database for %d warehouse(s)...\n", warehouses);
	else
		printf("Generating data files for %d warehouse(s)...\n", warehouses);

	tid = (pthread_t *)malloc(sizeof(pthread_t) * jobs);
	datagen_context = (struct datagen_context_t *)malloc(sizeof(struct datagen_context_t) * jobs);

	chunk = warehouses / jobs;
	rem = warehouses % jobs;
	curr_end = 1;
	for(j = 0; j < jobs; j++)
	{
		datagen_context[j].worker_id = j;
		datagen_context[j].start_warehouse = curr_end;
		if(rem > 0)
		{
			curr_end += chunk + 1;
			rem --;
		}
		else
			curr_end += chunk;
		datagen_context[j].end_warehouse = curr_end - 1;
		if (j == 0)
			datagen_context[j].gen_items = 1;
		else
			datagen_context[j].gen_items = 0;

		pthread_create(&tid[j], NULL, &datagen_worker, &datagen_context[j]);
	}

	/* wait all threads finished */
	for(j = 0; j < jobs; j++)
		pthread_join(tid[j], NULL);

	free(tid);
	free(datagen_context);
	return 0;
}
