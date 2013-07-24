/*
 * This file is released under the terms of the Artistic License.
 * Please see the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Open Source Development Labs, Inc.
 *               2002-2010 Mark Wong
 *
 * Based on TPC-C Standard Specification Revision 5.0.
 */

#include "common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <getopt.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include <pthread.h>

#define DATAFILE_EXT ".data"
#define ENV_SQL_CLIENT_NAME "SQL_CLIENT_PROGRAM"
#define DEFAULT_SQL_CLIENT "psql"

#define MODE_FLAT 0
#define MODE_DIRECT 1

FILE *open_output_stream(int worker_id, char *table_name);

void gen_customers(int worker_id, int start, int end);
void gen_districts(int worker_id, int start, int end);
void gen_history(int worker_id, int start, int end);
void gen_items();
void gen_new_order(int worker_id, int start, int end);
void gen_orders(int worker_id, int start, int end);
void gen_stock(int worker_id, int start, int end);
void gen_warehouses(int worker_id, int start, int end);

int warehouses = 0;
int customers = CUSTOMER_CARDINALITY;
int items = ITEM_CARDINALITY;
int orders = ORDER_CARDINALITY;
int new_orders = NEW_ORDER_CARDINALITY;

int jobs = 1;

int mode_load = MODE_FLAT;

char delimiter = ',';
char null_str[16] = "\"NULL\"";

char *sql_client = DEFAULT_SQL_CLIENT;

#define ERR_MSG( fn ) { (void)fflush(stderr); \
		(void)fprintf(stderr, __FILE__ ":%d:" #fn ": %s\n", \
		__LINE__, strerror(errno)); }
#define METAPRINTF( args ) if( fprintf args < 0  ) ERR_MSG( fn )

/* Oh my gosh, is there a better way to do this? */
#define FPRINTF(a, b, c) \
	METAPRINTF((a, b, c));

#define FPRINTF2(a, b) \
		METAPRINTF((a, b));

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

void print_timestamp(FILE *ofile, struct tm *date)
{
	METAPRINTF((ofile, "%04d-%02d-%02d %02d:%02d:%02d",
				date->tm_year + 1900, date->tm_mon + 1, date->tm_mday,
				date->tm_hour, date->tm_min, date->tm_sec));
}

FILE *open_output_stream(int worker_id, char *table_name)
{
	FILE *output;
	char filename[1024] = "\0";

	if (mode_load == MODE_FLAT) {
		if (strlen(output_path) > 0) {
			strcpy(filename, output_path);
			strcat(filename, "/");
		}
		sprintf(filename, "%s%s%d%s", filename, table_name, worker_id, DATAFILE_EXT);
		output = fopen(filename, "w");
		if (output == NULL) {
			printf("cannot open %s\n", table_name);
			return NULL;
		}
	} else if (mode_load == MODE_DIRECT) {
		output = popen(sql_client, "w");
		if (output == NULL) {
			printf("error cannot open pipe for direct load\n");
			return NULL;
		}
		/* FIXME: Handle properly instead of blindly reading the output. */
		while (fgetc(output) != EOF) ;

		fprintf(output, "BEGIN;\n");
		/* FIXME: Handle properly instead of blindly reading the output. */
		while (fgetc(output) != EOF) ;

		fprintf(output,
				"COPY %s FROM STDIN DELIMITER '%c' NULL '%s';\n",
				table_name, delimiter, null_str);
		/* FIXME: Handle properly instead of blindly reading the output. */
		while (fgetc(output) != EOF) ;
	}
	return output;
}

void close_output_stream(FILE *output)
{
	if (mode_load == MODE_FLAT) {
		fclose(output);
	} else {
		fprintf(output, "\\.\n");
		/* FIXME: Handle properly instead of blindly reading the output. */
		while (fgetc(output) != EOF) ;

		fprintf(output, "COMMIT;\n");
		/* FIXME: Handle properly instead of blindly reading the output. */
		while (fgetc(output) != EOF) ;

		pclose(output);
	}
}

/* Clause 4.3.3.1 */
void gen_customers(int worker_id, int start, int end)
{
	FILE *output;
	int i, j, k;
	char a_string[1024];
	struct tm *tm1;
	time_t t1;

	set_random_seed(0);
	printf("Generating customer table data...\n");

	output = open_output_stream(worker_id, "customer");
	if(output == NULL)
		return;

	for (i = start; i <= end; i++) {
		for (j = 0; j < DISTRICT_CARDINALITY; j++) {
			for (k = 0; k < customers; k++) {
				/* c_id */
				FPRINTF(output, "%d", k + 1);
				METAPRINTF((output, "%c", delimiter));

				/* c_d_id */
				FPRINTF(output, "%d", j + 1);
				METAPRINTF((output, "%c", delimiter));

				/* c_w_id */
				FPRINTF(output, "%d", i);
				METAPRINTF((output, "%c", delimiter));

				/* c_first */
				get_a_string(a_string, 8, 16);
				escape_me(a_string);
				FPRINTF(output, "%s", a_string);
				METAPRINTF((output, "%c", delimiter));

				/* c_middle */
				FPRINTF2(output, "OE");
				METAPRINTF((output, "%c", delimiter));

				/* c_last Clause 4.3.2.7 */
				if (k < 1000) {
					get_c_last(a_string, k);
				} else {
					get_c_last(a_string, get_nurand(255, 0, 999));
				}
				escape_me(a_string);
				FPRINTF(output, "%s", a_string);
				METAPRINTF((output, "%c", delimiter));

				/* c_street_1 */
				get_a_string(a_string, 10, 20);
				escape_me(a_string);
				FPRINTF(output, "%s", a_string);
				METAPRINTF((output, "%c", delimiter));

				/* c_street_2 */
				get_a_string(a_string, 10, 20);
				escape_me(a_string);
				FPRINTF(output, "%s", a_string);
				METAPRINTF((output, "%c", delimiter));

				/* c_city */
				get_a_string(a_string, 10, 20);
				escape_me(a_string);
				FPRINTF(output, "%s", a_string);
				METAPRINTF((output, "%c", delimiter));

				/* c_state */
				get_l_string(a_string, 2, 2);
				FPRINTF(output, "%s", a_string);
				METAPRINTF((output, "%c", delimiter));

				/* c_zip */
				get_n_string(a_string, 4, 4);
				FPRINTF(output, "%s11111", a_string);
				METAPRINTF((output, "%c", delimiter));

				/* c_phone */
				get_n_string(a_string, 16, 16);
				FPRINTF(output, "%s", a_string);
				METAPRINTF((output, "%c", delimiter));

				/* c_since */
				/*
				 * Milliseconds are not calculated.  This
				 * should also be the time when the data is
				 * loaded, I think.
				 */
				time(&t1);
				tm1 = localtime(&t1);
				print_timestamp(output, tm1);
				METAPRINTF((output, "%c", delimiter));

				/* c_credit */
				if (get_percentage() < .10) {
					FPRINTF2(output, "BC");
				} else {
					FPRINTF2(output, "GC");
				}
				METAPRINTF((output, "%c", delimiter));

				/* c_credit_lim */
				FPRINTF2(output, "50000.00");
				METAPRINTF((output, "%c", delimiter));

				/* c_discount */
				FPRINTF(output, "0.%04d", get_random(5000));
				METAPRINTF((output, "%c", delimiter));

				/* c_balance */
				FPRINTF2(output, "-10.00");
				METAPRINTF((output, "%c", delimiter));

				/* c_ytd_payment */
				FPRINTF2(output, "10.00");
				METAPRINTF((output, "%c", delimiter));

				/* c_payment_cnt */
				FPRINTF2(output, "1");
				METAPRINTF((output, "%c", delimiter));

				/* c_delivery_cnt */
				FPRINTF2(output, "0");
				METAPRINTF((output, "%c", delimiter));

				/* c_data */
				get_a_string(a_string, 300, 500);
				escape_me(a_string);
				FPRINTF(output, "%s", a_string);

				METAPRINTF((output, "\n"));
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
	FILE *output;
	int i, j;
	char a_string[48];

	set_random_seed(0);
	printf("Generating district table data...\n");

	output = open_output_stream(worker_id, "district");
	if(output == NULL)
		return;

	for (i = start; i <= end; i++) {
		for (j = 0; j < DISTRICT_CARDINALITY; j++) {
			/* d_id */
			FPRINTF(output, "%d", j + 1);
			METAPRINTF((output, "%c", delimiter));

			/* d_w_id */
			FPRINTF(output, "%d", i);
			METAPRINTF((output, "%c", delimiter));

			/* d_name */
			get_a_string(a_string, 6, 10);
			escape_me(a_string);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* d_street_1 */
			get_a_string(a_string, 10, 20);
			escape_me(a_string);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* d_street_2 */
			get_a_string(a_string, 10, 20);
			escape_me(a_string);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* d_city */
			get_a_string(a_string, 10, 20);
			escape_me(a_string);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* d_state */
			get_l_string(a_string, 2, 2);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* d_zip */
			get_n_string(a_string, 4, 4);
			FPRINTF(output, "%s11111", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* d_tax */
			FPRINTF(output, "0.%04d", get_random(2000));
			METAPRINTF((output, "%c", delimiter));

			/* d_ytd */
			FPRINTF2(output, "30000.00");
			METAPRINTF((output, "%c", delimiter));

			/* d_next_o_id */
			FPRINTF2(output, "3001");

			METAPRINTF((output, "\n"));
		}
	}

	close_output_stream(output);

	printf("Finished district table data...\n");
	return;
}

/* Clause 4.3.3.1 */
void gen_history(int worker_id, int start, int end)
{
	FILE *output;
	int i, j, k;
	char a_string[64];
	struct tm *tm1;
	time_t t1;

	set_random_seed(0);
	printf("Generating history table data...\n");

	output = open_output_stream(worker_id, "history");
	if(output == NULL)
		return;

	for (i = start; i <= end; i++) {
		for (j = 0; j < DISTRICT_CARDINALITY; j++) {
			for (k = 0; k < customers; k++) {
				/* h_c_id */
				FPRINTF(output, "%d", k + 1);
				METAPRINTF((output, "%c", delimiter));

				/* h_c_d_id */
				FPRINTF(output, "%d", j + 1);
				METAPRINTF((output, "%c", delimiter));

				/* h_c_w_id */
				FPRINTF(output, "%d", i);
				METAPRINTF((output, "%c", delimiter));

				/* h_d_id */
				FPRINTF(output, "%d", j + 1);
				METAPRINTF((output, "%c", delimiter));

				/* h_w_id */
				FPRINTF(output, "%d", i);
				METAPRINTF((output, "%c", delimiter));

				/* h_date */
				/*
				 * Milliseconds are not calculated.  This
				 * should also be the time when the data is
				 * loaded, I think.
				 */
				time(&t1);
				tm1 = localtime(&t1);
				print_timestamp(output, tm1);
				METAPRINTF((output, "%c", delimiter));

				/* h_amount */
				FPRINTF2(output, "10.00");
				METAPRINTF((output, "%c", delimiter));

				/* h_data */
				get_a_string(a_string, 12, 24);
				escape_me(a_string);
				FPRINTF(output, "%s", a_string);

				METAPRINTF((output, "\n"));
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
	FILE *output;
	int i;
	char a_string[128];
	int j;

	set_random_seed(0);
	printf("Generating item table data...\n");

	output = open_output_stream(0, "item");
	if(output == NULL)
		return;

	for (i = 0; i < items; i++) {
		/* i_id */
		FPRINTF(output, "%d", i + 1);
		METAPRINTF((output, "%c", delimiter));

		/* i_im_id */
		FPRINTF(output, "%d", get_random(9999) + 1);
		METAPRINTF((output, "%c", delimiter));

		/* i_name */
		get_a_string(a_string, 14, 24);
		escape_me(a_string);
		FPRINTF(output, "%s", a_string);
		METAPRINTF((output, "%c", delimiter));

		/* i_price */
		FPRINTF(output, "%0.2f", ((double) get_random(9900) + 100.0) / 100.0);
		METAPRINTF((output, "%c", delimiter));

		/* i_data */
		get_a_string(a_string, 26, 50);
		if (get_percentage() < .10) {
			j = get_random(strlen(a_string) - 8);
			strncpy(a_string + j, "ORIGINAL", 8);
		}
		escape_me(a_string);
		FPRINTF(output, "%s", a_string);

		METAPRINTF((output, "\n"));
	}

	close_output_stream(output);

	printf("Finished item table data...\n");
	return;
}

/* Clause 4.3.3.1 */
void gen_new_orders(int worker_id, int start, int end)
{
	FILE *output;
	int i, j, k;

	set_random_seed(0);
	printf("Generating new-order table data...\n");

	output = open_output_stream(worker_id, "new_order");
	if(output == NULL)
		return;

	for (i = start; i <= end; i++) {
		for (j = 0; j < DISTRICT_CARDINALITY; j++) {
			for (k = orders - new_orders; k < orders; k++) {
				/* no_o_id */
				FPRINTF(output, "%d", k + 1);
				METAPRINTF((output, "%c", delimiter));

				/* no_d_id */
				FPRINTF(output, "%d", j + 1);
				METAPRINTF((output, "%c", delimiter));

				/* no_w_id */
				FPRINTF(output, "%d", i);

				METAPRINTF((output, "\n"));
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
	FILE *order, *order_line;
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
	if(order == NULL)
		return;
	order_line = open_output_stream(worker_id, "order_line");
	if(order_line == NULL)
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
				FPRINTF(order, "%d", k + 1);
				METAPRINTF((order, "%c", delimiter));

				/* o_d_id */
				FPRINTF(order, "%d", j + 1);
				METAPRINTF((order, "%c", delimiter));

				/* o_w_id */
				FPRINTF(order, "%d", i);
				METAPRINTF((order, "%c", delimiter));

				/* o_c_id */
				FPRINTF(order, "%d", current->value);
				METAPRINTF((order, "%c", delimiter));
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
				METAPRINTF((order, "%c", delimiter));

				if (k < 2101) {
					FPRINTF(order, "%d", get_random(9) + 1);
				} else {
					METAPRINTF((order, "%s", null_str));
				}
				METAPRINTF((order, "%c", delimiter));

				/* o_ol_cnt */
				o_ol_cnt = get_random(10) + 5;
				FPRINTF(order, "%d", o_ol_cnt);
				METAPRINTF((order, "%c", delimiter));

				/* o_all_local */
				FPRINTF2(order, "1");

				METAPRINTF((order, "\n"));

				/*
				 * Generate data in the order-line table for
				 * this order.
				 */
				for (l = 0; l < o_ol_cnt; l++) {
					/* ol_o_id */
					FPRINTF(order_line, "%d", k + 1);
					METAPRINTF((order_line, "%c", delimiter));

					/* ol_d_id */
					FPRINTF(order_line, "%d", j + 1);
					METAPRINTF((order_line, "%c", delimiter));

					/* ol_w_id */
					FPRINTF(order_line, "%d", i);
					METAPRINTF((order_line, "%c", delimiter));

					/* ol_number */
					FPRINTF(order_line, "%d", l + 1);
					METAPRINTF((order_line, "%c", delimiter));

					/* ol_i_id */
					FPRINTF(order_line, "%d",
							get_random(ITEM_CARDINALITY - 1) + 1);
					METAPRINTF((order_line, "%c", delimiter));

					/* ol_supply_w_id */
					FPRINTF(order_line, "%d", i);
					METAPRINTF((order_line, "%c", delimiter));

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
						METAPRINTF((order_line, "%s", null_str));
					}

					METAPRINTF((order_line, "%c", delimiter));

					/* ol_quantity */
					FPRINTF2(order_line, "5");
					METAPRINTF((order_line, "%c", delimiter));

					/* ol_amount */
					if (k < 2101) {
						FPRINTF2(order_line, "0.00");
					} else {
						FPRINTF(order_line, "%f",
								(double) (get_random(999998) + 1) / 100.0);
					}
					METAPRINTF((order_line, "%c", delimiter));

					/* ol_dist_info */
					get_l_string(a_string, 24, 24);
					FPRINTF(order_line, "%s", a_string);

					METAPRINTF((order_line, "\n"));
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
	FILE *output;
	int i, j, k;
	char a_string[128];

	set_random_seed(0);
	printf("Generating stock table data...\n");

	output = open_output_stream(worker_id, "stock");
	if(output == NULL)
		return;

	for (i = start; i <= end; i++) {
		for (j = 0; j < items; j++) {
			/* s_i_id */
			FPRINTF(output, "%d", j + 1);
			METAPRINTF((output, "%c", delimiter));

			/* s_w_id */
			FPRINTF(output, "%d", i);
			METAPRINTF((output, "%c", delimiter));

			/* s_quantity */
			FPRINTF(output, "%d", get_random(90) + 10);
			METAPRINTF((output, "%c", delimiter));

			/* s_dist_01 */
			get_l_string(a_string, 24, 24);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* s_dist_02 */
			get_l_string(a_string, 24, 24);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* s_dist_03 */
			get_l_string(a_string, 24, 24);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* s_dist_04 */
			get_l_string(a_string, 24, 24);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* s_dist_05 */
			get_l_string(a_string, 24, 24);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* s_dist_06 */
			get_l_string(a_string, 24, 24);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* s_dist_07 */
			get_l_string(a_string, 24, 24);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* s_dist_08 */
			get_l_string(a_string, 24, 24);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* s_dist_09 */
			get_l_string(a_string, 24, 24);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* s_dist_10 */
			get_l_string(a_string, 24, 24);
			FPRINTF(output, "%s", a_string);
			METAPRINTF((output, "%c", delimiter));

			/* s_ytd */
			FPRINTF2(output, "0");
			METAPRINTF((output, "%c", delimiter));

			/* s_order_cnt */
			FPRINTF2(output, "0");
			METAPRINTF((output, "%c", delimiter));

			/* s_remote_cnt */
			FPRINTF2(output, "0");
			METAPRINTF((output, "%c", delimiter));

			/* s_data */
			get_a_string(a_string, 26, 50);
			if (get_percentage() < .10) {
				k = get_random(strlen(a_string) - 8);
				strncpy(a_string + k, "ORIGINAL", 8);
			}
			escape_me(a_string);
			FPRINTF(output, "%s", a_string);

			METAPRINTF((output, "\n"));
		}
	}

	close_output_stream(output);

	printf("Finished stock table data...\n");
	return;
}

/* Clause 4.3.3.1 */
void gen_warehouses(int worker_id, int start, int end)
{
	FILE *output;
	int i;
	char a_string[48];

	set_random_seed(0);
	printf("Generating warehouse table data...\n");

	output = open_output_stream(worker_id, "warehouse");
	if(output == NULL)
		return;

	for (i = start; i <= end; i++) {
		/* w_id */
		FPRINTF(output, "%d", i);
		METAPRINTF((output, "%c", delimiter));

		/* w_name */
		get_a_string(a_string, 6, 10);
		escape_me(a_string);
		FPRINTF(output, "%s", a_string);
		METAPRINTF((output, "%c", delimiter));

		/* w_street_1 */
		get_a_string(a_string, 10, 20);
		escape_me(a_string);
		FPRINTF(output, "%s", a_string);
		METAPRINTF((output, "%c", delimiter));

		/* w_street_2 */
		get_a_string(a_string, 10, 20);
		escape_me(a_string);
		FPRINTF(output, "%s", a_string);
		METAPRINTF((output, "%c", delimiter));

		/* w_city */
		get_a_string(a_string, 10, 20);
		escape_me(a_string);
		FPRINTF(output, "%s", a_string);
		METAPRINTF((output, "%c", delimiter));

		/* w_state */
		get_l_string(a_string, 2, 2);
		FPRINTF(output, "%s", a_string);
		METAPRINTF((output, "%c", delimiter));

		/* w_zip */
		get_n_string(a_string, 4, 4);
		FPRINTF(output, "%s11111", a_string);
		METAPRINTF((output, "%c", delimiter));

		/* w_tax */
		FPRINTF(output, "0.%04d", get_random(2000));
		METAPRINTF((output, "%c", delimiter));

		/* w_ytd */
		FPRINTF2(output, "300000.00");

		METAPRINTF((output, "\n"));
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

int main(int argc, char *argv[])
{
	struct stat st;

	/* For getoptlong(). */
	int c;
	pthread_t *tid;
	struct datagen_context_t *datagen_context;
	int j;
	int chunk, rem, curr_end;

	init_common();

	if (argc < 2) {
		printf("Usage: %s -w # [-c #] [-i #] [-o #] [-s #] [-n #] [-j #] [-d <str>]\n",
				argv[0]);
		printf("\n");
		printf("-w #\n");
		printf("\twarehouse cardinality\n");
		printf("-c #\n");
		printf("\tcustomer cardinality, default %d\n", CUSTOMER_CARDINALITY);
		printf("-i #\n");
		printf("\titem cardinality, default %d\n", ITEM_CARDINALITY);
		printf("-o #\n");
		printf("\torder cardinality, default %d\n", ORDER_CARDINALITY);
		printf("-n #\n");
		printf("\tnew-order cardinality, default %d\n", NEW_ORDER_CARDINALITY);
		printf("-j #\n");
		printf("\tNumber of worker threads within datagen, default is %d\n", jobs);
		printf("-d <path>\n");
		printf("\toutput path of data files\n");
		printf("--direct\n");
		printf("\tdon't generate flat files, load directly into database\n");
		return 1;
	}

	/* Parse command line arguments. */
	while (1) {
		int option_index = 0;
		static struct option long_options[] = {
			{ "direct", no_argument, &mode_load, MODE_DIRECT },
			{ 0, 0, 0, 0 }
		};

		c = getopt_long(argc, argv, "c:d:i:n:j:o:w:",
				long_options, &option_index);
		if (c == -1) {
			break;
		}

		switch (c) {
		case 0:
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
		case 'w':
			warehouses = atoi(optarg);
			break;
		default:
			printf("?? getopt returned character code 0%o ??\n", c);
			return 2;
		}
	}

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

	if (mode_load == MODE_DIRECT)
	{
		char *c = getenv(ENV_SQL_CLIENT_NAME);
		if (c != NULL)
			sql_client = c;
	}

	/* Set the correct delimiter. */
	delimiter = '\t';
	strcpy(null_str, "");

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
