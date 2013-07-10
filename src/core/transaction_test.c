/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 *
 * 24 June 2002
 */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <getopt.h>

#include "common.h"
#include "db.h"
#include "input_data_generator.h"
#include "logging.h"
#include "transaction_data.h"
#include "dbc.h"
int mode_altered = 0;
int main(int argc, char *argv[])
{
	int i;
	int transaction = -1;
	struct db_context_t *dbc;
	union transaction_data_t transaction_data;

	int c;
	char dbms_name[16] = "";
	struct option *dbms_long_options = NULL;

	init_common();
	init_logging();
	init_dbc_manager();

	if (argc < 3) {
		printf("usage: %s -t <dbms> -T d/n/o/p/s [-w #] [-c #] [-i #] [-o #] [-n #]",
			argv[0]);
		printf("\n");
		printf("-t <dbms>\n");
		printf("\tavailable:%s\n", dbc_manager_get_dbcnames());
		printf("%s", dbc_manager_get_dbcusages());
		printf("-T (d|n|o|p|s)\n");
		printf("\td = Delivery, n = New-Order, o = Order-Status,\n");
		printf("\tp = Payment, s = Stock-Level\n");
		printf("-w #\n");
		printf("\tnumber of warehouses, default 1\n");
		printf("-c #\n");
		printf("\tcustomer cardinality, default %d\n",
			CUSTOMER_CARDINALITY);
		printf("-i #\n");
		printf("\titem cardinality, default %d\n", ITEM_CARDINALITY);
		printf("-o #\n");
		printf("\torder cardinality, default %d\n", ORDER_CARDINALITY);
		printf("-n #\n");
		printf("\tnew-order cardinality, default %d\n",
			NEW_ORDER_CARDINALITY);
		return 1;
	}

	/* first stage choose dbms */
	opterr = 0;
	while(1) {
		int option_index = 0;

		static struct option long_options[] = {
			{ 0, 0, 0, 0 }
		};
		c = getopt_long(argc, argv, "t:",
			long_options, &option_index);
		if (c == -1) {
			break;
		}
		switch (c) {
		case 't':
			strncpy(dbms_name, optarg, sizeof(dbms_name));
			break;
		}
	}

	if(dbms_name[0] == '\0' || dbc_manager_set(dbms_name) != OK)
		return ERROR;

	/* second stage real parse */
	dbms_long_options = dbc_manager_get_dbcoptions();

	optind = 1;
	opterr = 1;
	while (1) {
		int option_index = 0;
		c = getopt_long(argc, argv, "c:i:n:o:t:T:w:",
			dbms_long_options, &option_index);
		if (c == -1) {
			break;
		}

		switch (c) {
		case 0:
			if(dbc_manager_set_dbcoption(dbms_long_options[option_index].name, optarg) == ERROR)
				return 3;
			break;
		case 'T':
			if (optarg[0] == 'd')
				transaction = DELIVERY;
			else if (optarg[0] == 'n')
				transaction = NEW_ORDER;
			else if (optarg[0] == 'o')
				transaction = ORDER_STATUS;
			else if (optarg[0] == 'p') 
				transaction = PAYMENT;
			else if (optarg[0] == 's') 
				transaction = STOCK_LEVEL;
			else {
				printf("unknown transaction: %s\n", optarg);
				return 3;
			}
			break;
			case 'w':
			table_cardinality.warehouses = atoi(optarg);
			break;
			case 'c':
			table_cardinality.customers = atoi(optarg);
			break;
			case 'i':
			table_cardinality.items = atoi(optarg);
			break;
			case 'o':
			table_cardinality.orders = atoi(optarg);
			break;
			case 'n':
			table_cardinality.new_orders = atoi(optarg);
			break;
		case '?':
			return 3;
			break;
		default:
			break;
		}
	}

	if (transaction == -1) {
		printf("-T flag was not used.\n");
		return 5;
	}

	set_sqlapi_operation(SQLAPI_SIMPLE);

	/* Double check database table cardinality. */
	printf("\n");
	printf("database table cardinalities:\n");
	printf("warehouses = %d\n", table_cardinality.warehouses);
	printf("districts = %d\n", table_cardinality.districts);
	printf("customers = %d\n", table_cardinality.customers);
	printf("items = %d\n", table_cardinality.items);
	printf("orders = %d\n", table_cardinality.orders);
	printf("stock = %d\n", table_cardinality.items);
	printf("new-orders = %d\n", table_cardinality.new_orders);
	printf("\n");

	set_random_seed(time(NULL));

	/* Generate input data. */
	bzero(&transaction_data, sizeof(union transaction_data_t));
	switch (transaction) {
	case DELIVERY:
		generate_input_data(DELIVERY,
			(void *) &transaction_data.delivery,
			get_random(table_cardinality.warehouses) + 1);
		break;
	case NEW_ORDER:
		generate_input_data(NEW_ORDER,
			(void *) &transaction_data.new_order,
			get_random(table_cardinality.warehouses) + 1);
		break;
	case ORDER_STATUS:
		generate_input_data(ORDER_STATUS,
			(void *) &transaction_data.order_status,
			get_random(table_cardinality.warehouses) + 1);
		break;
	case PAYMENT:
		generate_input_data(PAYMENT, (void *) &transaction_data.payment,
			get_random(table_cardinality.warehouses) + 1);
		break;
	case STOCK_LEVEL:
		generate_input_data2(STOCK_LEVEL,
			(void *) &transaction_data.stock_level,
			get_random(table_cardinality.warehouses) + 1,
			get_random(table_cardinality.districts) + 1);
		break;
	}

		/*
		 * Process transaction by connecting directly to the database.
		 */
		printf("connecting directly to the database...\n");

		dbc = db_init();

		if (connect_to_db(dbc) != OK) {
			printf("cannot establish a database connection\n");
			return 6;
		}
		if (process_transaction(transaction, dbc,
			(void *) &transaction_data) != OK) {
			disconnect_from_db(dbc);
			printf("transaction failed\n");
			return 11;
		}
		disconnect_from_db(dbc);

	dump(stdout, transaction, (void *) &transaction_data);
	printf("\ndone.\n");

	return 0;
}
