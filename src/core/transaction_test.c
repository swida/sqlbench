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

void
usage(const char *progname)
{
	printf("usage: %s -t <dbms> -T d/n/o/p/s [-I interface] [-w #] [-T #]", progname);
	printf("\n");
	printf("-t <dbms>\n");
	printf("\tavailable:%s\n", dbc_manager_get_dbcnames());
	printf("%s", dbc_manager_get_dbcusages());
	printf("-I interface\n");
	printf("\trun test using sql interface, available:\n");
	printf("\tsimple      default, just send sql statement to database\n");
	printf("\textended    use extended (prepare/bind/execute) protocol, better than simple\n");
	printf("\tstoreproc   use store procedure\n");
	printf("-T (d|n|o|p|s|i)\n");
	printf("\td = Delivery, n = New-Order, o = Order-Status,\n");
	printf("\tp = Payment, s = Stock-Level, i = Integrity\n");
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
}

int main(int argc, char *argv[])
{
	int transaction = -1;
	db_context_t *dbc;
	union transaction_data_t transaction_data;
	enum sqlapi_type use_sqlapi_type = SQLAPI_SIMPLE;

	int c;
	char dbms_name[16] = "";
	struct option *dbms_long_options = NULL;
	int rc;

	init_common();
	init_logging();
	init_dbc_manager();

	if (argc < 3) {
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
		c = getopt_long(argc, argv, "t:",
						long_options, &option_index);
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
	if(dbms_name[0] == '\0' || dbc_manager_set(dbms_name) != OK)
		return ERROR;

	/* second stage real parse */
	dbms_long_options = dbc_manager_get_dbcoptions();

	optind = 1;
	opterr = 1;
	while (1) {
		int option_index = 0;
		c = getopt_long(argc, argv, "c:i:I:n:o:t:T:w:?", dbms_long_options, &option_index);
		if (c == -1)
			break;
		switch (c) {
			case 0:
				if(dbc_manager_set_dbcoption(dbms_long_options[option_index].name, optarg) == ERROR)
					return 1;
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
				else if (optarg[0] == 'i')
					transaction = INTEGRITY;
				else {
					printf("unknown transaction type: %s\n", optarg);
					return 1;
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
			case 'I':
				if (strcasecmp(optarg, "simple") == 0)
					use_sqlapi_type = SQLAPI_SIMPLE;
				else if (strcasecmp(optarg, "extended") == 0)
					use_sqlapi_type = SQLAPI_EXTENDED;
				else if (strcasecmp(optarg, "storeproc") == 0)
					use_sqlapi_type = SQLAPI_STOREPROC;
				else
				{
					printf("unknown sql interface: %s\n", optarg);
					return 3;
				}
				break;
			case 'o':
				table_cardinality.orders = atoi(optarg);
				break;
			case 'n':
				table_cardinality.new_orders = atoi(optarg);
				break;
			case '?':
				usage(argv[0]);
				return 0;
			default:
				break;
		}
	}

	if (transaction == -1) {
		printf("-T flag was not used.\n");
		return 5;
	}

	set_sqlapi_operation(use_sqlapi_type);

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
		case INTEGRITY:
			generate_input_data(INTEGRITY,
								&transaction_data.integrity, table_cardinality.warehouses);
			break;
	}

	/*
	 * Process transaction by connecting directly to the database.
	 */
	printf("connecting directly to the database...\n");

	dbc = db_init();

	if (connect_to_db(dbc) != OK) {
		printf("cannot establish a database connection\n");
		return 1;
	}

	if (initialize_transactions(dbc) != OK) {
		printf("fail to initialize transactions\n");
		return 1;
	}

	process_transaction(transaction, dbc,
						(void *) &transaction_data);

	destroy_transactions(dbc);

	disconnect_from_db(dbc);

	dump(stdout, transaction, (void *) &transaction_data);
	printf("\ndone.\n");

	return 0;
}
