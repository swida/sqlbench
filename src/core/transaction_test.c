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

#include "common.h"
#include "client_interface.h"
#include "db.h"
#include "input_data_generator.h"
#include "logging.h"
#include "transaction_data.h"

#ifdef LIBPQ
char postmaster_port[32];
#endif /* LIBPQ */
char connect_str[32] = "";
int mode_altered = 0;

int main(int argc, char *argv[])
{
	int i;
	int transaction = -1;
	struct db_context_t dbc;
	union transaction_data_t transaction_data;

	int port = 0;
	int sockfd;
	struct client_transaction_t client_txn;

	init_common();
	init_logging();

	if (argc < 3) {
		printf("usage: %s -d <connect string> -t d/n/o/p/s [-w #] [-c #] [-i #] [-o #] [-n #] [-p #]",
			argv[0]);
#ifdef LIBPQ
		printf(" -l #");
#endif /* LIBPQ */
		printf("\n\n");
		printf("-d <connect string>\n");
#ifdef ODBC
		printf("\tdatabase connect string\n");
#endif /* ODBC */
#ifdef LIBPQ
		printf("\tdatabase hostname\n");
		printf("-l #\n");
		printf("\tport of the postmaster\n");
#endif /* LIBPQ */
		printf("-t (d|n|o|p|s)\n");
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
		printf("-p #\n");
		printf("\tport of client program, if -d is used, -d takes the address\n");
		printf("\tof the client program host system\n");
		return 1;
	}

	for (i = 1; i < argc; i += 2) {
		if (strlen(argv[i]) != 2) {
			printf("invalid flag: %s\n", argv[i]);
			return 2;
		}
		if (argv[i][1] == 'd') {
			strcpy(connect_str, argv[i + 1]);
		} else if (argv[i][1] == 't') {
			if (argv[i + 1][0] == 'd') {
				transaction = DELIVERY;
			} else if (argv[i + 1][0] == 'n') {
				transaction = NEW_ORDER;
			} else if (argv[i + 1][0] == 'o') {
				transaction = ORDER_STATUS;
			} else if (argv[i + 1][0] == 'p') {
				transaction = PAYMENT;
			} else if (argv[i + 1][0] == 's') {
				transaction = STOCK_LEVEL;
			} else {
				printf("unknown transaction: %s\n",
					argv[i + 1]);
				return 3;
			}
		} else if (argv[i][1] == 'w') {
			table_cardinality.warehouses = atoi(argv[i + 1]);
		} else if (argv[i][1] == 'c') {
			table_cardinality.customers = atoi(argv[i + 1]);
		} else if (argv[i][1] == 'i') {
			table_cardinality.items = atoi(argv[i + 1]);
		} else if (argv[i][1] == 'o') {
			table_cardinality.orders = atoi(argv[i + 1]);
		} else if (argv[i][1] == 'n') {
			table_cardinality.new_orders = atoi(argv[i + 1]);
		} else if (argv[i][1] == 'p') {
			port = atoi(argv[i + 1]);
#ifdef LIBPQ
		} else if (argv[i][1] == 'l') {
			strcpy(postmaster_port, argv[i + 1]);
#endif /* LIBPQ */
		} else {
			printf("invalid flag: %s\n", argv[i]);
			return 2;
		}
	}

	if (strlen(connect_str) == 0) {
		printf("-d flag was not used.\n");
		return 4;
	}

	if (transaction == -1) {
		printf("-t flag was not used.\n");
		return 5;
	}

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

	srand(time(NULL));

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

	if (port == 0) {
		/*
		 * Process transaction by connecting directly to the database.
		 */
		printf("connecting directly to the database...\n");
#ifdef ODBC
		db_init(connect_str, DB_USER, DB_PASS);
#endif /* ODBC */
#ifdef LIBPQ
		db_init(DB_NAME, connect_str, postmaster_port);
#endif /* LIBPQ */
		if (connect_to_db(&dbc) != OK) {
			printf("cannot establish a database connection\n");
			return 6;
		}
		if (process_transaction(transaction, &dbc,
			(void *) &transaction_data) != OK) {
			disconnect_from_db(&dbc);
			printf("transaction failed\n");
			return 11;
		}
		disconnect_from_db(&dbc);
	} else {
		/* Process transaction by connecting to the client program. */
		printf("connecting to client program on port %d...\n", port);

		sockfd = connect_to_client(connect_str, port);
		if (sockfd > 0) {
			printf("connected to client\n");
		}

		client_txn.transaction = transaction;
		memcpy(&client_txn.transaction_data, &transaction_data,
			sizeof(union transaction_data_t));
		printf("sending transaction data...\n");
		if (send_transaction_data(sockfd, &client_txn) != OK) {
			printf("send_transaction_data() error\n");
			return 7;
		}

		printf("receiving transaction data...\n");
		if (receive_transaction_data(sockfd, &client_txn) != OK) {
			printf("receive_transaction_data() error\n");
			return 8;
		}

		memcpy(&transaction_data, &client_txn.transaction_data,
			sizeof(union transaction_data_t));
	}

	dump(stdout, transaction, (void *) &transaction_data);
	printf("\ndone.\n");

	return 0;
}
