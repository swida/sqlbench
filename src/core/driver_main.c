/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Jenny Zhang &
 *                    Open Source Development Lab, Inc.
 *
 * 5 august 2002
 */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/stat.h>
#include <common.h>
#include <logging.h>
#include <driver.h>
#include <unistd.h>
#include <db_threadpool.h>
#include "dbc.h"

int exiting = 0;

int perform_integrity_check = 0;
int no_thinktime = 0;
/* use store procedure to test */
enum sqlapi_type use_sqlapi_type = SQLAPI_SIMPLE;
int parse_arguments(int argc, char *argv[]);

int main(int argc, char *argv[])
{
	struct stat st;

	/* Initialize various components. */
	init_common();
	init_dbc_manager();
	init_driver();

	if (parse_arguments(argc, argv) != OK) {
		printf("usage: %s -t <dbms> -c # -w # -l # [-r #] [-s #] [-e #] [-o p] [-z]",
			argv[0]);
		printf("\n");
		printf("-t <dbms>\n");
		printf("\tavailable:%s\n", dbc_manager_get_dbcnames());
		printf("%s", dbc_manager_get_dbcusages());
		printf("-c #\n");
		printf("\tnumber of database connections\n");
		printf("-w #\n");
		printf("\twarehouse cardinality, default 1\n");
		printf("-l #\n");
		printf("\tthe duration of the run in seconds\n");
		printf("\n");
		printf("-r #\n");
		printf("\tthe duration of ramp up in seconds\n");
		printf("-s #\n");
		printf("\tlower warehouse id, default 1\n");
		printf("-e #\n");
		printf("\tupper warehouse id, default <w>\n");
		printf("-o p\n");
		printf("\toutput directory of log files, default current directory\n");
		printf("-z\n");
		printf("\tperform database integrity check\n");
		printf("\n");
		printf("--customer #\n");
		printf("\tcustomer cardinality, default %d\n", CUSTOMER_CARDINALITY);
		printf("--item #\n");
		printf("\titem cardinality, default %d\n", ITEM_CARDINALITY);
		printf("--order #\n");
		printf("\torder cardinality, default %d\n", ORDER_CARDINALITY);
		printf("--new-order #\n");
		printf("\tnew-order cardinality, default %d\n", NEW_ORDER_CARDINALITY);
		printf("\n");
		printf("--mixp %%\n");
		printf("\tmix percentage of Payment transaction, default %0.2f\n",
				MIX_PAYMENT);
		printf("--mixo %%\n");
		printf("\tmix percentage of Order-Status transaction, default %0.2f\n",
				MIX_ORDER_STATUS);
		printf("--mixd %%\n");
		printf("\tmix percentage of Delivery transaction, default %0.2f\n",
				MIX_DELIVERY);
		printf("--mixs %%\n");
		printf("\tmix percentage of Stock-Level transaction, default %0.2f\n",
				MIX_STOCK_LEVEL);
		printf("\n");
		printf("--ktd #\n");
		printf("\tdelivery keying time, default %d s\n", KEY_TIME_DELIVERY);
		printf("--ktn #\n");
		printf("\tnew-order keying time, default %d s\n", KEY_TIME_NEW_ORDER);
		printf("--kto #\n");
		printf("\torder-status keying time, default %d s\n",
				KEY_TIME_ORDER_STATUS);
		printf("--ktp #\n");
		printf("\tpayment keying time, default %d s\n", KEY_TIME_PAYMENT);
		printf("--kts #\n");
		printf("\tstock-level keying time, default %d s\n",
				KEY_TIME_STOCK_LEVEL);
		printf("--ttd #\n");
		printf("\tdelivery thinking time, default %d ms\n",
				THINK_TIME_DELIVERY);
		printf("--ttn #\n");
		printf("\tnew-order thinking time, default %d ms\n",
				THINK_TIME_NEW_ORDER);
		printf("--tto #\n");
		printf("\torder-status thinking time, default %d ms\n",
				THINK_TIME_ORDER_STATUS);
		printf("--ttp #\n");
		printf("\tpayment thinking time, default %d ms\n", THINK_TIME_PAYMENT);
		printf("--tts #\n");
		printf("\tstock-level thinking time, default %d ms\n",
				THINK_TIME_STOCK_LEVEL);
		printf("--no-thinktime\n");
		printf("\tno think time and keying time to every transaction\n");
		printf("\n");
		printf("--tpw #\n");
		printf("\tterminals started per warehouse, default 10\n");

		printf("\n");
		printf("--seed #\n");
		printf("\trandom number seed\n");
		printf("--sleep #\n");
		printf("\tnumber of milliseconds to sleep between terminal creation, openning db connections\n");
		printf("--sqlapi\n");
		printf("\trun test using sql interface, available:\n");
		printf("\tsimple      default, just send sql statement to database\n");
		printf("\textended    use extended (prepare/bind/execute) protocol, better than simple\n");
		printf("\tstoreproc   use store procedure\n");
		return 1;
	}

	if(init_logging() != OK || init_driver_logging() != OK) {
		printf("cannot init driver\n");
		return 1;
	};

	if(use_sqlapi_type == SQLAPI_STOREPROC)
	{
		if(!dbc_manager_is_storeproc_supported())
		{
			printf("%s does not support store procedure interface\n", dbc_manager_get_name());
			return 1;
		}
		printf("use store procedure interface\n");
	}else if(use_sqlapi_type == SQLAPI_EXTENDED)
	{
		if (!dbc_manager_is_extended_supported())
		{
			printf("%s does not support extended protocol\n", dbc_manager_get_name());
			return 1;
		}
		printf("use extended sql interface\n");
	}
	set_sqlapi_operation(use_sqlapi_type);

	create_pid_file();

	if (db_connections == 0) {
		printf("-c not used\n");
		return 1;
	}

	if(w_id_min == 0)
		w_id_min = 1;
	if(w_id_max == 0)
		w_id_max = table_cardinality.warehouses;

	/* Sanity check on the parameters. */
	if (w_id_min > w_id_max) {
		printf("lower warehouse id cannot be larger than upper warehouse id\n");
		return 1;
	}
	if (w_id_max > table_cardinality.warehouses) {
		printf("upper warehouse id cannot be larger than warehouse cardinality\n");
		return 1;
	}

	if (recalculate_mix() != OK) {
		printf("invalid transaction mix: --mixd %0.2f. --mixo %0.2f. --mixp %0.2f. --mixs %0.2f. causes new-order mix of %0.2f.\n",
				transaction_mix.delivery_actual,
				transaction_mix.order_status_actual,
				transaction_mix.payment_actual,
				transaction_mix.stock_level_actual,
				transaction_mix.new_order_actual);
		return 1;
	}

	if(no_thinktime)
	{
		key_time.delivery = 0;
		key_time.new_order = 0;
		key_time.order_status = 0;
		key_time.payment = 0;
		key_time.stock_level = 0;

		think_time.delivery = 0;
		think_time.new_order = 0;
		think_time.order_status = 0;
		think_time.payment = 0;
		think_time.stock_level = 0;
	}

	if (strlen(output_path) > 0 && ((stat(output_path, &st) < 0) ||
			(st.st_mode & S_IFMT) != S_IFDIR)) {
		printf("Output directory of data files '%s' not exists\n", output_path);
		return 3;
	}

	if (strlen(output_path) > 0) {
		printf("Output directory of log files: %s\n",output_path);
	} else {
		printf("Output directory of log files: current directory\n");
	}
	/* Double database conections. */
	printf("\n");
	printf("database connection:\n");
	printf("connections = %d", db_connections);
	printf("\n");

	/* TODO: print dbms type, database host, database port */

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

	/* Double check the transaction mix. */
	printf("transaction mix:\n");
	printf("new-order mix %0.2f\n", transaction_mix.new_order_actual);
	printf("payment mix %0.2f\n", transaction_mix.payment_actual);
	printf("order-status mix %0.2f\n", transaction_mix.order_status_actual);
	printf("delivery mix %0.2f\n", transaction_mix.delivery_actual);
	printf("stock-level mix %0.2f\n", transaction_mix.stock_level_actual);
	printf("\n");

	/* Double check the transaction threshold. */
	printf("transaction thresholds:\n");
	printf("new-order threshold %0.2f\n",
			transaction_mix.new_order_threshold);
	printf("payment threshold %0.2f\n", transaction_mix.payment_threshold);
	printf("order-status threshold %0.2f\n",
			transaction_mix.order_status_threshold);
	printf("delivery threshold %0.2f\n",
			transaction_mix.delivery_threshold);
	printf("stock-level threshold %0.2f\n",
			transaction_mix.stock_level_threshold);
	printf("\n");

	/* Double check the keying time. */
	printf("delivery keying time %ds\n", key_time.delivery);
	printf("new_order keying time %ds\n", key_time.new_order);
	printf("order-status keying time %ds\n", key_time.order_status);
	printf("payment keying time %ds\n", key_time.payment);
	printf("stock-level keying time %ds\n", key_time.stock_level);
	printf("\n");

	/* Double check the thinking time. */

	printf("delivery thinking time %d ms\n", think_time.delivery);
	printf("new_order thinking time %d ms\n", think_time.new_order);
	printf("order-status thinking time %d ms\n", think_time.order_status);
	printf("payment thinking time %d ms\n", think_time.payment);
	printf("stock-level thinking time %d ms\n", think_time.stock_level);
	printf("\n");

	printf("w_id range %d to %d\n", w_id_min, w_id_max);
	printf("%d terminals per warehouse\n", terminals_per_warehouse);
	printf("\n");
	if(duration_rampup > 0)
		printf("%d seconds ramp up state duration\n", duration_rampup);
	printf("%d seconds steady state duration\n", duration);
	printf("\n");

	if (perform_integrity_check && integrity_terminal_worker() != OK) {
	   printf("You used wrong parameters or something wrong with database.\n");
	   return 1;
	}

	start_db_threadpool();
	start_driver();

	return 0;
}

int parse_arguments(int argc, char *argv[])
{
	int c;
	int rc = OK;
	int i, j;
	char dbms_name[16] = "";

	struct option *dbms_long_options = NULL;
	struct option *all_long_options;

	static struct option main_long_options[] = {
		{"customer", required_argument, 0, 1},
		{"item", required_argument, 0, 2},
		{"order", required_argument, 0, 3},
		{"stock", required_argument, 0, 4},
		{"new-order", required_argument, 0, 5},
		{"mixd", required_argument, 0, 6},
		{"mixo", required_argument, 0, 7},
		{"mixp", required_argument, 0, 8},
		{"mixs", required_argument, 0, 9},
		{"ktd", required_argument, 0, 10},
		{"ktn", required_argument, 0, 11},
		{"kto", required_argument, 0, 12},
		{"ktp", required_argument, 0, 13},
		{"kts", required_argument, 0, 14},
		{"ttd", required_argument, 0, 15},
		{"ttn", required_argument, 0, 16},
		{"tto", required_argument, 0, 17},
		{"ttp", required_argument, 0, 18},
		{"tts", required_argument, 0, 19},
		{"no-thinktime", no_argument, 0, 20},
		{"tpw", required_argument, 0, 21},
		{"sleep", required_argument, 0, 22},
		{"seed",  required_argument, 0, 23},
		{"sqlapi", required_argument, 0, 24},
		{0, 0, 0, 0}
	};

	if (argc < 4) {
		return ERROR;
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

	/* construct final long options */
	for(i = 0; main_long_options[i].name != 0; i++);
	for(j = 0; dbms_long_options[j].name != 0; j++);

	all_long_options = malloc(sizeof(struct option) * (i + j + 1));
	memcpy(all_long_options, main_long_options, i * sizeof(struct option));
	memcpy(all_long_options + i, dbms_long_options, (j + 1) * sizeof(struct option));
	optind = 1;
	opterr = 1;
	while (1) {
		int option_index = 0;

		c = getopt_long(argc, argv, "c:e:l:o:s:r:t:w:z",
						all_long_options, &option_index);
		if (c == -1) {
			break;
		}

		switch (c) {
		case 0:					/* dbc options */
			if(dbc_manager_set_dbcoption(all_long_options[option_index].name, optarg) == ERROR)
			{
				rc = ERROR;
				goto _end;
			}
			break;
		case 'c':
			db_connections = atoi(optarg);
			break;
		case 'e':
			w_id_max = atoi(optarg);
			break;
		case 'l':
			set_duration(atoi(optarg));
			break;
		case 'o':
			strcpy(output_path, optarg);
			break;
		case 'r':
			duration_rampup = atoi(optarg);
			break;
		case 's':
			w_id_min = atoi(optarg);
			break;
		case 'w':
			set_table_cardinality(TABLE_WAREHOUSE, atoi(optarg));
			break;
		case 'z':
			perform_integrity_check = 1;
			break;
		/* long options */
		case 1:
			set_table_cardinality(TABLE_CUSTOMER, atoi(optarg));
			break;
		case 2:
			set_table_cardinality(TABLE_ITEM, atoi(optarg));
			break;
		case 3:
			set_table_cardinality(TABLE_ORDER, atoi(optarg));
			break;
		case 4:
			set_table_cardinality(TABLE_STOCK, atoi(optarg));
			break;
		case 5:
			set_table_cardinality(TABLE_NEW_ORDER, atoi(optarg));
			break;
		case 6:
			set_transaction_mix(PAYMENT, atof(optarg));
			break;
		case 7:
			set_transaction_mix(ORDER_STATUS, atof(optarg));
			break;
		case 8:
			set_transaction_mix(DELIVERY, atof(optarg));
			break;
		case 9:
			set_transaction_mix(STOCK_LEVEL, atof(optarg));
			break;
		case 10:
			key_time.delivery = atoi(optarg);
			break;
		case 11:
			key_time.new_order = atoi(optarg);
			break;
		case 12:
			key_time.order_status = atoi(optarg);
			break;
		case 13:
			key_time.payment = atoi(optarg);
			break;
		case 14:
			key_time.stock_level = atoi(optarg);
			break;
		case 15:
			think_time.delivery = atoi(optarg);
			break;
		case 16:
			think_time.new_order = atoi(optarg);
			break;
		case 17:
			think_time.order_status = atoi(optarg);
			break;
		case 18:
			think_time.payment = atoi(optarg);
			break;
		case 19:
			think_time.stock_level = atoi(optarg);
			break;
		case 20:
			no_thinktime = 1;
			break;
		case 21:
			terminals_per_warehouse = atoi(optarg);
			break;
		case 22:
			db_conn_sleep = client_conn_sleep = atoi(optarg);
			break;
		case 23:
		{
			int count;
			int length;

			seed = 0;
			length = strlen(optarg);

			for (count = 0; count < length; count++) {
				seed += (optarg[count] - '0') *
						(unsigned int) pow(10, length - (count + 1));
			}
		}
		break;
		case 24:
			if (strcasecmp(optarg, "simple") == 0)
				use_sqlapi_type = SQLAPI_SIMPLE;
			else if (strcasecmp(optarg, "extended") == 0)
				use_sqlapi_type = SQLAPI_EXTENDED;
			else if (strcasecmp(optarg, "storeproc") == 0)
				use_sqlapi_type = SQLAPI_STOREPROC;
			else
			{
				rc = ERROR;
				goto _end;
			}
			break;
		case '?':
			rc = ERROR;
			goto _end;
			break;
		default:
			break;
		}
	}

_end:
	free(all_long_options);
	free(dbms_long_options);
	return rc;
}
