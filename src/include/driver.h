/*
 * driver.h
 *
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Lab, Inc.
 *
 * 7 august 2002
 */

#ifndef _DRIVER_H_
#define _DRIVER_H_

#define MIX_DELIVERY 0.04
#define MIX_ORDER_STATUS 0.04
#define MIX_PAYMENT 0.43
#define MIX_STOCK_LEVEL 0.04

#define KEY_TIME_DELIVERY 2
#define KEY_TIME_NEW_ORDER 18
#define KEY_TIME_ORDER_STATUS 2
#define KEY_TIME_PAYMENT 3
#define KEY_TIME_STOCK_LEVEL 2

#define THINK_TIME_DELIVERY 5000
#define THINK_TIME_NEW_ORDER 12000
#define THINK_TIME_ORDER_STATUS 10000
#define THINK_TIME_PAYMENT 12000
#define THINK_TIME_STOCK_LEVEL 5000

#define EXECUTING 0
#define KEYING 1
#define THINKING 2

struct key_time_t
{
	int delivery;
	int new_order;
	int order_status;
	int payment;
	int stock_level;
};

struct terminal_context_t
{
	int w_id;
	int d_id;
};

struct transaction_mix_t
{
	/*
	 * These are the numbers are the actual percentage a transaction is
	 * executed.
	 */
	double delivery_actual;
	double new_order_actual;
	double payment_actual;
	double order_status_actual;
	double stock_level_actual;

	/*
	 * These are the numbers checked against to determine the next
	 * transaction.
	 */
	double delivery_threshold;
	double new_order_threshold;
	double payment_threshold;
	double order_status_threshold;
	double stock_level_threshold;
};

struct think_time_t
{
	int delivery;
	int new_order;
	int order_status;
	int payment;
	int stock_level;
};

int init_driver();
int init_driver_logging();
int integrity_terminal_worker();
int recalculate_mix();
int set_client_hostname(char *addr);
int set_client_port(int port);
int set_duration(int seconds);
int set_table_cardinality(int table, int cardinality);
int set_transaction_mix(int transaction, double mix);
int start_driver();

extern struct transaction_mix_t transaction_mix;
extern struct key_time_t key_time;
extern struct think_time_t think_time;
extern char hostname[32];
extern int port;
extern int duration;
extern int w_id_min, w_id_max;
extern int terminals_per_warehouse;
extern int mode_altered;
extern unsigned int seed;
extern int client_conn_sleep;
extern int spread;

#endif /* _DRIVER_H_ */
