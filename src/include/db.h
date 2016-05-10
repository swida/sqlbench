/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 *
 * 16 June 2002
 */

#ifndef _DB_H_
#define _DB_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include "transaction_data.h"
enum sqlapi_type
{
	SQLAPI_SIMPLE,
	SQLAPI_EXTENDED,
	SQLAPI_STOREPROC
};

struct db_context_t {
	int need_reconnect;
};

struct sql_result_t
{
	void *result_set;
	int num_rows;				/* -1 is unknown, just order-status use it. */
	int current_row;
};

struct sqlapi_operation_t
{
	int (*execute_integrity) (struct db_context_t *dbc, struct integrity_t *data);
	int (*execute_delivery) (struct db_context_t *dbc, struct delivery_t *data);
	int (*execute_new_order) (struct db_context_t *dbc, struct new_order_t *data);
	int (*execute_order_status) (struct db_context_t *dbc, struct order_status_t *data);
	int (*execute_payment) (struct db_context_t *dbc, struct payment_t *data);
	int (*execute_stock_level) (struct db_context_t *dbc, struct stock_level_t *data);
};

extern int connect_to_db(struct db_context_t *dbc);
extern int need_reconnect_to_db(struct db_context_t *dbc);

struct db_context_t *db_init(void);

int disconnect_from_db(struct db_context_t *dbc);
int process_transaction(int transaction, struct db_context_t *dbc,
	union transaction_data_t *odbct);
void set_sqlapi_operation(enum sqlapi_type sa_type);

#endif /* _DB_H_ */
