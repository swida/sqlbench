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

typedef struct db_context_t {
	int need_reconnect;
	void *transaction_data[N_TRANSACTIONS];
} db_context_t;

struct sql_result_t
{
	void *result_set;
	int num_rows;				/* -1 is unknown, just order-status use it. */
	union
	{
		int current_row_num;
		void *current_row;
	};
};

typedef int (*trx_initializer_t) (db_context_t *dbc);
typedef int (*trx_executor_t) (
	db_context_t *dbc, union transaction_data_t *data);

struct sqlapi_operation_t
{
	/* Initialization functions for each transaction*/
	trx_initializer_t trx_initialize[N_TRANSACTIONS];
	trx_executor_t trx_execute[N_TRANSACTIONS];
};

extern int connect_to_db(db_context_t *dbc);
extern int need_reconnect_to_db(db_context_t *dbc);

db_context_t *db_init(void);

int disconnect_from_db(db_context_t *dbc);
int initialize_transactions(db_context_t *dbc);
int process_transaction(
	int transaction, db_context_t *dbc, union transaction_data_t *odbct);
void set_sqlapi_operation(enum sqlapi_type sa_type);

#endif /* _DB_H_ */
