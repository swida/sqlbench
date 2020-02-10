/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 *
 * 16 June 2002
 */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include "db.h"
#include "logging.h"
#include "dbc.h"

static struct sqlapi_operation_t *sbo;
extern struct sqlapi_operation_t simple_sqlapi_operation;
extern struct sqlapi_operation_t extended_sqlapi_operation;
extern struct sqlapi_operation_t storeproc_sqlapi_operation;
void set_sqlapi_operation(enum sqlapi_type sa_type)
{
	switch(sa_type) {
		case SQLAPI_SIMPLE:
			sbo = &simple_sqlapi_operation;
			break;
		case SQLAPI_EXTENDED:
			sbo = &extended_sqlapi_operation;
			break;
		case SQLAPI_STOREPROC:
			sbo = &storeproc_sqlapi_operation;
			break;
		default:
			break;
	}
}

int connect_to_db(struct db_context_t *dbc) {
	int rc;

	rc = dbc_connect_to_db(dbc);
	if (rc != OK) {
		return ERROR;
	}

	return OK;
}

struct db_context_t *db_init(void)
{
	return dbc_db_init();
}

int disconnect_from_db(struct db_context_t *dbc) {
	int rc;

	rc = dbc_disconnect_from_db(dbc);
	if (rc != OK) {
		return ERROR;
	}

	return OK;
}

int need_reconnect_to_db(struct db_context_t *dbc)
{
	return dbc->need_reconnect;
}

int
initialize_transactions(db_context_t *dbc)
{
	int rc = OK;
	int i;
	for(i = 0; i < N_TRANSACTIONS; i++) {
		trx_initializer_t trx_init = sbo->trx_initialize[i];
		if (trx_init && trx_init(dbc) != OK)
			rc = ERROR;
	}

	return rc;
}

int process_transaction(int transaction, db_context_t *dbc,
	union transaction_data_t *td)
{
	int rc;
	int i;
	int status;

	assert(transaction >= DELIVERY && transaction < N_TRANSACTIONS);
	trx_executor_t execute_transaction = sbo->trx_execute[transaction];

	if (transaction == NEW_ORDER) {
		td->new_order.o_all_local = 1;
		for (i = 0; i < td->new_order.o_ol_cnt; i++) {
			if (td->new_order.order_line[i].ol_supply_w_id !=
					td->new_order.w_id) {
				td->new_order.o_all_local = 0;
				break;
			}
		}

		rc = execute_transaction(dbc, td);

		if (rc != ERROR && td->new_order.rollback == 0) {
			/*
			 * Calculate the adjusted total_amount here to work
			 * around an issue with SAP DB stored procedures that
			 * does not allow any statements to execute after a
			 * SUBTRANS ROLLBACK without throwing an error.
	 		 */
			td->new_order.total_amount =
				td->new_order.total_amount *
				(1 - td->new_order.c_discount) *
				(1 + td->new_order.w_tax + td->new_order.d_tax);
		} else {
			rc = ERROR;
		}

	} else
		rc = execute_transaction(dbc, td);

	/* Commit or rollback the transaction. */
	if (rc == OK) {
		status = dbc_commit_transaction(dbc);
	} else {
		status = dbc_rollback_transaction(dbc);
	}

	return status;
}

void
destroy_transactions(db_context_t *dbc)
{
	int i;
	for(i = 0; i < N_TRANSACTIONS; i++) {
		trx_destroy_t trx_destroy = sbo->trx_destroy[i];
		if (trx_destroy)
			trx_destroy(dbc);
	}
}
