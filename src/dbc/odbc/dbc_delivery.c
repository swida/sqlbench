/*
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 *
 * 22 June 2002
 * Based on TPC-C Standard Specification Revision 5.0.
 */

#include <odbc_delivery.h>

int execute_delivery(struct db_context_t *odbcc, struct delivery_t *data)
{
	SQLRETURN rc;
	int i = 1;

	/* Perpare statement for the Delivery transaction. */
	rc = SQLPrepare(odbcc->hstmt, STMT_DELIVERY, SQL_NTS);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, odbcc->hstmt);
		return ERROR;
	}

	/* Bind variables for the Delivery transaction. */
	rc = SQLBindParameter(odbcc->hstmt,
		i++, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0,
		&data->w_id, 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, odbcc->hstmt);
		return ERROR;
	}
	rc = SQLBindParameter(odbcc->hstmt,
		i++, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0,
		&data->o_carrier_id, 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, odbcc->hstmt);
		return ERROR;
	}

	/* Execute stored procedure. */
	rc = SQLExecute(odbcc->hstmt);
	if (check_odbc_rc(SQL_HANDLE_STMT, odbcc->hstmt, rc) == ERROR) {   
		LOG_ODBC_ERROR(SQL_HANDLE_STMT, odbcc->hstmt);
		return ERROR;
	}

	return OK;
}
