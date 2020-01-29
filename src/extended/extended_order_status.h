/*
 * odbc_order_status.h
 *
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Lab, Inc.
 *
 * 16 july 2002
 * Based on TPC-C Standard Specification Revision 5.0.
 */

#ifndef _SIMPLE_ORDER_STATUS_H_
#define _SIMPLE_ORDER_STATUS_H_

#include <transaction_data.h>
#include <extended_common.h>

int extended_initialize_order_status(db_context_t *dbc);
int extended_execute_order_status(db_context_t *dbc, union transaction_data_t *data);

#endif /* _SIMPLE_ORDER_STATUS_H_ */
