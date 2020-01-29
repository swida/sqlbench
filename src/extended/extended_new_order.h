/*
 * odbc_new_order.h
 *
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Lab, Inc.
 *
 * 11 june 2002
 * Based on TPC-C Standard Specification Revision 5.0.
 */

#ifndef _SIMPLE_NEW_ORDER_H_
#define _SIMPLE_NEW_ORDER_H_

#include <transaction_data.h>
#include "extended_common.h"

int extended_initialize_new_order(db_context_t *dbc);
int extended_execute_new_order(db_context_t *dbc, union transaction_data_t *data);
void extended_destroy_new_order(db_context_t *dbc);

#endif /* _SIMPLE_NEW_ORDER_H_ */
