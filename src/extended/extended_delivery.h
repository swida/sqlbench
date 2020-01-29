/*
 * odbc_delivery.h
 *
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Lab, Inc.
 *
 * 22 july 2002
 * Based on TPC-C Standard Specification Revision 5.0.
 */

#ifndef _EXTENDED_DELIVERY_H_
#define _EXTENDED_DELIVERY_H_

#include <transaction_data.h>
#include <extended_common.h>

int extended_initialize_delivery(db_context_t *dbc);
int extended_execute_delivery(db_context_t *dbc, union transaction_data_t *data);
void extended_destroy_delivery(db_context_t *dbc);

#endif /* _EXTENDED_DELIVERY_H_ */
