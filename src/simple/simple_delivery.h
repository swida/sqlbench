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

#ifndef _SIMPLE_DELIVERY_H_
#define _SIMPLE_DELIVERY_H_

#include <transaction_data.h>
#include <simple_common.h>

int execute_delivery(struct db_context_t *dbc, union transaction_data_t *data);

#endif /* _SIMPLE_DELIVERY_H_ */
