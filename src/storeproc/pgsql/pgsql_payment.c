/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 *
 * 13 May 2003
 */

#include <stdio.h>

#include "common.h"
#include "logging.h"
#include "pgsql_common.h"

int pgsql_sp_payment(struct db_context_t *_dbc, struct payment_t *data)
{
	char stmt[128];

	/* Create the query and execute it. */
	sprintf(stmt, "SELECT payment(%d, %d, %d, %d, %d, '%s', %f)",
		data->w_id, data->d_id, data->c_id, data->c_w_id, data->c_d_id,
		data->c_last, data->h_amount);
	return dbc_sql_execute(_dbc, stmt, NULL, NULL);
}
