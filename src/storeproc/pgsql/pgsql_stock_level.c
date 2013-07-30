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
#include "dbc.h"

int pgsql_sp_stock_level(struct db_context_t *_dbc, struct stock_level_t *data)
{
	char stmt[128];

	/* Create the query and execute it. */
	sprintf(stmt, "SELECT stock_level(%d, %d, %d)",
		data->w_id, data->d_id, data->threshold);
	return dbc_sql_execute(_dbc, stmt, NULL, NULL);
	return OK;
}
