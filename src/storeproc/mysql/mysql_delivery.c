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
#include "mysql_common.h"
#include "dbc.h"

int mysql_sp_delivery(struct db_context_t *_dbc, struct delivery_t *data)
{
	char stmt[128];

	/* Create the query and execute it. */
	sprintf(stmt, "CALL delivery(%d, %d)",
			data->w_id, data->o_carrier_id);

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("execute_delivery stmt: %s\n", stmt);
#endif

	return dbc_sql_execute(_dbc, stmt, NULL, NULL);
}
