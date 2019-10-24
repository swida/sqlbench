/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 *
 * 13 May 2003
 */

#include <stdio.h>
#include <string.h>

#include "common.h"
#include "logging.h"
#include "mysql_common.h"
#include "dbc.h"

int mysql_sp_new_order(struct db_context_t *_dbc, struct new_order_t *data)
{
	char stmt[512];
	int rc;
	struct sql_result_t result;

	/* Create the query and execute it. */
	sprintf(stmt,
			"call new_order(%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d,\
                                 %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d,\
                                 %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d,\
                          %d, %d, @rc)",
			data->w_id, data->d_id, data->c_id, data->o_all_local,
			data->o_ol_cnt,
			data->order_line[0].ol_i_id,
			data->order_line[0].ol_supply_w_id,
			data->order_line[0].ol_quantity,
			data->order_line[1].ol_i_id,
			data->order_line[1].ol_supply_w_id,
			data->order_line[1].ol_quantity,
			data->order_line[2].ol_i_id,
			data->order_line[2].ol_supply_w_id,
			data->order_line[2].ol_quantity,
			data->order_line[3].ol_i_id,
			data->order_line[3].ol_supply_w_id,
			data->order_line[3].ol_quantity,
			data->order_line[4].ol_i_id,
			data->order_line[4].ol_supply_w_id,
			data->order_line[4].ol_quantity,
			data->order_line[5].ol_i_id,
			data->order_line[5].ol_supply_w_id,
			data->order_line[5].ol_quantity,
			data->order_line[6].ol_i_id,
			data->order_line[6].ol_supply_w_id,
			data->order_line[6].ol_quantity,
			data->order_line[7].ol_i_id,
			data->order_line[7].ol_supply_w_id,
			data->order_line[7].ol_quantity,
			data->order_line[8].ol_i_id,
			data->order_line[8].ol_supply_w_id,
			data->order_line[8].ol_quantity,
			data->order_line[9].ol_i_id,
			data->order_line[9].ol_supply_w_id,
			data->order_line[9].ol_quantity,
			data->order_line[10].ol_i_id,
			data->order_line[10].ol_supply_w_id,
			data->order_line[10].ol_quantity,
			data->order_line[11].ol_i_id,
			data->order_line[11].ol_supply_w_id,
			data->order_line[11].ol_quantity,
			data->order_line[12].ol_i_id,
			data->order_line[12].ol_supply_w_id,
			data->order_line[12].ol_quantity,
			data->order_line[13].ol_i_id,
			data->order_line[13].ol_supply_w_id,
			data->order_line[13].ol_quantity,
			data->order_line[14].ol_i_id,
			data->order_line[14].ol_supply_w_id,
			data->order_line[14].ol_quantity);

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("execute_new_order stmt: %s\n", stmt);
#endif

	if (!dbc_sql_execute(_dbc, stmt, NULL, NULL))
		return ERROR;

	if (dbc_sql_execute(_dbc, "select @rc", &result, NULL) && dbc_sql_fetchrow(_dbc, &result))
	{
		data->rollback=atoi(dbc_sql_getvalue(_dbc, &result, 0));
		if  (data->rollback)
			LOG_ERROR_MESSAGE("NEW_ORDER ROLLBACK RC %d\n",data->rollback);
		dbc_sql_close_cursor(_dbc, &result);
		return OK;
	}

	return ERROR;
}
