/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 * Copyright (C) 2004 Alexey Stroganov & MySQL AB.
 *
 */


#include "extended_stock_level.h"
static __thread int stock_level_initialized = 0;
static __thread char *params[5];
static int
stock_level(struct db_context_t *dbc, struct stock_level_t *data, char ** vals, int nvals);

int extended_execute_stock_level(struct db_context_t *dbc, struct stock_level_t *data)
{
	int rc;
	char *  vals[2];
	int nvals=2;
	if(stock_level_initialized == 0)
	{
		dbc_sql_prepare(dbc, STOCK_LEVEL_1, N_STOCK_LEVEL_1);
		dbc_sql_prepare(dbc, STOCK_LEVEL_2, N_STOCK_LEVEL_2);
		dbt2_init_params(params, 5, 24);
		stock_level_initialized = 1;
	}
	rc=stock_level(dbc, data, vals, nvals);

	if (rc == -1 )
	{
		LOG_ERROR_MESSAGE("STOCK LEVEL FINISHED WITH ERRORS \n");

		//should free memory that was allocated for nvals vars
		dbt2_free_values(vals, nvals);

		return ERROR;
	}

	return OK;
}


static int
stock_level(struct db_context_t *dbc, struct stock_level_t *data, char ** vals, int nvals)
{
	/* Input variables. */
	int w_id = data->w_id;
	int d_id = data->d_id;
	int threshold = data->threshold;

	struct sql_result_t result;

	int d_next_o_id = 0;
	int low_stock;

	int D_NEXT_O_ID=0;
	int LOW_STOCK=1;
	int num_params;

	dbt2_init_values(vals, nvals);

	sprintf(params[0], "%d", w_id);
	sprintf(params[1], "%d", d_id);
	num_params = 2;

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("%s query: %s, $1 = %d, $2 = %d\n",
					  N_STOCK_LEVEL_1, STOCK_LEVEL_1, w_id, d_id);
#endif
	if (dbc_sql_execute_prepared(dbc, params, num_params, &result, N_STOCK_LEVEL_1) && result.result_set)
	{
		dbc_sql_fetchrow(dbc, &result);
		vals[D_NEXT_O_ID] = dbc_sql_getvalue(dbc, &result, 0); //D_NEXT_O_ID
		dbc_sql_close_cursor(dbc, &result);

		if (!vals[D_NEXT_O_ID])
		{
            LOG_ERROR_MESSAGE("ERROR: D_NEXT_O_ID=NULL for query %s:\n%s\n", N_STOCK_LEVEL_1, STOCK_LEVEL_1);
            return -1;
		}
		d_next_o_id = atoi(vals[D_NEXT_O_ID]);
	}
	else //error
	{
		return -1;
	}
	sprintf(params[0], "%d", w_id);
	sprintf(params[1], "%d", d_id);
	sprintf(params[2], "%d", d_next_o_id - 1);
	sprintf(params[3], "%d", d_next_o_id - 20);
	sprintf(params[4], "%d", threshold);
	num_params = 5;

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("%s query: %s, $1 = %d, $2 = %d, $3 = %d, $4 = %d, $5 = %d\n",
					  N_STOCK_LEVEL_2, STOCK_LEVEL_2,
					  w_id, d_id, d_next_o_id - 1, d_next_o_id - 20, threshold);
#endif
	if (dbc_sql_execute_prepared(dbc, params, num_params, &result, N_STOCK_LEVEL_2) && result.result_set)
	{
		dbc_sql_fetchrow(dbc, &result);

		vals[LOW_STOCK]= dbc_sql_getvalue(dbc, &result, 0); //LOW_STOCK
		dbc_sql_close_cursor(dbc, &result);

		if (!vals[LOW_STOCK])
		{
            LOG_ERROR_MESSAGE("ERROR: LOW_STOCK=NULL for query %s:\n%s\n",
							  N_STOCK_LEVEL_2, STOCK_LEVEL_2);
            return -1;
		}
		low_stock = atoi(vals[LOW_STOCK]);
		(void)low_stock;
	}
	else //error
	{
		return -1;
	}

	dbt2_free_values(vals, nvals);

	return 1;
}

