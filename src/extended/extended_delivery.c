/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 * Copyright (C) 2004 Alexey Stroganov & MySQL AB.
 *
 */

#include "extended_delivery.h"

static __thread int delivery_initialized = 0;
static 	char *params[4];

static int
delivery(struct db_context_t *dbc, struct delivery_t *data, char ** vals, int nvals);

int extended_execute_delivery(struct db_context_t *dbc, struct delivery_t *data)
{
	int rc;
	int nvals=3;
	char * vals[3];

	if(delivery_initialized == 0)
	{
		dbc_sql_prepare(dbc, DELIVERY_1, N_DELIVERY_1);
		dbc_sql_prepare(dbc, DELIVERY_2, N_DELIVERY_2);
		dbc_sql_prepare(dbc, DELIVERY_3, N_DELIVERY_3);
		dbc_sql_prepare(dbc, DELIVERY_4, N_DELIVERY_4);
		dbc_sql_prepare(dbc, DELIVERY_5, N_DELIVERY_5);
		dbc_sql_prepare(dbc, DELIVERY_6, N_DELIVERY_6);
		dbc_sql_prepare(dbc, DELIVERY_7, N_DELIVERY_7);
		dbt2_init_params(params, 4, 24);
		delivery_initialized = 1;
	}
	dbt2_init_values(vals, nvals);

	rc=delivery(dbc, data, vals, nvals);

	if (rc == -1 )
	{
		LOG_ERROR_MESSAGE("DELIVERY FINISHED WITH ERRORS \n");

		//should free memory that was allocated for nvals vars
		dbt2_free_values(vals, nvals);

		return ERROR;
	}
	return OK;
}

static int
delivery(struct db_context_t *dbc, struct delivery_t *data, char ** vals, int nvals)
{
	/* Input variables. */
	int w_id = data->w_id;
	int o_carrier_id = data->o_carrier_id;

	struct sql_result_t result;
	int d_id;

	int  NO_O_ID=0;
	int  O_C_ID=1;
	int  OL_AMOUNT=2;
	int num_params;

	for (d_id = 1; d_id <= 10; d_id++)
	{
		sprintf(params[0], "%d", w_id);
		sprintf(params[1], "%d", d_id);
		num_params = 2;

#ifdef DEBUG_QUERY
		LOG_ERROR_MESSAGE("%s: %s, $1 = %d, $2 = %d\n",
						  N_DELIVERY_1, DELIVERY_1, w_id, d_id);
#endif
		if (dbc_sql_execute_prepared(dbc, params, num_params, &result, N_DELIVERY_1) && result.result_set)
		{
            dbc_sql_fetchrow(dbc, &result);
            vals[NO_O_ID]= (char *)dbc_sql_getvalue(dbc, &result, 0);  //NO_O_ID
            dbc_sql_close_cursor(dbc, &result);

            if (!vals[NO_O_ID])
            {
				LOG_ERROR_MESSAGE(
					"ERROR: NO_O_ID=NULL for query %s: $1 = %d, $2 = %d", N_DELIVERY_1, w_id, d_id);
            }
		}
		else
		{
            /* Nothing to delivery for this district, try next. */
            continue;
		}

		if (vals[NO_O_ID] && atoi(vals[NO_O_ID])>0)
		{
			sprintf(params[0], "%s", vals[NO_O_ID]);
			sprintf(params[1], "%d", w_id);
			sprintf(params[2], "%d", d_id);
			num_params = 3;

            if (!dbc_sql_execute_prepared(dbc, params, num_params, NULL, N_DELIVERY_2))
            {
				return -1;
            }

			/* params is same with prevous query */
            if (dbc_sql_execute_prepared(dbc, params, num_params, &result, N_DELIVERY_3) && result.result_set)
            {
				dbc_sql_fetchrow(dbc, &result);
				vals[O_C_ID]= (char *)dbc_sql_getvalue(dbc, &result, 0);  //O_C_ID
				dbc_sql_close_cursor(dbc, &result);

				if (!vals[O_C_ID])
				{
					LOG_ERROR_MESSAGE("%s:query %s, $1 = %s, $2 = %d, $3 = %d\n O_C_ID= NULL",
									  N_DELIVERY_3, DELIVERY_3, vals[NO_O_ID], w_id, d_id);
					return -1;
				}
            }
            else //error
            {
				return -1;
            }
			sprintf(params[0], "%d", o_carrier_id);
			sprintf(params[1], "%s",vals[NO_O_ID]);
			sprintf(params[2], "%d", w_id);
			sprintf(params[3], "%d", d_id);
			num_params = 4;

            if (!dbc_sql_execute_prepared(dbc, params, num_params, NULL, N_DELIVERY_4))
            {
				return -1;
            }
			sprintf(params[0], "%s", vals[NO_O_ID]);
			sprintf(params[1], "%d", w_id);
			sprintf(params[2], "%d", d_id);
			num_params = 3;

            if (!dbc_sql_execute_prepared(dbc, params, num_params, NULL, N_DELIVERY_5))
            {
				return -1;
            }

			/* params same with prevous one */
            if (dbc_sql_execute_prepared(dbc, params, num_params, &result, N_DELIVERY_6) && result.result_set)
            {
				dbc_sql_fetchrow(dbc, &result);
				vals[OL_AMOUNT]= (char *)dbc_sql_getvalue(dbc, &result, 0);  //OL_AMOUNT
				dbc_sql_close_cursor(dbc, &result);

				if (!vals[OL_AMOUNT])
				{
					return -1;
				}
            }
            else //error
            {
				return -1;
            }
            sprintf(params[0], "%s", vals[OL_AMOUNT]);
            sprintf(params[1], "%s", vals[O_C_ID]);
			sprintf(params[2], "%d", w_id);
			sprintf(params[3], "%d", d_id);
			num_params = 4;

            if (!dbc_sql_execute_prepared(dbc, params, num_params, NULL, N_DELIVERY_7))
            {
				LOG_ERROR_MESSAGE("%s: OL_AMOUNT: |%s| O_C_ID: |%s|", N_DELIVERY_7, vals[OL_AMOUNT],
								  vals[O_C_ID]);
				return -1;
            }
		}
		dbt2_free_values(vals, nvals);
	}

	return 1;
}
