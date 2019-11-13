/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 * Copyright (C) 2004 Alexey Stroganov & MySQL AB.
 *
 */


#include <extended_new_order.h>

static __thread int new_order_initialized = 0;
static __thread char *params[9];

static const char s_dist[10][11] = {
	"s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05",
	"s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10"
};
static int
new_order(struct db_context_t *dbc, struct new_order_t *data, char ** vals, int nvals);

int extended_execute_new_order(struct db_context_t *dbc, struct new_order_t *data)
{
	int rc;

	char * vals[6];
	int nvals=6;
	if(new_order_initialized == 0)
	{
		char query[128];
		char query_name[32];
		int i;
		dbc_sql_prepare(dbc, NEW_ORDER_1, N_NEW_ORDER_1);
		if(_is_forupdate_supported)
			dbc_sql_prepare(dbc, NEW_ORDER_2, N_NEW_ORDER_2);
		else
			dbc_sql_prepare(
				dbc, NEW_ORDER_2_WITHOUT_FORUPDATE, N_NEW_ORDER_2_WITHOUT_FORUPDATE);
		dbc_sql_prepare(dbc, NEW_ORDER_3, N_NEW_ORDER_3);
		dbc_sql_prepare(dbc, NEW_ORDER_4, N_NEW_ORDER_4);
		dbc_sql_prepare(dbc, NEW_ORDER_5, N_NEW_ORDER_5);
		dbc_sql_prepare(dbc, NEW_ORDER_6, N_NEW_ORDER_6);
		dbc_sql_prepare(dbc, NEW_ORDER_7, N_NEW_ORDER_7);
		for(i = 0; i < sizeof(s_dist) / sizeof(s_dist[0]); i++)
		{
			sprintf(query_name, "%s_%s", N_NEW_ORDER_8, s_dist[i]);
			sprintf(query, NEW_ORDER_8, s_dist[i]);
			dbc_sql_prepare(dbc, query, query_name);
		}
		dbc_sql_prepare(dbc, NEW_ORDER_9, N_NEW_ORDER_9);
		dbc_sql_prepare(dbc, NEW_ORDER_10, N_NEW_ORDER_10);
		dbt2_init_params(params, 8, 24);
		new_order_initialized = 1;
	}
	rc= new_order(dbc, data, vals, nvals);

	if (rc)
	{
		LOG_ERROR_MESSAGE("NEW_ORDER FINISHED WITH RC %d\n", rc);

		//should free memory that was allocated for nvals vars
		dbt2_free_values(vals, nvals);

		return ERROR;
	}

	return OK;
}

static int
new_order(struct db_context_t *dbc, struct new_order_t *data, char ** vals, int nvals)
{
	/* Input variables. */
	int w_id = data->w_id;
	int d_id = data->d_id;
	int c_id = data->c_id;
	int o_all_local = data->o_all_local;
	int o_ol_cnt = data->o_ol_cnt;

	int ol_i_id[15];
	int ol_supply_w_id[15];
	int ol_quantity[15];

	int i;
	int rc;

	char query_name[32];

	float ol_amount[15];
	float order_amount = 0.0;

	struct sql_result_t result;

	char * i_price[15];
	char * i_name[15];
	char * i_data[15];
	char * s_quantity[15];
	char * my_s_dist[15];
	char * s_data[15];
	int num_params;

	int W_TAX=0;
	int D_TAX=1;
	int D_NEXT_O_ID=2;
	int C_DISCOUNT=3;
	int C_LAST=4;
	int C_CREDIT=5;

	dbt2_init_values(vals, nvals);
	dbt2_init_values(i_price, 15);
	dbt2_init_values(i_name, 15);
	dbt2_init_values(i_data, 15);
	dbt2_init_values(s_quantity, 15);
	dbt2_init_values(my_s_dist, 15);
	dbt2_init_values(s_data, 15);

	/* Loop through the last set of parameters. */
	for (i = 0; i < 15; i++) {
		ol_i_id[i] = data->order_line[i].ol_i_id;
		ol_supply_w_id[i] = data->order_line[i].ol_supply_w_id;
		ol_quantity[i] = data->order_line[i].ol_quantity;
	}

	sprintf(params[0], "%d", w_id);
	num_params = 1;

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("%s: %s, $1 = %s\n",N_NEW_ORDER_1, NEW_ORDER_1, params[0]);
#endif
	if (dbc_sql_execute_prepared(dbc, params, num_params, &result, N_NEW_ORDER_1) && result.result_set)
	{
		dbc_sql_fetchrow(dbc, &result);

		vals[W_TAX]= dbc_sql_getvalue(dbc, &result, 0);  //W_TAX
		dbc_sql_close_cursor(dbc, &result);
	}
	else //error
	{
		return 10;
	}
	if(_is_forupdate_supported)
		sprintf(query_name, "%s", N_NEW_ORDER_2);
	else
		sprintf(query_name, "%s", N_NEW_ORDER_2_WITHOUT_FORUPDATE);
	sprintf(params[0], "%d", w_id);
	sprintf(params[1], "%d", d_id);
	num_params = 2;

#ifdef DEBUG_QUERY
	if(_is_forupdate_supported)
		LOG_ERROR_MESSAGE("%s query: %s, $1 = %d, $2 = %d\n", query_name, N_NEW_ORDER_2, w_id, d_id);
	else
		LOG_ERROR_MESSAGE("%s query: %s, $1 = %d, $2 = %d\n", query_name, N_NEW_ORDER_2_WITHOUT_FORUPDATE, w_id, d_id);
#endif

	if (dbc_sql_execute_prepared(dbc, params, num_params, &result, query_name) && result.result_set)
	{
		dbc_sql_fetchrow(dbc, &result);

		vals[D_TAX]= dbc_sql_getvalue(dbc, &result, 0);       //D_TAX
		vals[D_NEXT_O_ID]= dbc_sql_getvalue(dbc, &result, 1); //D_NEXT_O_ID
		dbc_sql_close_cursor(dbc, &result);

		if (!vals[D_NEXT_O_ID])
		{
			LOG_ERROR_MESSAGE("NULL or empty rows for D_NEXT_O_ID returned: %s, $1=%d, $2=%d \n",
							  _is_forupdate_supported ? N_NEW_ORDER_2 : N_NEW_ORDER_2_WITHOUT_FORUPDATE,
							  w_id, d_id);
			return 11;
		}
	}
	else //error
	{
		return 11;
	}

	/* parameters is same with prevous one */
#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("%s query: %s, $1 = %d, $2 = %d\n",
					  N_NEW_ORDER_3, NEW_ORDER_3, w_id, d_id);
#endif

	if (!dbc_sql_execute_prepared(dbc, params, num_params, NULL, N_NEW_ORDER_3))
	{
		return 12;
	}
	sprintf(params[2], "%d", c_id);
	num_params = 3;

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE(
		"%s: %s, $1 = %d, $2 = %d, $3 = %d\n",
		N_NEW_ORDER_4, NEW_ORDER_4, w_id, d_id, c_id);
#endif
	if (dbc_sql_execute_prepared(dbc, params, num_params, &result, N_NEW_ORDER_4) && result.result_set)
	{
		dbc_sql_fetchrow(dbc, &result);

		vals[C_DISCOUNT]= dbc_sql_getvalue(dbc, &result, 0);  //C_DISCOUNT
		vals[C_LAST]= dbc_sql_getvalue(dbc, &result, 1);      //C_LAST
		vals[C_CREDIT]= dbc_sql_getvalue(dbc, &result, 2);    //C_CREDIT

		dbc_sql_close_cursor(dbc, &result);
	}
	else //error
	{
		return 13;
	}

	/* Should insert in orders first, because there is a foreign key in new_order */
	sprintf(params[0], "%s", vals[D_NEXT_O_ID]);
	sprintf(params[1], "%d", d_id);
	sprintf(params[2], "%d", w_id);
	sprintf(params[3], "%d", c_id);
	sprintf(params[4], "%d", o_ol_cnt);
	sprintf(params[5], "%d", o_all_local);
	num_params = 6;

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE(
		"%s query: %s, $1 = %s, $2 = %d, $3 = %d, $4 = %d, $5 = %d, $6 = %d\n",
		N_NEW_ORDER_6, NEW_ORDER_6, vals[D_NEXT_O_ID] , d_id, w_id, c_id, o_ol_cnt, o_all_local);
#endif
	if (!dbc_sql_execute_prepared(dbc, params, num_params, NULL, N_NEW_ORDER_6))
	{
		return 15;
	}


	/* the first parameter is same with prevous one */
	sprintf(params[1], "%d", w_id);
	sprintf(params[2], "%d", d_id);
	num_params = 3;

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("%s query: %s, $1 = %s, $2 = %d, $3 = %d\n",
					  N_NEW_ORDER_5, NEW_ORDER_5, vals[D_NEXT_O_ID],
					  w_id, d_id);
#endif
	if (!dbc_sql_execute_prepared(dbc, params, num_params, NULL, N_NEW_ORDER_5))
	{
		return 14;
	}

	rc=0;

	for (i = 0; i < o_ol_cnt; i++)
	{
		sprintf(params[0], "%d", ol_i_id[i]);
		num_params = 1;
#ifdef DEBUG_QUERY
		LOG_ERROR_MESSAGE("%s query: %s, $1 = %d\n", ol_i_id[i]);
#endif
		if (ol_i_id[i] != 0)
		{
            if (dbc_sql_execute_prepared(dbc, params, num_params, &result, N_NEW_ORDER_7) && result.result_set)
            {
				dbc_sql_fetchrow(dbc, &result);

				i_price[i]= dbc_sql_getvalue(dbc, &result, 0);
				i_name[i]= dbc_sql_getvalue(dbc, &result, 1);
				i_data[i]= dbc_sql_getvalue(dbc, &result, 2);

				dbc_sql_close_cursor(dbc, &result);
				if (!i_price[i])
				{
					LOG_ERROR_MESSAGE("NULL or empty result return, NEW_ORDER_7: %s, $1 = %d",
									  i, NEW_ORDER_7, ol_i_id[i]);
					rc = -1;
					break;
				}
            }
            else
            {
				rc=-1;
				break;
            }
		}
		else //error
		{
            /* Item doesn't exist, rollback transaction. */
#ifdef DEBUG_QUERY
            LOG_ERROR_MESSAGE("ROLLBACK BECAUSE OL_I_ID[%d]= 0 for query:\nNEW_ORDER_7: %s", i, NEW_ORDER_7);
#endif
            rc=2;
            break;
		}

		ol_amount[i] = atof(i_price[i] ) * (float) ol_quantity[i];
		sprintf(query_name, "%s_%s", N_NEW_ORDER_8, s_dist[d_id - 1]);
		sprintf(params[0], "%d", ol_i_id[i]);
		sprintf(params[1], "%d", w_id);
		num_params = 2;

#ifdef DEBUG_QUERY
		LOG_ERROR_MESSAGE(
			"%s query: %s, %%s = %s, $2 = %d, $3 = %d\n",
			query_name, NEW_ORDER_8, s_dist[d_id - 1], ol_i_id[i], w_id);
#endif
		if (dbc_sql_execute_prepared(dbc, params, num_params, &result, query_name) && result.result_set)
		{
            dbc_sql_fetchrow(dbc, &result);

            s_quantity[i]= dbc_sql_getvalue(dbc, &result, 0);
            my_s_dist[i]= dbc_sql_getvalue(dbc, &result, 1);
            s_data[i]= dbc_sql_getvalue(dbc, &result, 2);

            dbc_sql_close_cursor(dbc, &result);
			if (!s_quantity[i] || !my_s_dist[i])
			{
				LOG_ERROR_MESSAGE("NULL or empty result returned: %s query %s, %%s = %s, $1 = %s, %2 = %s",
								  query_name, NEW_ORDER_8, s_dist[d_id -1], params[0], params[1]);
				rc = 16;
				break;
			}
		}
		else //error
		{
            LOG_ERROR_MESSAGE("%s query: %s, %%s = %s, $1 = %s, %2 = %s",
							  query_name, NEW_ORDER_8, s_dist[d_id -1], params[0], params[1]);
            rc=16;
            break;
		}

		order_amount += ol_amount[i];

		if (atoi(s_quantity[i] ) > ol_quantity[i] + 10)
		{
			sprintf(params[0], "%d", ol_quantity[i]);
		}
		else
		{
			sprintf(params[0], "%d", ol_quantity[i] - 91);
		}
		sprintf(params[1], "%d", ol_i_id[i]);
		sprintf(params[2], "%d", w_id);
		num_params = 3;
#ifdef DEBUG_QUERY
		LOG_ERROR_MESSAGE(
			"%s query: %s, $1 = %s, $2 = %s, $3 = %s\n",
			N_NEW_ORDER_9, NEW_ORDER_9, params[0], params[1], params[2]);
#endif
		if (!dbc_sql_execute_prepared(dbc, params, num_params, NULL, N_NEW_ORDER_9))
		{
            LOG_ERROR_MESSAGE(
				"%s query: %s, $1 = %s, $2 = %s, $3 = %s\n",
				N_NEW_ORDER_9, NEW_ORDER_9, params[0], params[1], params[2]);
            rc=17;
            break;
		}
		sprintf(params[0], "%s", vals[D_NEXT_O_ID]);
		sprintf(params[1], "%d", d_id);
		sprintf(params[2], "%d", w_id);
		sprintf(params[3], "%d", i + 1);
		sprintf(params[4], "%d", ol_i_id[i]);
		sprintf(params[5], "%d", ol_supply_w_id[i]);
		sprintf(params[6], "%d", ol_quantity[i]);
		sprintf(params[7], "%f", ol_amount[i]);
		params[8] = my_s_dist[i];
		num_params = 9;
#ifdef DEBUG_QUERY
		LOG_ERROR_MESSAGE(
		"%s query: %s, $1 = %s, $2 = %d, $3 = %d, $4 = %d, $5 = %d, "
		"$6 = %d, $7 = %d, $8 = %f, $9 = %s\n",
		N_NEW_ORDER_10, NEW_ORDER_10,
		vals[D_NEXT_O_ID] , d_id, w_id, i + 1, ol_i_id[i],
		ol_supply_w_id[i], ol_quantity[i], ol_amount[i], my_s_dist[i] );
#endif
		if (!dbc_sql_execute_prepared(dbc, params, num_params, NULL, N_NEW_ORDER_10))
		{
			LOG_ERROR_MESSAGE(
		"%s query: %s, $1 = %s, $2 = %d, $3 = %d, $4 = %d, $5 = %d, "
		"$6 = %d, $7 = %d, $8 = %f, $9 = %s\n",
		N_NEW_ORDER_10, NEW_ORDER_10,
		vals[D_NEXT_O_ID] , d_id, w_id, i + 1, ol_i_id[i],
		ol_supply_w_id[i], ol_quantity[i], ol_amount[i], my_s_dist[i] );
            rc=18;
            break;
		}
	}

	dbt2_free_values(i_price, 15);
	dbt2_free_values(i_name, 15);
	dbt2_free_values(i_data, 15);
	dbt2_free_values(s_quantity, 15);
	dbt2_free_values(my_s_dist, 15);
	dbt2_free_values(s_data, 15);

	dbt2_free_values(vals, nvals);

	return rc;
}
