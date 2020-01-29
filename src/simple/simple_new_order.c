/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 * Copyright (C) 2004 Alexey Stroganov & MySQL AB.
 *
 */


#include <simple_new_order.h>

const char s_dist[10][11] = {
	"s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05",
	"s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10"
};

static int new_order(struct db_context_t *dbc, struct new_order_t *data, char **vals, int nvals);

int
execute_new_order(struct db_context_t *dbc, union transaction_data_t *data)
{
	char * vals[6];
	int nvals=6;
	int rc = new_order(dbc, &data->new_order, vals, nvals);

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
new_order(struct db_context_t *dbc, struct new_order_t *data, char **vals, int nvals)
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

	char query[1024];

	float ol_amount[15];
	float order_amount = 0.0;

	struct sql_result_t result;

	char * i_price[15];
	char * i_name[15];
	char * i_data[15];
	char * s_quantity[15];
	char * my_s_dist[15];
	char * s_data[15];

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

	sprintf(query, NEW_ORDER_1, w_id);

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("NEW_ORDER_1: %s\n",query);
#endif
	if (dbc_sql_execute(dbc, query, &result, "NEW_ORDER_1") && result.result_set)
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
		sprintf(query, NEW_ORDER_2, w_id, d_id);
	else
		sprintf(query, NEW_ORDER_2__WITHOUT_FORUPDATE, w_id, d_id);

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("NEW_ORDER_2 query: %s\n", query);
#endif

	if (dbc_sql_execute(dbc, query, &result, "NEW_ORDER_2") && result.result_set)
	{
		dbc_sql_fetchrow(dbc, &result);

		vals[D_TAX]= dbc_sql_getvalue(dbc, &result, 0);       //D_TAX
		vals[D_NEXT_O_ID]= dbc_sql_getvalue(dbc, &result, 1); //D_NEXT_O_ID
		dbc_sql_close_cursor(dbc, &result);
	}
	else //error
	{
		return 11;
	}

	sprintf(query, NEW_ORDER_3, w_id, d_id);

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("NEW_ORDER_3 query: %s\n", query);
#endif

	if (!dbc_sql_execute(dbc, query, NULL, "NEW_ORDER_3"))
	{
		return 12;
	}

	sprintf(query, NEW_ORDER_4, w_id, d_id, c_id);

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("NEW_ORDER_4: %s\n", query);
#endif
	if (dbc_sql_execute(dbc, query, &result, "NEW_ORDER_4") && result.result_set)
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
	sprintf(query, NEW_ORDER_6, vals[D_NEXT_O_ID] , d_id, w_id, c_id, o_ol_cnt, o_all_local);

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("NEW_ORDER_6 query: %s\n", query);
#endif
	if (!dbc_sql_execute(dbc, query, NULL, "NEW_ORDER_6"))
	{
		return 15;
	}

	sprintf(query, NEW_ORDER_5, vals[D_NEXT_O_ID] , w_id, d_id);

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("NEW_ORDER_5 query: %s\n", query);
#endif
	if (!dbc_sql_execute(dbc, query, NULL, "NEW_ORDER_5"))
	{
		return 14;
	}

	rc=0;

	for (i = 0; i < o_ol_cnt; i++)
	{
		sprintf(query, NEW_ORDER_7, ol_i_id[i]);

#ifdef DEBUG_QUERY
		LOG_ERROR_MESSAGE("NEW_ORDER_7 query: %s\n", query);
#endif
		if (ol_i_id[i] != 0)
		{
            if (dbc_sql_execute(dbc, query, &result, "NEW_ORDER_7") && result.result_set)
            {
				dbc_sql_fetchrow(dbc, &result);

				i_price[i]= dbc_sql_getvalue(dbc, &result, 0);
				i_name[i]= dbc_sql_getvalue(dbc, &result, 1);
				i_data[i]= dbc_sql_getvalue(dbc, &result, 2);

				dbc_sql_close_cursor(dbc, &result);
				if (!i_price[i])
				{
					LOG_ERROR_MESSAGE("NULL or empty result set returned, NEW_ORDER_7 query: %s\n", query);
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
            LOG_ERROR_MESSAGE("ROLLBACK BECAUSE OL_I_ID[%d]= 0 for query:\nNEW_ORDER_7: %s", i, query);
#endif
            rc=2;
            break;
		}

		ol_amount[i] = atof(i_price[i] ) * (float) ol_quantity[i];

		sprintf(query, NEW_ORDER_8, s_dist[d_id - 1], ol_i_id[i], w_id);

#ifdef DEBUG_QUERY
		LOG_ERROR_MESSAGE("NEW_ORDER_8 query: %s\n", query);
#endif
		if (dbc_sql_execute(dbc, query, &result, "NEW_ORDER_8") && result.result_set)
		{
			dbc_sql_fetchrow(dbc, &result);

            s_quantity[i]= dbc_sql_getvalue(dbc, &result, 0);
            my_s_dist[i]= dbc_sql_getvalue(dbc, &result, 1);
            s_data[i]= dbc_sql_getvalue(dbc, &result, 2);

            dbc_sql_close_cursor(dbc, &result);
			if (!s_quantity[i])
			{
				LOG_ERROR_MESSAGE("NULL or empty result set returned, NEW_ORDER_8 query: %s\n", query);
				rc=16;
				break;
			}
		}
		else //error
		{
            LOG_ERROR_MESSAGE("NEW_ORDER_8 query: %s", query);
            rc=16;
            break;
		}

		order_amount += ol_amount[i];

		if (atoi(s_quantity[i] ) > ol_quantity[i] + 10)
		{
            sprintf(query, NEW_ORDER_9, ol_quantity[i],	ol_i_id[i], w_id);
		}
		else
		{
            sprintf(query, NEW_ORDER_9, ol_quantity[i] - 91, ol_i_id[i], w_id);
		}

#ifdef DEBUG_QUERY
		LOG_ERROR_MESSAGE("NEW_ORDER_9 query: %s\n", query);
#endif
		if (!dbc_sql_execute(dbc, query, NULL, "NEW_ORDER_9"))
		{
            LOG_ERROR_MESSAGE("NEW_ORDER_9 query: %s", query);
            rc=17;
            break;
		}

		sprintf(query, NEW_ORDER_10, vals[D_NEXT_O_ID] , d_id, w_id, i + 1, ol_i_id[i],
				ol_supply_w_id[i], ol_quantity[i], ol_amount[i], my_s_dist[i] );

#ifdef DEBUG_QUERY
		LOG_ERROR_MESSAGE("NEW_ORDER_10 query: %s\n", query);
#endif
		if (!dbc_sql_execute(dbc, query, NULL, "NEW_ORDER_10"))
		{
            LOG_ERROR_MESSAGE("NEW_ORDER_10 query: %s", query);
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
