/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 * Copyright (C) 2004 Alexey Stroganov & MySQL AB.
 *
 */


#include "simple_order_status.h"
#include <string.h>
int execute_order_status(struct db_context_t *dbc, struct order_status_t *data)
{
        int rc;

        int nvals=9;
        char * vals[9];

        rc= order_status(dbc, data, vals, nvals);
         
        if (rc == -1 )
        {
          LOG_ERROR_MESSAGE("ORDER_STATUS FINISHED WITH ERRORS %d\n", rc);

          //should free memory that was allocated for nvals vars
          dbt2_free_values(vals, nvals);

          return ERROR;
        }

	return OK;
}


int order_status(struct db_context_t *dbc, struct order_status_t *data, char ** vals, int  nvals)
{
	/* Input variables. */
	int c_id = data->c_id;
	int c_w_id = data->c_w_id;
	int c_d_id = data->c_d_id;

        char c_last[C_LAST_LEN+1];
	char query[512];

        struct sql_result_t result;

        int i;
	int my_c_id = 0;
     
        int TMP_C_ID=0;           
        int C_FIRST = 1;          
        int C_MIDDLE = 2;         
        int MY_C_BALANCE = 3;     
        int C_BALANCE = 4;        
        int O_ID = 5;             
        int O_CARRIER_ID = 6;     
        int O_ENTRY_D = 7;        
        int O_OL_CNT = 8;         

	char * ol_i_id[15];
        char * ol_supply_w_id[15];
        char * ol_quantity[15];
        char * ol_amount[15];
        char * ol_delivery_d[15];

        dbt2_init_values(vals, nvals);
        dbt2_init_values(ol_i_id, 15);
        dbt2_init_values(ol_supply_w_id, 15);
        dbt2_init_values(ol_quantity, 15);
        dbt2_init_values(ol_amount, 15);
        dbt2_init_values(ol_delivery_d, 15);

        snprintf(c_last, C_LAST_LEN+1, "%s", data->c_last);

	if (c_id == 0) 
        {
          sprintf(query, ORDER_STATUS_1, c_w_id, c_d_id, c_last);

#ifdef DEBUG_QUERY
          LOG_ERROR_MESSAGE("ORDER_STATUS_1 %s\n", query);
#endif

          if (dbc_sql_execute(dbc, query, &result, "ORDER_STATUS_1") && result.result_set)
          {
		  // 2.6.2.2 says the (n/2 rounded up)-th row
#ifdef HAVE_SQL_RESULT_NUM_ROWS
        	unsigned long skip_rows;
            skip_rows=(result.num_rows+1)/2;
			do {
              dbc_sql_fetchrow(dbc, &result);
              skip_rows--;
			} while (skip_rows > 0);
            vals[TMP_C_ID]= dbc_sql_getvalue(dbc, &result, 0); //TMP_C_ID

            if (!vals[TMP_C_ID])
            {
              LOG_ERROR_MESSAGE("ERROR: TMP_C_ID=NULL for query ORDER_STATUS_1:\n%s\n", query);
              return -1;
            }

            my_c_id = atoi(vals[TMP_C_ID]);
#else
			// A real PITA when we don't know the number of rows
			int c_ids_static_array[16];
			int * c_ids_array = c_ids_static_array;
			int c_ids_alloc_count = 0;
			int c_ids_result_count = 0;
			while (dbc_sql_fetchrow(dbc, &result))
			{
			  if (c_ids_result_count >= 16 
			      && c_ids_result_count >= c_ids_alloc_count)
			  {
			    if (c_ids_alloc_count == 0) 
				{
			      c_ids_alloc_count = 32;
				  if (!(c_ids_array = realloc(NULL, c_ids_alloc_count*sizeof(int))))
				  {
				    LOG_ERROR_MESSAGE("order_status: REALLOC FAILED trying to expand to 32 entries for middle result\n");
					return -1;
				  }
				  memcpy(c_ids_array, c_ids_static_array, sizeof(c_ids_static_array));
				}
				else
				{
			      c_ids_alloc_count *= 2;
				  if (!(c_ids_array = realloc(c_ids_array, c_ids_alloc_count*sizeof(int))))
				  {
				    LOG_ERROR_MESSAGE("order_status: REALLOC FAILED trying to expand to %d entries for middle result\n", c_ids_alloc_count);
					return -1;
				  }
				}
			  }
			  vals[TMP_C_ID]= dbc_sql_getvalue(dbc, &result, 0);

              if (vals[TMP_C_ID])
              {
			    c_ids_array[c_ids_result_count] = atoi(vals[TMP_C_ID]);
			    free(vals[TMP_C_ID]);
			  }
			  else
              {
			    c_ids_array[c_ids_result_count] = -1;
              }
			  c_ids_result_count++;
			  vals[TMP_C_ID] = NULL;
			}
            my_c_id = c_ids_array[(c_ids_result_count-1)/2];
			if (c_ids_alloc_count)
			{
				free(c_ids_array);
			}
			if (my_c_id == -1 || c_ids_result_count == 0)
			{
              LOG_ERROR_MESSAGE("ERROR: TMP_C_ID=NULL for query ORDER_STATUS_1:\n%s\n", query);
              return -1;
			}
#endif /* HAVE_SQL_RESULT_NUM_ROWS */
            dbc_sql_close_cursor(dbc, &result);

          }
          else //error
          {
            return -1;
          }
        } 
        else
        {
          my_c_id = c_id;
          vals[TMP_C_ID]=NULL;
	}

	sprintf(query, ORDER_STATUS_2, c_w_id, c_d_id, my_c_id);

#ifdef DEBUG_QUERY
        LOG_ERROR_MESSAGE("ORDER_STATUS_2 %s\n", query);
#endif
        if (dbc_sql_execute(dbc, query, &result, "ORDER_STATUS_2") && result.result_set)
        {
          dbc_sql_fetchrow(dbc, &result);
          
          vals[C_FIRST]= dbc_sql_getvalue(dbc, &result, 0); //C_FIRST C_MIDDLE MY_C_BALANCE C_BALANCE
          vals[C_MIDDLE]= dbc_sql_getvalue(dbc, &result, 1);
          vals[MY_C_BALANCE]= dbc_sql_getvalue(dbc, &result, 2);
          vals[C_BALANCE]= dbc_sql_getvalue(dbc, &result, 3);

          //FIXME: To add checks that vars above are not null
          dbc_sql_close_cursor(dbc, &result);
        }
        else //error
        {
          return -1;
        }

	sprintf(query, ORDER_STATUS_3, c_w_id, c_d_id, my_c_id);

#ifdef DEBUG_QUERY
        LOG_ERROR_MESSAGE("ORDER_STATUS_3 %s\n", query);
#endif
        if (dbc_sql_execute(dbc, query, &result, "ORDER_STATUS_3") && result.result_set)
        {
          dbc_sql_fetchrow(dbc, &result);

          vals[O_ID]= dbc_sql_getvalue(dbc, &result, 0); //O_ID O_CARRIER_ID O_ENTRY_D O_OL_CNT
          vals[O_CARRIER_ID]= dbc_sql_getvalue(dbc, &result, 1);
          vals[O_ENTRY_D]= dbc_sql_getvalue(dbc, &result, 2);
          vals[O_OL_CNT]= dbc_sql_getvalue(dbc, &result, 3);

          dbc_sql_close_cursor(dbc, &result);
        }
        else //error
        {
          return -1;
        }

	sprintf(query, ORDER_STATUS_4, c_w_id, c_d_id, vals[O_ID]);

#ifdef DEBUG_QUERY
        LOG_ERROR_MESSAGE("ORDER_STATUS_4 %s\n", query);
#endif

        if (dbc_sql_execute(dbc, query, &result, "ORDER_STATUS_4") && result.result_set)
        {
          i= 0;
          while (dbc_sql_fetchrow(dbc, &result))
          { 
            ol_i_id[i]= dbc_sql_getvalue(dbc, &result, 0);
            ol_supply_w_id[i]= dbc_sql_getvalue(dbc, &result, 1);
            ol_quantity[i]= dbc_sql_getvalue(dbc, &result, 2);
            ol_amount[i]= dbc_sql_getvalue(dbc, &result, 3);
            ol_delivery_d[i]= dbc_sql_getvalue(dbc, &result, 4);
            i++;
          }
          dbc_sql_close_cursor(dbc, &result);
        }
        else //error
        {
          return -1;
        }

        dbt2_free_values(vals, nvals);
        dbt2_free_values(ol_i_id, 15);
        dbt2_free_values(ol_supply_w_id, 15);
        dbt2_free_values(ol_quantity, 15);
        dbt2_free_values(ol_amount, 15);
        dbt2_free_values(ol_delivery_d, 15);

	return 1;
}

