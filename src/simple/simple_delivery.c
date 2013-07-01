/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 * Copyright (C) 2004 Alexey Stroganov & MySQL AB.
 *
 */

#include "simple_delivery.h"

int execute_delivery(struct db_context_t *dbc, struct delivery_t *data)
{
        int rc;
        int nvals=3;
        char * vals[3];

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

int delivery(struct db_context_t *dbc, struct delivery_t *data, char ** vals, int nvals)
{
	/* Input variables. */
	int w_id = data->w_id;
	int o_carrier_id = data->o_carrier_id;

        struct sql_result_t result;

	char query[256];
	int d_id;

        int  NO_O_ID=0;
        int  O_C_ID=1;
        int  OL_AMOUNT=2;
         
	for (d_id = 1; d_id <= 10; d_id++) 
        {
          sprintf(query, DELIVERY_1, w_id, d_id);

#ifdef DEBUG_QUERY
          LOG_ERROR_MESSAGE("DELIVERY_1: %s\n",query);
#endif
          if (dbc_sql_execute(dbc, query, &result, "DELIVERY_1") && result.result_set)
          { 
            dbc_sql_fetchrow(dbc, &result);
            vals[NO_O_ID]= (char *)dbc_sql_getvalue(dbc, &result, 0);  //NO_O_ID
            dbc_sql_close_cursor(dbc, &result);

            if (!vals[NO_O_ID])
            {
              LOG_ERROR_MESSAGE("ERROR: NO_O_ID=NULL for query DELIVERY_1:\n%s\n", query);
              //return -1;
            }
          }
          else
          { 
            /* Nothing to delivery for this district, try next. */
            continue;
          }

          if (vals[NO_O_ID] && atoi(vals[NO_O_ID])>0)
          {
            sprintf(query, DELIVERY_2, vals[NO_O_ID], w_id, d_id);

#ifdef DEBUG_QUERY
            LOG_ERROR_MESSAGE("DELIVERY_2: %s\n",query);
#endif
            if (!dbc_sql_execute(dbc, query, &result, "DELIVERY_2"))
            {
              return -1;
            }
            sprintf(query, DELIVERY_3, vals[NO_O_ID], w_id, d_id);

#ifdef DEBUG_QUERY
            LOG_ERROR_MESSAGE("DELIVERY_3: %s\n",query);
#endif
            if (dbc_sql_execute(dbc, query, &result, "DELIVERY_3") && result.result_set)
            { 
              dbc_sql_fetchrow(dbc, &result);
              vals[O_C_ID]= (char *)dbc_sql_getvalue(dbc, &result, 0);  //O_C_ID 
              dbc_sql_close_cursor(dbc, &result);
              
              if (!vals[O_C_ID])
              {
                LOG_ERROR_MESSAGE("DELIVERY_3:query %s\nO_C_ID= NULL", query);
                //return -1;
              }
            }
            else //error
            {
              return -1;
            }

            sprintf(query, DELIVERY_4, o_carrier_id, vals[NO_O_ID], w_id, d_id);

#ifdef DEBUG_QUERY
            LOG_ERROR_MESSAGE("DELIVERY_4: query %s\n", query);
#endif

            if (!dbc_sql_execute(dbc, query, &result, "DELIVERY_4"))
            {
              return -1;
            }

            sprintf(query, DELIVERY_5, vals[NO_O_ID], w_id, d_id);

#ifdef DEBUG_QUERY
            LOG_ERROR_MESSAGE("DELIVERY_5: query %s\n", query);
#endif

            if (!dbc_sql_execute(dbc, query, &result, "DELIVERY_5"))
            {
              return -1;
            }

            sprintf(query, DELIVERY_6, vals[NO_O_ID], w_id, d_id);

#ifdef DEBUG_QUERY
            LOG_ERROR_MESSAGE("DELIVERY_6: query %s\n", query);
#endif
            if (dbc_sql_execute(dbc, query, &result, "DELIVERY_6") && result.result_set)
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

            snprintf(query, 250,  DELIVERY_7, vals[OL_AMOUNT], vals[O_C_ID], w_id, d_id);

#ifdef DEBUG_QUERY
            LOG_ERROR_MESSAGE("DELIVERY_7: query %s LEN %d\n", query, strlen(query));
#endif
            if (!dbc_sql_execute(dbc, query, &result, "DELIVERY_7"))
            {
              LOG_ERROR_MESSAGE("DELIVERY_7: OL_AMOUNT: |%s| O_C_ID: |%s| query %s", vals[OL_AMOUNT], 
                                vals[O_C_ID], query);
              return -1;
            }
          }
          dbt2_free_values(vals, nvals);
        }
        return 1;
}
