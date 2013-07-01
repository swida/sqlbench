/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 * Copyright (C) 2004 Alexey Stroganov & MySQL AB.
 *
 */


#include <simple_payment.h>

int execute_payment(struct db_context_t *dbc, struct payment_t *data)
{
        int rc;

        char * vals[29];
        int nvals=29;

        rc=payment(dbc, data, vals, nvals);

        if (rc == -1 )
        {
          LOG_ERROR_MESSAGE("PAYMENT FINISHED WITH ERRORS \n");

          //should free memory that was allocated for nvals vars
          dbt2_free_values(vals, nvals);

          return ERROR;
        }

	return OK;
}

int  payment(struct db_context_t *dbc, struct payment_t *data, char ** vals, int  nvals)
{
	/* Input variables. */
	int w_id = data->w_id;
	int d_id = data->d_id;
	int c_id = data->c_id;
	int c_w_id = data->c_w_id;
	int c_d_id = data->c_d_id;
	char c_last[C_LAST_LEN+1];
	float h_amount = data->h_amount;

        struct sql_result_t result;

        int W_NAME=0;
        int W_STREET_1=1;
        int W_STREET_2=2;
        int W_CITY=3;
        int W_STATE=4;
        int W_ZIP=5;
        int D_NAME=6;
        int D_STREET_1=7;
        int D_STREET_2=8;
        int D_CITY=9;
        int D_STATE=10;
        int D_ZIP=11;
        int TMP_C_ID=12;
        int C_FIRST=13;
        int C_MIDDLE=14;
        int MY_C_LAST=15;
        int C_STREET_1=16;
        int C_STREET_2=17;
        int C_CITY=18;
        int C_STATE=19;
        int C_ZIP=20;
        int C_PHONE=21;
        int C_SINCE=22;
        int C_CREDIT=23;
        int C_CREDIT_LIM=24;
        int C_DISCOUNT=25;
        int C_BALANCE=26;
        int C_DATA=27;
        int C_YTD_PAYMENT=28;

	char query[4096];

	int my_c_id = 0;
	char my_w_name[20];
	char my_d_name[20];

        dbt2_init_values(vals, nvals);

        snprintf(c_last, C_LAST_LEN+1, "%s", data->c_last);

#ifdef DEBUG_INPUT_DATA
        LOG_ERROR_MESSAGE("PAYMENT_INPUT: w_id: %d\n         d_id: %d\n         c_id: %d\n         c_w_id: %d\n         c_d_id: %d\n",w_id,d_id,c_id,c_w_id,c_d_id);
#endif
	sprintf(query, PAYMENT_1, w_id);

#ifdef DEBUG_QUERY
        LOG_ERROR_MESSAGE("PAYMENT_1 query: %s\n",query);
#endif
        if (dbc_sql_execute(dbc, query, &result, "PAYMENT_1") && result.result_set)
        {
          dbc_sql_fetchrow(dbc, &result);

          vals[W_NAME]= dbc_sql_getvalue(dbc, &result, 0);
          vals[W_STREET_1]= dbc_sql_getvalue(dbc, &result, 1);
          vals[W_STREET_2]= dbc_sql_getvalue(dbc, &result, 2);
          vals[W_CITY]= dbc_sql_getvalue(dbc, &result, 3);
          vals[W_STATE]= dbc_sql_getvalue(dbc, &result, 4);
          vals[W_ZIP]= dbc_sql_getvalue(dbc, &result, 5);
                                                           //W_NAME W_STREET_1 W_STREET_2
                                                           //W_CITY W_STATE W_ZIP

          dbc_sql_close_cursor(dbc, &result);          
        }
        else //error
        {
          return -1;
        }

	sprintf(query, PAYMENT_2, h_amount, w_id);

#ifdef DEBUG_QUERY
        LOG_ERROR_MESSAGE("PAYMENT_2 query: %s\n",query);
#endif

        if (!dbc_sql_execute(dbc, query, &result, "PAYMENT_2"))
        {
          return -1;
        }

	sprintf(query, PAYMENT_3, d_id, w_id);

#ifdef DEBUG_QUERY
        LOG_ERROR_MESSAGE("PAYMENT_3 query: %s\n",query);
#endif

        if (dbc_sql_execute(dbc, query, &result, "PAYMENT_3") && result.result_set)
        {
          dbc_sql_fetchrow(dbc, &result);

          vals[D_NAME]= dbc_sql_getvalue(dbc, &result, 0);
          vals[D_STREET_1]= dbc_sql_getvalue(dbc, &result, 1);
          vals[D_STREET_2]= dbc_sql_getvalue(dbc, &result, 2);
          vals[D_CITY]= dbc_sql_getvalue(dbc, &result, 3);
          vals[D_STATE]= dbc_sql_getvalue(dbc, &result, 4);
          vals[D_ZIP]= dbc_sql_getvalue(dbc, &result, 5);
                                                           //D_NAME D_STREET_1 D_STREET_2
                                                           //D_CITY D_STATE D_ZIP
          
          dbc_sql_close_cursor(dbc, &result);
        }
        else //error
        {
          return -1;
        }

	sprintf(query, PAYMENT_4, h_amount, d_id, w_id);

#ifdef DEBUG_QUERY
        LOG_ERROR_MESSAGE("PAYMENT_4 query: %s\n", query);
#endif

        if (!dbc_sql_execute(dbc, query, &result, "PAYMENT_4"))
        {
          LOG_ERROR_MESSAGE("PAYMENT_4 query: %s", query);
          return -1;
        }

	if (c_id == 0) 
        {
          sprintf(query, PAYMENT_5, w_id, d_id, c_last);

#ifdef DEBUG_QUERY
          LOG_ERROR_MESSAGE("PAYMENT_5 query: %s\n", query);
#endif

          if (dbc_sql_execute(dbc, query, &result, "PAYMENT_5") && result.result_set)
          {
            dbc_sql_fetchrow(dbc, &result);
            vals[TMP_C_ID]= dbc_sql_getvalue(dbc, &result, 0);
            dbc_sql_close_cursor(dbc, &result);

            if (!vals[TMP_C_ID])
            { 
              LOG_ERROR_MESSAGE("ERROR: TMP_C_ID=NULL for query PAYMENT_5:\n%s\n", query);
              return -1;
            }                                                                  
            my_c_id = atoi(vals[TMP_C_ID]);
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

	sprintf(query, PAYMENT_6, c_w_id, c_d_id, my_c_id);

#ifdef DEBUG_QUERY
        LOG_ERROR_MESSAGE("PAYMENT_6 query: %s\n",query);
#endif
        if (dbc_sql_execute(dbc, query, &result, "PAYMENT_6") && result.result_set)
        {
          dbc_sql_fetchrow(dbc, &result);
                                                            //C_FIRST C_MIDDLE MY_C_LAST
                                                            //C_STREET_1 C_STREET_2 C_CITY
                                                            //C_STATE C_ZIP C_PHONE C_SINCE
                                                            //C_CREDIT C_CREDIT_LIM C_DISCOUNT
                                                            //C_BALANCE C_DATA C_YTD_PAYMENT
                                                            //C_BALANCE and C_YTD_PAYMENT can be NULL
          vals[C_FIRST]= dbc_sql_getvalue(dbc, &result, 0);
          vals[C_MIDDLE]= dbc_sql_getvalue(dbc, &result, 1);
          vals[MY_C_LAST]= dbc_sql_getvalue(dbc, &result, 2);
          vals[C_STREET_1]= dbc_sql_getvalue(dbc, &result, 3);
          vals[C_STREET_2]= dbc_sql_getvalue(dbc, &result, 4);
          vals[C_CITY]= dbc_sql_getvalue(dbc, &result, 5);
          vals[C_STATE]= dbc_sql_getvalue(dbc, &result, 6);
          vals[C_ZIP]= dbc_sql_getvalue(dbc, &result, 7);
          vals[C_PHONE]= dbc_sql_getvalue(dbc, &result, 8);
          vals[C_SINCE]= dbc_sql_getvalue(dbc, &result, 9);
          vals[C_CREDIT]= dbc_sql_getvalue(dbc, &result, 10);
          vals[C_CREDIT_LIM]= dbc_sql_getvalue(dbc, &result, 11);
          vals[C_DISCOUNT]= dbc_sql_getvalue(dbc, &result, 12);
          vals[C_BALANCE]= dbc_sql_getvalue(dbc, &result, 13);
          vals[C_DATA]= dbc_sql_getvalue(dbc, &result, 14);
          vals[C_YTD_PAYMENT]= dbc_sql_getvalue(dbc, &result, 15);
          
          dbc_sql_close_cursor(dbc, &result);

          if (!vals[C_CREDIT])
          {
            LOG_ERROR_MESSAGE("ERROR: C_CREDIT=NULL for query PAYMENT_6:\n%s\n", query);
            return -1;
          }
        }
        else //error
        {
          return -1;
        }
      
	/* It's either "BC" or "GC". */
	if (vals[C_CREDIT][0] == 'G') 
        {
          sprintf(query, PAYMENT_7_GC, h_amount, my_c_id, c_w_id, c_d_id);

#ifdef DEBUG_QUERY
          LOG_ERROR_MESSAGE("PAYMENT_7_GC query: %s\n",query);
#endif
          if (!dbc_sql_execute(dbc, query, &result, "PAYMENT_7_GC"))
          {
            return -1;
          }
        }
        else
        {
          char my_c_data[1000];
          sprintf(my_c_data, "%d %d %d %d %d %f ", my_c_id, c_d_id,
			c_w_id, d_id, w_id, h_amount);
	  /* Copy and escape all at once! */
	  dbt2_escape_str(vals[C_DATA], my_c_data);
          sprintf(query, PAYMENT_7_BC, h_amount, my_c_data, my_c_id, c_w_id, c_d_id);

#ifdef DEBUG_QUERY
          LOG_ERROR_MESSAGE("PAYMENT_7_BC query: %s\n",query);
#endif

          if (!dbc_sql_execute(dbc, query, &result, "PAYMENT_7_BC"))
          {
            return -1;
          }
	}

	/* Escape special characters. */
	dbt2_escape_str(vals[W_NAME], my_w_name);
	dbt2_escape_str(vals[D_NAME], my_d_name);

	sprintf(query, PAYMENT_8, my_c_id, c_d_id, c_w_id, d_id, w_id,
		h_amount, my_w_name, my_d_name);

#ifdef DEBUG_QUERY
        LOG_ERROR_MESSAGE("PAYMENT_8 query: %s\n",query);
#endif

        if (!dbc_sql_execute(dbc, query, &result, "PAYMENT_8"))
        {
          return -1;
        }

        dbt2_free_values(vals, nvals);

        return 1;
}

