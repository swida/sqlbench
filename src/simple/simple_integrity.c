/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 * Copyright (C) 2004 Alexey Stroganov & MySQL AB.
 *
 */

#include "simple_integrity.h"

int execute_integrity(struct db_context_t *dbc, struct integrity_t *data)
{
        int rc;
        char *  vals[1];
        int nvals=1;

        rc=integrity(dbc, data, vals, nvals);

        if (rc == -1 )
        {
          LOG_ERROR_MESSAGE("TEST FINISHED WITH ERRORS \n");

          //should free memory that was allocated for nvals vars
          dbt2_free_values(vals, nvals);

          return ERROR;
        }
        return OK;
}

int integrity(struct db_context_t *dbc, struct integrity_t *data, char ** vals, int nvals)
{
	/* Input variables. */
	int w_id = data->w_id;

        struct sql_result_t result;
	char query[256];
        int W_ID=0;
 
        dbt2_init_values(vals, nvals);

	sprintf(query, INTEGRITY_1);

#ifdef DEBUG_QUERY
        LOG_ERROR_MESSAGE("INTEGRITY_1 query: %s\n",query);
#endif
        if (dbc_sql_execute(dbc, query, &result, "INTEGRITY_1") && result.result_set)
        {
          dbc_sql_fetchrow(dbc, &result);
          vals[W_ID] = dbc_sql_getvalue(dbc, &result, 0); //W_ID
          dbc_sql_close_cursor(dbc, &result);

          if (!vals[W_ID])
          {
            LOG_ERROR_MESSAGE("ERROR: W_ID is NULL for query INTEGRITY_1:\n%s\n", query);
            return -1;
          }

          if (atoi(vals[W_ID]) != w_id)
          {
            LOG_ERROR_MESSAGE("ERROR: Expect W_ID = %d Got W_ID = %d", w_id, atoi(vals[W_ID]));
            return -1;
          }
        }
        else //error
        {
          return -1;
        }

        dbt2_free_values(vals, nvals);

        return 1;
}
