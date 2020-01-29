/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 * Copyright (C) 2004 Alexey Stroganov & MySQL AB.
 *
 */

#include "extended_integrity.h"
static int integrity(db_context_t *dbc, struct integrity_t *data, char ** vals, int nvals);

int
extended_initialize_integrity(db_context_t *dbc)
{
	void *stmt = dbc_sql_prepare(dbc, INTEGRITY_1, N_INTEGRITY_1);
	if (!stmt)
		return ERROR;

	dbc->transaction_data[INTEGRITY] = stmt;

	return OK;
}

int
extended_execute_integrity(db_context_t *dbc, struct integrity_t *data)
{
	char *vals[1];
	int nvals=1;

	if (integrity(dbc, data, vals, nvals) == -1)
	{
		LOG_ERROR_MESSAGE("TEST FINISHED WITH ERRORS \n");

		//should free memory that was allocated for nvals vars
		dbt2_free_values(vals, nvals);

		return ERROR;
	}
	return OK;
}

static int
integrity(struct db_context_t *dbc, struct integrity_t *data, char ** vals, int nvals)
{
	void *stmt = dbc->transaction_data[INTEGRITY];
	/* Input variables. */
	int w_id = data->w_id;

	struct sql_result_t result;
	int W_ID=0;

	dbt2_init_values(vals, nvals);

#ifdef DEBUG_QUERY
	LOG_ERROR_MESSAGE("%s query: %s\n", N_INTEGRITY_1, INTEGRITY_1);
#endif
	if (dbc_sql_execute_prepared(dbc, NULL, 0, &result, stmt) && result.result_set)
	{
		dbc_sql_fetchrow(dbc, &result);
		vals[W_ID] = dbc_sql_getvalue(dbc, &result, 0); //W_ID
		dbc_sql_close_cursor(dbc, &result);

		if (!vals[W_ID])
		{
            LOG_ERROR_MESSAGE("ERROR: W_ID is NULL for query %s:\n%s\n", N_INTEGRITY_1, INTEGRITY_1);
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
