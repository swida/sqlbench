/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2003-2006 Mark Wong & Open Source Development Labs, Inc.
 *
 * Based on TPC-C Standard Specification Revision 5.0 Clause 2.8.2.
 */

#include <sys/types.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>    /* for OIDs */
#include <executor/spi.h> /* this should include most necessary APIs */
#include <funcapi.h> /* for returning set of rows in order_status */
#include <utils/builtins.h>

#include "dbt2common.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
 * Order Status transaction SQL statements.
 */

#define ORDER_STATUS_1 statements[0].plan
#define ORDER_STATUS_2 statements[1].plan
#define ORDER_STATUS_3 statements[2].plan
#define ORDER_STATUS_4 statements[3].plan


static cached_statement statements[] = {
	{ /* ORDER_STATUS_1 */
	"SELECT c_id\n" \
	"FROM customer\n" \
	"WHERE c_w_id = $1\n" \
	"  AND c_d_id = $2\n" \
	"  AND c_last = $3\n" \
	"ORDER BY c_first ASC",
	3, { INT4OID, INT4OID, TEXTOID }
	},

	{ /* ORDER_STATUS_2 */
	"SELECT c_first, c_middle, c_last, c_balance\n" \
	"FROM customer\n" \
	"WHERE c_w_id = $1\n" \
	"  AND c_d_id = $2\n" \
	"  AND c_id = $3",
	3, { INT4OID, INT4OID, INT4OID }
	},

	{ /* ORDER_STATUS_3 */
	"SELECT o_id, o_carrier_id, o_entry_d, o_ol_cnt\n" \
	"FROM orders\n" \
	"WHERE o_w_id = $1\n" \
	"  AND o_d_id = $2\n" \
	"  AND o_c_id = $3\n" \
	"ORDER BY o_id DESC",
	3, { INT4OID, INT4OID, INT4OID }
	},

	{ /* ORDER_STATUS_4 */
	"SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount,\n" \
	"	ol_delivery_d\n" \
	"FROM order_line\n" \
	"WHERE ol_w_id = $1\n" \
	"  AND ol_d_id = $2\n" \
	"  AND ol_o_id = $3",
	3, { INT4OID, INT4OID, INT4OID }
	},

	{ NULL }
};


/* Prototypes to prevent potential gcc warnings. */

Datum order_status(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(order_status);

Datum order_status(PG_FUNCTION_ARGS)
{
	/* for Set Returning Function (SRF) */
	FuncCallContext  *funcctx;
	MemoryContext     oldcontext;

	/* tuple manipulating variables */
	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;

	/* loop variable */
	int j;

	if (SRF_IS_FIRSTCALL()) {

		/* Input variables. */
		int32 c_id = PG_GETARG_INT32(0);
		int32 c_w_id = PG_GETARG_INT32(1);
		int32 c_d_id = PG_GETARG_INT32(2);
		text *c_last = PG_GETARG_TEXT_P(3);

		/* temp variables */
		int ret;
		int count;

		char *tmp_c_id;
		int my_c_id = 0;

		char *c_first = NULL;
		char *c_middle = NULL;
		char *my_c_last = NULL;
		char *c_balance = NULL;

		int o_id;
		char *o_carrier_id = NULL;
		char *o_entry_d = NULL;
		char *o_ol_cnt = NULL;

		Datum args[4];
		char nulls[4] = { ' ', ' ', ' ', ' ' };

		/* SRF setup */
		funcctx = SRF_FIRSTCALL_INIT();

		/* Connect to SPI manager */
		if (( ret = SPI_connect()) < 0) {
			/* internal error */
			elog(ERROR, "order_status: SPI connect returned %d", ret);
		}

		plan_queries(statements);

		/*
		 * switch into the multi_call_memory_ctx, anything that is
		 * palloc'ed will be preserved across calls .
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (c_id == 0) {
			args[0] = Int32GetDatum(c_w_id);
			args[1] = Int32GetDatum(c_d_id);
			args[2] = PointerGetDatum(c_last);
			ret = SPI_execute_plan(ORDER_STATUS_1, args, nulls, true, 0);
			count = SPI_processed;
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;
				tuple = tuptable->vals[count / 2];

				tmp_c_id = SPI_getvalue(tuple, tupdesc, 1);
				elog(DEBUG1, "c_id = %s, %d total, selected %d",
						tmp_c_id, count, count / 2);
				my_c_id = atoi(tmp_c_id);
			} else {
				SPI_finish();
				SRF_RETURN_DONE(funcctx);
			}
		} else {
			my_c_id = c_id;
		}

		args[0] = Int32GetDatum(c_w_id);
		args[1] = Int32GetDatum(c_d_id);
		args[2] = Int32GetDatum(my_c_id);
		ret = SPI_execute_plan(ORDER_STATUS_2, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			c_first = SPI_getvalue(tuple, tupdesc, 1);
			c_middle = SPI_getvalue(tuple, tupdesc, 2);
			my_c_last = SPI_getvalue(tuple, tupdesc, 3);
			c_balance = SPI_getvalue(tuple, tupdesc, 4);
			elog(DEBUG1, "c_first = %s", c_first);
			elog(DEBUG1, "c_middle = %s", c_middle);
			elog(DEBUG1, "c_last = %s", my_c_last);
			elog(DEBUG1, "c_balance = %s", c_balance);
		} else {
			SPI_finish();
			SRF_RETURN_DONE(funcctx);
		}

		/* Maybe this should be a join with the previous query. */
		args[0] = Int32GetDatum(c_w_id);
		args[1] = Int32GetDatum(c_d_id);
		args[2] = Int32GetDatum(my_c_id);
		ret = SPI_execute_plan(ORDER_STATUS_3, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			o_id = atoi(SPI_getvalue(tuple, tupdesc, 1));
			o_carrier_id = SPI_getvalue(tuple, tupdesc, 2);
			o_entry_d = SPI_getvalue(tuple, tupdesc, 3);
			o_ol_cnt = SPI_getvalue(tuple, tupdesc, 4);
			elog(DEBUG1, "o_id = %d", o_id);
			elog(DEBUG1, "o_carrier_id = %s", o_carrier_id);
			elog(DEBUG1, "o_entry_d = %s", o_entry_d);
			elog(DEBUG1, "o_ol_cnt = %s", o_ol_cnt);
		} else {
			SPI_finish();
			SRF_RETURN_DONE(funcctx);
		}

		args[0] = Int32GetDatum(c_w_id);
		args[1] = Int32GetDatum(c_d_id);
		args[2] = Int32GetDatum(o_id);
		ret = SPI_execute_plan(ORDER_STATUS_4, args, nulls, true, 0);
		count = SPI_processed;

		elog(DEBUG1, "##  ol_i_id  ol_supply_w_id  ol_quantity  ol_amount  ol_delivery_d");
		elog(DEBUG1, "--  -------  --------------  -----------  ---------  -------------");
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;

			for (j = 0; j < count; j++) {

				char *ol_i_id[15];
				char *ol_supply_w_id[15];
				char *ol_quantity[15];
				char *ol_amount[15];
				char *ol_delivery_d[15];

				/* 15 is the buffer size */
				int idx = j % 15;

				tuple = tuptable->vals[j];

				ol_i_id[idx] = SPI_getvalue(tuple, tupdesc, 1);
				ol_supply_w_id[idx] = SPI_getvalue(tuple, tupdesc, 2);
				ol_quantity[idx] = SPI_getvalue(tuple, tupdesc, 3);
				ol_amount[idx] = SPI_getvalue(tuple, tupdesc, 4);
				ol_delivery_d[idx] = SPI_getvalue(tuple, tupdesc, 5);
				elog(DEBUG1, "%2d  %7s  %14s  %11s  %9.2f  %13s",
						j + 1, ol_i_id[idx], ol_supply_w_id[idx],
						ol_quantity[idx], atof(ol_amount[idx]),
						ol_delivery_d[idx]);
			}
		} else {
			SPI_finish();
			SRF_RETURN_DONE(funcctx);
		}

		/* get tupdesc from the type name */
		tupdesc = RelationNameGetTupleDesc("status_info");

		/*
		 * generate attribute metadata needed later to produce tuples
		 * from raw C strings
		 */
		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

		/* save SPI data for use across calls */
		funcctx->user_fctx = tuptable;

		/* total number of tuples to be returned */
		funcctx->max_calls = count;

		/* switch out of the multi_call_memory_ctx */
		MemoryContextSwitchTo(oldcontext);
	}

	/* SRF setup */
	funcctx = SRF_PERCALL_SETUP();

	/* test if we are done */
	if (funcctx->call_cntr < funcctx->max_calls) {
		/* Here we want to return another item: */

		/* setup some variables */
		Datum result;
		char** cstr_values;
		HeapTuple result_tuple;
		tuptable = (SPITupleTable*) funcctx->user_fctx;
		tupdesc = tuptable->tupdesc;
		tuple = tuptable->vals[funcctx->call_cntr]; /* ith row */

		/* TODO: get rid of the hard coded 5! */
		cstr_values = (char **) palloc(5 * sizeof(char *));
		for(j = 0; j < 5; j++) {
			cstr_values[j] = SPI_getvalue(tuple, tupdesc, j+1);
		}

		/* build a tuple */
		result_tuple = BuildTupleFromCStrings(funcctx->attinmeta, cstr_values);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(result_tuple);
		SRF_RETURN_NEXT(funcctx, result);
	} else {
		/* Here we are done returning items and just need to clean up: */
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}
