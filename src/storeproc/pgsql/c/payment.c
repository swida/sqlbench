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
#include <catalog/pg_type.h> /* for type OIDs */
#include <executor/spi.h> /* this should include most necessary APIs */
#include <funcapi.h> /* for returning set of rows in order_status */
#include <utils/builtins.h>

#include "dbt2common.h"

/*
 * Payment transaction SQL statements.
 */

#define PAYMENT_1 statements[0].plan
#define PAYMENT_2 statements[1].plan
#define PAYMENT_3 statements[2].plan
#define PAYMENT_4 statements[3].plan
#define PAYMENT_5 statements[4].plan
#define PAYMENT_6 statements[5].plan
#define PAYMENT_7_GC statements[6].plan
#define PAYMENT_7_BC statements[7].plan
#define PAYMENT_8 statements[8].plan

static cached_statement statements[] =
{
	{ /* PAYMENT_1 */
	"SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip\n" \
	"FROM warehouse\n" \
	"WHERE w_id = $1",
	1, { INT4OID }
	},

	{ /* PAYMENT_2 */
	"UPDATE warehouse\n" \
	"SET w_ytd = w_ytd + $1\n" \
	"WHERE w_id = $2",
	2, { FLOAT4OID, INT4OID }
	},

	{ /* PAYMENT_3 */
	"SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip\n" \
	"FROM district\n" \
	"WHERE d_id = $1\n" \
	"  AND d_w_id = $2",
	2, { INT4OID, INT4OID }
	},

	{ /* PAYMENT_4 */
	"UPDATE district\n" \
	"SET d_ytd = d_ytd + $1\n" \
	"WHERE d_id = $2\n" \
	"  AND d_w_id = $3",
	3, { FLOAT4OID, INT4OID, INT4OID }
	},

	{ /* PAYMENT_5 */
	"SELECT c_id\n" \
	"FROM customer\n" \
	"WHERE c_w_id = $1\n" \
	"  AND c_d_id = $2\n" \
	"  AND c_last = $3\n" \
	"ORDER BY c_first ASC",
	3, { INT4OID, INT4OID, TEXTOID }
	},

	{ /* PAYMENT_6 */
	"SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city,\n" \
	"       c_state, c_zip, c_phone, c_since, c_credit,\n" \
	"       c_credit_lim, c_discount, c_balance, c_data, c_ytd_payment\n" \
	"FROM customer\n" \
	"WHERE c_w_id = $1\n" \
	"  AND c_d_id = $2\n" \
	"  AND c_id = $3",
	3, { INT4OID, INT4OID, INT4OID }
	},

	{ /* PAYMENT_7_GC */
	"UPDATE customer\n" \
	"SET c_balance = c_balance - $1,\n" \
	"    c_ytd_payment = c_ytd_payment + 1\n" \
	"WHERE c_id = $2\n" \
	"  AND c_w_id = $3\n" \
	"  AND c_d_id = $4",
	4, { FLOAT4OID, INT4OID, INT4OID, INT4OID }
	},

	{ /* PAYMENT_7_BC */
	"UPDATE customer\n" \
	"SET c_balance = c_balance - $1,\n" \
	"    c_ytd_payment = c_ytd_payment + 1,\n" \
	"    c_data = substring($2 || c_data, 1 , 500)\n" \
	"WHERE c_id = $3\n" \
	"  AND c_w_id = $4\n" \
	"  AND c_d_id = $5",
	5, { FLOAT4OID, TEXTOID, INT4OID, INT4OID, INT4OID }
	},

	{ /* PAYMENT_8 */
	"INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id,\n" \
	"		     h_date, h_amount, h_data)\n" \
	"VALUES ($1, $2, $3, $4, $5, current_timestamp, $6, $7 || '    ' || $8)",
	8, { INT4OID, INT4OID, INT4OID, INT4OID, INT4OID, FLOAT4OID, TEXTOID, TEXTOID }
	},

	{ NULL }
};

/* Prototypes to prevent potential gcc warnings. */
Datum payment(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(payment);

Datum payment(PG_FUNCTION_ARGS)
{
	/* Input variables. */
	int32 w_id = PG_GETARG_INT32(0);
	int32 d_id = PG_GETARG_INT32(1);
	int32 c_id = PG_GETARG_INT32(2);
	int32 c_w_id = PG_GETARG_INT32(3);
	int32 c_d_id = PG_GETARG_INT32(4);
	text *c_last = PG_GETARG_TEXT_P(5);
	float4 h_amount = PG_GETARG_FLOAT4(6);

	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;

	int ret;
	char *w_name = NULL;
	char *w_street_1 = NULL;
	char *w_street_2 = NULL;
	char *w_city = NULL;
	char *w_state = NULL;
	char *w_zip = NULL;

	char *d_name = NULL;
	char *d_street_1 = NULL;
	char *d_street_2 = NULL;
	char *d_city = NULL;
	char *d_state = NULL;
	char *d_zip = NULL;

	char *tmp_c_id = NULL;
	int my_c_id = 0;
	int count;

	char *c_first = NULL;
	char *c_middle = NULL;
	char *my_c_last = NULL;
	char *c_street_1 = NULL;
	char *c_street_2 = NULL;
	char *c_city = NULL;
	char *c_state = NULL;
	char *c_zip = NULL;
	char *c_phone = NULL;
	char *c_since = NULL;
	char *c_credit = NULL;
	char *c_credit_lim = NULL;
	char *c_discount = NULL;
	char *c_balance = NULL;
	char *c_data = NULL;
	char *c_ytd_payment = NULL;

	Datum	args[8];
	char	nulls[8] = { ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ' };

	elog(DEBUG1, "w_id = %d", w_id);
	elog(DEBUG1, "d_id = %d", d_id);
	elog(DEBUG1, "c_id = %d", c_id);
	elog(DEBUG1, "c_w_id = %d", c_w_id);
	elog(DEBUG1, "c_d_id = %d", c_d_id);
	elog(DEBUG1, "c_last = %s",
			DatumGetCString(DirectFunctionCall1(textout,
			PointerGetDatum(c_last))));
	elog(DEBUG1, "h_amount = %f", h_amount);

	SPI_connect();

	plan_queries(statements);

	args[0] = Int32GetDatum(w_id);
	ret = SPI_execute_plan(PAYMENT_1, args, nulls, true, 0);
	if (ret == SPI_OK_SELECT && SPI_processed > 0) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];

		w_name = SPI_getvalue(tuple, tupdesc, 1);
		w_street_1 = SPI_getvalue(tuple, tupdesc, 2);
		w_street_2 = SPI_getvalue(tuple, tupdesc, 3);
		w_city = SPI_getvalue(tuple, tupdesc, 4);
		w_state = SPI_getvalue(tuple, tupdesc, 5);
		w_zip = SPI_getvalue(tuple, tupdesc, 6);
		elog(DEBUG1, "w_name = %s", w_name);
		elog(DEBUG1, "w_street_1 = %s", w_street_1);
		elog(DEBUG1, "w_street_2 = %s", w_street_2);
		elog(DEBUG1, "w_city = %s", w_city);
		elog(DEBUG1, "w_state = %s", w_state);
		elog(DEBUG1, "w_zip = %s", w_zip);
	} else {
		SPI_finish();
		PG_RETURN_INT32(-1);
	}

	args[0] = Float4GetDatum(h_amount);
	args[1] = Int32GetDatum(w_id);
	ret = SPI_execute_plan(PAYMENT_2, args, nulls, false, 0);
	if (ret != SPI_OK_UPDATE) {
		SPI_finish();
		PG_RETURN_INT32(-1);
	}

	args[0] = Int32GetDatum(d_id);
	args[1] = Int32GetDatum(w_id);
	ret = SPI_execute_plan(PAYMENT_3, args, nulls, true, 0);
	if (ret == SPI_OK_SELECT && SPI_processed > 0) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];

		d_name = SPI_getvalue(tuple, tupdesc, 1);
		d_street_1 = SPI_getvalue(tuple, tupdesc, 2);
		d_street_2 = SPI_getvalue(tuple, tupdesc, 3);
		d_city = SPI_getvalue(tuple, tupdesc, 4);
		d_state = SPI_getvalue(tuple, tupdesc, 5);
		d_zip = SPI_getvalue(tuple, tupdesc, 6);
		elog(DEBUG1, "d_name = %s", d_name);
		elog(DEBUG1, "d_street_1 = %s", d_street_1);
		elog(DEBUG1, "d_street_2 = %s", d_street_2);
		elog(DEBUG1, "d_city = %s", d_city);
		elog(DEBUG1, "d_state = %s", d_state);
		elog(DEBUG1, "d_zip = %s", d_zip);
	} else {
		SPI_finish();
		PG_RETURN_INT32(-1);
	}

	args[0] = Float4GetDatum(h_amount);
	args[1] = Int32GetDatum(d_id);
	args[2] = Int32GetDatum(w_id);
	ret = SPI_execute_plan(PAYMENT_4, args, nulls, false, 0);
	if (ret != SPI_OK_UPDATE) {
		SPI_finish();
		PG_RETURN_INT32(-1);
	}

	if (c_id == 0) {
		args[0] = Int32GetDatum(w_id);
		args[1] = Int32GetDatum(d_id);
		args[2] = PointerGetDatum(c_last);
		ret = SPI_execute_plan(PAYMENT_5, args, nulls, true, 0);
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
			PG_RETURN_INT32(-1);
		}
	} else {
		my_c_id = c_id;
	}

	args[0] = Int32GetDatum(c_w_id);
	args[1] = Int32GetDatum(c_d_id);
	args[2] = Int32GetDatum(my_c_id);
	ret = SPI_execute_plan(PAYMENT_6, args, nulls, true, 0);
	if (ret == SPI_OK_SELECT && SPI_processed > 0) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];

		c_first = SPI_getvalue(tuple, tupdesc, 1);
		c_middle = SPI_getvalue(tuple, tupdesc, 2);
		my_c_last = SPI_getvalue(tuple, tupdesc, 3);
		c_street_1 = SPI_getvalue(tuple, tupdesc, 4);
		c_street_2 = SPI_getvalue(tuple, tupdesc, 5);
		c_city = SPI_getvalue(tuple, tupdesc, 6);
		c_state = SPI_getvalue(tuple, tupdesc, 7);
		c_zip = SPI_getvalue(tuple, tupdesc, 8);
		c_phone = SPI_getvalue(tuple, tupdesc, 9);
		c_since = SPI_getvalue(tuple, tupdesc, 10);
		c_credit = SPI_getvalue(tuple, tupdesc, 11);
		c_credit_lim = SPI_getvalue(tuple, tupdesc, 12);
		c_discount = SPI_getvalue(tuple, tupdesc, 13);
		c_balance = SPI_getvalue(tuple, tupdesc, 14);
		c_data = SPI_getvalue(tuple, tupdesc, 15);
		c_ytd_payment = SPI_getvalue(tuple, tupdesc, 16);
		elog(DEBUG1, "c_first = %s", c_first);
		elog(DEBUG1, "c_middle = %s", c_middle);
		elog(DEBUG1, "c_last = %s", my_c_last);
		elog(DEBUG1, "c_street_1 = %s", c_street_1);
		elog(DEBUG1, "c_street_2 = %s", c_street_2);
		elog(DEBUG1, "c_city = %s", c_city);
		elog(DEBUG1, "c_state = %s", c_state);
		elog(DEBUG1, "c_zip = %s", c_zip);
		elog(DEBUG1, "c_phone = %s", c_phone);
		elog(DEBUG1, "c_since = %s", c_since);
		elog(DEBUG1, "c_credit = %s", c_credit);
		elog(DEBUG1, "c_credit_lim = %s", c_credit_lim);
		elog(DEBUG1, "c_discount = %s", c_discount);
		elog(DEBUG1, "c_balance = %s", c_balance);
		elog(DEBUG1, "c_data = %s", c_data);
		elog(DEBUG1, "c_ytd_payment = %s", c_ytd_payment);
	} else {
		SPI_finish();
		PG_RETURN_INT32(-1);
	}

	/* It's either "BC" or "GC". */
	if (c_credit[0] == 'G') {
		args[0] = Float4GetDatum(h_amount);
		args[1] = Int32GetDatum(my_c_id);
		args[2] = Int32GetDatum(c_w_id);
		args[3] = Int32GetDatum(c_d_id);
		ret = SPI_execute_plan(PAYMENT_7_GC, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			SPI_finish();
			PG_RETURN_INT32(-1);
		}
	} else {
		char my_c_data[1000];

		sprintf(my_c_data, "%d %d %d %d %d %f ", my_c_id, c_d_id,
				c_w_id, d_id, w_id, h_amount);

		args[0] = Float4GetDatum(h_amount);
		args[1] = CStringGetTextDatum(my_c_data);
		args[2] = Int32GetDatum(my_c_id);
		args[3] = Int32GetDatum(c_w_id);
		args[4] = Int32GetDatum(c_d_id);
		ret = SPI_execute_plan(PAYMENT_7_BC, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			SPI_finish();
			PG_RETURN_INT32(-1);
		}
	}

	args[0] = Int32GetDatum(my_c_id);
	args[1] = Int32GetDatum(c_d_id);
	args[2] = Int32GetDatum(c_w_id);
	args[3] = Int32GetDatum(d_id);
	args[4] = Int32GetDatum(w_id);
	args[5] = Float4GetDatum(h_amount);
	args[6] = CStringGetTextDatum(w_name);
	args[7] = CStringGetTextDatum(d_name);
	ret = SPI_execute_plan(PAYMENT_8, args, nulls, false, 0);
	if (ret != SPI_OK_INSERT) {
		SPI_finish();
		PG_RETURN_INT32(-1);
	}

	SPI_finish();
	PG_RETURN_INT32(1);
}
