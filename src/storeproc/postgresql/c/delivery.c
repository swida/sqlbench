/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2003-2008 Mark Wong & Open Source Development Labs, Inc.
 *
 * Based on TPC-C Standard Specification Revision 5.0 Clause 2.8.2.
 */

#include <sys/types.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>    /* for INT4OID */
#include <executor/spi.h>       /* this should include most necessary APIs */
#include <utils/builtins.h>     /* for numeric_in() */

#include "dbt2common.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
 * Delivery transaction SQL statements.
 */

#define DELIVERY_1 statements[0].plan
#define DELIVERY_2 statements[1].plan
#define DELIVERY_3 statements[2].plan
#define DELIVERY_4 statements[3].plan
#define DELIVERY_5 statements[4].plan
#define DELIVERY_6 statements[5].plan
#define DELIVERY_7 statements[6].plan

static cached_statement statements[] =
{
	/* DELIVERY_1 */
	{
	"SELECT no_o_id\n" \
	"FROM new_order\n" \
	"WHERE no_w_id = $1\n" \
	"  AND no_d_id = $2" \
	"ORDER BY no_o_id " \
	"LIMIT 1",
	2,
	{ INT4OID, INT4OID }
	},

	/* DELIVERY_2 */
	{
	"DELETE FROM new_order\n" \
	"WHERE no_o_id = $1\n" \
	"  AND no_w_id = $2\n" \
	"  AND no_d_id = $3",
	3,
	{ INT4OID, INT4OID, INT4OID }
	},

	/* DELIVERY_3 */
	{
	"SELECT o_c_id\n" \
	"FROM orders\n" \
	"WHERE o_id = $1\n" \
	"  AND o_w_id = $2\n" \
	"  AND o_d_id = $3",
	3,
	{ INT4OID, INT4OID, INT4OID }
	},

	/* DELIVERY_4 */
	{
	"UPDATE orders\n" \
	"SET o_carrier_id = $1\n" \
	"WHERE o_id = $2\n" \
	"  AND o_w_id = $3\n" \
	"  AND o_d_id = $4",
	4,
	{ INT4OID, INT4OID, INT4OID, INT4OID }
	},

	/* DELIVERY_5 */
	{
	"UPDATE order_line\n" \
	"SET ol_delivery_d = current_timestamp\n" \
	"WHERE ol_o_id = $1\n" \
	"  AND ol_w_id = $2\n" \
	"  AND ol_d_id = $3",
	3,
	{ INT4OID, INT4OID, INT4OID }
	},

	/* DELIVERY_6 */
	{
	"SELECT SUM(ol_amount * ol_quantity)\n" \
	"FROM order_line\n" \
	"WHERE ol_o_id = $1\n" \
	"  AND ol_w_id = $2\n" \
	"  AND ol_d_id = $3",
	3,
	{ INT4OID, INT4OID, INT4OID }
	},

	/* DELIVERY_7 */
	{
	"UPDATE customer\n" \
	"SET c_delivery_cnt = c_delivery_cnt + 1,\n" \
	"    c_balance = c_balance + $1\n" \
	"WHERE c_id = $2\n" \
	"  AND c_w_id = $3\n" \
	"  AND c_d_id = $4",
	4,
	{ NUMERICOID, INT4OID, INT4OID, INT4OID }
	},

	{ NULL }
};

/* Prototypes to prevent potential gcc warnings. */

Datum delivery(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(delivery);

Datum delivery(PG_FUNCTION_ARGS)
{
	/* Input variables. */
	int32 w_id = PG_GETARG_INT32(0);
	int32 o_carrier_id = PG_GETARG_INT32(1);

	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;

	int d_id;
	int ret;
	char *ol_amount;
	int no_o_id;
	int o_c_id;

	SPI_connect();

	plan_queries(statements);

	for (d_id = 1; d_id <= 10; d_id++) {
		Datum	args[4];
		char	nulls[4] = { ' ', ' ', ' ', ' ' };

		args[0] = Int32GetDatum(w_id);
		args[1] = Int32GetDatum(d_id);
		ret = SPI_execute_plan(DELIVERY_1, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			no_o_id = atoi(SPI_getvalue(tuple, tupdesc, 1));
			elog(DEBUG1, "no_o_id = %d", no_o_id);
		} else {
			/* Nothing to deliver for this district, try next district. */
			continue;
		}

		args[0] = Int32GetDatum(no_o_id);
		args[1] = Int32GetDatum(w_id);
		args[2] = Int32GetDatum(d_id);
		ret = SPI_execute_plan(DELIVERY_2, args, nulls, false, 0);
		if (ret != SPI_OK_DELETE) {
			SPI_finish();
			PG_RETURN_INT32(-1);
		}

		args[0] = Int32GetDatum(no_o_id);
		args[1] = Int32GetDatum(w_id);
		args[2] = Int32GetDatum(d_id);
		ret = SPI_execute_plan(DELIVERY_3, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			o_c_id = atoi(SPI_getvalue(tuple, tupdesc, 1));
			elog(DEBUG1, "o_c_id = %d", o_c_id);
		} else {
			SPI_finish();
			PG_RETURN_INT32(-1);
		}

		args[0] = Int32GetDatum(o_carrier_id);
		args[1] = Int32GetDatum(no_o_id);
		args[2] = Int32GetDatum(w_id);
		args[3] = Int32GetDatum(d_id);
		ret = SPI_execute_plan(DELIVERY_4, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			SPI_finish();
			PG_RETURN_INT32(-1);
		}

		args[0] = Int32GetDatum(no_o_id);
		args[1] = Int32GetDatum(w_id);
		args[2] = Int32GetDatum(d_id);
		ret = SPI_execute_plan(DELIVERY_5, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			SPI_finish();
			PG_RETURN_INT32(-1);
		}

		args[0] = Int32GetDatum(no_o_id);
		args[1] = Int32GetDatum(w_id);
		args[2] = Int32GetDatum(d_id);
		ret = SPI_execute_plan(DELIVERY_6, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			ol_amount = SPI_getvalue(tuple, tupdesc, 1);
			elog(DEBUG1, "ol_amount = %s", ol_amount);
		} else {
			SPI_finish();
			PG_RETURN_INT32(-1);
		}

		args[0] = DirectFunctionCall3(numeric_in, CStringGetDatum(ol_amount),
				ObjectIdGetDatum(InvalidOid),
				Int32GetDatum(((24 << 16) | 12) + VARHDRSZ));
		args[1] = Int32GetDatum(o_c_id);
		args[2] = Int32GetDatum(w_id);
		args[3] = Int32GetDatum(d_id);
		ret = SPI_execute_plan(DELIVERY_7, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			SPI_finish();
			PG_RETURN_INT32(-1);
		}
	}

	SPI_finish();
	PG_RETURN_INT32(1);
}
