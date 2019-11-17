/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2003-2008 Mark Wong & Open Source Development Labs, Inc.
 *
 * Based on TPC-C Standard Specification Revision 5.0 Clause 2.8.2.
 */

#ifndef _DBT2COMMON_H_
#define _DBT2COMMON_H_

/* PostgreSQL < 8.4 didn't have this handy macro */
#ifndef CStringGetTextDatum
#define CStringGetTextDatum(s) DirectFunctionCall1(textin, PointerGetDatum(s))
#endif

/*
 * cached_statement encapsulates all we need to know about a query to prepare
 * it using SPI_prepare().
 */
typedef struct
{
	const char *sql;		/* statement text */
	int		nargs;			/* number of arguments in the query */
	Oid		argtypes[10];	/* argument types */
	SPIPlanPtr plan;		/* plan_queries() stores the prepared plan here */
} cached_statement;

/*
 * Prepares all plans in the 'statements' array using SPI_prepare(). The
 * 'statements' array must be terminated by an entry with NULL sql text.
 *
 * The plans are marked permanent using SPI_keepplan(), so that they don't
 * go away at the end of transaction, and saved in the 'plan' field of each
 * statement struct.
 */
static void plan_queries(cached_statement *statements)
{
	cached_statement *s;

	/* On first invocation, plan all the queries */
	for (s = statements; s->sql != NULL; s++)
	{
		if (s->plan == NULL)
		{
			SPIPlanPtr plan = SPI_prepare(s->sql, s->nargs, s->argtypes);
			if (plan == NULL)
				elog(ERROR, "failed to plan query: %s", s->sql);
			s->plan = SPI_saveplan(plan);
			SPI_freeplan(plan);
		}
	}
}

#endif /* _DBT2COMMON_H_ */
