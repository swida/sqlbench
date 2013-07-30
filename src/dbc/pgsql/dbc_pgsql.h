#ifndef DBC_PGSQL_H
#define DBC_PGSQL_H
#include <libpq-fe.h>
#include "db.h"

struct pgsql_context_t
{
	struct db_context_t base;
	PGconn *conn;
	int inTransaction;
};

#endif
