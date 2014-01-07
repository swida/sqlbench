#ifndef DBC_PGSQL_H
#define DBC_PGSQL_H
#include <libkci.h>
#include "db.h"

struct kingbase_context_t
{
	struct db_context_t base;
	KCIConnection *conn;
	int inTransaction;
};

#endif
