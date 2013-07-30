#include "dbc.h"
#include "pgsql_common.h"

struct dbc_storeproc_operation_t pgsql_storeproc_operation =
{
	pgsql_sp_integrity,
	pgsql_sp_delivery,
	pgsql_sp_new_order,
	pgsql_sp_order_status,
	pgsql_sp_payment,
	pgsql_sp_stock_level
};
