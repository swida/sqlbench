#include "dbc.h"
#include "mysql_common.h"

struct dbc_storeproc_operation_t mysql_storeproc_operation =
{
	mysql_sp_integrity,
	mysql_sp_delivery,
	mysql_sp_new_order,
	mysql_sp_order_status,
	mysql_sp_payment,
	mysql_sp_stock_level
};
