#include "extended_common.h"
#include "extended_delivery.h"
#include "extended_new_order.h"
#include "extended_order_status.h"
#include "extended_payment.h"
#include "extended_stock_level.h"
#include "extended_integrity.h"

struct sqlapi_operation_t extended_sqlapi_operation =
{
	extended_execute_integrity,
	extended_execute_delivery,
	extended_execute_new_order,
	extended_execute_order_status,
	extended_execute_payment,
	extended_execute_stock_level
};
