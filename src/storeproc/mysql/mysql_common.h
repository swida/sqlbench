#ifndef MYSQL_COMMON_H
#define MYSQL_COMMON_H
#include "transaction_data.h"
#include "db.h"
#include "dbc.h"

extern int dbt2_init_values(char ** values, int max_values);
extern int dbt2_free_values(char ** values, int max_values);


extern int mysql_sp_integrity(struct db_context_t *dbc, struct integrity_t *data);
extern int mysql_sp_delivery(struct db_context_t *dbc, struct delivery_t *data);
extern int mysql_sp_new_order(struct db_context_t *dbc, struct new_order_t *data);
extern int mysql_sp_order_status(struct db_context_t *dbc, struct order_status_t *data);
extern int mysql_sp_stock_level(struct db_context_t *dbc, struct stock_level_t *data);
extern int mysql_sp_payment(struct db_context_t *dbc, struct payment_t *data);
#endif
