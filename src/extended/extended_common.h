/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *  
 * Copyright (C) 2002 Mark Wong & Open Source Development Labs, Inc.
 *
 * 11 june 2002
 */

#ifndef _SIMPLE_COMMON_H_
#define _SIMPLE_COMMON_H_

#include "common.h"
#include "logging.h"
#include "transaction_data.h"
#include "dbc.h"

void dbt2_escape_str(char *orig_str, char *esc_str);
int dbt2_init_values(char ** values, int max_values);
int dbt2_init_params(char ** values, int max_values, int max_length);
int dbt2_free_params(char ** values, int max_values);
int dbt2_free_values(char ** values, int max_values);
#define N_DELIVERY_1 "DELIVERY_1"
#define DELIVERY_1 \
	"SELECT no_o_id\n" \
	"FROM new_order\n" \
	"WHERE no_w_id = $1\n" \
	"  AND no_d_id = $2"

#define N_DELIVERY_2 "DELIVERY_2"
#define DELIVERY_2 \
	"DELETE FROM new_order\n" \
	"WHERE no_o_id = $1\n" \
	"  AND no_w_id = $2\n" \
	"  AND no_d_id = $3"

#define N_DELIVERY_3 "DELIVERY_3"
#define DELIVERY_3 \
	"SELECT o_c_id\n" \
	"FROM orders\n" \
	"WHERE o_id = $1\n" \
	"  AND o_w_id = $2\n" \
	"  AND o_d_id = $3"

#define N_DELIVERY_4 "DELIVERY_4"
#define DELIVERY_4 \
	"UPDATE orders\n" \
	"SET o_carrier_id = $1\n" \
	"WHERE o_id = $2\n" \
	"  AND o_w_id = $3\n" \
	"  AND o_d_id = $4"

#define N_DELIVERY_5 "DELIVERY_5"
#define DELIVERY_5 \
	"UPDATE order_line\n" \
	"SET ol_delivery_d = current_timestamp\n" \
	"WHERE ol_o_id = $1\n" \
	"  AND ol_w_id = $2\n" \
	"  AND ol_d_id = $3"

#define N_DELIVERY_6 "DELIVERY_6"
#define DELIVERY_6 \
	"SELECT SUM(ol_amount * ol_quantity)\n" \
	"FROM order_line\n" \
	"WHERE ol_o_id = $1\n" \
	"  AND ol_w_id = $2\n" \
	"  AND ol_d_id = $3"

#define N_DELIVERY_7 "DELIVERY_7"
#define DELIVERY_7 \
	"UPDATE customer\n" \
	"SET c_delivery_cnt = c_delivery_cnt + 1,\n" \
	"    c_balance = c_balance + $1\n" \
	"WHERE c_id = $2\n" \
	"  AND c_w_id = $3\n" \
	"  AND c_d_id = $4"

#define N_NEW_ORDER_1 "NEW_ORDER_1"
#define NEW_ORDER_1 \
	"SELECT w_tax\n" \
	"FROM warehouse\n" \
	"WHERE w_id = $1"

#define N_NEW_ORDER_2 "NEW_ORDER_2"
#define NEW_ORDER_2 \
	"SELECT d_tax, d_next_o_id\n" \
	"FROM district \n" \
	"WHERE d_w_id = $1\n" \
	"  AND d_id = $2\n" \
	"FOR UPDATE"

#define N_NEW_ORDER_2_WITHOUT_FORUPDATE "NEW_ORDER_2_WITHOUT_FORUPDATE"
#define NEW_ORDER_2_WITHOUT_FORUPDATE \
   "SELECT d_tax, d_next_o_id\n" \
   "FROM district \n" \
   "WHERE d_w_id = $1\n" \
   "  AND d_id = $2\n"

#define N_NEW_ORDER_3 "NEW_ORDER_3"
#define NEW_ORDER_3 \
	"UPDATE district\n" \
	"SET d_next_o_id = d_next_o_id + 1\n" \
	"WHERE d_w_id = $1\n" \
	"  AND d_id = $2"

#define N_NEW_ORDER_4 "NEW_ORDER_4"
#define NEW_ORDER_4 \
	"SELECT c_discount, c_last, c_credit\n" \
	"FROM customer\n" \
	"WHERE c_w_id = $1\n" \
	"  AND c_d_id = $2\n" \
	"  AND c_id = $3"

#define N_NEW_ORDER_5 "NEW_ORDER_5"
#define NEW_ORDER_5 \
	"INSERT INTO new_order (no_o_id, no_w_id, no_d_id)\n" \
        "VALUES ($1, $2, $3)"

#define N_NEW_ORDER_6 "NEW_ORDER_6"
#define NEW_ORDER_6 \
	"INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d,\n" \
	"                    o_carrier_id, o_ol_cnt, o_all_local)\n" \
	"VALUES ($1, $2, $3, $4, current_timestamp, NULL, $5, $6)"

#define N_NEW_ORDER_7 "NEW_ORDER_7"
#define NEW_ORDER_7 \
	"SELECT i_price, i_name, i_data\n" \
	"FROM item\n" \
	"WHERE i_id = $1"

#define N_NEW_ORDER_8 "NEW_ORDER_8"
#define NEW_ORDER_8 \
	"SELECT s_quantity, %s, s_data\n" \
	"FROM stock\n" \
	"WHERE s_i_id = $1\n" \
	"  AND s_w_id = $2"

#define N_NEW_ORDER_9 "NEW_ORDER_9"
#define NEW_ORDER_9 \
	"UPDATE stock\n" \
	"SET s_quantity = s_quantity - $1\n" \
	"WHERE s_i_id = $2\n" \
	"  AND s_w_id = $3"

#define N_NEW_ORDER_10 "NEW_ORDER_10"
#define NEW_ORDER_10 \
	"INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number,\n" \
	"                        ol_i_id, ol_supply_w_id, ol_delivery_d,\n" \
	"                        ol_quantity, ol_amount, ol_dist_info)\n" \
	"VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8, $9)"

#define N_ORDER_STATUS_1 "ORDER_STATUS_1"
#define ORDER_STATUS_1 \
	"SELECT c_id\n" \
	"FROM customer\n" \
	"WHERE c_w_id = $1\n" \
	"  AND c_d_id = $2\n" \
	"  AND c_last = $3\n" \
	"ORDER BY c_first ASC"

#define N_ORDER_STATUS_2 "ORDER_STATUS_2"
#define ORDER_STATUS_2 \
	"SELECT c_first, c_middle, c_last, c_balance\n" \
	"FROM customer\n" \
	"WHERE c_w_id = $1\n" \
	"  AND c_d_id = $2\n" \
	"  AND c_id = $3"

#define N_ORDER_STATUS_3 "ORDER_STATUS_3"
#define ORDER_STATUS_3 \
	"SELECT o_id, o_carrier_id, o_entry_d, o_ol_cnt\n" \
	"FROM orders\n" \
	"WHERE o_w_id = $1\n" \
	"  AND o_d_id = $2\n" \
	"  AND o_c_id = $3\n" \
	"ORDER BY o_id DESC"

#define N_ORDER_STATUS_4 "ORDER_STATUS_4"
#define ORDER_STATUS_4 \
	"SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount,\n" \
	"       ol_delivery_d\n" \
	"FROM order_line\n" \
	"WHERE ol_w_id = $1\n" \
	"  AND ol_d_id = $2\n" \
	"  AND ol_o_id = $3"

#define N_PAYMENT_1 "PAYMENT_1"
#define PAYMENT_1 \
	"SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip\n" \
	"FROM warehouse\n" \
	"WHERE w_id = $1"

#define N_PAYMENT_2 "PAYMENT_2"
#define PAYMENT_2 \
	"UPDATE warehouse\n" \
	"SET w_ytd = w_ytd + $1\n" \
	"WHERE w_id = $2"

#define N_PAYMENT_3 "PAYMENT_3"
#define PAYMENT_3 \
	"SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip\n" \
	"FROM district\n" \
	"WHERE d_id = $1\n" \
	"  AND d_w_id = $2"

#define N_PAYMENT_4 "PAYMENT_4"
#define PAYMENT_4 \
	"UPDATE district\n" \
	"SET d_ytd = d_ytd + $1\n" \
	"WHERE d_id = $2\n" \
	"  AND d_w_id = $3"

#define N_PAYMENT_5 "PAYMENT_5"
#define PAYMENT_5 \
	"SELECT c_id\n" \
	"FROM customer\n" \
	"WHERE c_w_id = $1\n" \
	"  AND c_d_id = $2\n" \
	"  AND c_last = $3\n" \
	"ORDER BY c_first ASC"

#define N_PAYMENT_6 "PAYMENT_6"
#define PAYMENT_6 \
	"SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city,\n" \
	"       c_state, c_zip, c_phone, c_since, c_credit,\n" \
	"       c_credit_lim, c_discount, c_balance, c_data, c_ytd_payment\n" \
	"FROM customer\n" \
	"WHERE c_w_id = $1\n" \
	"  AND c_d_id = $2\n" \
	"  AND c_id = $3"

#define N_PAYMENT_7_GC "PAYMENT_7_GC"
#define PAYMENT_7_GC \
	"UPDATE customer\n" \
	"SET c_balance = c_balance - $1,\n" \
	"    c_ytd_payment = c_ytd_payment + 1\n" \
	"WHERE c_id = $2\n" \
	"  AND c_w_id = $3\n" \
	"  AND c_d_id = $4"

#define N_PAYMENT_7_BC "PAYMENT_7_BC"
#define PAYMENT_7_BC \
	"UPDATE customer\n" \
	"SET c_balance = c_balance - $1,\n" \
	"    c_ytd_payment = c_ytd_payment + 1,\n" \
	"    c_data = $2\n" \
	"WHERE c_id = $3\n" \
	"  AND c_w_id = $4\n" \
	"  AND c_d_id = $5"

#define N_PAYMENT_8 "PAYMENT_8"
#define PAYMENT_8 \
	"INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id,\n" \
	"                     h_date, h_amount, h_data)\n" \
	"VALUES ($1, $2, $3, $4, $5, current_timestamp, $6, $7)"

#define N_STOCK_LEVEL_1 "STOCK_LEVEL_1"
#define STOCK_LEVEL_1 \
	"SELECT d_next_o_id\n" \
	"FROM district\n" \
	"WHERE d_w_id = $1\n" \
	"AND d_id = $2"

#define N_STOCK_LEVEL_2 "STOCK_LEVEL_2"
#define STOCK_LEVEL_2 \
	"SELECT COUNT(DISTINCT s_i_id)\n" \
	"FROM order_line, stock\n" \
	"WHERE ol_w_id = $1\n" \
	"  AND ol_d_id= $2\n" \
	"  AND ol_o_id < $3\n" \
	"  AND ol_o_id >= $4\n" \
	"  AND s_w_id = $1\n" \
	"  AND s_i_id = ol_i_id\n" \
	"  AND s_quantity < $5"

#define N_INTEGRITY_1 "INTEGRITY_1"
#define INTEGRITY_1 \
	"SELECT count(*)\n" \
	"FROM warehouse\n" 

#endif /* _SIMPLE_COMMON_H_ */
