/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2003 Mark Wong & Open Source Development Lab, Inc.
 *
 * Based on TPC-C Standard Specification Revision 5.0 Clause 2.8.2.
 */
CREATE OR REPLACE FUNCTION new_order_2 (INTEGER, INTEGER, INTEGER, INTEGER, NUMERIC, TEXT, TEXT, INTEGER, NUMERIC, INTEGER, INTEGER) RETURNS INTEGER AS '
DECLARE
	in_w_id ALIAS FOR $1;
	in_d_id ALIAS FOR $2;
	in_ol_i_id ALIAS FOR $3;
	in_ol_quantity ALIAS FOR $4;
	in_i_price ALIAS FOR $5;
	in_i_name ALIAS FOR $6;
	in_i_data ALIAS FOR $7;
	in_ol_o_id ALIAS FOR $8;
	in_ol_amount ALIAS FOR $9;
	in_ol_supply_w_id ALIAS FOR $10;
	in_ol_number ALIAS FOR $11;

	out_s_quantity INTEGER;

	tmp_s_dist VARCHAR;
	tmp_s_data VARCHAR;
BEGIN
	IF in_d_id = 1 THEN
		SELECT s_quantity, s_dist_01, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSIF in_d_id = 2 THEN
		SELECT s_quantity, s_dist_02, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSIF in_d_id = 3 THEN
		SELECT s_quantity, s_dist_03, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSIF in_d_id = 4 THEN
		SELECT s_quantity, s_dist_04, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSIF in_d_id = 5 THEN
		SELECT s_quantity, s_dist_05, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSIF in_d_id = 6 THEN
		SELECT s_quantity, s_dist_06, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSIF in_d_id = 7 THEN
		SELECT s_quantity, s_dist_07, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSIF in_d_id = 8 THEN
		SELECT s_quantity, s_dist_08, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSIF in_d_id = 9 THEN
		SELECT s_quantity, s_dist_09, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSIF in_d_id = 10 THEN
		SELECT s_quantity, s_dist_10, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	END IF;

	IF out_s_quantity > in_ol_quantity + 10 THEN
		UPDATE stock
		SET s_quantity = s_quantity - in_ol_quantity
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSE
		UPDATE stock
		SET s_quantity = s_quantity - in_ol_quantity + 91
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	END IF;

	INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id,
	                        ol_supply_w_id, ol_delivery_d, ol_quantity,
                                ol_amount, ol_dist_info)
	VALUES (in_ol_o_id, in_d_id, in_w_id, in_ol_number, in_ol_i_id,
	        in_ol_supply_w_id, NULL, in_ol_quantity, in_ol_amount,
	        tmp_s_dist);

	RETURN out_s_quantity;
END;
' LANGUAGE 'plpgsql';
