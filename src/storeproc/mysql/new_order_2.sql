/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2003 Mark Wong & Open Source Development Lab, Inc.
 * Copyright (C) 2004 Alexey Stroganov & MySQL AB.
 *
 * Based on TPC-C Standard Specification Revision 5.0 Clause 2.8.2.
 */

drop procedure if exists new_order_2;

delimiter |


CREATE PROCEDURE new_order_2 (in_w_id INT,
	                      in_d_id INT,
	                      in_ol_i_id INT,
	                      in_ol_quantity INT,
	                      in_i_price NUMERIC,
	                      in_i_name TEXT,
	                      in_i_data TEXT,
	                      in_ol_o_id INT,
	                      in_ol_amount NUMERIC,
	                      in_ol_supply_w_id INT,
	                      in_ol_number INT,
                              out out_s_quantity INT)

BEGIN

DECLARE	tmp_s_dist VARCHAR(255);
DECLARE	tmp_s_data VARCHAR(255);

	IF in_d_id = 1 THEN
		SELECT s_quantity, s_dist_01, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSEIF in_d_id = 2 THEN
		SELECT s_quantity, s_dist_02, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSEIF in_d_id = 3 THEN
		SELECT s_quantity, s_dist_03, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSEIF in_d_id = 4 THEN
		SELECT s_quantity, s_dist_04, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSEIF in_d_id = 5 THEN
		SELECT s_quantity, s_dist_05, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSEIF in_d_id = 6 THEN
		SELECT s_quantity, s_dist_06, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSEIF in_d_id = 7 THEN
		SELECT s_quantity, s_dist_07, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSEIF in_d_id = 8 THEN
		SELECT s_quantity, s_dist_08, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSEIF in_d_id = 9 THEN
		SELECT s_quantity, s_dist_09, s_data
		INTO out_s_quantity, tmp_s_dist, tmp_s_data
		FROM stock
		WHERE s_i_id = in_ol_i_id
		  AND s_w_id = in_w_id;
	ELSEIF in_d_id = 10 THEN
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
END|
delimiter ;

