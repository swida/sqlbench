/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2003 Mark Wong & Open Source Development Lab, Inc.
 *
 * Based on TPC-C Standard Specification Revision 5.0 Clause 2.8.2.
 */
CREATE OR REPLACE FUNCTION delivery (INTEGER, INTEGER) RETURNS INTEGER AS '
DECLARE
	in_w_id ALIAS FOR $1;
	in_o_carrier_id ALIAS FOR $2;

	out_c_id INTEGER;
	out_ol_amount NUMERIC;

	tmp_d_id INTEGER;
	tmp_o_id INTEGER;

	i INTEGER;
BEGIN
	FOR tmp_d_id IN 1..10 LOOP
		SELECT no_o_id
		INTO tmp_o_id
		FROM new_order
		WHERE no_w_id = in_w_id
		  AND no_d_id = tmp_d_id
		ORDER BY no_o_id
		LIMIT 1;

		IF tmp_o_id > 0 THEN
			DELETE FROM new_order
			WHERE no_o_id = tmp_o_id
			  AND no_w_id = in_w_id
			  AND no_d_id = tmp_d_id;

			SELECT o_c_id
			INTO out_c_id
			FROM orders
			WHERE o_id = tmp_o_id
			  AND o_w_id = in_w_id
			  AND o_d_id = tmp_d_id;

			UPDATE orders
			SET o_carrier_id = in_o_carrier_id
			WHERE o_id = tmp_o_id
			  AND o_w_id = in_w_id
			  AND o_d_id = tmp_d_id;

			UPDATE order_line
			SET ol_delivery_d = current_timestamp
			WHERE ol_o_id = tmp_o_id
			  AND ol_w_id = in_w_id
			  AND ol_d_id = tmp_d_id;

			SELECT SUM(ol_amount * ol_quantity)
			INTO out_ol_amount
			FROM order_line
			WHERE ol_o_id = tmp_o_id
			  AND ol_w_id = in_w_id
			  AND ol_d_id = tmp_d_id;

			UPDATE customer
			SET c_delivery_cnt = c_delivery_cnt + 1,
			    c_balance = c_balance + out_ol_amount
			WHERE c_id = out_c_id
			  AND c_w_id = in_w_id
			  AND c_d_id = tmp_d_id;
		END IF;
	END LOOP;
	RETURN 1;
END;
' LANGUAGE 'plpgsql';
