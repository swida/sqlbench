/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2003 Mark Wong & Open Source Development Lab, Inc.
 *
 * Based on TPC-C Standard Specification Revision 5.0 Clause 2.6.2.
 */
CREATE OR REPLACE FUNCTION order_status (INTEGER, INTEGER, INTEGER, TEXT) RETURNS SETOF record AS '
DECLARE
	in_c_id ALIAS FOR $1;
	in_c_w_id ALIAS FOR $2;
	in_c_d_id ALIAS FOR $3;
	in_c_last ALIAS FOR $4;

	out_c_id INTEGER;
	out_c_first VARCHAR;
	out_c_middle VARCHAR;
	out_c_last VARCHAR;
	out_c_balance NUMERIC;

	out_o_id INTEGER;
	out_o_carrier_id INTEGER;
	out_o_entry_d VARCHAR;
	out_o_ol_cnt INTEGER;

	ol RECORD;
BEGIN
	/*
	 * Pick a customer by searching for c_last, should pick the one in the
	 * middle, not the first one.
	 */
	IF in_c_id = 0 THEN
		SELECT c_id
		INTO out_c_id 
		FROM customer
		WHERE c_w_id = in_c_w_id
		  AND c_d_id = in_c_d_id
		  AND c_last = in_c_last
		ORDER BY c_first ASC;
	ELSE
		out_c_id = in_c_id;
	END IF;

	SELECT c_first, c_middle, c_last, c_balance
	INTO out_c_first, out_c_middle, out_c_last, out_c_balance
	FROM customer
	WHERE c_w_id = in_c_w_id   
	  AND c_d_id = in_c_d_id
	  AND c_id = out_c_id;

	SELECT o_id, o_carrier_id, o_entry_d, o_ol_cnt
	INTO out_o_id, out_o_carrier_id, out_o_entry_d, out_o_ol_cnt
	FROM orders
	WHERE o_w_id = in_c_w_id
  	AND o_d_id = in_c_d_id
  	AND o_c_id = out_c_id
	ORDER BY o_id DESC;

	FOR ol IN
		SELECT out_c_id, out_c_first, out_c_middle, out_c_last,
		       out_c_balance, out_o_id, out_o_carrier_id,
		       out_o_entry_d, out_o_ol_cnt, ol_i_id, ol_supply_w_id,
		       ol_quantity, ol_amount, ol_delivery_d
		FROM order_line
		WHERE ol_w_id = in_c_w_id  
		  AND ol_d_id = in_c_d_id
		  AND ol_o_id = out_o_id
	LOOP
		RETURN NEXT ol;
	END LOOP;

	RETURN null;
END;
' LANGUAGE 'plpgsql';
