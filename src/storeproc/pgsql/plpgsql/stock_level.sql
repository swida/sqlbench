/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2003 Mark Wong & Open Source Development Lab, Inc.
 *
 * Based on TPC-C Standard Specification Revision 5.0 Clause 2.8.2.
 */
CREATE OR REPLACE FUNCTION stock_level (INTEGER, INTEGER, INTEGER) RETURNS INTEGER AS '
DECLARE
	in_w_id ALIAS FOR $1;
	in_d_id ALIAS FOR $2;
	in_threshold ALIAS FOR $3;

	tmp_d_next_o_id INTEGER;

	low_stock INTEGER;
BEGIN
	SELECT d_next_o_id
	INTO tmp_d_next_o_id
	FROM district
	WHERE d_w_id = in_w_id
	AND d_id = in_d_id;

	SELECT COUNT(DISTINCT s_i_id)
	INTO low_stock
	FROM order_line, stock
	WHERE ol_w_id = in_w_id
	  AND ol_d_id= in_d_id
	  AND ol_o_id < (tmp_d_next_o_id - 1)
	  AND ol_o_id >= (tmp_d_next_o_id - 20)
	  AND s_w_id = in_w_id
	  AND s_i_id = ol_i_id
	  AND s_quantity < in_threshold;

	RETURN low_stock;
END;
' LANGUAGE 'plpgsql';
