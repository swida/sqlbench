/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2003 Mark Wong & Open Source Development Lab, Inc.
 *
 * Based on TPC-C Standard Specification Revision 5.0 Clause 2.8.2.
 */
CREATE OR REPLACE FUNCTION payment (INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, TEXT, NUMERIC) RETURNS INTEGER AS '
DECLARE
	in_w_id ALIAS FOR $1;
	in_d_id ALIAS FOR $2;
	in_c_id ALIAS FOR $3;
	in_c_w_id ALIAS FOR $4;
	in_c_d_id ALIAS FOR $5;
	in_c_last ALIAS FOR $6;
	in_h_amount ALIAS FOR $7;

	/* Look at all these output variables, and no way to return them. */
	out_w_name VARCHAR;
	out_w_street_1 VARCHAR; 
	out_w_street_2 VARCHAR;
	out_w_city VARCHAR;
	out_w_state VARCHAR;
	out_w_zip VARCHAR; 

	out_d_name VARCHAR;
	out_d_street_1 VARCHAR;
	out_d_street_2 VARCHAR;
	out_d_city VARCHAR;
	out_d_state VARCHAR;
	out_d_zip VARCHAR;

	out_c_id INTEGER;
	out_c_first VARCHAR;
	out_c_middle VARCHAR;
	out_c_last VARCHAR;
	out_c_street_1 VARCHAR;
	out_c_street_2 VARCHAR;
	out_c_city VARCHAR;
	out_c_state VARCHAR;
	out_c_zip VARCHAR;
	out_c_phone VARCHAR;
	out_c_since VARCHAR;
	out_c_credit VARCHAR;
	out_c_credit_lim VARCHAR;
	out_c_discount REAL;
	out_c_balance NUMERIC;
	out_c_data VARCHAR;
	out_c_ytd_payment INTEGER;

	/* Goofy temporaty variables. */
	tmp_c_id VARCHAR;
	tmp_c_d_id VARCHAR;
	tmp_c_w_id VARCHAR;
	tmp_d_id VARCHAR;
	tmp_w_id VARCHAR;
	tmp_h_amount VARCHAR;

	/* This one is not goofy. */
	tmp_h_data VARCHAR;
BEGIN
	SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip
	INTO out_w_name, out_w_street_1, out_w_street_2, out_w_city,
	     out_w_state, out_w_zip
	FROM warehouse
	WHERE w_id = in_w_id;

	UPDATE warehouse
	SET w_ytd = w_ytd + in_h_amount
	WHERE w_id = in_w_id;

	SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip
	INTO out_d_name, out_d_street_1, out_d_street_2, out_d_city,
	     out_d_state, out_d_zip
	FROM district
	WHERE d_id = in_d_id
	  AND d_w_id = in_w_id;

	UPDATE district
	SET d_ytd = d_ytd + in_h_amount
	WHERE d_id = in_d_id
	  AND d_w_id = in_w_id;

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

	SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city,
	       c_state, c_zip, c_phone, c_since, c_credit,
               c_credit_lim, c_discount, c_balance, c_data,
	       cast(c_ytd_payment AS INTEGER)
	INTO out_c_first, out_c_middle, out_c_last, out_c_street_1,
	     out_c_street_2, out_c_city, out_c_state, out_c_zip, out_c_phone,
	     out_c_since, out_c_credit, out_c_credit_lim, out_c_discount,
	     out_c_balance, out_c_data, out_c_ytd_payment
	FROM customer
	WHERE c_w_id = in_c_w_id
	  AND c_d_id = in_c_d_id
	  AND c_id = out_c_id;

	/* Check credit rating. */
	IF out_c_credit = ''BC'' THEN
		SELECT cast(out_c_id AS VARCHAR)
		INTO tmp_c_id;
		SELECT cast(in_c_d_id AS VARCHAR)
		INTO tmp_c_d_id;
		SELECT cast(in_c_w_id AS VARCHAR)
		INTO tmp_c_w_id;
		SELECT cast(in_d_id AS VARCHAR)
		INTO tmp_d_id;
		SELECT cast(in_w_id AS VARCHAR)
		INTO tmp_w_id;
		/* Boo hoo, cannot convert REAL to VARCHAR. */
/*
		SELECT cast(in_h_amount AS VARCHAR)
		INTO tmp_h_amount;
		out_c_data = tmp_c_id || '' '' || tmp_c_d_id || '' '' || tmp_c_w_id || '' '' || tmp_d_id || '' '' || tmp_w_id || '' '' || tmp_h_amount || '' '' || out_c_data;
*/
		out_c_data = tmp_c_id || '' '' || tmp_c_d_id || '' '' || tmp_c_w_id || '' '' || tmp_d_id || '' '' || tmp_w_id || '' '' || out_c_data;

		UPDATE customer
		SET c_balance = out_c_balance - in_h_amount,
		    c_ytd_payment = out_c_ytd_payment + 1,
		    c_data = out_c_data
		WHERE c_id = out_c_id
		  AND c_w_id = in_c_w_id
		  AND c_d_id = in_c_d_id;
	ELSE
		UPDATE customer
		SET c_balance = out_c_balance - in_h_amount,
		    c_ytd_payment = out_c_ytd_payment + 1
		WHERE c_id = out_c_id
		  AND c_w_id = in_c_w_id
		  AND c_d_id = in_c_d_id;
	END IF;

	tmp_h_data = out_w_name || ''    '' || out_d_name;
	INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id,
	                     h_date, h_amount, h_data)
	VALUES (out_c_id, in_c_d_id, in_c_w_id, in_d_id, in_w_id,
		current_timestamp, in_h_amount, tmp_h_data);

	RETURN out_c_id;
END;
' LANGUAGE 'plpgsql';
