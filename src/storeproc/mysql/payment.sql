/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2003 Mark Wong & Open Source Development Lab, Inc.
 * Copyright (C) 2004 Alexey Stroganov & MySQL AB.
 *
 * Based on TPC-C Standard Specification Revision 5.0 Clause 2.5.2.
 * July 10, 2002
 *     Not selecting n/2 for customer search by c_last.
 * July 12, 2002
 *     Not using c_d_id and c_w_id when searching for customers by last name
 *     since there are cases with 1 warehouse where no customers are found.
 * August 13, 2002
 *     Not appending c_data to c_data when credit is bad.
 */

drop procedure if exists payment;

delimiter |

CREATE PROCEDURE payment(in_w_id INT, in_d_id INT, in_c_id INT, in_c_w_id INT, in_c_d_id INT,
                         in_c_last VARCHAR(16), in_h_amount INT)
BEGIN

DECLARE  out_w_name VARCHAR(10);
DECLARE  out_w_street_1 VARCHAR(20);
DECLARE  out_w_street_2 VARCHAR(20);
DECLARE  out_w_city VARCHAR(10);
DECLARE  out_w_state VARCHAR(2);
DECLARE  out_w_zip VARCHAR(9);

DECLARE  out_d_name VARCHAR(10);
DECLARE  out_d_street_1 VARCHAR(20);
DECLARE  out_d_street_2 VARCHAR(20);
DECLARE  out_d_city VARCHAR(20);
DECLARE  out_d_state VARCHAR(2);
DECLARE  out_d_zip VARCHAR(9);

DECLARE  out_c_id INTEGER;
DECLARE  out_c_first VARCHAR(16);
DECLARE  out_c_middle VARCHAR(2);
DECLARE  out_c_last VARCHAR(20);
DECLARE  out_c_street_1 VARCHAR(20);
DECLARE  out_c_street_2 VARCHAR(20);
DECLARE  out_c_city VARCHAR(20);
DECLARE  out_c_state VARCHAR(2);
DECLARE  out_c_zip VARCHAR(9);
DECLARE  out_c_phone VARCHAR(16);
DECLARE  out_c_since VARCHAR(28);
DECLARE  out_c_credit VARCHAR(2);
DECLARE  out_c_credit_lim FIXED(24, 12);
DECLARE  out_c_discount REAL;
DECLARE  out_c_balance NUMERIC;
DECLARE  out_c_data VARCHAR(500);
DECLARE  out_c_ytd_payment INTEGER;


#        /* Goofy temporaty variables. */
DECLARE  tmp_c_id VARCHAR(30);
DECLARE  tmp_c_d_id VARCHAR(30);
DECLARE  tmp_c_w_id VARCHAR(30);
DECLARE  tmp_d_id VARCHAR(30);
DECLARE  tmp_w_id VARCHAR(30);
DECLARE  tmp_h_amount VARCHAR(30);

#        /* This one is not goofy. */
DECLARE  tmp_h_data VARCHAR(30);


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

#        /*
#         * Pick a customer by searching for c_last, should pick the one in the
#         * middle, not the first one.
#         */
        IF in_c_id = 0 THEN
                SELECT c_id
                INTO out_c_id
                FROM customer
                WHERE c_w_id = in_c_w_id
                  AND c_d_id = in_c_d_id
                  AND c_last = in_c_last
                ORDER BY c_first ASC limit 1;
        ELSE
                SET out_c_id = in_c_id;
        END IF;

        SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city,
               c_state, c_zip, c_phone, c_since, c_credit,
               c_credit_lim, c_discount, c_balance, c_data,
               c_ytd_payment
        INTO out_c_first, out_c_middle, out_c_last, out_c_street_1,
             out_c_street_2, out_c_city, out_c_state, out_c_zip, out_c_phone,
             out_c_since, out_c_credit, out_c_credit_lim, out_c_discount,
             out_c_balance, out_c_data, out_c_ytd_payment
        FROM customer
        WHERE c_w_id = in_c_w_id
          AND c_d_id = in_c_d_id
          AND c_id = out_c_id;

#        /* Check credit rating. */
        IF out_c_credit = 'BC' THEN
                SELECT out_c_id
                INTO tmp_c_id;
                SELECT in_c_d_id 
                INTO tmp_c_d_id;
                SELECT in_c_w_id
                INTO tmp_c_w_id;
                SELECT in_d_id
                INTO tmp_d_id;
                SELECT in_w_id
                INTO tmp_w_id;

                SET out_c_data = concat(tmp_c_id,' ',tmp_c_d_id,' ',tmp_c_w_id,' ',tmp_d_id,' ',tmp_w_id);

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

        SET tmp_h_data = concat(out_w_name,' ', out_d_name);
        INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id,
                             h_date, h_amount, h_data)
        VALUES (out_c_id, in_c_d_id, in_c_w_id, in_d_id, in_w_id,
                current_timestamp, in_h_amount, tmp_h_data);

#        RETURN out_c_id;
END|

delimiter ;





