/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2003 Mark Wong & Open Source Development Lab, Inc.
 * Copyright (C) 2004 Alexey Stroganov & MySQL AB.
 *
 * Based on TPC-C Standard Specification Revision 5.0 Clause 2.6.2.
 */


drop procedure if exists order_status;

delimiter |


CREATE PROCEDURE order_status (in_c_id INT,
         	               in_c_w_id INT,
	                       in_c_d_id INT,
	                       in_c_last TEXT)
BEGIN
DECLARE out_c_first TEXT;
DECLARE out_c_middle char(2);
DECLARE out_c_balance NUMERIC;
DECLARE out_o_id INT;
DECLARE out_o_carrier_id INT;
DECLARE out_o_entry_d VARCHAR(28);
DECLARE out_o_ol_cnt INT;
DECLARE out_ol_supply_w_id1 INT; 
DECLARE out_ol_i_id1 INT;
DECLARE out_ol_quantity1 INT; 
DECLARE out_ol_amount1 NUMERIC;
DECLARE out_ol_delivery_d1 VARCHAR(28);
DECLARE out_ol_supply_w_id2 INT;
DECLARE out_ol_i_id2 INT;
DECLARE out_ol_quantity2 INT;
DECLARE out_ol_amount2 NUMERIC;
DECLARE out_ol_delivery_d2 VARCHAR(28);
DECLARE out_ol_supply_w_id3 INT;
DECLARE out_ol_i_id3 INT;
DECLARE out_ol_quantity3 INT;
DECLARE out_ol_amount3 NUMERIC;
DECLARE out_ol_delivery_d3 VARCHAR(28);
DECLARE out_ol_supply_w_id4 INT;
DECLARE out_ol_i_id4 INT;
DECLARE out_ol_quantity4 INT; 
DECLARE out_ol_amount4 NUMERIC;
DECLARE out_ol_delivery_d4 VARCHAR(28);
DECLARE out_ol_supply_w_id5 INT; 
DECLARE out_ol_i_id5 INT;
DECLARE out_ol_quantity5 INT; 
DECLARE out_ol_amount5 NUMERIC;
DECLARE out_ol_delivery_d5 VARCHAR(28);
DECLARE out_ol_supply_w_id6 INT; 
DECLARE out_ol_i_id6 INT;
DECLARE out_ol_quantity6 INT; 
DECLARE out_ol_amount6 NUMERIC;
DECLARE out_ol_delivery_d6 VARCHAR(28);
DECLARE out_ol_supply_w_id7 INT; 
DECLARE out_ol_i_id7 INT;
DECLARE out_ol_quantity7 INT; 
DECLARE out_ol_amount7 NUMERIC;
DECLARE out_ol_delivery_d7 VARCHAR(28);
DECLARE out_ol_supply_w_id8 INT; 
DECLARE out_ol_i_id8 INT;
DECLARE out_ol_quantity8 INT; 
DECLARE out_ol_amount8 NUMERIC;
DECLARE out_ol_delivery_d8 VARCHAR(28);
DECLARE out_ol_supply_w_id9 INT; 
DECLARE out_ol_i_id9 INT;
DECLARE out_ol_quantity9 INT; 
DECLARE out_ol_amount9 NUMERIC;
DECLARE out_ol_delivery_d9 VARCHAR(28);
DECLARE out_ol_supply_w_id10 INT; 
DECLARE out_ol_i_id10 INT;
DECLARE out_ol_quantity10 INT; 
DECLARE out_ol_amount10 NUMERIC;
DECLARE out_ol_delivery_d10 VARCHAR(28);
DECLARE out_ol_supply_w_id11 INT; 
DECLARE out_ol_i_id11 INT;
DECLARE out_ol_quantity11 INT; 
DECLARE out_ol_amount11 NUMERIC;
DECLARE out_ol_delivery_d11 VARCHAR(28);
DECLARE out_ol_supply_w_id12 INT; 
DECLARE out_ol_i_id12 INT;
DECLARE out_ol_quantity12 INT; 
DECLARE out_ol_amount12 NUMERIC;
DECLARE out_ol_delivery_d12 VARCHAR(28);
DECLARE out_ol_supply_w_id13 INT; 
DECLARE out_ol_i_id13 INT;
DECLARE out_ol_quantity13 INT; 
DECLARE out_ol_amount13 NUMERIC;
DECLARE out_ol_delivery_d13 VARCHAR(28);
DECLARE out_ol_supply_w_id14 INT; 
DECLARE out_ol_i_id14 INT;
DECLARE out_ol_quantity14 INT; 
DECLARE out_ol_amount14 NUMERIC;
DECLARE out_ol_delivery_d14 VARCHAR(28);
DECLARE out_ol_supply_w_id15 INT; 
DECLARE out_ol_i_id15 INT;
DECLARE out_ol_quantity15 INT; 
DECLARE out_ol_amount15 NUMERIC;
DECLARE out_ol_delivery_d15 VARCHAR(28);

	DECLARE out_c_id INT;
	DECLARE out_c_last VARCHAR(255);
        declare rc int default 0;

        declare c cursor for SELECT ol_i_id, ol_supply_w_id, ol_quantity, 
                                    ol_amount, ol_delivery_d
                             FROM order_line
                             WHERE ol_w_id = in_c_w_id
                               AND ol_d_id = in_c_d_id
                               AND ol_o_id = out_o_id;

        declare continue handler for sqlstate '02000' set rc = 1;

#	out_c_first VARCHAR(255);
#	out_c_middle VARCHAR(255);
#	out_c_balance NUMERIC;
#	out_o_id INT;
#	out_o_carrier_id INT;
#	out_o_entry_d VARCHAR(255);
#	out_o_ol_cnt INT;

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
		ORDER BY c_first ASC limit 1;
	ELSE
		set out_c_id = in_c_id;
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
	ORDER BY o_id DESC limit 1;

        open c;

        fetch_block:
        BEGIN
         fetch c into out_ol_i_id1, out_ol_supply_w_id1, out_ol_quantity1, 
                      out_ol_amount1, out_ol_delivery_d1;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id2, out_ol_supply_w_id2, out_ol_quantity2, 
                      out_ol_amount2, out_ol_delivery_d2;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id3, out_ol_supply_w_id3, out_ol_quantity3, 
                      out_ol_amount3, out_ol_delivery_d3;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id4, out_ol_supply_w_id4, out_ol_quantity4, 
                      out_ol_amount4, out_ol_delivery_d4;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id5, out_ol_supply_w_id5, out_ol_quantity5, 
                      out_ol_amount5, out_ol_delivery_d5;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id6, out_ol_supply_w_id6, out_ol_quantity6, 
                      out_ol_amount6, out_ol_delivery_d6;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id7, out_ol_supply_w_id7, out_ol_quantity7, 
                      out_ol_amount7, out_ol_delivery_d7;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id8, out_ol_supply_w_id8, out_ol_quantity8, 
                      out_ol_amount8, out_ol_delivery_d8;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id9, out_ol_supply_w_id9, out_ol_quantity9, 
                      out_ol_amount9, out_ol_delivery_d9;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id10, out_ol_supply_w_id10, out_ol_quantity10, 
                      out_ol_amount10, out_ol_delivery_d10;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id11, out_ol_supply_w_id11, out_ol_quantity11, 
                      out_ol_amount11, out_ol_delivery_d11;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id12, out_ol_supply_w_id12, out_ol_quantity12, 
                      out_ol_amount12, out_ol_delivery_d12;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id13, out_ol_supply_w_id13, out_ol_quantity13, 
                      out_ol_amount13, out_ol_delivery_d13;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id14, out_ol_supply_w_id14, out_ol_quantity14, 
                      out_ol_amount14, out_ol_delivery_d14;
         if rc then
            leave fetch_block;
         end if;
         fetch c into out_ol_i_id15, out_ol_supply_w_id15, out_ol_quantity15, 
                      out_ol_amount15, out_ol_delivery_d15;
       end fetch_block;

       close c;

END|

delimiter ;
