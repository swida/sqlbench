/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2003 Mark Wong & Open Source Development Lab, Inc.
 * Copyright (C) 2004 Alexey Stroganov & MySQL AB.
 *
 * Based on TPC-C Standard Specification Revision 5.0 Clause 2.8.2.
 */
drop procedure if exists new_order;

delimiter |

CREATE PROCEDURE new_order(tmp_w_id INT,           
                           tmp_d_id INT,           
                           tmp_c_id INT,           
                           tmp_o_all_local INT,
                           tmp_o_ol_cnt INT,           
                           ol_i_id1 INT,           
                           ol_supply_w_id1 INT,           
                           ol_quantity1 INT,           
                           ol_i_id2 INT,           
                           ol_supply_w_id2 INT,           
                           ol_quantity2 INT,           
                           ol_i_id3 INT,           
                           ol_supply_w_id3 INT,           
                           ol_quantity3 INT,           
                           ol_i_id4 INT,           
                           ol_supply_w_id4 INT,           
                           ol_quantity4 INT,           
                           ol_i_id5 INT,           
                           ol_supply_w_id5 INT,           
                           ol_quantity5 INT,           
                           ol_i_id6 INT,           
                           ol_supply_w_id6 INT,           
                           ol_quantity6 INT,           
                           ol_i_id7 INT,           
                           ol_supply_w_id7 INT,           
                           ol_quantity7 INT,           
                           ol_i_id8 INT,           
                           ol_supply_w_id8 INT,           
                           ol_quantity8 INT,           
                           ol_i_id9 INT,           
                           ol_supply_w_id9 INT,           
                           ol_quantity9 INT,           
                           ol_i_id10 INT,           
                           ol_supply_w_id10 INT,           
                           ol_quantity10 INT,           
                           ol_i_id11 INT,           
                           ol_supply_w_id11 INT,           
                           ol_quantity11 INT,           
                           ol_i_id12 INT,           
                           ol_supply_w_id12 INT,           
                           ol_quantity12 INT,           
                           ol_i_id13 INT,           
                           ol_supply_w_id13 INT,           
                           ol_quantity13 INT,           
                           ol_i_id14 INT,           
                           ol_supply_w_id14 INT,           
                           ol_quantity14 INT,           
                           ol_i_id15 INT,           
                           ol_supply_w_id15 INT,           
                           ol_quantity15 INT,
                           out rc int)
                              
BEGIN

  DECLARE out_c_credit VARCHAR(255);
  DECLARE tmp_i_name VARCHAR(255);
  DECLARE tmp_i_data VARCHAR(255);
  DECLARE out_c_last VARCHAR(255);

  DECLARE tmp_ol_supply_w_id INT;
  DECLARE tmp_ol_quantity INT;
  DECLARE out_d_next_o_id INT;
  DECLARE tmp_i_id INT;

  DECLARE tmp_s_quantity INT;

  DECLARE out_w_tax REAL;
  DECLARE out_d_tax REAL;
  DECLARE out_c_discount REAL;
  DECLARE tmp_i_price REAL;
  DECLARE tmp_ol_amount REAL;
  DECLARE tmp_total_amount REAL;

  DECLARE o_id INT;

  declare exit handler for sqlstate '02000' set rc = 1;

  SET rc=0;

  SET o_id = 0;

  SELECT w_tax
  INTO out_w_tax
  FROM warehouse
  WHERE w_id = tmp_w_id;

  SELECT d_tax, d_next_o_id
  INTO out_d_tax, out_d_next_o_id
  FROM district   
  WHERE d_w_id = tmp_w_id
    AND d_id = tmp_d_id FOR UPDATE;

  SET o_id=out_d_next_o_id;

  UPDATE district
  SET d_next_o_id = d_next_o_id + 1
  WHERE d_w_id = tmp_w_id
    AND d_id = tmp_d_id;

  SELECT c_discount , c_last, c_credit
  INTO out_c_discount, out_c_last, out_c_credit
  FROM customer
  WHERE c_w_id = tmp_w_id
    AND c_d_id = tmp_d_id
    AND c_id = tmp_c_id;

  INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
  VALUES (out_d_next_o_id, tmp_d_id, tmp_w_id);

  INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d,
	                    o_carrier_id, o_ol_cnt, o_all_local)
  VALUES (out_d_next_o_id, tmp_d_id, tmp_w_id, tmp_c_id,
	        current_timestamp, NULL, tmp_o_ol_cnt, tmp_o_all_local);

  SET tmp_total_amount = 0;

    IF tmp_o_ol_cnt > 0 
    THEN

      SET tmp_i_id = ol_i_id1;
      SET tmp_ol_supply_w_id = ol_supply_w_id1;
      SET tmp_ol_quantity = ol_quantity1;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 
      THEN
	SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;

	call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
                    	 tmp_ol_quantity, tmp_i_price,
 			 tmp_i_name, tmp_i_data,
  			 out_d_next_o_id, tmp_ol_amount,
                       	 tmp_ol_supply_w_id, 1, tmp_s_quantity);

	SET tmp_total_amount = tmp_ol_amount;
      END IF;
    END IF;
    IF tmp_o_ol_cnt > 1 
    THEN
      SET tmp_i_id = ol_i_id2;
      SET tmp_ol_supply_w_id = ol_supply_w_id2;
      SET tmp_ol_quantity = ol_quantity2;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 
      THEN
	SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;

	call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
                    	 tmp_ol_quantity, tmp_i_price,
			 tmp_i_name, tmp_i_data,
			 out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 2, tmp_s_quantity);

	SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;
    IF tmp_o_ol_cnt > 2 THEN

      SET tmp_i_id = ol_i_id3;
      SET tmp_ol_supply_w_id = ol_supply_w_id3;
      SET tmp_ol_quantity = ol_quantity3;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 THEN
	SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;

	call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
                         tmp_ol_quantity, tmp_i_price,
			 tmp_i_name, tmp_i_data,
			 out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 3, tmp_s_quantity);

	SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;
    IF tmp_o_ol_cnt > 3 THEN

      SET tmp_i_id = ol_i_id4;
      SET tmp_ol_supply_w_id = ol_supply_w_id4;
      SET tmp_ol_quantity = ol_quantity4;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 THEN
	SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;

	call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
                    	 tmp_ol_quantity, tmp_i_price,
			 tmp_i_name, tmp_i_data,
			 out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 4, tmp_s_quantity);

	SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;
    IF tmp_o_ol_cnt > 4 THEN

      SET tmp_i_id = ol_i_id5;
      SET tmp_ol_supply_w_id = ol_supply_w_id5;
      SET tmp_ol_quantity = ol_quantity5;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 THEN
	SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;

	call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
                         tmp_ol_quantity, tmp_i_price,
			 tmp_i_name, tmp_i_data,
			 out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 5, tmp_s_quantity);

	SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;
    IF tmp_o_ol_cnt > 5 THEN
      SET tmp_i_id = ol_i_id6;
      SET tmp_ol_supply_w_id = ol_supply_w_id6;
      SET tmp_ol_quantity = ol_quantity6;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 THEN
        SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;

	call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
                     	 tmp_ol_quantity, tmp_i_price,
			 tmp_i_name, tmp_i_data,
			 out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 6, tmp_s_quantity);

	SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;
    IF tmp_o_ol_cnt > 6 THEN
      SET tmp_i_id = ol_i_id7;
      SET tmp_ol_supply_w_id = ol_supply_w_id7;
      SET tmp_ol_quantity = ol_quantity7;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 THEN
	SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;

	call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
                   	 tmp_ol_quantity, tmp_i_price,
			 tmp_i_name, tmp_i_data,
			 out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 7, tmp_s_quantity);

	SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;
    IF tmp_o_ol_cnt > 7 THEN
      SET tmp_i_id = ol_i_id8;
      SET tmp_ol_supply_w_id = ol_supply_w_id8;
      SET tmp_ol_quantity = ol_quantity8;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 THEN
        SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;
        call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
                         tmp_ol_quantity, tmp_i_price,
		         tmp_i_name, tmp_i_data,
		         out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 8, tmp_s_quantity);

        SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;
    IF tmp_o_ol_cnt > 8 THEN
      SET tmp_i_id = ol_i_id9;
      SET tmp_ol_supply_w_id = ol_supply_w_id9;
      SET tmp_ol_quantity = ol_quantity9;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 THEN
        SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;
        call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
                         tmp_ol_quantity, tmp_i_price,
                         tmp_i_name, tmp_i_data,
		         out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 9, tmp_s_quantity);

        SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;

    IF tmp_o_ol_cnt > 9 THEN
      SET tmp_i_id = ol_i_id10;
      SET tmp_ol_supply_w_id = ol_supply_w_id10;
      SET tmp_ol_quantity = ol_quantity10;

      SELECT i_price, i_name, i_data 
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 THEN
        SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;
	call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
	                 tmp_ol_quantity, tmp_i_price,
			 tmp_i_name, tmp_i_data,
			 out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 10, tmp_s_quantity);

	SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;

    IF tmp_o_ol_cnt > 10 THEN
      SET tmp_i_id = ol_i_id11;
      SET tmp_ol_supply_w_id = ol_supply_w_id11;
      SET tmp_ol_quantity = ol_quantity11;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 THEN
        SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;
	call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
	             	 tmp_ol_quantity, tmp_i_price,
			 tmp_i_name, tmp_i_data,
			 out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 11, tmp_s_quantity);

	SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;

    IF tmp_o_ol_cnt > 11 THEN
      SET tmp_i_id = ol_i_id12;
      SET tmp_ol_supply_w_id = ol_supply_w_id12;
      SET tmp_ol_quantity = ol_quantity12;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 THEN
 	SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;
 	call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
           	         tmp_ol_quantity, tmp_i_price,
			 tmp_i_name, tmp_i_data,
			 out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 12, tmp_s_quantity);

	SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;

    IF tmp_o_ol_cnt > 12 THEN
      SET tmp_i_id = ol_i_id13;
      SET tmp_ol_supply_w_id = ol_supply_w_id13;
      SET tmp_ol_quantity = ol_quantity13;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 THEN
 	SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;
	call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
                   	 tmp_ol_quantity, tmp_i_price,
			 tmp_i_name, tmp_i_data,
			 out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 13, tmp_s_quantity);

	SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;

    IF tmp_o_ol_cnt > 13 THEN
      SET tmp_i_id = ol_i_id14;
      SET tmp_ol_supply_w_id = ol_supply_w_id14;
      SET tmp_ol_quantity = ol_quantity14;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 THEN
	SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;
	call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
                         tmp_ol_quantity, tmp_i_price,
			 tmp_i_name, tmp_i_data,
			 out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 14, tmp_s_quantity);

	SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;

    IF tmp_o_ol_cnt > 14 THEN

      SET tmp_i_id = ol_i_id15;
      SET tmp_ol_supply_w_id = ol_supply_w_id15;
      SET tmp_ol_quantity = ol_quantity15;

      SELECT i_price, i_name, i_data
      INTO tmp_i_price, tmp_i_name, tmp_i_data
      FROM item
      WHERE i_id = tmp_i_id;

      IF tmp_i_price > 0 THEN
 	SET tmp_ol_amount = tmp_i_price * tmp_ol_quantity;
	call new_order_2(tmp_w_id, tmp_d_id, tmp_i_id,
                  	 tmp_ol_quantity, tmp_i_price,
			 tmp_i_name, tmp_i_data,
			 out_d_next_o_id, tmp_ol_amount,
                         tmp_ol_supply_w_id, 15, tmp_s_quantity);

	SET tmp_total_amount = tmp_total_amount + tmp_ol_amount;
      END IF;
    END IF;

END|
delimiter ;

