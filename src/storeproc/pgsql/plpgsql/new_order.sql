CREATE OR REPLACE FUNCTION new_order (INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER,
       INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER,
       INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER,
       INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, INTEGER,
       INTEGER, INTEGER, INTEGER, INTEGER, INTEGER) RETURNS INTEGER AS '
DECLARE
	tmp_w_id ALIAS FOR $1;
	tmp_d_id ALIAS FOR $2;
	tmp_c_id ALIAS FOR $3;
	tmp_o_all_local ALIAS FOR $4;
	tmp_o_ol_cnt ALIAS FOR $5;
	tmp_i_id1 ALIAS FOR $6;
	tmp_ol_supply_w_id1 ALIAS FOR $7;
	tmp_ol_quantity1 ALIAS FOR $8;
	tmp_i_id2 ALIAS FOR $9;
	tmp_ol_supply_w_id2 ALIAS FOR $10;
	tmp_ol_quantity2 ALIAS FOR $11;
	tmp_i_id3 ALIAS FOR $12;
	tmp_ol_supply_w_id3 ALIAS FOR $13;
	tmp_ol_quantity3 ALIAS FOR $14;
	tmp_i_id4 ALIAS FOR $15;
	tmp_ol_supply_w_id4 ALIAS FOR $16;
	tmp_ol_quantity4 ALIAS FOR $17;
	tmp_i_id5 ALIAS FOR $18;
	tmp_ol_supply_w_id5 ALIAS FOR $19;
	tmp_ol_quantity5 ALIAS FOR $20;
	tmp_i_id6 ALIAS FOR $21;
	tmp_ol_supply_w_id6 ALIAS FOR $22;
	tmp_ol_quantity6 ALIAS FOR $23;
	tmp_i_id7 ALIAS FOR $24;
	tmp_ol_supply_w_id7 ALIAS FOR $25;
	tmp_ol_quantity7 ALIAS FOR $26;
	tmp_i_id8 ALIAS FOR $27;
	tmp_ol_supply_w_id8 ALIAS FOR $28;
	tmp_ol_quantity8 ALIAS FOR $29;
	tmp_i_id9 ALIAS FOR $30;
	tmp_ol_supply_w_id9 ALIAS FOR $31;
	tmp_ol_quantity9 ALIAS FOR $32;
	tmp_i_id10 ALIAS FOR $33;
	tmp_ol_supply_w_id10 ALIAS FOR $34;
	tmp_ol_quantity10 ALIAS FOR $35;
	tmp_i_id11 ALIAS FOR $36;
	tmp_ol_supply_w_id11 ALIAS FOR $37;
	tmp_ol_quantity11 ALIAS FOR $38;
	tmp_i_id12 ALIAS FOR $39;
	tmp_ol_supply_w_id12 ALIAS FOR $40;
	tmp_ol_quantity12 ALIAS FOR $41;
	tmp_i_id13 ALIAS FOR $42;
	tmp_ol_supply_w_id13 ALIAS FOR $43;
	tmp_ol_quantity13 ALIAS FOR $44;
	tmp_i_id14 ALIAS FOR $45;
	tmp_ol_supply_w_id14 ALIAS FOR $46;
	tmp_ol_quantity14 ALIAS FOR $47;
	tmp_i_id15 ALIAS FOR $48;
	tmp_ol_supply_w_id15 ALIAS FOR $49;
	tmp_ol_quantity15 ALIAS FOR $50;

	out_w_tax NUMERIC;

	out_d_tax NUMERIC;
	out_d_next_o_id INTEGER;

	out_c_discount NUMERIC;
	out_c_last VARCHAR;
	out_c_credit VARCHAR;

	tmp_i_id INTEGER;
	tmp_i_price NUMERIC;
	tmp_i_name VARCHAR;
	tmp_i_data VARCHAR;

	tmp_ol_amount NUMERIC;
	tmp_ol_supply_w_id INTEGER;
	tmp_ol_quantity INTEGER;

	tmp_s_quantity INTEGER;

	tmp_total_amount NUMERIC;
BEGIN
	SELECT w_tax
	INTO out_w_tax
	FROM warehouse
	WHERE w_id = tmp_w_id;
	IF NOT FOUND THEN
		RAISE WARNING ''NEW_ORDER_1 failed'';
		RETURN 10;
	END IF;
	SELECT d_tax, d_next_o_id
	INTO out_d_tax, out_d_next_o_id
	FROM district
	WHERE d_w_id = tmp_w_id
	  AND d_id = tmp_d_id
	FOR UPDATE;
	IF NOT FOUND THEN
		RAISE DEBUG ''NEW_ORDER_3 failed'';
		RETURN 11;
	END IF;
	UPDATE district
	SET d_next_o_id = d_next_o_id + 1
	WHERE d_w_id = tmp_w_id
	  AND d_id = tmp_d_id;

	IF NOT FOUND THEN
		RAISE WARNING ''NEW_ORDER_3 failed'';
		RETURN 12;
	END IF;

	SELECT c_discount, c_last, c_credit
	INTO out_c_discount, out_c_last, out_c_credit
	FROM customer
	WHERE c_w_id = tmp_w_id
	  AND c_d_id = tmp_d_id
	  AND c_id = tmp_c_id;

	IF NOT FOUND THEN
		RAISE WARNING ''NEW_ORDER_4 failed'';
		RETURN 13;
	END IF;

	INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d,
	                    o_carrier_id, o_ol_cnt, o_all_local)
	VALUES (out_d_next_o_id, tmp_d_id, tmp_w_id, tmp_c_id,
	        current_timestamp, NULL, tmp_o_ol_cnt, tmp_o_all_local);

	INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
	VALUES (out_d_next_o_id, tmp_d_id, tmp_w_id);

	tmp_total_amount = 0;

	/* More goofy logic. :( */
	IF tmp_o_ol_cnt > 0 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id1;
		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity1;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id1,
		                   	tmp_ol_quantity1, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id1, 1)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 1 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id2;

		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity2;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id2,
		                   	tmp_ol_quantity2, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id2, 2)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 2 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id3;

		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity3;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id3,
		                   	tmp_ol_quantity3, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id3, 3)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 3 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id4;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity4;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id4,
		                   	tmp_ol_quantity4, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id4, 4)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 4 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id5;

		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity5;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id5,
		                   	tmp_ol_quantity5, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id5, 5)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 5 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id6;

		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity6;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id6,
		                   	tmp_ol_quantity6, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id6, 6)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 6 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id7;

		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity7;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id7,
		                   	tmp_ol_quantity7, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id7, 7)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 7 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id8;

		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity8;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id8,
		                   	tmp_ol_quantity8, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id8, 8)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 8 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id9;

		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity9;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id9,
		                   	tmp_ol_quantity9, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id9, 9)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 9 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id10;

		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity10;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id10,
		                   	tmp_ol_quantity10, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id10, 10)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 10 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id11;

		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity11;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id11,
		                   	tmp_ol_quantity11, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id11, 11)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 11 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id12;

		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity12;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id12,
		                   	tmp_ol_quantity12, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id12, 12)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 12 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id13;

		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity13;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id13,
		                   	tmp_ol_quantity13, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id13, 13)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 13 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id14;

		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity14;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id14,
		                   	tmp_ol_quantity14, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id14, 14)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;
	IF tmp_o_ol_cnt > 14 THEN
		SELECT i_price, i_name, i_data
		INTO tmp_i_price, tmp_i_name, tmp_i_data
		FROM item
		WHERE i_id = tmp_i_id15;

		IF NOT FOUND THEN
			RETURN 2;
		END IF;

		IF tmp_i_price > 0 THEN
			tmp_ol_amount = tmp_i_price * tmp_ol_quantity15;
			SELECT new_order_2(tmp_w_id, tmp_d_id, tmp_i_id15,
		                   	tmp_ol_quantity15, tmp_i_price,
					tmp_i_name, tmp_i_data,
					out_d_next_o_id, tmp_ol_amount,
                                   	tmp_ol_supply_w_id15, 15)
			INTO tmp_s_quantity;
			tmp_total_amount = tmp_total_amount + tmp_ol_amount;
		ELSE
			RETURN 1;
		END IF;
	END IF;

	RETURN 0;
END;
' LANGUAGE 'plpgsql';
