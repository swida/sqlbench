ALTER TABLE warehouse
ADD CONSTRAINT pk_warehouse
PRIMARY KEY (w_id);

ALTER TABLE district
ADD CONSTRAINT pk_district
PRIMARY KEY (d_w_id, d_id);

ALTER TABLE customer
ADD CONSTRAINT pk_customer
PRIMARY KEY (c_w_id, c_d_id, c_id);

ALTER TABLE new_order
ADD CONSTRAINT pk_new_order
PRIMARY KEY (no_w_id, no_d_id, no_o_id);

ALTER TABLE orders
ADD CONSTRAINT pk_orders
PRIMARY KEY (o_w_id, o_d_id, o_id);

ALTER TABLE order_line
ADD CONSTRAINT pk_order_line
PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number);

ALTER TABLE item
ADD CONSTRAINT pk_item
PRIMARY KEY (i_id);

ALTER TABLE stock
ADD CONSTRAINT pk_stock
PRIMARY KEY (s_w_id, s_i_id);

CREATE INDEX i_orders
ON orders (o_w_id, o_d_id, o_c_id);

CREATE INDEX i_customer
ON customer (c_w_id, c_d_id, c_last, c_first, c_id);

