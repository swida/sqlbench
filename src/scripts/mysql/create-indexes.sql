CREATE INDEX i_orders
ON orders (o_w_id, o_d_id, o_c_id);

CREATE INDEX i_customer
ON customer (c_w_id, c_d_id, c_last, c_first, c_id);

