CREATE TABLE warehouse (
    w_id INTEGER,
    w_name VARCHAR(10),
    w_street_1 VARCHAR(20),
    w_street_2 VARCHAR(20),
    w_city VARCHAR(20),
    w_state char(2),
    w_zip char(9),
    w_tax REAL,
    w_ytd NUMERIC(24, 12),
    CONSTRAINT pk_warehouse PRIMARY KEY (w_id));

CREATE TABLE district (
    d_id INTEGER,
    d_w_id INTEGER,
    d_name VARCHAR(10),
    d_street_1 VARCHAR(20),
    d_street_2 VARCHAR(20),
    d_city VARCHAR(20),
    d_state char(2),
    d_zip char(9),
    d_tax REAL,
    d_ytd NUMERIC(24, 12),
    d_next_o_id INTEGER,
    CONSTRAINT pk_district PRIMARY KEY (d_w_id, d_id));

CREATE TABLE customer (
    c_id INTEGER,
    c_d_id INTEGER,
    c_w_id INTEGER,
    c_first VARCHAR(16),
    c_middle char(2),
    c_last VARCHAR(16),
    c_street_1 VARCHAR(20),
    c_street_2 VARCHAR(20),
    c_city VARCHAR(20),
    c_state char(2),
    c_zip char(9),
    c_phone char(16),
    c_since TIMESTAMP,
    c_credit char(2),
    c_credit_lim NUMERIC(24, 12),
    c_discount REAL,
    c_balance NUMERIC(24, 12),
    c_ytd_payment NUMERIC(24, 12),
    c_payment_cnt REAL,
    c_delivery_cnt REAL,
    c_data VARCHAR(500),
    CONSTRAINT pk_customer PRIMARY KEY (c_w_id, c_d_id, c_id));

CREATE TABLE history (
    h_c_id INTEGER,
    h_c_d_id INTEGER,
    h_c_w_id INTEGER,
    h_d_id INTEGER,
    h_w_id INTEGER,
    h_date TIMESTAMP,
    h_amount REAL,
    h_data VARCHAR(24) );

CREATE TABLE new_order (
    no_o_id INTEGER,
    no_d_id INTEGER,
    no_w_id INTEGER,
    CONSTRAINT pk_new_order PRIMARY KEY (no_w_id, no_d_id, no_o_id));

CREATE TABLE orders (
    o_id INTEGER,
    o_d_id INTEGER,
    o_w_id INTEGER,
    o_c_id INTEGER,
    o_entry_d TIMESTAMP,
    o_carrier_id INTEGER,
    o_ol_cnt INTEGER,
    o_all_local REAL,
    CONSTRAINT pk_orders PRIMARY KEY (o_w_id, o_d_id, o_id));

CREATE TABLE order_line (
    ol_o_id INTEGER,
    ol_d_id INTEGER,
    ol_w_id INTEGER,
    ol_number INTEGER,
    ol_i_id INTEGER,
    ol_supply_w_id INTEGER,
    ol_delivery_d TIMESTAMP,
    ol_quantity REAL,
    ol_amount REAL,
    ol_dist_info VARCHAR(24),
    CONSTRAINT pk_order_line PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number));

CREATE TABLE item (
    i_id INTEGER,
    i_im_id INTEGER,
    i_name VARCHAR(24),
    i_price REAL,
    i_data VARCHAR(50),
    CONSTRAINT pk_item PRIMARY KEY (i_id));

CREATE TABLE stock (
    s_i_id INTEGER,
    s_w_id INTEGER,
    s_quantity REAL,
    s_dist_01 VARCHAR(24),
    s_dist_02 VARCHAR(24),
    s_dist_03 VARCHAR(24),
    s_dist_04 VARCHAR(24),
    s_dist_05 VARCHAR(24),
    s_dist_06 VARCHAR(24),
    s_dist_07 VARCHAR(24),
    s_dist_08 VARCHAR(24),
    s_dist_09 VARCHAR(24),
    s_dist_10 VARCHAR(24),
    s_ytd NUMERIC(16, 8),
    s_order_cnt REAL,
    s_remote_cnt REAL,
    s_data VARCHAR(50),
    CONSTRAINT pk_stock PRIMARY KEY (s_w_id, s_i_id, s_quantity));
