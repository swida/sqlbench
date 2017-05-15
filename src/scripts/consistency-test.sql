--------------------- Consistency Condition 1 -------------------------------
-- Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship:
-- W_YTD = sum(D_YTD)
-- for each warehouse defined by (W_ID = D_W_ID).
select t1.w_id, t1.w_ytd, sum(t2.d_ytd)
      from warehouse t1, district t2
      where t1.w_id = t2.d_w_id
      group by w_id having  t1.w_ytd <> sum(t2.d_ytd);

------------------- Consistency Condition 2 -----------------------------------
-- Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
-- D_NEXT_O_ID – 1 = max(O_ID) = max(NO_O_ID)
-- for each district defined by (D_W_ID = O_W_ID = NO_W_ID) and (D_ID = O_D_ID = NO_D_ID).
-- This condition does not apply to the NEW-ORDER table for any districts
-- which have no outstanding new orders (i.e., the number of rows is zero).
select t1.d_w_id, t1.d_id, t1.max_o_id1, t2.max_o_id2, t3.max_o_id3
from
    (select d_w_id, d_id, d_next_o_id - 1 as max_o_id1
    from district) t1,
    (select o_w_id, o_d_id, max(o_id) as max_o_id2
    from orders
    group by o_w_id, o_d_id) t2,
    (select no_w_id, no_d_id, max(no_o_id) as max_o_id3
    from new_order
    group by no_w_id, no_d_id) t3
where t1.d_w_id = t2.o_w_id
and t1.d_w_id = t3.no_w_id
and t1.d_id = t2.o_d_id
and t1.d_id = t3.no_d_id
and (t1.max_o_id1 != t2.max_o_id2 or t1.max_o_id1 != t3.max_o_id3 or t2.max_o_id2 != t3.max_o_id3);

----------------------- Consistency Condition 3 -------------------------------------
-- Entries in the NEW-ORDER table must satisfy the relationship:
-- max(NO_O_ID) – min(NO_O_ID) + 1 = [number of rows in the NEW-ORDER table for this district]
-- for each district defined by NO_W_ID and NO_D_ID. This condition does not apply to
-- any districts which have no outstanding new orders (i.e., the number of rows is zero).
select t2.*, t2.maxmin - t2.tot
from (
    select t1.no_w_id, t1.no_d_id, max(no_o_id) - min(no_o_id) + 1 as maxmin, count(*) as tot
    from new_order t1
    group by t1.no_w_id, t1.no_d_id
) t2
where t2.maxmin - t2.tot != 0
order by t2.no_w_id, t2.no_d_id;

------------------------- Consistency Condition 4 ----------------------------
-- Entries in the ORDER and ORDER-LINE tables must satisfy the relationship:
-- sum(O_OL_CNT) = [number of rows in the ORDER-LINE table for this district]
-- for each district defined by (O_W_ID = OL_W_ID) and (O_D_ID = OL_D_ID).

select t1.o_w_id, t1.o_d_id, t1.tot_cnt, t2.tot, t1.tot_cnt - t2.tot
from
    (select o_w_id, o_d_id, sum(o_ol_cnt) as tot_cnt
    from orders
    group by o_w_id, o_d_id) t1,
    (select ol_w_id, ol_d_id, count(*) as tot
    from order_line
    group by ol_w_id, ol_d_id) t2
where t1.o_w_id = t2.ol_w_id
and t1.o_d_id = t2.ol_d_id
and t1.tot_cnt - t2.tot != 0
order by t1.o_w_id, t1.o_d_id;

---------------------------- Consistency Condition 5 -------------------------
-- For any row in the ORDER table, O_CARRIER_ID is set to a null value if and only if
-- there is a corresponding row in the NEW-ORDER table defined by
--(O_W_ID, O_D_ID, O_ID) = (NO_W_ID, NO_D_ID, NO_O_ID).

select * from
    (select count(*) as tot_null_cnt1
           from orders
           where o_carrier_id is null) t3,
    (select count(*) as tot_null_cnt
             from orders t1, new_order t2
	     where t1.o_w_id = t2.no_w_id
                   and t1.o_d_id = t2.no_d_id
                   and t1.o_id = t2.no_o_id
                   and t1.o_carrier_id is null) t4
	where t4.tot_null_cnt != t3.tot_null_cnt1;

-------------------- Consistency Condition 6 --------------------------
-- For any row in the ORDER table, O_OL_CNT must equal the number of rows in the
-- ORDER-LINE table for the corresponding order defined by
-- (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID).

select *
from
    (select t1.o_w_id, t1.o_d_id, t1.o_id, min(t1.o_ol_cnt) as min_ol_cnt, count(*) as ol_cnts
    from orders t1, order_line t2
    where t1.o_w_id = t2.ol_w_id
    and t1.o_d_id = t2.ol_d_id
    and t1.o_id = t2.ol_o_id
    group by t1.o_w_id, t1.o_d_id, t1.o_id) t3
where min_ol_cnt != ol_cnts;

-------------------- Consistency Condition 7 ---------------------------
-- For any row in the ORDER-LINE table, OL_DELIVERY_D is set to a null date/time
-- if and only if the corresponding row in the ORDER table defined by
-- (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID) has O_CARRIER_ID set to a null value.

select t1.o_w_id, t1.o_d_id, t1.o_id, t1.o_carrier_id, t2.ol_delivery_d
from orders t1, order_line t2
where t1.o_w_id = t2.ol_w_id
and t1.o_d_id = t2.ol_d_id
and t1.o_id = t2.ol_o_id
and ((t1.o_carrier_id is null and t2.ol_delivery_d is not null)
    or (t1.o_carrier_id is not null and t2.ol_delivery_d is null));

----------------- Consistency Condition 8 --------------------------------
-- Entries in the WAREHOUSE and HISTORY tables must satisfy the relationship:
-- W_YTD = sum(H_AMOUNT)
-- for each warehouse defined by (W_ID = H_W_ID).

select t1.w_id, t1.w_ytd, t2.sum_amount, t1.w_ytd - t2.sum_amount
from
    (select w_id, w_ytd from warehouse) t1,
    (select h_w_id, sum(h_amount) as sum_amount
    from history
    group by h_w_id) t2
where t1.w_id = t2.h_w_id
and t1.w_ytd != t2.sum_amount;

-------------------- Consistency Condition 9 ------------------------

-- Entries in the DISTRICT and HISTORY tables must satisfy the relationship:
-- D_YTD = sum(H_AMOUNT)
-- for each district defined by (D_W_ID, D_ID) = (H_W_ID, H_D_ID).

select t1.d_w_id, t1.d_id, t1.d_ytd, t2.sum_amount
from
    (select d_w_id, d_id, d_ytd from district) t1,
    (select h_w_id, h_d_id, sum(h_amount) as sum_amount
    from history
    group by h_w_id, h_d_id) t2
where t1.d_w_id = t2.h_w_id
and t1.d_id = t2.h_d_id
and t1.d_ytd != t2.sum_amount;

------------------------- Consistency Condition 10 -----------------------------
-- Entries in the CUSTOMER, HISTORY, ORDER, and ORDER-LINE tables must satisfy the relationship:
--   C_BALANCE = sum(OL_AMOUNT) – sum(H_AMOUNT)
-- where:
--    H_AMOUNT is selected by (C_W_ID, C_D_ID, C_ID) = (H_C_W_ID, H_C_D_ID, H_C_ID)
-- and
--    OL_AMOUNT is selected by:
--    (OL_W_ID, OL_D_ID, OL_O_ID) = (O_W_ID, O_D_ID, O_ID) and
--    (O_W_ID, O_D_ID, O_C_ID) = (C_W_ID, C_D_ID, C_ID) and
--    (OL_DELIVERY_D is not a null value)

select c_w_id, c_d_id, c_id, c_balance
from customer
where c_w_id = 1 and c_d_id = 1 and c_id = 2;
select t1.c_w_id, t1.c_d_id, t1.c_id, sum(t2.h_amount)
from customer t1, history t2
where t1.c_w_id = t2.h_c_w_id
and t1.c_d_id = t2.h_c_d_id
and t1.c_id = t2.h_c_id
and t1.c_w_id = 1 and t1.c_d_id = 1 and t1.c_id = 2
group by t1.c_w_id, t1.c_d_id, t1.c_id;
select t1.c_w_id, t1.c_d_id, t1.c_id, sum(t3.ol_amount) as sum_ol_amount
from customer t1, orders t2, order_line t3
where t3.ol_w_id = t2.o_w_id
and t3.ol_d_id = t2.o_d_id
and t3.ol_o_id = t2.o_id
and t2.o_w_id = t1.c_w_id
and t2.o_d_id = t1.c_d_id
and t2.o_c_id = t1.c_id
and t3.ol_delivery_d is not null
and t1.c_w_id = 1 and t1.c_d_id = 1 and t1.c_id = 2
group by t1.c_w_id, t1.c_d_id, t1.c_id;

WITH wcd (
	w_id
	,wcd_c_id
	,d_id
	)
AS (
	SELECT w_id
		,c_id AS wcd_c_id
		,d_id
	FROM warehouse
		,customer
		,district
	)
	,h_amount_sum (
	sum_h_amount
	,w_id
	,wcd_c_id
	,d_id
	)
AS (
	SELECT sum(h_amount)
		,w_id
		,wcd_c_id
		,d_id
	FROM customer
		,history
		,wcd
	WHERE c_w_id = h_c_w_id
		AND c_d_id = h_c_d_id
		AND c_id = h_c_id
		AND w_id = c_w_id
		AND c_id = wcd_c_id
		AND d_id = c_d_id
	GROUP BY 2
		,3
		,4
	)
	,ol_amount_sum (
	sum_ol_amount
	,w_id
	,wcd_c_id
	,d_id
	)
AS (
	SELECT sum(ol_amount)
		,w_id
		,wcd_c_id
		,d_id
	FROM order_line
		,orders
		,customer
		,wcd
	WHERE ol_w_id = o_w_id
		AND ol_d_id = o_d_id
		AND ol_o_id = o_id
		AND o_w_id = c_w_id
		AND o_d_id = c_d_id
		AND o_c_id = c_id
		AND w_id = ol_w_id
		AND wcd_c_id = c_id
		AND d_id = ol_d_id
		AND ol_delivery_d IS NOT NULL
	GROUP BY 2
		,3
		,4
	)
SELECT c_balance
	,sum_h_amount
	,sum_ol_amount
FROM customer
	,h_amount_sum
	,ol_amount_sum
WHERE h_amount_sum.w_id = ol_amount_sum.w_id
	AND h_amount_sum.wcd_c_id = ol_amount_sum.wcd_c_id
	AND h_amount_sum.d_id = ol_amount_sum.d_id
	AND customer.c_id = h_amount_sum.wcd_c_id
	AND c_balance != sum_ol_amount - sum_h_amount;

------------------------ Consistency Condition 11 ---------------------------
-- Entries in the CUSTOMER, ORDER and NEW-ORDER tables must satisfy the relationship:
--    (count(*) from ORDER) – (count(*) from NEW-ORDER) = 2100
-- for each district defined by (O_W_ID, O_D_ID) = (NO_W_ID, NO_D_ID) = (C_W_ID, C_D_ID).

select o_w_id, o_d_id, count(*) from orders, customer
where o_w_id = c_w_id
and o_d_id = c_d_id
group by o_w_id, o_d_id
order by o_w_id, o_d_id;
select no_w_id, no_d_id, count(*) from new_order
group by no_w_id, no_d_id
order by no_w_id, no_d_id;
select c_w_id, c_d_id, count(*) from customer
group by c_w_id, c_d_id
order by c_w_id, c_d_id;
select t1.o_w_id, t1.o_d_id, t1.orders_cnt, t2.new_orders_cnt, t1.orders_cnt – t2.new_orders_cnt as diff
from
    (select o_w_id, o_d_id, count(*) as orders_cnt
    from orders, customer
    where o_w_id = c_w_id
    and o_d_id = c_d_id
    group by o_w_id, o_d_id) t1,
    (select no_w_id, no_d_id, count(*) as new_orders_cnt
    from new_orders, customer
    where no_w_id = c_w_id
    and no_d_id = c_d_id
    group by no_w_id, no_d_id) t2
where t1.o_w_id = t2.no_w_id
and t1.o_d_id = t2.no_d_id
and t1.orders_cnt – t2.new_orders_cnt != 2100;

------------------------------- Consistency Condition 12 --------------------------------
-- Entries in the CUSTOMER and ORDER-LINE tables must satisfy the relationship:
--   C_BALANCE + C_YTD_PAYMENT = sum(OL_AMOUNT)
-- for any randomly selected customers and where OL_DELIVERY_D is not set to a null date/time.
select t4.c_w_id, t4.c_d_id, t4.c_id, t4.sum_ol_amount, t5.sum_bp, t5.sum_bp - t4.sum_ol_amount
from
    (select t1.c_w_id, t1.c_d_id, t1.c_id, sum(t3.ol_amount) as sum_ol_amount
    from customer t1, orders t2, order_line t3
    where t3.ol_w_id = t2.o_w_id
    and t3.ol_d_id = t2.o_d_id
    and t3.ol_o_id = t2.o_id
    and t2.o_w_id = t1.c_w_id
    and t2.o_d_id = t1.c_d_id
    and t2.o_c_id = t1.c_id
    and t3.ol_delivery_d is not null
    and t1.c_id = 2 and t1.c_w_id = 1 and t1.c_d_id = 1
    group by t1.c_w_id, t1.c_d_id, t1.c_id) t4,
    (select c_id, c_w_id, c_d_id, sum(c_balance + c_ytd_payment) as sum_bp
    from customer
    where c_id = 2 and c_w_id = 1 and c_d_id = 1) t5
where t4.c_w_id = t5.c_w_id
and t4.c_d_id = t5.c_d_id
and t4.c_id = t5.c_id;
select t3.c_id, t3.c_sum, t4.tot_amount, t3.c_sum - t4.tot_amount
from
    (select c_id, c_w_id, c_d_id, c_balance + c_ytd_payment as c_sum
    from customer) t3,
    (select t1.o_c_id, t1.o_w_id, t1.o_d_id, sum(ol_amount) as tot_amount
    from orders t1, order_line t2
    where t1.o_id = t2.ol_o_id
    and t1.o_d_id = t2.ol_d_id
    and t1.o_w_id = t2.ol_w_id
    and t2.ol_delivery_d is not null
    group by t1.o_c_id, t1.o_w_id, t1.o_d_id
    ) t4
where t3.c_id = t4.o_c_id
and t3.c_w_id = t4.o_w_id
and t3.c_d_id = t4.o_d_id
and t3.c_sum != t4.tot_amount;
select t3.c_id, t3.c_sum, t4.tot_amount, t3.c_sum - t4.tot_amount
from
    (select c_id, c_w_id, c_d_id, c_balance + c_ytd_payment as c_sum
    from customer where c_id = 1 and c_w_id = 1 and c_d_id = 1) t3,
    (select t1.o_c_id, t1.o_w_id, t1.o_d_id, sum(ol_amount) as tot_amount
    from orders t1, order_line t2
    where t1.o_id = t2.ol_o_id
    and t1.o_d_id = t2.ol_d_id
    and t1.o_w_id = t2.ol_w_id
    and t1.o_c_id = 1 and t1.o_w_id = 1 and t1.o_d_id = 1
    and t2.ol_delivery_d is not null
    group by t1.o_c_id, t1.o_w_id, t1.o_d_id
    ) t4
where t3.c_id = t4.o_c_id
and t3.c_w_id = t4.o_w_id
and t3.c_d_id = t4.o_d_id
and t3.c_sum != t4.tot_amount;
