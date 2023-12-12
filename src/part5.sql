CREATE OR REPLACE FUNCTION function_defining_offers_focused_increasing_frequency_visits(
    start_dt VARCHAR,
    end_dt VARCHAR,
    num_of_transactions INTEGER,
    max_churn_index NUMERIC,
    max_sku_share_percentage NUMERIC,
    max_margin_share_percentage NUMERIC
) RETURNS TABLE (
    Customer_ID INTEGER,
    Start_Date TIMESTAMP,
    End_Date TIMESTAMP,
    Required_Transactions_Count INTEGER,
    Group_Name VARCHAR,
    Offer_Discount_Depth NUMERIC
) AS $$
    SET datestyle TO dmy;
select c.customer_id,
    start_dt::timestamp as Start_Date,
    end_dt::timestamp as End_Date,
    round((
        EXTRACT(EPOCH FROM (end_dt::timestamp)) -
        EXTRACT(EPOCH FROM (start_dt::timestamp))
    ) / 86400 / c.customer_frequency)::integer + num_of_transactions as Offer_Discount_Depth,
    table1.group_name as group_name,
    table1.Offer_Discount_Depth as Offer_Discount_Depth
FROM customer_view c
JOIN (
SELECT g.customer_id,
       g.group_id,
       sg.group_name,
       avg((ph.group_summ - ph.group_cost) / ph.group_summ),
       ceil(g.group_minimum_discount / 0.05) * 5 as Offer_Discount_Depth,
       row_number() OVER (PARTITION BY g.customer_id, g.group_id ORDER BY group_affinity_index DESC) as rn
FROM Groups_View g
    LEFT JOIN View_Purchase_History ph ON ph.customer_id = g.customer_id AND
                                     g.group_id = ph.group_id
    LEFT JOIN product_group sg ON g.group_id = sg.group_id
WHERE g.group_churn_rate <= max_churn_index AND
      g.group_discount_share * 100 <= max_sku_share_percentage
GROUP BY g.customer_id,
         g.group_id,
         sg.group_name,
         g.group_affinity_index,
         g.group_minimum_discount
HAVING (avg((ph.group_summ - ph.group_cost) / ph.group_summ) * max_margin_share_percentage >=
        ceil(g.group_minimum_discount / 0.05) * 5)
ORDER BY g.customer_id,
         g.group_affinity_index DESC
) table1 ON table1.customer_id = c.customer_id AND table1.rn = 1;
$$ LANGUAGE sql;

SELECT * FROM function_defining_offers_focused_increasing_frequency_visits('18.08.2022 00:00:00', '18.08.2022 00:00:00', 1, 3, 70, 30);
