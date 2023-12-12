CREATE OR REPLACE FUNCTION function_defining_cross_selling_oriented_offers(
    num_of_groups INTEGER,
    max_churn_index NUMERIC,
    max_consumption_stability_index NUMERIC,
    max_sku_share_percentage NUMERIC,
    max_margin_share_percentage NUMERIC
) RETURNS table (
    Customer_ID          INTEGER,
    SKU_Name             varchar,
    Offer_Discount_Depth NUMERIC
) as $$
WITH cte_store AS (
	SELECT  *
    FROM (SELECT st.transaction_store_id, st.sku_id, group_id, sku_name,
		 (sku_retail_price - sku_purchase_price) / sku_retail_price * max_margin_share_percentage AS margin_share,
		  row_number() 
		  OVER (PARTITION BY transaction_store_id, group_id ORDER BY (sku_retail_price - sku_purchase_price) DESC) AS rn
		  FROM store_product st
    LEFT JOIN product USING (sku_id)) table1
    WHERE rn = 1
),
cte_customer_data AS (
	SELECT customer_id, customer_primary_store 
    FROM customer_view
 ), 
 cte_sku_group_share AS (
     SELECT * 
     FROM (SELECT DISTINCT customer_id, sku_id, group_id, 
     	  	              (SELECT count(*) FROM (SELECT DISTINCT unnest(arr_tr_sku)) usku)::numeric / (SELECT count(*) 
     	                   FROM (SELECT DISTINCT unnest(arr_tr_grp)) ugrp) * 100 AS sku_grp_share
           FROM (SELECT customer_id, ch.sku_id, phv.group_id,
                        array_agg(phv.transaction_id) OVER (PARTITION BY customer_id, phv.group_id) arr_tr_grp,
                        array_agg(phv.transaction_id) OVER (PARTITION BY customer_id, ch.sku_id) arr_tr_sku
                 FROM (SELECT DISTINCT customer_id, transaction_id, group_id 
                 	   FROM View_Purchase_History) phv
                 LEFT JOIN transaction_detail ch ON ch.transaction_id = phv.transaction_id
                 JOIN product s ON s.sku_id = ch.sku_id AND phv.group_id = s.group_id
                 ORDER BY customer_id, ch.sku_id) t
		   ) t1
	WHERE sku_grp_share <= max_sku_share_percentage
)
select g.customer_id, swg.sku_name, ceil(g.group_minimum_discount / 0.05) * 5 as Offer_Discount_Depth
FROM (select customer_id, group_id, group_minimum_discount,
          	 row_number() OVER (PARTITION BY customer_id ORDER BY group_stability_index DESC) as rn
      FROM Groups_View
      WHERE group_churn_rate <= max_churn_index and group_stability_index <= max_consumption_stability_index
) g
LEFT JOIN cte_customer_data cv ON g.customer_id = cv.customer_id
LEFT JOIN cte_store swg ON cv.customer_primary_store = swg.transaction_store_id 
						and g.group_id = swg.group_id
JOIN cte_sku_group_share sgs ON g.customer_id = sgs.customer_id 
							 and g.group_id = sgs.group_id 
							 and swg.sku_id = sgs.sku_id
WHERE g.rn <= num_of_groups AND swg.margin_share >= ceil(g.group_minimum_discount / 0.05) * 5 
ORDER BY g.customer_id, g.group_id
$$ LANGUAGE SQL;

SELECT * FROM function_defining_cross_selling_oriented_offers(5, 3, 0.5, 100, 30)
