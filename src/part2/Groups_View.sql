CREATE OR REPLACE VIEW Demand_calculation AS
sELECT ph.customer_id,
       p.group_id,
       group_purchase / COUNT(ph.transaction_id)::NUMERIC AS Group_Affinity_Index
FROM View_Purchase_History ph
JOIN periods_view p USING (customer_id)
WHERE ph.transaction_datetime BETWEEN first_group_purchase_date AND last_group_purchase_date
GROUP BY ph.customer_id, p.group_id, group_purchase
ORDER BY customer_id;

--select * from Demand_calculation;

CREATE OR REPLACE VIEW churn_rate AS
SELECT ph.customer_id,
       ph.group_id,
       (extract(epoch FROM(SELECT analysis_formation FROM analysis LIMIT 1))
       - extract(epoch FROM MAX(ph.transaction_datetime)))
       /  86400::NUMERIC/ (group_frequency) AS Group_Churn_Rate
FROM transaction t
JOIN View_Purchase_History ph USING (transaction_id)
JOIN periods_view p ON ph.group_id = p.group_id AND p.customer_id = ph.customer_id
GROUP BY ph.customer_id, ph.group_id, group_frequency
ORDER BY customer_id, group_id;

--select * from churn_rate; -- as

CREATE OR REPLACE VIEW intervals AS
SELECT ph.customer_id,
       ph.transaction_id,
       ph.group_id,
       ph.transaction_datetime,
       EXTRACT(DAY FROM (transaction_datetime - LAG(transaction_datetime)
       		OVER (PARTITION BY ph.customer_id, ph.group_id ORDER BY transaction_datetime))) AS interval
FROM View_Purchase_History ph
JOIN periods_view p ON p.customer_id = ph.customer_id AND p.group_id = ph.group_id
GROUP BY ph.customer_id, transaction_id, ph.group_id, transaction_datetime
ORDER BY customer_id, group_id;

--select * from intervals; -- as

CREATE OR REPLACE VIEW stability_index as
SELECT i.customer_id,
       i.group_id,
       avg(
           CASE
               WHEN (i.interval - p.group_frequency) > 0::NUMERIC THEN (i.interval - p.group_frequency)
               ELSE (i.interval - p.group_frequency) * '-1'::INTEGER::NUMERIC
           END / p.group_frequency
           ) AS group_stability_index
FROM intervals i
    JOIN periods_view p ON p.customer_id = i.customer_id AND i.group_id = p.group_id
GROUP BY i.customer_id, i.group_id
ORDER BY customer_id, group_id;

--select * from stability_index; -- as

CREATE OR REPLACE VIEW margin as
    SELECT customer_id,
           group_id,
           sum(group_summ_paid - group_cost)::NUMERIC AS Group_Margin
    FROM View_Purchase_History
    GROUP BY customer_id, group_id
    ORDER BY customer_id, group_id;

--select * from margin;
   
CREATE OR REPLACE view count_discount_Share AS 
    SELECT DISTINCT p.customer_id,
                    cm.group_id,
                    CASE
                        WHEN max(sku_discount) = 0 THEN count(transaction_detail.transaction_id)
                        ELSE count(transaction_detail.transaction_id) FILTER (WHERE sku_discount> 0)
                    END AS count_share
    FROM customer p
        JOIN customer_card USING (customer_id)
        JOIN transaction USING (customer_card_id)
        JOIN transaction_detail USING (transaction_id)
        JOIN product cm USING (sku_id)
    GROUP BY p.customer_id, cm.group_id
    ORDER BY customer_id;
   
--select * from count_discount_Share;
   
CREATE OR REPLACE view discount_share as
    SELECT DISTINCT c.customer_id,
                    c.group_id,
                    count_share / group_purchase::NUMERIC AS Group_Discount_Share
    FROM count_discount_Share c
        JOIN periods_view p ON c.group_id = p.group_id AND p.customer_id = c.customer_id
    GROUP BY c.customer_id, c.group_id, Group_Discount_Share;
   
--select * from discount_share;

CREATE OR REPLACE view minimum_discount AS
    SELECT customer_id,
           group_id,
           min(group_minimum_discount) AS Group_Minimum_Discount
    FROM periods_view p
    GROUP BY customer_id, group_id
    ORDER BY customer_id, group_id;

--select * from minimum_discount;
   
CREATE OR REPLACE view group_average_discount AS
    SELECT customer_id,
           group_id,
           SUM(View_Purchase_History.group_summ_paid) / SUM(View_Purchase_History.group_summ) AS Group_Average_Discount
    FROM View_Purchase_History
    join transaction_detail t using (transaction_id)
    WHERE t.sku_discount <> 0
    GROUP BY customer_id, group_id
    ORDER BY customer_id, group_id;
   
--select * from group_average_discount;

CREATE OR REPLACE view Groups_View AS
SELECT distinct customer.customer_id,
				aga.group_id,
				Group_Affinity_Index,
				Group_Churn_Rate,
				COALESCE(Group_Stability_Index, 0) AS Group_Stability_Index,
				Group_Margin,
				Group_Discount_Share,
				Group_Minimum_Discount,
				Group_Average_Discount
from customer
left join Demand_calculation as aga using(customer_id)
left join churn_rate on aga.customer_id = churn_rate.customer_id 
				and aga.group_id = churn_rate.group_id
left join stability_index on aga.customer_id = stability_index.customer_id 
					 and aga.group_id = stability_index.group_id
left join margin on aga.customer_id = margin.customer_id 
					 and aga.group_id = margin.group_id
left join discount_share on aga.customer_id = discount_share.customer_id 
					and aga.group_id = discount_share.group_id
left join minimum_discount on aga.customer_id = minimum_discount.customer_id 
					  and aga.group_id = minimum_discount.group_id
left join group_average_discount on aga.customer_id = group_average_discount.customer_id 
							and aga.group_id = group_average_discount.group_id
group by customer.customer_id,
		 aga.group_id,
		 Group_Affinity_Index,
		 Group_Churn_Rate,
		 Group_Stability_Index,
		 Group_Margin,
		 Group_Discount_Share,
		 Group_Minimum_Discount,
		 Group_Average_Discount
order by customer.customer_id,
		 aga.group_id;
		
select * from Groups_View;
