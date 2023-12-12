CREATE OR REPLACE VIEW Segmentation_By_Average_Check_Size AS
SELECT
    c.Customer_ID,
    AVG(t.Transaction_Summ) AS Customer_Average_Check,
    CASE
        WHEN PERCENT_RANK() OVER (ORDER BY AVG(t.Transaction_Summ) DESC) <= 0.10 THEN 'High'
        WHEN PERCENT_RANK() OVER (ORDER BY AVG(t.Transaction_Summ) DESC) <= 0.35 THEN 'Medium'
        ELSE 'Low'
    END AS Customer_Average_Check_Segment
from customer_card c
join Transaction t using(customer_card_id)
GROUP by c.Customer_ID;

select * from Segmentation_By_Average_Check_Size;
   
CREATE OR REPLACE VIEW Segmentation_by_frequency_of_visits AS
SELECT distinct
    Customer_ID,
    Customer_Frequency,
    CASE
        WHEN customer_frequency is null THEN null
        WHEN frequency_rank <= 0.1 * COUNT(*) OVER() THEN 'Often'
        WHEN frequency_rank <= 0.35 * COUNT(*) OVER() THEN 'Occasionally'
        ELSE 'Rarely'
    END AS Customer_Frequency_Segment
from (SELECT distinct
    Customer_ID,
    Customer_Frequency,
    RANK() OVER (ORDER BY Customer_Frequency) AS frequency_rank
FROM (
    SELECT distinct
    p.Customer_ID,
    EXTRACT(epoch from (MAX(Transaction_DateTime) - MIN(Transaction_DateTime))) / (COUNT(DISTINCT Transaction_ID)::FLOAT)
        / 86400.0 AS Customer_Frequency
	FROM transaction
	join customer_card c on transaction.customer_card_id = c.customer_card_id
	inner join customer p on p.customer_id = c.customer_id
	GROUP BY p.Customer_ID
));

select * from Segmentation_by_frequency_of_visits;

CREATE OR REPLACE VIEW Segmentation_by_churn_probability AS
SELECT distinct
    Customer_ID,
    aga.Customer_Inactive_Period,
    Customer_Churn_Rate,
    CASE
        WHEN Customer_Churn_Rate IS NULL THEN NULL
        WHEN Customer_Churn_Rate BETWEEN 0 AND 2 THEN 'Low'
        WHEN Customer_Churn_Rate BETWEEN 2 AND 5 THEN 'Medium'
        ELSE 'High'
    END AS Customer_Churn_Segment
FROM (
	SELECT distinct
    cip.Customer_ID,
    Customer_Inactive_Period,
    case
        when Customer_Frequency > 0 then
            cip.Customer_Inactive_Period::float / Customer_Frequency::float
        else
            Customer_Inactive_Period
    end as Customer_Churn_Rate
FROM (
	SELECT
    	p.Customer_ID,
    	abs(EXTRACT(EPOCH FROM (SELECT analysis_formation FROM analysis LIMIT 1) - MAX(t.Transaction_DateTime))) / 86400.0  AS Customer_Inactive_Period
	FROM transaction t
	JOIN customer_card c ON t.customer_card_id = c.customer_card_id
	inner JOIN customer p ON p.customer_id = c.customer_id
	GROUP BY p.Customer_ID
) as cip
inner JOIN (
	SELECT distinct
    p.Customer_ID,
    EXTRACT(epoch from (MAX(Transaction_DateTime) - MIN(Transaction_DateTime))) / (COUNT(DISTINCT Transaction_ID)::FLOAT)
        / 86400.0 AS Customer_Frequency
	FROM transaction
	join customer_card c on transaction.customer_card_id = c.customer_card_id
	inner join customer p on p.customer_id = c.customer_id
	GROUP BY p.Customer_ID
) USING (Customer_ID)
) as aga;

select * from Segmentation_by_churn_probability;

CREATE OR REPLACE VIEW Assigning_segment_number_to_client AS
SELECT distinct
    p.Customer_ID,
    (select s.segments from segments s where s.average_check = a.Customer_Average_Check_Segment
        and s.frequency_of_purchases = f.Customer_Frequency_Segment
        and s.churn_probability = cr.Customer_Churn_Segment limit 1) as Customer_Segment
FROM customer p
inner JOIN Segmentation_By_Average_Check_Size a using(Customer_ID)
inner JOIN segmentation_by_frequency_of_visits f USING (Customer_ID)
inner JOIN Segmentation_by_churn_probability cr USING (Customer_ID)
order by Customer_Segment;

select * from Assigning_segment_number_to_client;

CREATE OR REPLACE VIEW Identification_of_the_customer_main_store AS
SELECT Customer_ID, MAX(Customer_Primary_Store) AS Customer_Primary_Store
FROM (
    SELECT
        ts.Customer_ID,
        CASE
            WHEN ltt.Transaction_Store_ID IS NOT NULL THEN ltt.Transaction_Store_ID
            ELSE FIRST_VALUE(ts.Transaction_Store_ID) OVER (PARTITION BY ts.Customer_ID ORDER BY ts.Transaction_Share DESC, ts.Transaction_DateTime DESC)
        END as Customer_Primary_Store
    from (
    SELECT
    	p.Customer_ID,
    	t.Transaction_Store_ID,
    	t.transaction_datetime,
    CASE
        WHEN SUM(COUNT(t.Transaction_ID)) OVER (PARTITION BY p.Customer_ID) IS NOT NULL THEN
            COUNT(t.Transaction_ID) * 1.0 / NULLIF(SUM(COUNT(t.Transaction_ID)) OVER (PARTITION BY p.Customer_ID), 0)
        ELSE COUNT(t.Transaction_ID) * 1.0
    END AS Transaction_Share
	FROM
    	transaction t
	JOIN customer_card c ON t.customer_card_id = c.customer_card_id
	inner JOIN customer p ON p.customer_id = c.customer_id
	GROUP BY
    	p.Customer_ID,
    	t.Transaction_Store_ID,
    	t.transaction_datetime
    ) ts
    LEFT JOIN
        (SELECT Customer_ID, Transaction_Store_ID FROM (
        SELECT
    		p.Customer_ID,
    		t.Transaction_Store_ID,
    	ROW_NUMBER() OVER (PARTITION BY p.Customer_ID ORDER BY t.Transaction_DateTime DESC) as rn
		FROM transaction t
		join customer_card c on t.customer_card_id = c.customer_card_id
		inner join Customer p on p.customer_id = c.customer_id    
        ) WHERE rn <= 3 GROUP BY Customer_ID, Transaction_Store_ID HAVING COUNT(*) = 3) ltt
        ON ts.Customer_ID = ltt.Customer_ID
) subquery
GROUP BY Customer_ID;

select * from Identification_of_the_customer_main_store;

CREATE OR REPLACE VIEW customer_view AS
SELECT distinct
	c.Customer_ID,
	c1.Customer_Average_Check,
	c1.Customer_Average_Check_Segment,
	c2.Customer_Frequency,
	c2.Customer_Frequency_Segment,
	c3.Customer_Inactive_Period,
	c3.Customer_Churn_Rate,
	c3.Customer_Churn_Segment,
	c4.Customer_Segment,
	c5.Customer_Primary_Store
from Customer c
left join Segmentation_By_Average_Check_Size c1 USING (Customer_ID)
left join Segmentation_by_frequency_of_visits c2 USING (Customer_ID)
left join Segmentation_by_churn_probability c3 USING (Customer_ID)
left join Assigning_segment_number_to_client c4 USING (Customer_ID)
left join Identification_of_the_customer_main_store c5 USING (Customer_ID)

select * from customer_view;

