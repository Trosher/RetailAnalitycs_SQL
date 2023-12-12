CREATE OR REPLACE FUNCTION fnc_average_ticket_growth(
    work_mode INTEGER,
    start_date VARCHAR,
    end_date VARCHAR,
    number_transactions INTEGER,
    coefficient_increase_average_check NUMERIC,
    maximum_churn_index NUMERIC,
    maximum_share_transactions_discount NUMERIC,
    acceptable_margin_share NUMERIC
)
RETURNS TABLE (
    Customer_ID BIGINT,
    Required_Check_Measure NUMERIC,
    Group_Name varchar,
    Offer_Discount_Depth float
) AS $$
DECLARE
    start_period_date TIMESTAMP;
    end_period_date TIMESTAMP;
BEGIN
    SET datestyle TO dmy;
    SELECT
        CASE
            WHEN start_date::TIMESTAMP < min(transaction_datetime) THEN
                min(transaction_datetime)
            ELSE start_date::TIMESTAMP
        END,
        CASE
            WHEN end_date::TIMESTAMP > max(transaction_datetime)  THEN
                 max(transaction_datetime)
            ELSE end_date::TIMESTAMP
        END
    FROM View_Purchase_History
    INTO start_period_date, end_period_date;
    RETURN QUERY
        SELECT distinct pi.customer_id,
            round(sum(table1.tr_sum) OVER (PARTITION BY pi.customer_id)
            / count(*) OVER (PARTITION BY pi.customer_id) * coefficient_increase_average_check, 2) AS Required_Check_Measure,
            table2.group_name,
            table2.Offer_Discount_Depth
        FROM customer pi
        JOIN (select phv.*, row_number() OVER (PARTITION BY phv.customer_id) AS rn
              FROM (SELECT ph.customer_id, ph.transaction_id, ph.transaction_datetime, sum(ph.group_summ) AS tr_sum
                	FROM View_Purchase_History ph
                	GROUP BY ph.customer_id, ph.transaction_id, ph.transaction_datetime
            	    ) phv
              WHERE phv.transaction_id IS NOT NULL
        ) table1 ON pi.customer_id = table1.customer_id AND
            (CASE
                WHEN work_mode = 1 THEN table1.transaction_datetime BETWEEN start_period_date AND end_period_date
                WHEN work_mode = 2 THEN table1.rn <= number_transactions
            END)
            
        left JOIN (SELECT g.customer_id, g.group_id, sg.group_name, avg((ph.group_summ - ph.group_cost) / ph.group_summ),
                   ceil(g.group_minimum_discount / 0.05) * 5 as Offer_Discount_Depth,
                   row_number() OVER (PARTITION BY g.customer_id, g.group_id ORDER BY group_affinity_index DESC) AS rn
              FROM Groups_View g
              LEFT JOIN View_Purchase_History ph ON ph.customer_id = g.customer_id and g.group_id = ph.group_id
              LEFT JOIN product_group sg ON g.group_id = sg.group_id
              WHERE g.group_churn_rate <= maximum_churn_index AND
                    g.group_discount_share * 100 <= maximum_share_transactions_discount
              GROUP BY g.customer_id, g.group_id, sg.group_name, g.group_affinity_index, g.group_minimum_discount
              HAVING (avg((ph.group_summ - ph.group_cost) / ph.group_summ) * acceptable_margin_share >=
                      ceil(g.group_minimum_discount / 0.05) * 5)
              ORDER BY g.customer_id, g.group_affinity_index DESC
        ) table2 ON table2.customer_id = table1.customer_id AND table2.rn = 1
        ORDER BY table2.Offer_Discount_Depth, Required_Check_Measure;
END;
$$ LANGUAGE plpgsql;

select * from fnc_average_ticket_growth(1, '02.01.2021', '02.10.2021', 0, 1.15, 3, 70, 30);
select * from fnc_average_ticket_growth(2, '02.01.2021', '02.10.2021', 100, 1.15, 3, 70, 30);