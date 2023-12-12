create or replace view View_Purchase_History as
select customer.Customer_ID as customer_id,
	   transaction.transaction_id as transaction_ID,
	   transaction.transaction_datetime as transaction_dateTime,
	   product.group_id as group_id,
	   sum(store_product.sku_purchase_price * transaction_detail.sku_amount) as group_cost,
	   sum(transaction_detail.sku_summ) as group_summ,
       sum(transaction_detail.sku_summ_paid) as group_summ_paid
from customer
left join customer_card using (customer_id)
left join transaction using (customer_card_id)
left join transaction_detail using (transaction_id)
left join product using (sku_id)
left join store_product using (sku_id, transaction_store_id)
group by customer.Customer_ID,  transaction.transaction_id,
		 transaction.transaction_datetime, product.group_id
order by customer.Customer_ID;
		 
select * from View_Purchase_History;