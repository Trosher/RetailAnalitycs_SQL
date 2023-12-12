create or replace function group_minimum_discount(id bigint, group_2 bigint)
returns float
as $$
declare
begin
    return
    (select min(sku_discount / sku_summ) as discont
        from transaction_detail
        left join transaction using(transaction_id)
        left join customer_card using(customer_card_id)
        left join product using(sku_id)
        where sku_discount / sku_summ != 0 and customer_id = id and group_id = group_2
        group by customer_id,group_id limit 1);
end;
$$ language plpgsql;

create or replace view periods_view as
select
    customer.customer_id as customer_id,
    phs.group_id as group_id ,
    min(t.transaction_datetime) as first_group_purchase_date,
    max(t.transaction_datetime) as last_group_purchase_date,
    case when phs.group_id is not null then count(*) else null end as group_purchase,
    (((extract(epoch from (max(t.transaction_datetime) - min(t.transaction_datetime)))::float / 86400.0 + 1)*1.0) / count(*)*1.0) as group_frequency,
    group_minimum_discount(phs.customer_id, phs.group_id) as group_minimum_discount
from customer
left join View_Purchase_History phs using(customer_id)
left join transaction t using(transaction_id)
group by  customer.customer_id,phs.group_id,phs.customer_id
order by customer.customer_id,phs.group_id;

select * from periods_view;
