insert into dds.dm_orders(order_id, order_ts, delivery_id, sum)
select
order_id,
order_ts,
delivery_id,
sum
from stg.dm_deliveries dd 
where order_id not in (select order_id from dds.dm_orders)
;