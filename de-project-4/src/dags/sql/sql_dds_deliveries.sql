insert into dds.dm_deliveries(delivery_id, delivery_ts, courier_id, address, rate, tip_sum)
select
delivery_id,
delivery_ts,
courier_id,
address,
rate,
tip_sum
from stg.dm_deliveries dd 
where delivery_id not in (select delivery_id from dds.dm_deliveries)
;