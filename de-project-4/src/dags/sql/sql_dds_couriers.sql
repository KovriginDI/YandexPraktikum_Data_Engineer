insert into dds.dm_couriers(courier_id, "name")
select
id as courier_id,
"name"
from stg.dm_couriers
where id not in (select courier_id from dds.dm_couriers)
;