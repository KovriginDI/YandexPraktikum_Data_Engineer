insert into dds.dm_restaurants(restaurant_id, "name")
select
id as restaurant_id,
"name"
from stg.dm_restaurants 
where id not in (select restaurant_id from dds.dm_restaurants);