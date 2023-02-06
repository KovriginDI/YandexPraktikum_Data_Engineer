insert into cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
with courier_stats as
(
    select 
        d.courier_id,
        date_part('year', o.order_ts) as settlement_year,
        date_part('month', o.order_ts) as settlement_month,
        count(o.order_id) as orders_count,
        sum(o.sum) as orders_total_sum,
        avg(d.rate) as rate_avg,
        sum(o.sum * 0.25) as order_processing_fee,
        sum(d.tip_sum) as courier_tips_sum
    from dds.dm_orders o
    left join dds.dm_deliveries d on o.delivery_id = d.delivery_id
    group by 1,2,3
),
courier_order_sum as
(
    select 
        courier_id,
        settlement_year,
        settlement_month,
        sum(courier_order_sum) as courier_order_sum
    from 
    (
    with courier_rate as
    (
        select
            d.courier_id,
            date_part('year', o.order_ts) as settlement_year,
            date_part('month', o.order_ts) as settlement_month,
            d.rate,
            case 
         	    when d.rate < 4 then o.sum * 0.05
            	when 4 <= d.rate and d.rate < 4.5 then o.sum * 0.07
            	when 4.5 <= d.rate and d.rate < 4.9 then o.sum * 0.08
 	            when 4.9 <= d.rate then o.sum * 0.1
            end as courier_order_sum
        from dds.dm_orders o
        left join dds.dm_deliveries d on o.delivery_id = d.delivery_id
    )
    select
        courier_id,
        settlement_year,
        settlement_month,
        rate,
        courier_order_sum as courier_order_sum_rate,
        case 
	        when rate < 4 and courier_order_sum < 100 then 100
         	when 4 <= rate and rate < 4.5 and courier_order_sum < 150 then 150
	        when 4.5 <= rate and rate < 4.9 and courier_order_sum < 175 then 175
	        when 4.9 <= rate and courier_order_sum < 200 then 200
	        else courier_order_sum
      	end as courier_order_sum
    from courier_rate
    ) cr
    group by 1,2,3
)
select 
    cs.courier_id,
    c.name as courier_name,
    cs.settlement_year,
    cs.settlement_month,
    cs.orders_count,
    cs.orders_total_sum,
    cs.rate_avg,
    cs.order_processing_fee,
    co.courier_order_sum,
    cs.courier_tips_sum,
    co.courier_order_sum + cs.courier_tips_sum * 0.95 as courier_reward_sum
from courier_stats cs
left join courier_order_sum co 
    on cs.courier_id = co.courier_id 
    and cs.settlement_year = co.settlement_year 
    and cs.settlement_month = co.settlement_month
left join dds.dm_couriers c on cs.courier_id = c.courier_id 
;