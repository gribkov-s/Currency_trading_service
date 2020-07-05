--create or replace view trading.currency_rates_current_hourly as


with h_n as (
select 0 n union
select 1 n union
select 2 n union
select 3 n union
select 4 n union
select 5 n union
select 6 n union
select 7 n union
select 8 n union
select 9 n union
select 10 n union
select 11 n union
select 12 n union
select 13 n union
select 14 n union
select 15 n union
select 16 n union
select 17 n union
select 18 n union
select 19 n union
select 20 n union
select 21 n union
select 22 n union
select 23
),

t_daily as (
select 	date + interval '1 hour' * n date_hour,
		currency,
		rate
from 	trading.currency_rates_daily daily
cross join
		h_n h_n
where not "date" in (
					select  date_trunc('day', datetime )
					from	trading.currency_rates_current
					)
),

t_fact_current as (
select	date_trunc('hour', datetime ) as date_hour,
		date_trunc('day', datetime ) as date,
		row_number() over 
		(partition by currency, date_trunc('hour', datetime ) order by datetime desc) row_n,
		currency,
		rate
from	trading.currency_rates_current
),

t_predicted as (
select	date_trunc('hour', datetime) as date_hour_future,
		datetime_hour date_hour,
		row_number() over 
		(partition by currency, date_trunc('hour', datetime_hour) order by datetime desc) row_n,
		currency,
		rate rate_predicted
from	trading.currency_rates_predicted
),

t_fact_current_gen_hours as (
select	date_hour,
		date_hour_next,
		date_hour_prev,		
		date + interval '1 hour' * n date_hour_gen,
		currency,
		rate
from	(
		select	date_hour,
				lead(date_hour, 1, null) over 
				(partition by currency, date order by date_hour asc) date_hour_next,
				lag(date_hour, 1, null) over 
				(partition by currency, date order by date_hour asc) date_hour_prev,				
				date,
				currency,
				rate
		from	t_fact_current
		where	row_n = 1
		) f
cross join
		h_n h_n
),


t_fact_by_hours as (
select	date_hour_gen date_hour,
		currency,
		rate
from	t_fact_current_gen_hours
where	   (date_hour_gen >= date_hour and date_hour_gen < date_hour_next)
		or (date_hour_next is null and date_hour_gen >= date_hour)
		or (date_hour_prev is null and date_hour_gen <= date_hour)

union all 

select	date_hour,
		currency,
		rate
from	t_daily	
)

select	f.date_hour,
		d.currency,
		d.id currency_id,
		cast(f.rate as numeric(13,8)) rate,
		p.rate_predicted,
		p_fut.rate_predicted rate_future
from	t_fact_by_hours f
	left join
		trading.dim_currency d
on f.currency = d.currency	
	left join
			(
			select	date_hour,
					currency,
					rate_predicted
			from	t_predicted
			where	row_n = 1		
			) p
on	f.date_hour = p.date_hour
	and f.currency = p.currency	

	left join
			(
			select	date_hour_future,
					currency,
					rate_predicted
			from	t_predicted
			where	row_n = 1		
			) p_fut
on	f.date_hour = p_fut.date_hour_future
	and f.currency = p_fut.currency		

where f.date_hour between  now() - interval '1 month' and now()
