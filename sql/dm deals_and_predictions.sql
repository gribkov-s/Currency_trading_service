--create or replace view trading.deals_and_predictions as


with 

t_dim as (
select	currency, 
		concat(currency, ' (', cast("scale" as varchar(3)), ')') as currency_name
from	trading.dim_currency
where	is_basic = false
),

t_predicted as (
select	date_trunc('hour', datetime ) as date_hour_deal,
		date_trunc('hour', datetime_hour) as date_hour_predicted_for,
		row_number() over 
		(partition by currency, date_trunc('hour', datetime_hour) order by datetime desc) row_n,
		currency,
		rate
from	trading.currency_rates_predicted
),

t_fact as (
select	date_trunc('hour', datetime ) as date_hour,
		row_number() over 
		(partition by currency, date_trunc('hour', datetime ) order by datetime desc) row_n,
		currency,
		rate
from	trading.currency_rates_current
),

t_deals as (
select	fact.date_hour,
		future.date_hour_predicted_for,
		dim.currency_name,
		dim.currency,
		fact.rate rate_fact,
		future.rate rate_future,
		case when future.rate > fact.rate then -1000000 * fact.rate else
			case when future.rate < fact.rate then 1000000 * fact.rate else 0 end
			end amount_rub,
		case when future.rate > fact.rate then 1000000 else
			case when future.rate < fact.rate then -1000000 else 0 end
			end amount_cur						
from		
		(
		select	date_hour,
				currency,
				rate		
		from 	t_fact
		where	row_n = 1
		) fact		
left join
		(
		select	date_hour_deal,
				date_hour_predicted_for,
				currency,
				rate		
		from 	t_predicted
		where	row_n = 1
		) future
on	fact.date_hour = future.date_hour_deal
	and fact.currency = future.currency		
left join
		t_dim dim
on 	fact.currency = dim.currency	
),

t_deals_with_all_prev as (
select	date_hour,
		date_part('hour', date_hour - 
			lag(date_hour, 1, null) over 
			(partition by currency_name order by date_hour asc)
		) = 1 calc_profit,
		currency_name,
		currency,
		lag(rate_fact, 1, null) over 
		(partition by currency_name order by date_hour asc) prev_rate_fact,
		rate_fact,
		lag(rate_future, 1, null) over 
		(partition by currency_name order by date_hour asc) prev_rate_future,		
		rate_future,
		lag(amount_rub, 1, null) over 
		(partition by currency_name order by date_hour asc) prev_amount_rub,		
		amount_rub amount_rub,
		lag(amount_cur, 1, null) over 
		(partition by currency_name order by date_hour asc) prev_amount_cur,		
		amount_cur
from	t_deals
),

t_deals_with_prev_hour as (
select	date_hour,
		calc_profit,
		currency_name,
		currency,
		case when calc_profit = true then prev_rate_fact else null end prev_rate_fact,
		rate_fact,
		case when calc_profit = true then prev_rate_future else null end prev_rate_future,		
		rate_future,
		case when calc_profit = true then prev_amount_rub else null end prev_deal_input_amount_rub,	
		amount_rub next_deal_amount_rub,
		case when calc_profit = true then prev_amount_cur else null end prev_deal_input_amount_cur,
		amount_cur next_deal_amount_cur
from	t_deals_with_all_prev
),

t_deals_for_profit_calc as (
select	date_hour,
		currency_name,
		currency,
		prev_rate_fact,
		rate_fact,
		prev_rate_future rate_predicted,		
		rate_future,
		
		case when prev_rate_future > prev_rate_fact then 'up' else
			case when prev_rate_future < prev_rate_fact then 'down' else 
				case when prev_rate_future = prev_rate_fact then 'no motion' else null 
				end
			end
		end motion_predicted,
			
		case when rate_fact > prev_rate_fact then 'up' else
			case when rate_fact < prev_rate_fact then 'down' else 
				case when rate_fact = prev_rate_fact then 'no motion' else null 
				end
			end
		end motion_fact,
		
		case when rate_future > rate_fact then 'up' else
			case when rate_future < rate_fact then 'down' else 
				case when rate_future = rate_fact then 'no motion' else null 
				end
			end
		end motion_future,		
			
		prev_deal_input_amount_rub,
		prev_deal_input_amount_cur,
		prev_deal_input_amount_cur * rate_fact prev_deal_output_amount_rub,	
		(prev_deal_input_amount_cur * -1) prev_deal_output_amount_cur,	
		next_deal_amount_rub,
		next_deal_amount_cur
from	t_deals_with_prev_hour
)

select	date_hour,
		currency_name,
		prev_rate_fact,
		rate_predicted,			
		rate_fact,	
		rate_future,
		motion_predicted,
		motion_fact,
		motion_future,
		--prev_deal_input_amount_rub,
		--prev_deal_output_amount_rub,
		--next_deal_amount_rub,
		--prev_deal_input_amount_cur,
		--prev_deal_output_amount_cur,
		--next_deal_amount_cur,
		@(rate_predicted - prev_rate_fact) * 1000000 profit_predicted,
		prev_deal_output_amount_rub + prev_deal_input_amount_rub profit_fact,
		@(rate_future - rate_fact) * 1000000 profit_future,
		currency
from	t_deals_for_profit_calc
