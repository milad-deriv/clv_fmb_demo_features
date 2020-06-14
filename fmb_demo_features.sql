with fmb_vr as (
select binary_user_id,
account_id,
loginid,
purchase_time,
sell_price,
buy_price,
underlying_symbol,
bet_class,
first_value(sell_price - buy_price) over (partition by binary_user_id order by purchase_time asc) as first_PnL,
case when SUBSTR(underlying_symbol,1,1) in ('R', 'C', 'B') then 'Non-financial' else 'Financial' end as symbol_type,
case when sell_price > buy_price then 'Win' else 'Lose' end as win_or_loss
from `business-intelligence-240201.bi.vr_transaction_first24h` fmb 
where DATE(purchase_time)>='2020-05-10' and id is not null
),
major_symbol_tbl as (
select distinct foo.binary_user_id, first_value(foo.underlying_symbol) over (partition by binary_user_id order by cnt desc) as major_symbol from
(select binary_user_id, underlying_symbol, count(*) as cnt from fmb_vr
group by binary_user_id,underlying_symbol
order by binary_user_id, cnt desc) as foo 
),
major_won_symbol_tbl as (
select distinct foo.binary_user_id, first_value(foo.underlying_symbol) over (partition by binary_user_id order by profit desc) as major_won_symbol from
(select binary_user_id, underlying_symbol, sum(sell_price - buy_price) as profit from fmb_vr where sell_price >= buy_price
group by binary_user_id,underlying_symbol
order by binary_user_id, profit desc) as foo 
),
major_bet_class_tbl as (
select distinct foo.binary_user_id, first_value(foo.bet_class) over (partition by binary_user_id order by cnt desc) as major_bet_class from
(select binary_user_id, bet_class, count(*) as cnt from fmb_vr
group by binary_user_id,bet_class
order by binary_user_id, cnt desc) as foo 
)
SELECT fmb.binary_user_id,
sum(sell_price - buy_price) as PnL,
count(fmb.binary_user_id) as number_of_trades,
sum(case when symbol_type='Financial' then 1 else 0 end) as number_of_financial_trades,
coalesce(safe_divide(sum(case when win_or_loss='Win' then 1 else 0 end)*100,count(*)),0) as win_rate,
coalesce(safe_divide(sum(sell_price - buy_price)*100,sum(buy_price)),0) as profit_percentage,
coalesce(safe_divide(sum(case when symbol_type='Financial' and win_or_loss='Win' then 1 else 0 end)*100,sum(case when symbol_type='Financial' then 1 else 0 end)),0) as win_rate_financial,
coalesce(safe_divide(sum(case when symbol_type='Non-financial' and win_or_loss='Win' then 1 else 0 end)*100,sum(case when symbol_type='Non-financial' then 1 else 0 end)),0) as win_rate_non_financial,
count(distinct symbol_type) as number_of_traded_markets,
case when count(distinct symbol_type)>1 then 'both' else min(symbol_type) end as traded_markets,
sum(case when win_or_loss='Win' then 1 else 0 end) number_of_wins,
count(distinct underlying_symbol) as number_of_traded_symbols,
timestamp_diff(max(purchase_time),min(purchase_time),HOUR) as active_hours,
max(major_symbol) as major_symbol,
max(major_bet_class) as major_bet_class,
max(major_won_symbol) as major_won_symbol,
coalesce(safe_divide(sum(sell_price - buy_price),count(*)),0) as mean_profit,
min(first_PnL) as first_PnL,
greatest(max(sell_price - buy_price),0) as largest_win,
least(min(sell_price - buy_price),0) largest_lose,
count(distinct account_id) as number_of_accounts
FROM fmb_vr fmb
join major_symbol_tbl ms on ms.binary_user_id=fmb.binary_user_id
join major_bet_class_tbl mb on mb.binary_user_id = fmb.binary_user_id
join major_won_symbol_tbl mw on mw.binary_user_id = fmb.binary_user_id
group by binary_user_id 
