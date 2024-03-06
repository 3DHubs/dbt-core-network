select dd.date,
        sum(case when type = 'cost' then coalesce(amount, 0) else 0 end) as recognized_cost,
        sum(case when type = 'revenue' then coalesce(amount, 0) else 0 end) as recognized_revenue
from {{ source('int_analytics','dim_dates') }} as dd
left join (select recognized_date as date, type as type, sum(amount_usd) as amount
    from {{ ref('fact_contribution_margin') }} group by 1, 2) rev
on rev.date = dd.date
where dd.date = dateadd(day, -1, getdate()::date) and extract(dayofweek from getdate()) not in (6,0)
group by 1
having not (recognized_cost < 0 or recognized_revenue > 0)