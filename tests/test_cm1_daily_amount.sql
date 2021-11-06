select dd.date,
       sum(coalesce(amount, 0)) total_amount
from {{ source('data_lake', 'dim_dates') }} dd
left join (select recognized_date as date, sum(amount_usd) as amount
    from{{ ref('fact_contribution_margin' )}} group by 1 ) rev
on rev.date = dd.date
where dd.date = dateadd(day, -1, getdate()::date) and extract(dayofweek from getdate()) not in (6,0)
group by 1
having not (total_amount > 0)

