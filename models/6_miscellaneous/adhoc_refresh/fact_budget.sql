-- In this query we combine the budget seed with business days data to have a daily budget per business day instead of a monthly budget. The holiday_flag field can be adjusted to exclude more days from the calculation.
with business_days as (

    select date_trunc('month', date) date,
           sum(case when not weekend_flag and not holiday_flag then 1 else 0 end) as days --todo-migration-test: replaced is false for not {field}
    from int_analytics.dim_dates group by 1
    order by 1
)

select dd.date,
       bd.days as business_days,
       case when not weekend_flag and not holiday_flag then true else false end as is_business_day, --todo-migration-test: replaced is false for not {field}
       case when not weekend_flag and not holiday_flag_us then true else false end as is_business_day_us, --todo-migration-test: replaced is false for not {field}
       b.month,
       b.integration,
       b.kpi,
       b.market,
       b.technology_name,
       case when is_business_day then b.value *1.0 / bd.days else 0 end  as value
       

FROM
int_analytics.dim_dates dd
left join business_days bd on date_trunc('month', dd."date") = bd.date
inner join {{ ref('seed_budget') }} b on b.month =  date_trunc('month', dd.date)

--todo-migration-adhoc: hardcoded reference to redshift
