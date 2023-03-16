-- This model is built to forecast the current month recognized revenue.

-- Check the average run rate of orders in the past 6 weeks on business days
with
    prep_run_rate as (
        select
            getdate()::date as start_date,
            date_trunc('month', date_add('month', 1, getdate())) as end_date,
            lead_time,
            technology_name,
            sum(subtotal_closed_amount_usd) * 1.0 / 30 as subtotal_closed_amount_usd,
            sum(is_closed::int) * 1.0 / 30 as orders
        from {{ ref("fact_orders") }}
        where
            sourced_at >= date_add('days', -42, getdate())
            and sourced_at::date < getdate()::date
            and date_part(dow, sourced_at) not in (0, 6)
        group by 1, 2, 3, 4
    ),
    -- Use that runrate to forecast the sourced orders in the current month for days that are left
    forecast_sourced as (
        select p.*, d.date as sourced_at
        from prep_run_rate p
        inner join
            {{ source("data_lake", "dim_dates") }} d
            on p.start_date <= d.date
            and p.end_date > d.date
        where d.weekend_flag is false and d.holiday_flag is false
    ),
    -- Get the historic time to recognize values per leadtime and technology
    recognized_prep as (
        select
            lead_time,
            technology_name,
            date_diff('days', sourced_at, recognized_at) as time_to_recognize,
            sum(subtotal_closed_amount_usd) as closed_sales,
            sum(recognized_revenue_amount_usd) as recognized_amount,
            recognized_amount *1.0 / nullif(closed_sales,0) as margin_leakage,
            ratio_to_report(closed_sales) over (
                partition by technology_name, lead_time
            )*margin_leakage as percent_of_total
        from {{ ref("fact_orders") }}
        where
            true
            and sourced_at >= date_add('months', -15, getdate())
            and sourced_at < date_add('months', -3, getdate())
        group by 1, 2, 3
    ),
    -- For the days that are coming and sourced sales that on average should come in the month,
    -- estimate the recognized amount that can be expected
    forecast_recognized as (
        select
            date_trunc(
                'day', date_add('days', time_to_recognize, sourced_at)
            ) estimated_recognized_at,
            fo.lead_time,
            fo.technology_name,
            sum(subtotal_closed_amount_usd * percent_of_total * 0.98) as recognized_amount
        from forecast_sourced fo
        inner join
            recognized_prep rp
            on rp.lead_time = fo.lead_time
            and fo.technology_name = rp.technology_name
        where
            date_trunc('month', date_add('days', time_to_recognize, sourced_at))
            = date_trunc('month', getdate())
        group by 1,2,3
    ),
    -- For the orders that were already sourced, get the expected recognized amounts for this month.
    recognized_actual as (
        select
            date_trunc(
                'day', date_add('days', time_to_recognize, sourced_at)
            ) estimated_recognized_at,
            fo.lead_time,
            fo.technology_name,
            sum(subtotal_closed_amount_usd * percent_of_total * 0.98) as recognized_amount
        from  {{ ref("fact_orders") }} fo
        inner join
            recognized_prep rp
            on rp.lead_time = fo.lead_time
            and fo.technology_name = rp.technology_name
        where
            date_trunc('month', date_add('days', time_to_recognize, sourced_at))
            = date_trunc('month', getdate())
        group by 1,2,3
    )
        select *
        from recognized_actual
        union all
        select *
        from forecast_recognized
