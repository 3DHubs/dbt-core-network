-- This model is built to forecast the current month recognized revenue.

-- Check the average run rate of orders in the past 6 weeks on business days
with
    prep_run_rate as (
        select
            getdate()::date as start_date,
            date_trunc('month', dateadd(month, 12, current_date)) as end_date, --todo-migration-test dateadd current_date
            lead_time,
            technology_name,
            destination_region,
            sum(subtotal_sourced_amount_usd) * 1.0 / 30 as subtotal_sourced_amount_usd,
            sum(is_sourced::int) * 1.0 / 30 as orders
        from {{ ref("fact_orders") }}
        where            
            sourced_at >= dateadd(day, -42, current_date) --todo-migration-test dateadd current_date
            and sourced_at::date < getdate()::date
            and date_part(dow, sourced_at) not in (0, 6)
            and subtotal_sourced_amount_usd < 50000
        group by 1, 2, 3, 4,5
    ),
    -- Use that runrate to forecast the sourced orders in the current month for days that are left
    forecast_sourced as (
        select p.*, d.date as sourced_at
        from prep_run_rate p
        inner join
            {{ source("int_analytics", "dim_dates") }} d 
            on p.start_date <= d.date
            and p.end_date > d.date
        where d.weekend_flag = false and d.holiday_flag = false 
    ),
    -- Get the historic time to recognize values per leadtime and technology
    recognized_prep as (
        select
            lead_time,
            technology_name,
            destination_region,
            --todo-migration-test datediff
            datediff('days', sourced_at, recognized_at) as time_to_recognize,
            sum(subtotal_sourced_amount_usd) as sourced_sales,
            sum(recognized_revenue_amount_usd) as recognized_amount,
            recognized_amount *1.0 / nullif(sourced_sales,0) as margin_leakage,
            ratio_to_report(sourced_sales) over (
                partition by technology_name, lead_time
            )*margin_leakage as percent_of_total
        from {{ ref("fact_orders") }}
        where
            true
            and sourced_at >= dateadd(month, -15, current_date) --todo-migration-test dateadd current_date
            and sourced_at < dateadd(month, -3, current_date) --todo-migration-test dateadd current_date
            and subtotal_sourced_amount_usd < 50000
        group by 1, 2, 3,4
    ),
    -- For the days that are coming and sourced sales that on average should come in the month,
    -- estimate the recognized amount that can be expected
forecast_recognized as (
        select
            date_trunc(
                'day', dateadd(day, coalesce(time_to_recognize, (fo.lead_time + 7)), sourced_at) --todo-migration-test dateadd
            ) estimated_recognized_at,
            fo.lead_time,
            fo.technology_name,
            fo.destination_region,
            sum(subtotal_sourced_amount_usd * coalesce(percent_of_total,1) * 0.98) as recognized_amount
        from forecast_sourced fo
        left join
            recognized_prep rp
            on rp.lead_time = fo.lead_time
            and fo.technology_name = rp.technology_name
        where
            date_trunc('month', dateadd(day, time_to_recognize, sourced_at)) 
            >= date_trunc('month', current_date) --todo-migration-test dateadd current_date
        group by 1,2,3,4
    ),
    -- For the orders that were already sourced, get the expected recognized amounts for this month.
    recognized_actual as (
        select
            date_trunc(
                'day', dateadd(day, coalesce(time_to_recognize, (fo.lead_time + 7)), sourced_at) --todo-migration-test dateadd
            ) estimated_recognized_at,
            fo.lead_time,
            fo.technology_name,
            fo.destination_region,
            sum(subtotal_sourced_amount_usd * coalesce(percent_of_total,1) * 0.98) as recognized_amount
        from  {{ ref("fact_orders") }} fo
        left join
            recognized_prep rp
            on rp.lead_time = fo.lead_time
            and fo.technology_name = rp.technology_name
        where
            date_trunc('month', dateadd(day, time_to_recognize, sourced_at)) 
            >= date_trunc('month', current_date) --todo-migration-test dateadd current_date
            and subtotal_sourced_amount_usd < 50000
            and percent_of_total > 0
        group by 1,2,3,4
    ),
    -- take actuals for 50K+ deals when recognized
    fiftyk_plus as (
        select 
            fcm.recognized_date as estimated_recognized_at,
            fo.lead_time,
            fo.technology_name,
            destination_region,
            coalesce(sum(case when (fcm.type = 'revenue') then fcm.amount_usd else null end), 0) as recognized_amount

        from {{ ref("fact_orders") }}  fo
    left join {{ ref("fact_contribution_margin") }} fcm ON fo.order_uuid = fcm.order_uuid
    where date_trunc('month', recognized_date) >= date_trunc('month', current_date) --todo-migration-test current_date
    and subtotal_sourced_amount_usd >= 50000
    group by 1,2,3,4)
        select *
        from recognized_actual
        union all
        select *
        from forecast_recognized
        union all
        select *
        from fiftyk_plus
