with seed_sales_targets as (
    select * from {{ ref('seed_sales_targets') }}
    union all
    select * from {{ ref('seed_sales_targets_archive') }}
),
sales_target as (
        select
               hubspot_id,
               role,
               first_value(role) over (partition by hubspot_id order by d.date desc rows between unbounded preceding and unbounded following) as latest_role,
               region,
               monthly_target,
               monthly_lead_target,
               reports_to_lead,
               d.date,
               own.name           as employee,
               lead.name           as sales_lead,
               compensation_value
        from seed_sales_targets s
            inner join {{ source('int_analytics', 'dim_dates') }} d on --case when s.start_date = '2022-01-01' then '2021-01-01' else s.start_date end -- for testing purpose
                                                      s.start_date <= d.date AND coalesce(s.end_date, '2025-01-01') > d.date
            left join {{ ref('hubspot_owners') }} own on own.owner_id = s.hubspot_id
            left join {{ ref('hubspot_owners') }}lead on lead.owner_id = s.reports_to_lead 
        where day = 1
        order by date
    ),
    employee_status as (
    select hubspot_id,
           max(coalesce(s.end_date,'2100-01-01')) as  final_end_date,
           case when final_end_date < '2100-01-01' then false else true end as active,
           min(s.start_date) as start_date
    from seed_sales_targets s
    group by 1)
    select
    s.hubspot_id,
    role,
    latest_role,
    s.region,
    case when role='director' then 0 else monthly_target end as monthly_target,
    s.monthly_lead_target,
    case when role='director' then monthly_target else 0 end as monthly_director_target,
    s.date as target_date,
    sales_lead,
    s.employee,
    reports_to_lead as sales_lead_id,
    max(monthly_target) over (partition by s.hubspot_id) as max_target,
    case when compensation_value > 0 then true else false end as is_ramp_up,
    dr.employee as director,
    dr.hubspot_id as director_id,
    es.active as employee_active_status,
    start_date,
    final_end_date as end_date


    from sales_target s
    left join (select region, hubspot_id, date, employee from sales_target where role='director') dr on dr.region = s.region and dr.date = s.date
    left join employee_status es on es.hubspot_id = s.hubspot_id
