with sales_target as (
        select
               hubspot_id,
               role,
               region,
               monthly_target,
               monthly_lead_target,
               reports_to_lead,
               d.date,
               own.name           as employee,
               lead.name           as sales_lead
        from {{ ref('seed_sales_targets') }} s
            inner join {{ source('data_lake', 'dim_dates') }} d on --case when s.start_date = '2022-01-01' then '2021-01-01' else s.start_date end -- for testing purpose
                                                      s.start_date <= d.date AND coalesce(s.end_date, '2024-01-01') > d.date
            left join {{ ref('hubspot_owners') }} own on own.owner_id = s.hubspot_id
            left join {{ ref('hubspot_owners') }}lead on lead.owner_id = s.reports_to_lead 
        where day = 1
        order by date
    ),
    employee_status as (
    select hubspot_id,
           max(coalesce(s.end_date,'2100-01-01')) as  final_end_date,
           case when final_end_date < '2100-01-01' then false else true end as active
    from {{ ref('seed_sales_targets') }} s
    group by 1)
    select
    s.hubspot_id,
    role,
    s.region,
    case when role='director' then 0 else monthly_target end as monthly_target,
    s.monthly_lead_target,
    case when role='director' then monthly_target else 0 end as monthly_director_target,
    s.date as target_date,
    sales_lead,
    s.employee,
    reports_to_lead as sales_lead_id,
    max(monthly_target) over (partition by s.hubspot_id) as max_target,
    case when monthly_target <> max_target then true else false end as is_ramp_up,
    dr.employee as director,
    dr.hubspot_id as director_id,
    es.active as employee_active_status


    from sales_target s
    left join (select region, hubspot_id, date, employee from sales_target where role='director') dr on dr.region = s.region and dr.date = s.date
    left join employee_status es on es.hubspot_id = s.hubspot_id