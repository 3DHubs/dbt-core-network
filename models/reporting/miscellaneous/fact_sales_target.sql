with sales_target as (
        select
               hubspot_id,
               role,
               region,
               monthly_target,
               reports_to_lead,
               d.date,
               own.first_name || ' ' || own.last_name            as employee,
               lead.first_name || ' ' || lead.last_name            as sales_lead
        from {{ ref('seed_sales_targets') }} s
            inner join {{ source('data_lake', 'dim_dates') }} d on --case when s.start_date = '2022-01-01' then '2021-01-01' else s.start_date end -- for testing purpose
                                                      s.start_date <= d.date AND coalesce(s.end_date, '2023-01-01') > d.date
            left join {{ source('data_lake', 'hubspot_owners') }} own on own.owner_id = s.hubspot_id and own.is_current is true
            left join {{ source('data_lake', 'hubspot_owners') }}lead on lead.owner_id = s.reports_to_lead and lead.is_current is true
        where day = 1
        order by date
    )
    select
    s.hubspot_id,
    role,
    s.region,
    monthly_target,
    s.date as target_date,
    sales_lead,
    s.employee,
    reports_to_lead as sales_lead_id,
    max(monthly_target) over (partition by s.hubspot_id) as max_target,
    case when monthly_target <> max_target then true else false end as is_ramp_up,
    dr.employee as director,
    dr.hubspot_id as director_id

    from sales_target s
    left join (select region, hubspot_id, date, employee from sales_target where role='director') dr on dr.region = s.region and dr.date = s.date