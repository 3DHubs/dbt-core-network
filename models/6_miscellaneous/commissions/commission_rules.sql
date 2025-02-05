-- This table is not materialized in the database

{{ 
    config(
        materialized='ephemeral'
    )
}}

with seed_sales_targets as (
    select * from {{ ref('seed_sales_targets') }}
    union all
    select * from {{ ref('seed_sales_targets_archive') }}
)
            select
                   hubspot_id,
                   role,
                   region,
                   monthly_target,
                   monthly_lead_target,
                   monthly_fee,
                   quarterly_fee,
                   above_quarterly_target_fee,
                   compensation_value,
                   reports_to_lead,
                   d.date,
                   own.name            as name
            from seed_sales_targets s
                     inner join int_analytics.dim_dates d on --case when s.start_date = '2022-01-01' then '2021-01-01' else s.start_date end -- for testing purpose
                                                          s.start_date <= d.date AND coalesce(s.end_date, '2026-01-01') > d.date
                 left join dbt_prod_core.hubspot_owners own on own.owner_id = s.hubspot_id
            where day = 1 --and own.name = 'Philippe Tarjan'
            order by date