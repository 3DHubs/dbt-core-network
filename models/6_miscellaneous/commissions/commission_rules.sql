-- This table is not materialized in the database

{{ 
    config(
        materialized='ephemeral'
    )
}}

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
            from dbt_clara_seed.seed_sales_targets s
                     inner join data_lake.dim_dates d on --case when s.start_date = '2022-01-01' then '2021-01-01' else s.start_date end -- for testing purpose
                                                          s.start_date <= d.date AND coalesce(s.end_date, '2024-01-01') > d.date
                 left join dbt_prod_core.hubspot_owners own on own.owner_id = s.hubspot_id
            where day = 1
            order by date