
{{
    config(
        enabled=false,
        pre_hook=["
                drop table if exists {{ source('dbt_backups', 'backup_fact_contribution_margin') }};
                Create table {{ source('dbt_backups', 'backup_fact_contribution_margin') }} as
                SELECT getdate() as backup_date, * FROM dbt_prod_reporting.fact_contribution_margin;

                drop table if exists {{ source('dbt_backups', 'backup_fact_orders') }};
                Create table {{ source('dbt_backups', 'backup_fact_orders') }} as
                SELECT getdate() as backup_date, * FROM dbt_prod_reporting.fact_contribution_margin;

                drop table if exists {{ source('dbt_backups', 'backup_full_order_history_events') }};
                Create table {{ source('dbt_backups', 'backup_full_order_history_events') }} as
                SELECT getdate() as backup_date, * FROM data_lake.full_order_history_events
                "]
    )
}}

select getdate()