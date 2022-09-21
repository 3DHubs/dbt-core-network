{{
    config(
        pre_hook=["
                drop table if exists {{ source('dbt_backups', 'backup_fact_contribution_margin') }};
                create table {{ source('dbt_backups', 'backup_fact_contribution_margin') }} as
                select getdate() as backup_date, * from dbt_prod_reporting.fact_contribution_margin;

                drop table if exists {{ source('dbt_backups', 'backup_fact_orders') }};
                create table {{ source('dbt_backups', 'backup_fact_orders') }} as
                select getdate() as backup_date, * from dbt_prod_reporting.fact_orders;

                drop table if exists {{ source('dbt_backups', 'backup_dim_companies') }};
                create table {{ source('dbt_backups', 'backup_dim_companies') }} as
                select getdate() as backup_date, * from dbt_prod_reporting.dim_companies;

                drop table if exists {{ source('dbt_backups', 'backup_dim_contacts') }};
                create table {{ source('dbt_backups', 'backup_dim_contacts') }} as
                select getdate() as backup_date, * from dbt_prod_reporting.dim_contacts;                                               
                
                drop table if exists {{ source('dbt_backups', 'backup_full_order_history_events') }};
                create table {{ source('dbt_backups', 'backup_full_order_history_events') }} as
                select getdate() as backup_date, * from data_lake.full_order_history_events
                "]
    )
}}

select getdate()