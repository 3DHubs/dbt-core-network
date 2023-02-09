{{ config(
    post_hook=["
        {{unload_backups_to_s3('dbt_backups', 'fact_contribution_margin', True)}};
        {{unload_backups_to_s3('dbt_backups', 'fact_orders', True)}};
        {{unload_backups_to_s3('dbt_backups', 'dim_companies', True)}};
        {{unload_backups_to_s3('dbt_backups', 'dim_contacts', True)}};                
        {{unload_backups_to_s3('data_lake', 'full_order_history_events', False)}}
    "],
) }}

select getdate()