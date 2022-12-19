{{ config(
    post_hook=["
        {{unload_backups_to_s3('fact_contribution_margin')}};
        {{unload_backups_to_s3('fact_orders')}};
        {{unload_backups_to_s3('dim_companies')}};
        {{unload_backups_to_s3('dim_contacts')}};                
        {{unload_datalake_backups_to_s3('full_order_history_events')}}
    "],
) }}

select getdate()