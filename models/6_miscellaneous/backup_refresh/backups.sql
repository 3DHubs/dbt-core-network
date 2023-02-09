
{{
    config(
        pre_hook=["
                {{back_up_to_datalake('dbt_prod_reporting', 'fact_contribution_margin', True)}};
                {{back_up_to_datalake('dbt_prod_reporting', 'dim_companies', True)}};
                {{back_up_to_datalake('dbt_prod_reporting', 'dim_contacts', True)}};
                {{back_up_to_datalake('dbt_prod_reporting', 'fact_orders', True)}};
                {{back_up_to_datalake('data_lake', 'full_order_history_events', False)}};
                "]
    )
}}

select getdate()