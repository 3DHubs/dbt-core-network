                select fo.supplier_id,
                       d.date                           as production_at,
                       fo.order_uuid,
                       sum(subtotal_sourced_amount_usd) as subtotal_sourced_amount_usd,
                       count(1)                         as orders,
                       null as seen_rate,
                       null as seen_volume,
                       null as positive_response_rate,
                       null as positive_volume,
                       null as is_shipped_on_time_by_supplier
                from dbt_prod_reporting.fact_orders fo
                         inner join
                     int_analytics.dim_dates d
                     on fo.sourced_at <= d.date
                         and coalesce(fo.order_shipped_at,promised_shipping_at_by_supplier) > d.date
                         and order_status != 'canceled'
                         and d.date>= date_add('year',-3,getdate())
                group by 1,2,3
                union all
                select sa_supplier_id                                                                             as supplier_id,
                       fact_rda_behaviour.sa_assigned_at::date,
                       fo.order_uuid,
                       null as subtotal_sourced_amount_usd,
                       null as orders,
                       (COUNT(CASE
                                  WHEN (fact_rda_behaviour.sa_first_seen_at IS NOT NULL) THEN fact_rda_behaviour.sa_uuid
                                  ELSE NULL END)) * 1.0 /
                       nullif((COUNT(DISTINCT fact_rda_behaviour.sa_uuid)), 0)                                    as seen_rate,
                       COUNT(CASE
                                 WHEN (fact_rda_behaviour.sa_first_seen_at IS NOT NULL) THEN fact_rda_behaviour.sa_uuid
                                 ELSE NULL END)                                                                   as seen_volume,
                       (COUNT(CASE
                                  WHEN (fact_rda_behaviour.response_type IN ('accepted', 'countered'))
                                      THEN fact_rda_behaviour.sa_uuid
                                  ELSE NULL END)) * 1.0 /
                       nullif((COUNT(DISTINCT fact_rda_behaviour.sa_uuid)), 0)                                    as positive_response_rate,
                       (COUNT(CASE
                                  WHEN (fact_rda_behaviour.response_type IN ('accepted', 'countered'))
                                      THEN fact_rda_behaviour.sa_uuid
                                  ELSE NULL END))                                                                 as positive_volume,
                        null as is_shipped_on_time_by_supplier

                FROM dbt_prod_reporting.fact_auction_behaviour AS fact_rda_behaviour
                left join dbt_prod_reporting.fact_orders fo on fo.order_uuid = fact_rda_behaviour.order_uuid
                WHERE ((fact_rda_behaviour.is_rfq = 'false'))
                   and fact_rda_behaviour.sa_assigned_at >= date_add('year',-3,getdate())
                group by 1, 2,3
                union all
                select fo.supplier_id,
                       fot.promised_shipping_at_by_supplier                           as date,
                       fo.order_uuid,
                       null as subtotal_sourced_amount_usd,
                       null as orders,
                       null as seen_rate,
                       null as seen_volume,
                       null as positive_response_rate,
                       null as positive_volume,
                       fot.is_shipped_on_time_by_supplier

                from dbt_prod_reporting.fact_orders fo
                left join dbt_prod_reporting.fact_otr fot ON fo.order_uuid = fot.order_uuid
                where fot.promised_shipping_at_by_supplier >= dateadd('year',-3,current_date()) --todo-migration-test

--todo-migration-adhoc: hardcoded references to Redshift schemas
    
