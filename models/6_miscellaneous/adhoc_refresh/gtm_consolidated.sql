with data as (with test as (with services as (select f.account_id,
                                                     services_count,
                                                     listagg(distinct service, ',') within group (order by service) as services
                                              from dbt_dev_analytics_jgroot_core.fact_orders_factory f
                                                       left join
                                                   (select account_id, count(distinct service) as services_count
                                                    from dbt_dev_analytics_jgroot_core.fact_orders_factory
                                                    group by 1) s on s.account_id = f.account_id
                                              group by 1, 2)
                            select ac.account_id,
                                   ac.account_name,
                                   network_account_id,
                                   network_account_name,
                                   last_order_date,
                                   order_count,
                                   order_amount,
                                   factory_last_order_date,
                                   factory_order_count,
                                   factory_order_amount,
                                   network_last_order_date,
                                   network_order_count,
                                   network_order_amount,
                                   primary_account_nr,
                                   type,
                                   s.services_count                                                           as factory_services_count,
                                   s.services                                                                 as factory_services,
                                   sn.services_count                                                          as network_services_count,
                                   sn.services                                                                as network_services,
                                   factory_services + network_services                                        as services_combined,
                                   da.region                                                                  as factory_account_region,
                                   dc.region                                                                  as network_account_region,
                                   lower(fi.industry)                                                         as factory_industry,
                                   dc.industry_mapped                                                         as network_industry,
                                   coalesce(coalesce(lower(coalesce(fi.industry, im.industry)), tfi.industry),
                                            'unknown')                                                        as industry,
                                   coalesce(lower(billing_country), dc.country_iso2)                          as country_code,
                                   pc.name                                                                    as country_name,
                                   coalesce(network_account_region, lower(factory_account_region))            as region_consolidated,
                                   datediff('day', last_order_date, '2024-10-01')                             as recency,
                                   order_count                                                                as frequency,
                                   round(order_amount * 1.00 / order_count, 2)                                as monetary,
                                   da.account_owner                                                           as factory_owner,
                                   dc.hubspot_owner_name                                                      as network_owner,
                                   case
                                       when da.employee_count_total = 'nan' then null
                                       else CAST(TRIM(TRAILING '.0' FROM da.employee_count_total) AS INT) end as factory_number_of_employees,
                                   dc.number_of_employees                                                     as network_number_of_employees

                            from dbt_dev_analytics_jgroot_core.accounts_clean ac
                                     left join services s on s.account_id = ac.account_id
                                     left join services sn on sn.account_id = ac.network_account_id
                                     left join dbt_dev_analytics_jgroot_core.dim_factory_accounts_25 da
                                               on da.account_id = ac.account_id
                                     left join dbt_prod_reporting.dim_companies dc
                                               on dc.hubspot_company_id = ac.network_account_id
                                     left join dbt_dev_analytics_jgroot_core.factory_industry fi on fi.id = da.industry
                                     left join dbt_dev_analytics_jgroot_core.network_to_factory_industry_mapping im
                                               on im.industry_mapped = dc.industry_mapped
                                     left join dbt_prod_core.prep_countries pc on lower(pc.alpha2_code) =
                                                                                  coalesce(lower(billing_country), dc.country_iso2)
                                     left join dbt_dev_analytics_jgroot_seed.temp_factory_industry tfi
                                               on tfi.account_id = ac.account_id)
              select account_id,
                     account_name,
                     network_account_id,
                     network_account_name,
                     last_order_date,
                     order_count,
                     order_amount,
                     factory_last_order_date,
                     factory_order_count,
                     factory_order_amount,
                     network_last_order_date,
                     network_order_count,
                     network_order_amount,
                     primary_account_nr,
                     type,
                     factory_services_count,
                     factory_services,
                     network_services_count,
                     network_services,
                     services_combined,
                     factory_account_region,
                     network_account_region,
                     factory_industry,
                     network_industry,
                     industry,
                     region_consolidated,
                     country_name,
                     country_code,
                     recency,
                     frequency,
                     monetary,
                      case when order_amount > 200000 and monetary > 2500 and recency <= 720
        then '1 Strategic & Production'
        when monetary > 1000 and frequency >= 3 and order_amount > 20000
        then '2 Grow & Expand'
        else
        '3 New & Transactional' end as customer_segment,
                     factory_number_of_employees,
                     network_number_of_employees,
                     factory_owner,
                     network_owner,
                     case
                         when coalesce(factory_order_amount, 0) > coalesce(network_order_amount, 0) then 'factory'
                         else 'network' end                                                           as leading_monetory,
                     case
                         when leading_monetory = 'factory' then factory_number_of_employees
                         else network_number_of_employees end                                         as number_of_employees,
                     case when leading_monetory = 'factory' then factory_owner else network_owner end as lead_owner
              from test where region_consolidated = 'emea')
select *
from data

--todo-migration-adhoc: hardcoded reference to personal schema in Redshift