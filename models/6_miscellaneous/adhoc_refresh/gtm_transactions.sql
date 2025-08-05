with factory as (
select f.account_id, f.ordered_date::date as ordered_date, f.service, f.contact_email,
sum(replace(line_amount_usd, 'nan', 0)::decimal(10, 2))   as factory_order_amount,
count(distinct sales_order_id) as order_count
     from dbt_dev_analytics_jgroot_core.fact_orders_factory f
                                  left join dbt_dev_analytics_jgroot_core.dim_factory_accounts_25 a
                                            on a.account_id = f.account_id
                         where factory_network = 'Factory'
                           and line_status != 'Canceled'
                           and replace(line_amount_usd, 'nan', 0)::decimal(10, 2) > 0
                           and  ordered_date >= '2022-01-01'
                          -- and lower(a.region) = 'emea'
                         group by 1,2,3,4),
network as (select fo.hubspot_company_id,fo.technology_name, email,
                                sourced_at::date as ordered_date,
                                count(distinct order_uuid) as order_count,
                                sum(subtotal_sourced_amount_usd)                                                  as order_amount
                         from dbt_prod_reporting.fact_orders fo
                                  left join dbt_prod_core.stg_hs_contacts_attributed_prep c
                                            on c.contact_id = fo.hubspot_contact_id
                                  left join dbt_prod_reporting.dim_companies dc
                                            on dc.hubspot_company_id = c.hs_company_id
                         where fo.hubspot_company_id is not null
                           and sourced_at is not null
                           and fo.order_status != 'canceled'
                           and sourced_at >= '2022-01-01' and sourced_at < '2025-01-01'
                           and dc.name is not null
                           --and dc.region = 'emea' 
                           group by 1,2,3,4),
network_clean as (
select case when  type='factory_network' then gcc.account_id else hubspot_company_id::varchar end as account_id,
       n.email,
       concat(case when type='factory_network' then gcc.account_id else hubspot_company_id::varchar end,
              coalesce(n.email, 'no_email'))                                                             as id,
       n.order_amount,
       n.order_count,
       n.ordered_date,
       case when n.technology_name in ('3DP', 'CNC', 'IM', 'SM') then n.technology_name else 'Other' end as service,
       'network'                                                                                         as fulfillment_type, primary_account_nr,
       rank()
                                over (partition by n.email order by primary_account_nr asc) as primary_account_nr_rank
from network n
         left join dbt_dev_analytics_jgroot_core.gtm_contacts_consolidated gcc on gcc.email = n.email
         left join dbt_dev_analytics_jgroot_core.gtm_consolidated gc on gc.account_id = gcc.account_id)
select  f.account_id, f.contact_email, concat(f.account_id, f.contact_email) as id,   f.factory_order_amount as order_amount, f.order_count, f.ordered_date, case when f.service ='CNC Machining' then 'CNC'
    when f.service = '3D Printing' then '3DP'
    when f.service = 'Injection Molding' then 'IM'
    when f.service = 'Sheet Metal' then 'SM' else 'Other'
        end as service
        , 'factory' as fulfillment_type  from factory f
union all
select account_id,
       email,
       id,
       order_amount,
       order_count,
       ordered_date,
       service,
       fulfillment_type

from network_clean where primary_account_nr_rank = 1

--todo-migration adhoc