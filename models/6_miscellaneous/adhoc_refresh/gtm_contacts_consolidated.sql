with contacts as (with factory_account as (select f.account_id,
                                contact_email,
                                account_name,
                                max(case
                                        when ordered_date > date_add('months', -12, getdate()) then ordered_date
                                        else '2000-01-01' end)                                             as last_order_date_f,
                                max(ordered_date::timestamp) as factory_last_order_date,
                                count(distinct sales_order_id) as factory_order_count,
                                sum(replace(line_amount_usd, 'nan', 0)::decimal(10, 2))   as factory_order_amount,
                                row_number()
                                over (partition by contact_email order by last_order_date_f desc, factory_order_amount desc) as primary_account_nr
                         from dbt_dev_analytics_jgroot_core.fact_orders_factory f
                                  left join dbt_dev_analytics_jgroot_core.dim_factory_accounts_25 a
                                            on a.account_id = f.account_id
                         where factory_network = 'Factory'
                           and line_status != 'Canceled'
                           and replace(line_amount_usd, 'nan', 0)::decimal(10, 2) > 0
                           and  ordered_date >= '2022-01-01'
                         group by 1, 2, 3),
     network_account as (select fo.hubspot_company_id,
                                c.email,
                                dc.name,
                                dc.closed_sales_usd as hubspot_company_order_amount,
                                max(case
                                        when sourced_at > date_add('months', -12, getdate()) then sourced_at
                                        else '2000-01-01' end)                                                    as last_order_date_n,
                                max(sourced_at::timestamp) as last_order_date,
                                count(distinct order_uuid) as order_count,
                                sum(subtotal_sourced_amount_usd)                                                  as order_amount,
                                row_number()
                                over (partition by c.email order by last_order_date_n desc, order_amount desc)              as primary_account_nr
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
                           --and fo.hubspot_company_id in ('668159799')
--and email ~ 'timberline-designs.com'
                         group by 1, 2, 3,4),
     integration as (select account_id,
                            n.hubspot_company_id as network_account_id,
                            contact_email as email,
                            account_name as factory_account_name,
                            f.factory_last_order_date as factory_last_order_date,
                            f.factory_order_count  as factory_order_count,
                            f.factory_order_amount as factory_order_amount,
                            name as network_account_name,
                            n.last_order_date as network_last_order_date,
                            n.order_count as network_order_count,
                            n.order_amount as network_order_amount,

                            rank() OVER (PARTITION BY account_id ORDER BY hubspot_company_order_amount DESC) AS sales_rank,
                            concat(account_id, contact_email) as account_identifier

                     from factory_account f
                              inner join network_account n
                                         on f.contact_email = n.email
                                                and f.primary_account_nr = 1 and n.primary_account_nr = 1
                     --where f.account_id ='001i000001AlqCCAAZ'
                     ),-- select * from integration;
     integration_prep as (
         select account_id,
                email,
                factory_account_name,
                network_account_id,
                i.network_account_name,
                -- greatest(factory_last_order_date, network_last_order_date) as last_order_date,
                sum(factory_order_count + i.network_order_count) as order_count,
                sum(factory_order_amount + i.network_order_amount) as order_amount,
                max(case when factory_last_order_date > i.network_last_order_date then factory_last_order_date else i.network_last_order_date end) as factory_last_order_date,
                sum(factory_order_count) as factory_order_count,
                sum(factory_order_amount) as factory_order_amount,
                max(i.network_last_order_date) as network_last_order_date,
                sum(i.network_order_count) as network_order_count,
                sum(i.network_order_amount) as network_order_amount_total,
                row_number() over (partition by network_account_id order by network_order_amount_total  desc)              as primary_account_nr
         from integration i
         where sales_rank =1
         group by 1,2,3,4,5), --select * from integration_prep;,
     final_integration as (
         select i.account_id,
                i.email,
                i.factory_account_name,
                network_account_id,
                i.network_account_name,
                order_count + coalesce(c.network_order_count,0) + coalesce(f.factory_order_count,0) as order_count,
                order_amount + coalesce(c.network_order_amount,0) + coalesce(f.factory_order_amount,0) as order_amount,
               greatest(i.factory_last_order_date,f.factory_last_order_date) as factory_last_order_date_p,
               factory_last_order_date_p as factory_last_order_date,
                i.factory_order_count + coalesce(f.factory_order_count,0) as factory_order_count,
                i.factory_order_amount + coalesce(f.factory_order_amount,0) as factory_order_amount,
               greatest(i.network_last_order_date, c.network_last_order_date)  as network_last_order_date_p,
               network_last_order_date_p as network_last_order_date,
               greatest(network_last_order_date_p, factory_last_order_date_p)  as last_order_date,
                i.network_order_count + coalesce(c.network_order_count,0) as network_order_count,
                network_order_amount_total + coalesce(c.network_order_amount,0) as network_order_amount,
                primary_account_nr,
                f.contact_email as account_email,
                c.email as network_email
                --, f.*,c.*,
         from integration_prep i
                  left join (
             select
                 account_id,
                 account_name,
                 contact_email,
                 max(factory_last_order_date) as factory_last_order_date,
                 sum(factory_order_count) as factory_order_count,
                 sum(factory_order_amount) as factory_order_amount
             from factory_account f
             where concat(account_id,contact_email) not in (select account_identifier from integration where sales_rank =1 )
             group by 1,2,3
         ) f on f.account_id = i.account_id
                  left join (
             select
                 hubspot_company_id,
                 name as network_account_name,
                 email,
                 max(last_order_date) as network_last_order_date,
                 sum(order_count) as network_order_count,
                 sum(order_amount) as network_order_amount
             from network_account n
             where email not in (select email from integration where sales_rank=1)
             group by 1,2,3
         ) c on c.hubspot_company_id = i.network_account_id and i.primary_account_nr=1
     ), --select * from final_integration;,
     factory_agg as (
         select
             account_id,
             contact_email as email,
             account_name as factory_account_name,
             max(factory_last_order_date) as factory_last_order_date,
             sum(factory_order_count) as factory_order_count,
             sum(factory_order_amount) as factory_order_amount
         from factory_account
         where account_id not in (select account_id from final_integration)
         group by 1,2,3
     ),
     network_agg as (
         select
             hubspot_company_id,
             email,
             name as network_account_name,
             max(last_order_date) as network_last_order_date,
             sum(order_count) as network_order_count,
             sum(order_amount) as network_order_amount
         from network_account
         where hubspot_company_id not in (select network_account_id from final_integration)
         group by 1,2,3
     )
select distinct account_id,
       email,
       network_account_id::varchar as network_account_id,
       primary_account_nr,
       account_email,
       network_email
from final_integration
union all
select distinct
    account_id,
    email,
    null::varchar  as      network_account_id,
    null::int as primary_account_nr,
    null as account_email,
    null as network_email
from factory_agg
union all
select distinct
    n.hubspot_company_id::varchar as account_id,
    email,
    hubspot_company_id::varchar  as      network_account_id,
    null::int,
    null as account_email,
    null as network_email
from network_agg n),
--select * from contacts where account_id='001i000001MuLOsAAN'
combine as (
select account_id, email, concat(account_id, coalesce(email,'no_email')) as id from contacts
union all 
select account_id, network_email as email, concat(account_id, network_email) as id from contacts
union all 
select account_id, account_email as email, concat(account_id, account_email) as id from contacts
)
select distinct * from combine  --where account_id='0013100001elQvFAAU'
