with account as (select creator_account_id,
                        max(factory_seller) as factory_seller,
                        max(account_name)     as account_name,
                        max(industry)         as industry,
                        max(replace(order_count_2022,'#N/A',0)::int) as factory_order_count_2022,
                        max(replace(order_count_2023,'#N/A',0)::int) as factory_order_count_2023,
                        max(replace(order_count_2024,'#N/A',0)::int) as factory_order_count_2024,
                        max(replace(order_count,'#N/A',0)::int)      as factory_order_count
                 from dbt_dev_analytics_jgroot_seed.seed_gtm_emea_2024
                 --where quote_creator_email = 'dominic.haider@yageo.com'
                 group by 1),
     factory as (select factory_seller,
                        creator_account_id,
                        account_name,
                        g.quote_creator_email as quote_creator_email,
                        replace(REPLACE(REPLACE("total_price_2022", '$', ''), ',', ''), '-',
                                '0')::int                                                         as total_price_2022,
                        replace(REPLACE(REPLACE("total_price_2023", '$', ''), ',', ''), '-',
                                '0')::int                                                         as total_price_2023,
                        replace(REPLACE(REPLACE("total_price_2024", '$', ''), ',', ''), '-',
                                '0')::int                                                         as total_price_2024,
                        replace(REPLACE(REPLACE("total_price", '$', ''), ',', ''), '-', '0')::int as total_price,
                        last_order_date,
                        industry,
                        account_domain,
                        contactdomain,
                        case
                            when d.correct_account_id = g.creator_account_id then true
                            when g.quote_creator_email is null then false
                            when d.correct_account_id is null then true
                            when d.correct_account_id != g.creator_account_id then false
                            end                                                                   as active_account
                 from dbt_dev_analytics_jgroot_seed.seed_gtm_emea_2024 g
                          left join dbt_dev_analytics_jgroot_seed.seed_gtm_emea_duplicate_contacts d
                                    on d.quote_creator_email = g.quote_creator_email
                 --where g.quote_creator_email = 'dominic.haider@yageo.com'
                 )
        ,
     network as (select dc.hubspot_company_id,
                        fo.hubspot_contact_id,
                        c.email,
                        max(sourced_at::timestamp)                                                                   as last_order_date,
                        count(distinct order_uuid)                                                                   as network_order_count,
                         count( distinct case
                                when DATE_PART('year', sourced_at) = 2022
                                    then order_uuid end)                                            as network_order_count_2022,
                        count(distinct case
                                when DATE_PART('year', sourced_at) = 2023
                                    then order_uuid end)                                            as network_order_count_2023,
                        count(distinct case
                                when DATE_PART('year', sourced_at) = 2024
                                    then order_uuid end)                                            as network_order_count_2024,
                        sum(subtotal_sourced_amount_usd)                                                             as order_amount,
                        sum(case
                                when DATE_PART('year', sourced_at) = 2022
                                    then subtotal_sourced_amount_usd end)                                            as order_amount_2022,
                        sum(case
                                when DATE_PART('year', sourced_at) = 2023
                                    then subtotal_sourced_amount_usd end)                                            as order_amount_2023,
                        sum(case
                                when DATE_PART('year', sourced_at) = 2024
                                    then subtotal_sourced_amount_usd end)                                            as order_amount_2024,
                        max(case
                                when sourced_at > date_add('months', -12, getdate()) then sourced_at
                                else '2000-01-01' end)                                                               as last_order_date_n,
                        row_number()
                        over (partition by dc.hubspot_company_id order by last_order_date_n desc, order_amount desc) as primary_account_nr

                 from dbt_prod_reporting.fact_orders fo
                          left join dbt_prod_core.stg_hs_contacts_attributed_prep c
                                    on c.contact_id = fo.hubspot_contact_id
                          left join dbt_prod_reporting.dim_companies dc
                                    on dc.hubspot_company_id = c.hs_company_id
                 where sourced_at is not null
                   and destination_region = 'emea'
                   and is_papi_integration = false
                   and fo.order_status != 'canceled'
                   and sourced_at >= '2022-01-01'
                   and sourced_at < '2025-01-01'
                   and dc.name is not null
                 group by 1, 2, 3),
<<<<<<< HEAD
    country_network as (
        select
                        coalesce(dc.hubspot_company_id,fo.hubspot_contact_id) as client_id,
                        destination_country,
                        sum(subtotal_sourced_amount_usd)                                                             as order_amount,

                        row_number()
                        over (partition by client_id order by order_amount desc) as primary_country

                 from dbt_prod_reporting.fact_orders fo
                                   left join dbt_prod_core.stg_hs_contacts_attributed_prep c
                                    on c.contact_id = fo.hubspot_contact_id
                          left join dbt_prod_reporting.dim_companies dc
                                    on dc.hubspot_company_id = c.hs_company_id
                 where sourced_at is not null
                   and destination_region = 'emea'
                   and is_papi_integration = false
                   and fo.order_status != 'canceled'
                   and sourced_at >= '2022-01-01'
                   and sourced_at < '2025-01-01'
                   and dc.name is not null
                 group by dc.hubspot_company_id,fo.hubspot_contact_id, destination_country,client_id
                 ),
=======
>>>>>>> db5a75096963b5274ba5f7bf8da09f9fec264685
     match as (select quote_creator_email,
                      creator_account_id,
                      n.hubspot_company_id,
                      row_number()
                      over (partition by n.hubspot_company_id order by n.primary_account_nr) as primary_account_nr,
                      g.total_price_2022,
                      g.total_price_2023,
                      g.total_price_2024,
                      g.total_price,
                    n.order_amount_2022,
                    n.order_amount_2023,
                    n.order_amount_2024,
                    n.order_amount,
                    n.network_order_count_2022,
                    n.network_order_count_2023,
                    n.network_order_count_2024,
                    n.network_order_count

               from factory g
                        inner join network n on n.email = g.quote_creator_email
               where active_account) --select * from match;
        ,
     match_additional_contacts as (select n.email,
                                          creator_account_id,
                                          n.hubspot_company_id,
                                          n.order_amount_2022,
                                          n.order_amount_2023,
                                          n.order_amount_2024,
                                          n.order_amount,
                                          n.network_order_count_2022,
                                          n.network_order_count_2023,
                                          n.network_order_count_2024,
                                          n.network_order_count
                                   from match m
                                            left join network n on n.hubspot_company_id = m.hubspot_company_id
                                   where n.email not in (select quote_creator_email from match)
                                     and m.primary_account_nr = 1),
     contact_set as (select quote_creator_email,
                            creator_account_id,
                            hubspot_company_id,
                            total_price_2022 as factory_order_amount_2022,
                            total_price_2023 as factory_order_amount_2023,
                            total_price_2024 as factory_order_amount_2024,
                            total_price      as factory_order_amount,
                            order_amount_2022 as network_order_amount_2022,
                            order_amount_2023 as network_order_amount_2023,
                            order_amount_2024 as network_order_amount_2024,
                            order_amount      as network_order_amount,
                            network_order_count_2022,
                            network_order_count_2023,
                            network_order_count_2024,
                            network_order_count

                     from match
                     union all
                     select email,
                            creator_account_id,
                            hubspot_company_id,
                            0                 as factory_order_amount_2022,
                            0                 as factory_order_amount_2023,
                            0                 as factory_order_amount_2024,
                            0                 as factory_order_amount,
                            order_amount_2022 as network_order_amount_2022,
                            order_amount_2023 as network_order_amount_2023,
                            order_amount_2024 as network_order_amount_2024,
                            order_amount      as network_order_amount,
                            network_order_count_2022,
                            network_order_count_2023,
                            network_order_count_2024,
                            network_order_count

                     from match_additional_contacts
                     union all
                     select quote_creator_email,
                            creator_account_id,
                            null             as hubspot_company_id,
                            total_price_2022 as factory_order_amount_2022,
                            total_price_2023 as factory_order_amount_2023,
                            total_price_2024 as factory_order_amount_2024,
                            total_price      as factory_order_amount,
                            0                as network_order_amount_2022,
                            0                as network_order_amount_2023,
                            0                as network_order_amount_2024,
                            0                as network_order_amount,
                            0                as network_order_count_2022,
                            0                as network_order_count_2023,
                            0                as network_order_count_2024,
                            0                as network_order_count
                     from factory
                     where quote_creator_email not in (select quote_creator_email from match) or active_account = false
                     union all
                     select email,
                            coalesce(hubspot_company_id, hubspot_contact_id)::varchar as account_id,
                            hubspot_company_id,
                            0                                                         as factory_order_amount_2022,
                            0                                                         as factory_order_amount_2023,
                            0                                                         as factory_order_amount_2024,
                            0                                                         as factory_order_amount,
                            order_amount_2022                                         as network_order_amount_2022,
                            order_amount_2023                                         as network_order_amount_2023,
                            order_amount_2024                                         as network_order_amount_2024,
                            order_amount                                              as network_order_amount,
                           network_order_count_2022,
                            network_order_count_2023,
                            network_order_count_2024,
                            network_order_count
                     from network
                     where network.hubspot_company_id not in (select match.hubspot_company_id from match)),
account_level as (
select
        creator_account_id,
<<<<<<< HEAD
        max(hubspot_company_id) as hubspot_company_id,
        count(distinct hubspot_company_id) as hubspot_company_id_count,
=======
        hubspot_company_id,
>>>>>>> db5a75096963b5274ba5f7bf8da09f9fec264685
        sum(factory_order_amount_2022) as factory_order_amount_2022,
        sum(factory_order_amount_2023) as factory_order_amount_2023,
        sum(factory_order_amount_2024) as factory_order_amount_2024,
        sum(factory_order_amount) as factory_order_amount ,
        sum(network_order_amount_2022) as network_order_amount_2022 ,
        sum(network_order_amount_2023) as network_order_amount_2023 ,
        sum(network_order_amount_2024) as network_order_amount_2024 ,
        sum(network_order_amount) as network_order_amount,
        sum(network_order_count_2022) as network_order_count_2022,
        sum(network_order_count_2023) as network_order_count_2023,
        sum(network_order_count_2024) as network_order_count_2024,
        sum(network_order_count  ) as network_order_count,
        count(distinct quote_creator_email) as number_of_active_contacts
from contact_set c
<<<<<<< HEAD
group by 1)
select al.creator_account_id,
       a.account_name as factory_account_name,
       a.factory_seller,
       al.hubspot_company_id,
       dc.name as hubspot_account_name,
       coalesce(factory_account_name,hubspot_account_name) as account_name,
       dc.hubspot_owner_name as network_seller,
       coalesce(a.factory_seller,dc.hubspot_owner_name) as seller,
        coalesce(coalesce(lower(coalesce(a.industry, im.industry)), tfi.industry),
                                            'unknown')                                                        as industry,
       coalesce(pc.name,cn.destination_country) as country,
       coalesce(coalesce(ac.company_size,
       case when number_of_employees < 10 then '<10'
when number_of_employees < 101 then '10-100'
when number_of_employees < 501 then '100-500'
when number_of_employees < 1001 then '500-1000'
when number_of_employees < 10001 then '10000-50000'
when number_of_employees <= 100001 then '50000-100000'
when number_of_employees > 100001 then '1000001+'
end
       ),'Unknown') as number_of_employees,
       dc.is_strategic as is_strategic,
=======
group by 1,2)
select al.creator_account_id,
       a.account_name,
       a.factory_seller,
       al.hubspot_company_id,
       dc.name as hubspot_account_name,
       dc.hubspot_owner_name as network_seller,
        coalesce(coalesce(lower(coalesce(a.industry, im.industry)), tfi.industry),
                                            'unknown')                                                        as industry,
       coalesce(dc.country_name,dcs.country_name) as country,
       coalesce(dc.number_of_employees,1) as number_of_employees,
       dcs.is_strategic as is_strategic,
>>>>>>> db5a75096963b5274ba5f7bf8da09f9fec264685
       factory_order_amount_2022,
       factory_order_amount_2023,
       factory_order_amount_2024,
       factory_order_amount,
       factory_order_count_2022,
       factory_order_count_2023,
       factory_order_count_2024,
       factory_order_count,
       network_order_amount_2022,
       network_order_amount_2023,
       network_order_amount_2024,
       network_order_amount,
       network_order_count_2022,
       network_order_count_2023,
       network_order_count_2024,
       network_order_count,
       number_of_active_contacts,
       case when factory_order_amount_2024 or network_order_amount_2024 > 0 then '2024'
       when factory_order_amount_2023 or network_order_amount_2023 > 0 then '2023'
       when factory_order_amount_2022 or network_order_amount_2022 > 0 then '2022' end as last_order_year,
       coalesce(factory_order_amount,0) + coalesce(network_order_amount,0) as total_amount,
       coalesce(factory_order_count,0) + coalesce(network_order_count,0) as total_order_count,
<<<<<<< HEAD
       total_amount * 1.0 / nullif(total_order_count,0) as total_aov,
        case when total_amount > 200000 and total_aov > 2500 and last_order_year >= 2023 then '1. Strategic & Production'
           when total_amount > 20000 and total_aov > 1000 and total_order_count >= 3 then '2. Growth & Expand'
           else '3. New & Transactional' end as customer_segment,
        hubspot_company_id_count as number_of_total_associated_hubspot_companies
=======
       total_amount * 1.0 / nullif(total_order_count,0) as total_aov
>>>>>>> db5a75096963b5274ba5f7bf8da09f9fec264685

from account_level al
left join account a on a.creator_account_id = al.creator_account_id
left join dbt_prod_reporting.dim_companies dc on dc.hubspot_company_id = al.hubspot_company_id
left join dbt_prod_reporting.dim_contacts dcs on dcs.hubspot_contact_id = al.hubspot_company_id
left join dbt_dev_analytics_jgroot_core.network_to_factory_industry_mapping im
                                               on im.industry_mapped = dc.industry_mapped
left join dbt_dev_analytics_jgroot_seed.temp_factory_industry tfi
                                               on tfi.account_id = al.creator_account_id
<<<<<<< HEAD
left join dbt_dev_analytics_jgroot_seed.seed_emea_2025_additional_columns ac on ac.creator_account_id = al.creator_account_id
left join dbt_prod_core.prep_countries pc on pc.alpha2_code =(ac.country_iso)
left join country_network cn on cn.client_id = al.hubspot_company_id and primary_country = 1
order by customer_segment, total_amount desc
=======
                                               order by is_strategic desc
>>>>>>> db5a75096963b5274ba5f7bf8da09f9fec264685
