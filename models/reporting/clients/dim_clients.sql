{{
    config(
        post_hook = "analyze {{ this }}"
    )
}}

with stg_agg_contact as (
    select distinct

        reporting_cube_deals.hubspot_contact_id as hubspot_contact_id,
        first_value(country_iso2)
        over (
             partition by
            hubspot_contact_id
             order by
            closed_date asc
            rows between unbounded preceding and unbounded following
         ) as first_quote_country_iso2
    from {{ source('reporting', 'cube_deals') }} as reporting_cube_deals
),

mqls as (
    select * from {{ ref('fact_mqls') }}
),

company_mqls as (
    select
         company_id,
        min(mql_date) as became_mql_date
    from mqls
    where company_id is not null
    group by 1
),

contact_mqls as (
    select
         contact_id,
        case
            when mqls.company_id is null then mql_date
        end as became_mql_date,
        case
            when
                mqls.company_id is not null and mql_date > company_mqls.became_mql_date then mql_date
        end as became_inside_mql_date
    from mqls
    left join company_mqls on company_mqls.company_id = mqls.company_id
    group by 1, 2, 3
),

stg_contacts_w_comp as (
    select
         hubspot_contact_id,
        first_value(is_legacy_order)
        over (
            partition by
                hubspot_contact_id
            order by
                quote_submitted_date asc
            rows between unbounded preceding and unbounded following
        ) as first_contact_deal_is_legacy,
        min(
            closed_date
        ) over (partition by hubspot_contact_id) as closed_date,
        min(
            became_opportunity_date
        ) over (partition by hubspot_contact_id) as became_opportunity_date,
        min(
            quote_submitted_date
        ) over (partition by hubspot_contact_id) as quote_submitted_date,
        min(
            became_customer_date
        ) over (partition by hubspot_contact_id) as became_customer_date
    from {{ source('reporting', 'cube_deals') }}
    where hubspot_company_id is not null
),

agg_contact_w_company as (
    select
         hubspot_contact_id,
        case when not Bool_or(first_contact_deal_is_legacy) and
                (min(quote_submitted_date) > min(became_opportunity_date) or
                    min(became_opportunity_date) is null)
                then min(quote_submitted_date)
        end as became_inside_opportunity_date,
        case when min(closed_date) > min(became_customer_date)
                then min(closed_date)
        end as became_inside_customer_date
    from stg_contacts_w_comp
    group by 1
),

stg_companies as (
    select
         client_id,
         closed_date,
         count(
             order_uuid
         ) over (partition by client_id) as total_number_of_quotes,
         count(case when is_closed_won then order_uuid end)
         over (
             partition by client_id
         ) as total_number_of_closed_orders,
         max(
             closed_date
         ) over (partition by client_id) as recent_closed_order_date,
         lag(
             closed_date
         ) over (
        partition by client_id order by closed_date
    ) as previous_closed_order_date
    from {{ source('reporting', 'cube_deals') }} as reporting_cube_deals
),

agg_companies as (
    select
         client_id,
        min(
            total_number_of_quotes
        ) as total_number_of_quotes,
        min(
            total_number_of_closed_orders
        ) as total_number_of_closed_orders,
        min(
            recent_closed_order_date
        ) as recent_closed_order_date,
        round(avg(Extract(day from closed_date - previous_closed_order_date)),
                           1
         ) as average_days_between_closed_orders,
        Median(
            extract(day from closed_date - previous_closed_order_date)
        ) as median_days_between_closed_orders
    from stg_companies
    group by 1
),

stg_clients as (
    select distinct

        reporting_cube_deals.client_id as client_id,
        min(
            reporting_cube_deals.became_opportunity_date
        ) over (partition by client_id) as became_opportunity_date,
        min(
            reporting_cube_deals.became_customer_date
        ) over (partition by client_id) as became_customer_date,
        nullif(
            sum(
                reporting_cube_deals.closed_sales_usd
            ) over (partition by client_id),
            0
        ) as total_closed_sales_usd,
        first_value(technology_name)
        over (
            partition by
                client_id
            order by
                quote_submitted_date asc
            rows between unbounded preceding and unbounded following
        ) as first_quote_technology,
        first_value(
            case when is_closed_won then technology_name end)
        -- returns the technology of the first order that is_closed_won, null when the client has no closed_won orders
        over (
            partition by
                client_id
            order by
                is_closed_won desc, closed_date asc
            rows between unbounded preceding and unbounded following
        ) as first_order_technology,
        first_value(
            case when is_closed_won then process_name end)
        over (
            partition by
                client_id
            order by
                is_closed_won desc, closed_date asc
            rows between unbounded preceding and unbounded following
        ) as first_order_process_name,
        nth_value(case when is_closed_won then closed_date end, 2)
        over (
            partition by
                client_id
            order by
                is_closed_won desc, closed_date asc
            rows between unbounded preceding and unbounded following
        ) as second_order_closed_date,
        sum(
            case
                when is_closed_won and is_new_customer then closed_sales_usd
            end
        )
        over (
            partition by client_id
        ) as new_customer_closed_sales_usd,
        sum(
            case
                when
                    is_closed_won and is_new_customer then (
                        reporting_cube_deals.sourced_sales_usd - reporting_cube_deals.sourced_cost_usd
                    )
            end
        )
        over (
            partition by client_id
        ) as new_customer_precalc_margin_usd

    from {{ source('reporting', 'cube_deals') }} as reporting_cube_deals
)


select stg_clients."created_date",
       stg_clients."client_id",
       stg_clients."type",
       stg_clients."name",
       stg_clients."number_of_employees",
       stg_clients."industry",
       stg_clients."industry_mapped",
       stg_clients."founded_year",
       stg_clients."is_funded",
       stg_clients."hs_company_id",
       stg_clients."hs_contact_id",
       stg_clients."hutk_analytics_source",
       stg_clients."hutk_analytics_source_data_1",
       stg_clients."hutk_analytics_source_data_2",
       stg_clients."hutk_analytics_first_url",
       stg_clients."hutk_analytics_first_visit_timestamp",
       stg_clients."channel_type",
       stg_clients."channel",
       stg_clients."channel_grouped",
       stg_clients."first_page_seen",
       stg_clients."first_page_seen_grouped",
       stg_clients."first_page_seen_query",
       stg_clients."utm_campaign",
       stg_clients."utm_content",
       stg_clients."channel_drilldown_1",
       stg_clients."channel_drilldown_2",
       stg_clients."attempted_to_contact_at",
       stg_clients."connected_at",
       stg_clients."became_lead_date",
       coalesce(
               company_mqls.became_mql_date,
               contact_mqls.became_mql_date
           )   as became_mql_date,
       stg_clients."became_sql_date",
       stg_clients."became_inside_lead_date",
       stg_clients."became_inside_sql_date",
       stg_clients."hubspot_owner_id",
       stg_clients."became_ae_account_date",
       stg_clients."hubspot_owner_name",
       stg_clients."hubspot_owner_primary_team_name",
       stg_clients."bdr_owner_id",
       stg_clients."bdr_owner_name",
       stg_clients."ae_id",
       stg_clients."ae_name",
       stg_clients."hubspot_owner_assigned_date",
       stg_clients."became_strategic_date",
       stg_clients."account_category",
       stg_clients."email_type",
       stg_clients."contact_source",
       stg_clients."country_name",
       stg_clients."market",
       stg_clients."region",
       stg_clients."continent",
       stg_clients."hs_lead_status",
       stg_clients."is_sales_qualified",
       stg_clients."job_role",
       stg_clients."is_deactivated",
       stg_clients."deactivated_date",
       stg_clients."is_reactivated_opportunity",
       stg_clients."reactivated_opportunity_date",
       stg_clients."is_reactivated_customer",
       stg_clients."reactivated_customer_date",
       stg_clients."lead_score",
       stg_clients."tier",
       stg_clients."is_qualified",
       stg_advertising_data.advertising_gclid,
       stg_advertising_data.advertising_msclkid,
       stg_advertising_data.advertising_click_date,
       stg_advertising_data.advertising_click_device,
       stg_advertising_data.advertising_source,
       stg_advertising_data.advertising_account_id,
       stg_advertising_data.advertising_campaign_id,
       stg_advertising_data.advertising_adgroup_id,
       stg_advertising_data.advertising_keyword_id,
       stg_advertising_data.advertising_campaign_group,
       agg_contact_w_company.became_inside_opportunity_date,
       agg_contact_w_company.became_inside_customer_date,
       fda.became_opportunity_date,
       fda.became_customer_date,
       fda.total_closed_sales_usd,
       fda.first_quote_technology,
       fda.first_order_technology,
       fda.first_order_process_name,
       fda.second_order_closed_date,
       fda.new_customer_closed_sales_usd,
       fda.new_customer_precalc_margin_usd,
       agg_companies.total_number_of_quotes,
       agg_companies.total_number_of_closed_orders,
       agg_companies.average_days_between_closed_orders,
       agg_companies.median_days_between_closed_orders,
       agg_companies.recent_closed_order_date,
       contact_mqls.became_inside_mql_date,
       coalesce(
               stg_agg_contact.first_quote_country_iso2, stg_clients.country_iso2
           )   as country_iso2,
       case
           when
               type = 'company' then count(
                                     agg_contact_w_company.became_inside_opportunity_date
               )
                                     over (partition by stg_clients.hs_company_id)
           end as number_of_inside_opportunities,
       case
           when
               type = 'company' then count(
                                     agg_contact_w_company.became_inside_customer_date
               )
                                     over (partition by stg_clients.hs_company_id)
           end as number_of_inside_customers

from {{ ref('stg_clients') }} as stg_clients
         left outer join
     {{ ref('stg_advertising_data') }} as stg_advertising_data on
         (stg_clients.client_id = stg_advertising_data.client_id)
         left outer join
     stg_agg_contact on
         stg_agg_contact.hubspot_contact_id = stg_clients.hs_contact_id
         left outer join
     agg_contact_w_company on
         agg_contact_w_company.hubspot_contact_id = stg_clients.hs_contact_id
         left outer join stg_clients as fda on fda.client_id = stg_clients.client_id
         left outer join agg_companies on stg_clients.client_id = agg_companies.client_id
         left outer join
     company_mqls on
             company_mqls.company_id = stg_clients.hs_company_id and stg_clients.type = 'company'
         left outer join
     contact_mqls on
             contact_mqls.contact_id = stg_clients.hs_contact_id and stg_clients.type = 'contact'