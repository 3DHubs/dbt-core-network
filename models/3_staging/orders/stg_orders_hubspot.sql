----------------------------------------------------------------
-- HUBSPOT ORDER (a.k.a DEAL) FIELDS
-- QUOTES & PURCHASE ORDERS
----------------------------------------------------------------

{{ config(materialized='table',
    tags=["multirefresh"]
) }}

-- Sources: Hubspot Deals, Hubspot Dealstages, Dim Order Status and Hubspot Owners
with stg as (
    select
        -- Main Fields
        hs.deal_id                                                                        as hubspot_deal_id,
        hs.amount                                                                         as hubspot_amount_usd,
        hs.estimated_close_amount                                                         as hubspot_estimated_close_amount_usd,
        hs.high_risk                                                                      as is_high_risk,
        hs.hs_priority                                                                    as rfq_priority,
        hs.pipeline                                                                       as hubspot_pipeline,

        -- Foreign Fields
        hcon.hs_company_id                                                                as hubspot_company_id, --JG: Decided on 26-11-21 to use active company id of contact instead of original_hubspot_company_id
        case when regexp_like(hcon.email, '@(3d)?hubs.com') or regexp_like(hcon.email, 'protolabs') then true 
             else false end                                                               as hubspot_contact_email_from_internal, --todo-migration-test: replaced ~
        hs.hs_latest_associated_company_id                                                as original_hubspot_company_id, --JG: Kept for reference
        hcom.name                                                                         as hubspot_company_name,
        hs.pl_cross_sell_company_name,
        hs.hs_latest_associated_contact_id                                                as hubspot_contact_id, --JG: Fix 2022-01-28 to have a contact id for all orders where company is not null and contact id is null
        hs.bdr_company_source                                                             as hubspot_company_source,
        htm.technology_id                                                                 as hubspot_technology_id,
        technologies.name                                                                 as hubspot_technology_name,
        case when regexp_like(hcon.hutk_analytics_first_url, 'utm_campaign=protolabssales') then 'Sales Referral'
             else hs.pl_cross_sell_channel  end                                           as hubspot_pl_cross_sell_channel, --todo-migration-test: replaced ~

        -- Dates
        hs.createdate                                                                     as hubspot_created_at,
        hs.closedate                                                                      as hubspot_closed_at,
        hs.first_time_quote_sent_date                                                     as first_time_quote_sent_at,
        hs.first_time_response_date                                                       as first_time_response_at,
        hs.im_hs_promised_shipping_at_by_supplier,

        -- Lifecycle
        dealstage.dealstage_mapped_value                                                  as hubspot_dealstage_mapped,
        dealstage.sort_index                                                              as hubspot_dealstage_mapped_sort_index,
        status.mapped_value                                                               as hubspot_status_mapped,
        nullif(hs.in_review_reason, '')                                                   as in_review_reason,
        nullif(hs.review_type, '')                                                        as in_review_type,
        nullif(hs.closed_lost_reason, '')                                                 as hubspot_closed_lost_reason,
        nullif(hs.cancellation_reason, '')                                                as hubspot_cancellation_reason,
        scr.reason_mapped                                                                 as hubspot_cancellation_reason_mapped,
        hs.qc_inspection_result                                                           as qc_inspection_result,
        case
            when hs.closedate > '2020-01-01'
                then nullif(regexp_replace(hs.delay_liability, 'liability_', ''), '') end as delay_liability,
        case
            when hs.closedate > '2020-01-01'
                then nullif(regexp_replace(hs.delay_reason, 'delay_reason_', ''), '') end as delay_reason,    
        hs.delay_status                                                                   as delay_status,
        case when regexp_like(hcon.hutk_analytics_first_url, 'utm_source=protolabs') or regexp_like(hcon.hutk_analytics_first_url, 'utm_campaign=protolabssales') then true else false end as is_integration_mql_contact, --todo-migration-test: replaced ~


        -- Owners       
        hs.hubspot_owner_id,
        own.name                                                                          as hubspot_owner_name,
        own.primary_team_name                                                             as hubspot_owner_primary_team,
        own.office_location,
        date_trunc('day', hs.hubspot_owner_assigneddate)                                  as hubspot_owner_assigned_date, -- Not a timestamp --todo-migration-test
        fst.sales_lead_id                                                                 as sales_lead_id,
        fst.sales_lead                                                                    as sales_lead_name,
        hs.bdr_assigned                                                                   as bdr_owner_id,
        bdr.name                                                                          as bdr_owner_name,
        bdr.primary_team_name                                                             as bdr_owner_primary_team,
        csr.name                                                                          as customer_success_representative_name,
        psr.name                                                                          as partner_support_representative_name,
        hs.sales_engineer                                                                 as mechanical_engineer_id,
        me.name                                                                           as mechanical_engineer_name,
        me_buddy.name                                                                     as mechanical_engineer_deal_buddy_name,
        hs.purchasing_manager                                                             as hubspot_purchasing_manager,
        hs.review_owner                                                                   as hubspot_technical_review_owner,
        hs.sourcing_owner                                                                 as hubspot_sourcing_owner_id,
        so.name                                                                           as hubspot_sourcing_owner_name,
        pm.owner_id                                                                       as hubspot_im_project_manager_id,
        pm.name                                                                           as hubspot_im_project_manager_name,
        hs.paid_sales_rep_id                                                              as hubspot_paid_sales_rep_id,
        paid_sr.name                                                                      as hubspot_paid_sales_rep_name,
        hs.pl_sales_rep_name                                                              as pl_sales_rep_name,
        hs.pl_sales_rep_manager_name                                                      as pl_sales_rep_manager_name,
        hs.sales_support_id                                                               as sales_support_id,
        ss.name                                                                           as sales_support_name,
        pl_bdm.name                                                                       as pl_business_development_manager_name,
        nss.name                                                                          as hubspot_network_sales_specialist_name,
        co.name                                                                           as hubspot_company_owner_name,
        hs.quality_resolution_specialist_id                                               as hubspot_quality_resolution_specialist_id,
        qrs.name                                                                          as hubspot_quality_resolution_specialist_name,
        hs.technical_program_manager_id                                                   as hubspot_technical_program_manager_id,
        t_pm.name                                                                         as hubspot_technical_program_manager_name,

        -- TEAM FIELDS
        -- Properties added by the different teams
        
        -- Fulfillment Fields
        rfq_type,
        target_price as is_target_price_met,
        match_lead_time as is_target_lead_time_met,
        review_outcome,
        me_team_review_results,
        is_production_rfq,

        -- Project Operation Fields
        approved_by_services as custom_approval,
        rejected_reason,
        im_deal_type,
        original_im_deal_s_order_number as original_im_order_document_number,
        critical_to_quality_check_complete as ctq_check,
        hs.mp_concerning_actions,

        -- Sales Fields
        is_sales_priced,
        hs.is_strategic,
        hs.is_ultra_strategic as is_priority_deal,
        closing_probability,
        hs.hubspot_signed_customer_quote_pdf_link,
        hs.why_still_in_production,

        -- Sourcing Fields
        is_manually_resourced,
        resourced_deal_original_order_number,

        -- Supply Fields
        latest_qc_result as qc_inspection_result_latest,
        in_country_qc_status,

        -- Logistics Fields
        is_delayed_due_to_customs,
        hs.is_hubs_arranged_direct_shipping,
        hs.is_logistics_shipping_quote_used,

        -- IM (Injection Molding) Fields
        hs.im_post_sales_value_score,
        hs.im_post_sales_concerning_actions,

        -- UTM Tags / traffic details
        nullif({{dbt_utils.get_url_parameter('last_page_seen', 'utm_campaign') }}, '') as utm_campaign,
        nullif({{dbt_utils.get_url_parameter('last_page_seen', 'utm_content') }}, '') as utm_content,
        nullif(coalesce({{dbt_utils.get_url_parameter('last_page_seen', 'utm_source') }},{{dbt_utils.get_url_parameter('last_page_seen', 'utmsource') }}), '') as utm_source,
        nullif({{dbt_utils.get_url_parameter('last_page_seen', 'utm_term') }}, '') as utm_term,
        nullif({{dbt_utils.get_url_parameter('last_page_seen', 'utm_medium') }}, '') as utm_medium,
        last_traffic_source,

        -- Window Functions
        row_number() over (partition by hubspot_deal_id order by random())             as rn

    from {{ ref('hubspot_deals') }} as hs
            left join {{ ref('seed_hubspot_dealstages') }} as dealstage
    on hs.dealstage = dealstage.dealstage_internal_label
        left join {{ ref('seed_order_status') }} as status
            on dealstage.dealstage_mapped_value = status.hubspot_status_value
        left join {{ ref ('hubspot_owners') }} as own
            on own.owner_id = hs.hubspot_owner_id
            -- and coalesce (hubspot_owner_assigneddate, createdate) between own.start_date and own.end_date JG 202207 We stopped tracking history of teams
        left join {{ ref ('hubspot_owners') }} as bdr
            on bdr.owner_id = hs.bdr_assigned --and createdate between bdr.start_date and bdr.end_date JG We stopped tracking history of teams
        left join {{ ref ('hubspot_owners') }} as me
            on me.owner_id = hs.sales_engineer
        left join {{ ref ('hubspot_owners') }} as me_buddy
            on me_buddy.owner_id = hs.sales_engineer_deal_buddy
        left join {{ ref ('hubspot_owners') }} as csr
            on csr.owner_id = hs.customer_success_manager
        left join {{ ref ('hubspot_owners') }} as psr
            on psr.owner_id = hs.supply_owner 
        left join {{ ref ('hubspot_owners') }} as so
            on so.owner_id = hs.sourcing_owner
        left join {{ ref ('hubspot_owners') }} as ss
            on ss.owner_id = hs.sales_support_id
        left join {{ ref ('hubspot_owners') }} as pm
            on pm.owner_id = hs.im_pm
        left join {{ ref ('hubspot_owners') }} as pl_bdm
            on pl_bdm.owner_id = hs.pl_business_development_manager_id
        left join {{ ref ('hubspot_owners') }} as t_pm
            on t_pm.owner_id = hs.technical_program_manager_id
        left join {{ ref ('hubspot_owners') }} as co
            on co.owner_id = hs.company_owner_id
        left join {{ ref ('hubspot_owners') }} as nss
            on nss.owner_id = hs.network_sales_specialist_id
        left join {{ ref ('hubspot_owners') }} as qrs
            on qrs.owner_id = hs.quality_resolution_specialist_id
        left join {{ ref ('hubspot_owners') }} as paid_sr
            on paid_sr.owner_id = hs.paid_sales_rep_id
        left join {{ref('fact_sales_target') }} as fst
                on fst.hubspot_id = hs.hubspot_owner_id and fst.target_date::date = coalesce(date_trunc('month',hs.closedate),'2022-01-01') 
        left join {{ ref('seed_hubspot_technology_mapping') }} as htm
            on hs.technologies = htm.hubspot_technology
        left join {{ ref ('technologies') }} as technologies
            on htm.technology_id = technologies.technology_id
        left join {{ ref('stg_hs_contacts_attributed_prep') }} as hcon
            on hs.hs_latest_associated_contact_id = hcon.contact_id
        left join {{ ref('hubspot_companies') }} as hcom 
            on hcon.hs_company_id = hcom.hubspot_company_id
        left join {{ ref('seed_cancellation_reasons') }} scr 
            on lower(scr.reason) = lower(hs.cancellation_reason)
        )
        
        
select stg.*, coalesce(ac.name, bc.name) as utm_campaign_name
from stg
 left join {{ ref('google_ads_campaigns') }} as ac on ac.id = case when regexp_like(stg.utm_campaign, '^[0-9]+$') then cast(stg.utm_campaign as bigint) else null end --todo-migration-test: replaced ~
 left join {{ ref('bing_ads_campaigns') }} as bc on bc.id = case when regexp_like(stg.utm_campaign, '^[0-9]+$') then cast(stg.utm_campaign as bigint) else null end --todo-migration-test: replaced ~
where rn = 1
