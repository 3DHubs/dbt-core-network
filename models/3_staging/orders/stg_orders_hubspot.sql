----------------------------------------------------------------
-- HUBSPOT ORDER (a.k.a DEAL) FIELDS
-- QUOTES & PURCHASE ORDERS
----------------------------------------------------------------

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
        hcon.associatedcompanyid                                                          as hubspot_company_id, --JG: Decided on 26-11-21 to use active company id of contact instead of original_hubspot_company_id
        hs.hs_latest_associated_company_id                                                as original_hubspot_company_id, --JG: Kept for reference
        hcom.name                                                                         as hubspot_company_name,
        hs.hs_latest_associated_contact_id                                                as hubspot_contact_id, --JG: Fix 2022-01-28 to have a contact id for all orders where company is not null and contact id is null
        hs.bdr_company_source                                                             as hubspot_company_source,
        htm.technology_id                                                                 as hubspot_technology_id,
        technologies.name                                                                 as hubspot_technology_name,

        -- Dates
        hs.createdate                                                                     as hubspot_created_at,
        hs.closedate                                                                      as hubspot_closed_at,
        hs.first_time_quote_sent_date                                                     as first_time_quote_sent_at,
        hs.first_time_response_date                                                       as first_time_response_at,

        -- Lifecycle
        dealstage.dealstage_mapped_value                                                  as hubspot_dealstage_mapped,
        dealstage.sort_index                                                              as hubspot_dealstage_mapped_sort_index,
        status.mapped_value                                                               as hubspot_status_mapped,
        nullif(hs.in_review_reason, '')                                                   as in_review_reason,
        nullif(hs.review_type, '')                                                        as in_review_type,
        nullif(hs.closed_lost_reason, '')                                                 as hubspot_closed_lost_reason,
        nullif(hs.cancellation_reason, '')                                                as hubspot_cancellation_reason,
        hs.qc_inspection_result                                                           as qc_inspection_result,
        case
            when hs.closedate > '2020-01-01'
                then nullif(regexp_replace(hs.delay_liability, 'liability_', ''), '') end as delay_liability,
        hs.delay_status                                                                   as delay_status,

        -- Owners
        hs.hubspot_owner_id,
        own2.name                                                                         as hubspot_owner_name,
        own.primary_team_name                                                             as hubspot_owner_primary_team,
        trunc(hs.hubspot_owner_assigneddate)                                              as hubspot_owner_assigned_date, -- Not a timestamp
        fst.sales_lead_id                                                                 as sales_lead_id,
        fst.sales_lead                                                                    as sales_lead_name,
        hs.bdr_assigned                                                                   as bdr_owner_id,
        bdr2.name                                                                         as bdr_owner_name,
        bdr.primary_team_name                                                             as bdr_owner_primary_team,
        csr.name                                                                          as customer_success_representative_name,
        psr.name                                                                          as partner_support_representative_name,
        hs.sales_engineer                                                                 as mechanical_engineer_id,
        me.name                                                                           as mechanical_engineer_name,
        hs.purchasing_manager                                                             as hubspot_purchasing_manager,
        hs.review_owner                                                                   as hubspot_technical_review_owner,
        hs.sourcing_owner                                                                 as hubspot_sourcing_owner_id,
        so.name                                                                           as hubspot_sourcing_owner_name,
        pm.owner_id                                                                       as hubspot_im_project_manager_id,
        pm.name                                                                           as hubspot_im_project_manager_name,

        -- TEAM FIELDS
        -- Properties added by the different teams
        
            -- Fulfillment Fields
            rfq_type,
            target_price as is_target_price_met,
            match_lead_time as is_target_lead_time_met,
            review_outcome,
            me_team_review_results,

            -- Project Operation Fields
            approved_by_services as custom_approval,
            rejected_reason,
            im_deal_type,
            original_im_deal_s_order_number as original_im_order_document_number,
            critical_to_quality_check_complete as ctq_check,

            -- Sales Fields
            is_strategic,
            bdr_source as bdr_campaign,
            closing_probability,

            -- Supply Fields
            latest_qc_result as qc_inspection_result_latest,
            in_country_qc_status,

            -- Logistics Fields
            is_delayed_due_to_customs,

        -- Window Functions
        row_number() over (partition by hubspot_deal_id order by random())             as rn

    from {{ source('data_lake', 'hubspot_deals_stitch') }} as hs
            left join {{ ref('seed_hubspot_dealstages') }} as dealstage
    on hs.dealstage = dealstage.dealstage_internal_label
        left join {{ ref('seed_order_status') }} as status
            on dealstage.dealstage_mapped_value = status.hubspot_status_value
        left join {{ ref ('hubspot_owners') }} as own
            on own.owner_id = hs.hubspot_owner_id
            and coalesce (hubspot_owner_assigneddate, createdate) between own.start_date and own.end_date
        left join {{ ref ('hubspot_owners') }} own2 on own2.owner_id = hs.hubspot_owner_id 
        left join {{ ref ('hubspot_owners') }} as bdr
            on bdr.owner_id = hs.bdr_assigned and createdate between bdr.start_date and bdr.end_date
        left join {{ ref ('hubspot_owners') }} as bdr2
            on bdr2.owner_id = hs.bdr_assigned
        left join {{ ref ('hubspot_owners') }} as me
            on me.owner_id = hs.sales_engineer
        left join {{ ref ('hubspot_owners') }} as csr
            on csr.owner_id = hs.customer_success_manager
        left join {{ ref ('hubspot_owners') }} as psr
            on psr.owner_id = hs.supply_owner 
        left join {{ ref ('hubspot_owners') }} as so
            on so.owner_id = hs.sourcing_owner
        left join {{ ref ('hubspot_owners') }} as pm
            on pm.owner_id = hs.im_pm
        left join {{ref('fact_sales_target') }} as fst
                on fst.hubspot_id = hs.hubspot_owner_id and fst.target_date::date = coalesce(date_trunc('month',hs.closedate),'2022-01-01') 
        left join {{ ref('seed_hubspot_technology_mapping') }} as htm
            on hs.technologies = htm.hubspot_technology
        left join {{ ref ('technologies') }} as technologies
            on htm.technology_id = technologies.technology_id
        left join {{ source('data_lake', 'hubspot_contacts_stitch') }} as hcon
            on hs.hs_latest_associated_contact_id = hcon.contact_id
        left join {{ source('data_lake', 'hubspot_companies_stitch') }} as hcom 
            on hcon.associatedcompanyid = hcom.hubspot_company_id)
select *
from stg
where rn = 1
