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
        hs.deal_category                                                                  as hubspot_deal_category,
        hs.strategic                                                                      as is_hubspot_strategic_deal,
        hs.high_risk                                                                      as is_hubspot_high_risk,
        hs.pipeline                                                                       as hubspot_pipeline,

        -- Foreign Fields
        hs.hs_latest_associated_company_id                                                as hubspot_company_id,
        hcom.name                                                                         as hubspot_company_name,
        hcom.strategic = 'true'                                                           as is_hubspot_strategic_company,
        hs.hs_latest_associated_contact_id                                                as hubspot_contact_id,
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
        case
            when hs.closedate > '2020-01-01'
                then nullif(hs.delay_reason, '') end                                      as delay_reason,
        hs.delay_status                                                                   as delay_status,
        nullif(replace(hs.dispute_liability, 'dl_', ''), '')                              as dispute_liability,
        nullif(hs.dispute_outcome, '')                                                    as dispute_outcome,
        nullif(replace(hs.dispute_liability, 'dl_', ''), '')                              as dispute_reason,


        -- Owners
        hs.hubspot_owner_id,
        own2.first_name || ' ' || own2.last_name                                          as hubspot_owner_name,
        own.primary_team_name                                                             as hubspot_owner_primary_team_name,
        trunc(hs.hubspot_owner_assigneddate)                                              as hubspot_owner_assigned_at,
        hs.bdr_assigned                                                                   as bdr_owner_id,
        bdr2.first_name || ' ' || bdr2.last_name                                          as bdr_owner_name,
        bdr.primary_team_name                                                             as bdr_owner_primary_team_name,
        csr.first_name || ' ' || csr.last_name                                            as customer_success_representative_name,
        psr.first_name || ' ' || psr.last_name                                            as partner_support_representative_name,
        hs.sales_engineer                                                                 as mechanical_engineer_id,
        me.first_name || ' ' || me.last_name                                              as mechanical_engineer_name,
        hs.purchasing_manager                                                             as hubspot_purchasing_manager,
        hs.review_owner                                                                   as hubspot_technical_review_owner,
        hs.sourcing_owner                                                                 as hubspot_sourcing_owner_id,
        so.first_name || ' ' || so.last_name                                              as hubspot_sourcing_owner_name,

        -- Window Functions
        row_number() over (partition by hubspot_deal_id order by random())             as rn

    from {{ source('data_lake', 'hubspot_deals') }} as hs
            left join {{ ref('hubspot_dealstages') }} as dealstage
    on hs.dealstage = dealstage.dealstage_internal_label
        left join {{ ref('order_status') }} as status
            on dealstage.dealstage_mapped_value = status.hubspot_status_value
        left join {{ source('data_lake', 'hubspot_owners') }} as own
            on own.owner_id = hs.hubspot_owner_id
            and coalesce (hubspot_owner_assigneddate, createdate) between own.start_date and own.end_date
        left join {{ source('data_lake', 'hubspot_owners') }} own2 on own2.owner_id = hs.hubspot_owner_id and own2.is_current is true
        left join {{ source('data_lake', 'hubspot_owners') }} as bdr
            on bdr.owner_id = hs.bdr_assigned and createdate between bdr.start_date and bdr.end_date
        left join {{ source('data_lake', 'hubspot_owners') }} as bdr2
            on bdr2.owner_id = hs.bdr_assigned and bdr2.is_current is true
        left join {{ source('data_lake', 'hubspot_owners') }} as me
            on me.owner_id = hs.sales_engineer and me.is_current is true
        left join {{ source('data_lake', 'hubspot_owners') }} as csr
            on csr.owner_id = hs.customer_success_manager and csr.is_current is true
        left join {{ source('data_lake', 'hubspot_owners') }} as psr
            on psr.owner_id = hs.supply_owner and psr.is_current is true
        left join {{ source('data_lake', 'hubspot_owners') }} as so
            on so.owner_id = hs.sourcing_owner and so.is_current is true
        left join {{ ref('hubspot_technology_mapping') }} as htm
            on hs.technologies = htm.hubspot_technology
        left join {{ ref ('technologies') }} as technologies
            on htm.technology_id = technologies.technology_id
        left join {{ source('data_lake', 'hubspot_companies') }} as hcom 
            on hs.hs_latest_associated_company_id = hcom.company_id
)
select *
from stg
where rn = 1
