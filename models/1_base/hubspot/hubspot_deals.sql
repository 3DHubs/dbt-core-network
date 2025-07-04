-- source from Hubspot Stitch setup initially by Nihad.
with
    deals as (
        select
            *,
            row_number() over (
                partition by dealid order by _sdc_received_at desc, _sdc_sequence
            ) as rown
        from {{ source("ext_hubspot", "deals") }}
    )

select
    dealid::bigint as deal_id,
    ehdacomp.value::bigint as hs_latest_associated_company_id,
    ehdacont.value::bigint as hs_latest_associated_contact_id,
    nullif(property_deal_category__value, '')::varchar(512) as deal_category,
    nullif(property_closed_lost_reason__value, '')::varchar(124) as closed_lost_reason,
    property_amount__value__double as amount,
    property_closedate__value::timestamp without time zone as closedate,
    property_createdate__value::timestamp without time zone as createdate,
    nullif(property_technologies__value, '')::varchar(128) as technologies,
    nullif(property_pipeline__value, '')::varchar(128) as pipeline,
    nullif(property_dealstage__value, '')::varchar(128) as dealstage,
    nullif(property_hubspot_owner_id__value, '')::bigint as hubspot_owner_id,
    (property_hubspot_owner_assigneddate__value)::timestamp without time zone
    as hubspot_owner_assigneddate,
    nullif(property_supply_owner__value, '')::varchar(124) as supply_owner,
    nullif(property_review_type__value, '')::varchar(124) as review_type,
    nullif(property_sales_engineer__value, '')::int as sales_engineer,
    nullif(property_me_deal_buddy__value, '')::int as sales_engineer_deal_buddy,
    nullif(property_bdr_assigned__value, '')::int as bdr_assigned,
    nullif(property_sales_support_specialist__value, '')::int as sales_support_id,
    nullif(property_delay_liability__value, '')::varchar(124) as delay_liability,
    nullif(property_delay_reason__value, '')::varchar(124) as delay_reason,
    nullif(property_in_review_reason__value, '')::varchar(124) as in_review_reason,
    nullif(property_cancellation_reason__value, '')::varchar(124) as cancellation_reason,
    nullif(property_mp_concerning_actions__value, '')::varchar(124) as mp_concerning_actions,
    case when property_manually_resourced_deal__value = 'true' then true else false end as is_manually_resourced,
    nullif(property_resourced_deal_original_order_number__value, '')::varchar(124) as resourced_deal_original_order_number,
    (
        timestamp 'epoch'
        + property_first_time_quote_sent_date__value / 1000 * interval '1 second'
    )::timestamp without time zone as first_time_quote_sent_date,
    nullif(property_dispute_liability__value, '')::varchar(124) as dispute_liability,
    (
        timestamp 'epoch'
        + property_first_time_response_date__value / 1000 * interval '1 second'
    )::date as first_time_response_date,
    -- trunc(property_first_time_response_date__value)::date as
    -- first_time_response_date,
    case when property_expected_shipping_date__value !~ '^[0-9]+$' then null
    else ( timestamp 'epoch'
        + property_expected_shipping_date__value / 1000 * interval '1 second'
    )::date end as im_hs_promised_shipping_at_by_supplier, -- The date entered by the IM deal owner to move the deal stage to "Won In Production." This is referred to as the "Latest Ship By Date" in HubSpot.
    case
        when property_high_risk__value = 'Yes'
        then true
        when property_high_risk__value = ''
        then null
    end::boolean as high_risk,
    nullif(property_customer_success_manager__value, '')::bigint
    as customer_success_manager,
    nullif(property_bdr_company_source__value, '')::varchar(
        65535
    ) as bdr_company_source,
    property_estimated_close_amount__value__double as estimated_close_amount,
    nullif(property_qc_inspection_result__value, '')::varchar(
        65535
    ) as qc_inspection_result,
    nullif(property_purchasing_manager__value, '')::bigint as purchasing_manager,
    nullif(property_delay_status__value, '')::varchar(65535) as delay_status,
    nullif(property_review_owner__value, '')::varchar(65535) as review_owner,
    nullif(property_sourcing_owner__value, '')::bigint as sourcing_owner,
    nullif(property_company_owner__value, '')::bigint as company_owner_id,
    nullif(property_network_sales_specialist__value, '')::bigint as network_sales_specialist_id,
    nullif(property_complaint_manager__value, '')::bigint as quality_resolution_specialist_id,
    nullif(property_paid_sales_rep__value, '')::bigint as paid_sales_rep_id,
    case
        when property_strategic__value = 'true'
        then true
        when property_strategic__value = 'false'
        then false
        when property_strategic__value = ''
        then null
    end::boolean as is_strategic,
    case
        when property_ultra_strategic__value = 'true'
        then true
        when property_ultra_strategic__value = 'false'
        then false
        when property_ultra_strategic__value = ''
        then null
    end::boolean as is_ultra_strategic,
    case
        when property_hubs_arranges_direct_shipping__ds___value = 'true'
        then true
        when property_hubs_arranges_direct_shipping__ds___value = 'false'
        then false
        when property_hubs_arranges_direct_shipping__ds___value = ''
        then null
    end::boolean as is_hubs_arranged_direct_shipping,
    case
        when property_production_rfq__value = 'true'
        then true
        else false
    end::boolean as is_production_rfq,
    case
        when property_was_logistics_shipping_quote_used___value = 'true'
        then true
        else false
    end as is_logistics_shipping_quote_used,
    nullif(property_closing_probability__value, '')::varchar(
        2048
    ) as closing_probability,
    nullif(property_latest_qc_result__value, '')::varchar(2048) as latest_qc_result,
    nullif(property_in_country_qc_status__value, '')::varchar(
        2048
    ) as in_country_qc_status,
    nullif(property_review_outcome__value, '')::varchar(2048) as review_outcome,
    nullif(nullif(property_rfq_type__value, 'None'), '')::varchar(2048) as rfq_type,
    nullif(property_target_price__value, '')::varchar(128) as target_price,
    nullif(property_match_lead_time__value, '')::varchar(128) as match_lead_time,
    nullif(property_approved_by_services__value, '')::varchar(
        2048
    ) as approved_by_services,
    nullif(property_rejected_reason__value, '')::varchar(2048) as rejected_reason,
    nullif(property_im_deal_type__value, '')::varchar(2048) as im_deal_type,
    nullif(property_original_im_deal_s_order_number__value, '')::varchar(
        2048
    ) as original_im_deal_s_order_number,
    nullif(property_im_post_sales_value_score__value, '')::varchar(128) as im_post_sales_value_score,
    nullif(property_im_post_sales_concerning_actions__value, '')::varchar(128) as im_post_sales_concerning_actions,
    nullif(property_critical_to_quality_check_complete__value, '')::varchar(
        2048
    ) as critical_to_quality_check_complete,
    nullif(property_hs_priority__value, '')::varchar(64) as hs_priority,
    case
        when property_delayed_due_to_customs__value = 'true'
        then true
        when property_delayed_due_to_customs__value = 'false'
        then false
        when property_delayed_due_to_customs__value = ''
        then null
    end::boolean as is_delayed_due_to_customs,
    nullif(property_im_pm__value, '')::bigint as im_pm,
    nullif(property_me_team_review__value, '')::varchar(2048) as me_team_review_results,
    nullif(property_protolabs_cross_sell_company__value, '')::varchar(
        2048
    ) as pl_cross_sell_company_name,
    nullif(property_protolabs_cross_sell_salesperson__value, '')::varchar(
        2048
    ) as pl_sales_rep_name,
      nullif(property_protolabs_cross_sell_sales_manager__value, '')::varchar(
        2048
    ) as pl_sales_rep_manager_name,
    nullif(property_pl_cross_sell_channel__value, '')::varchar(
        2048
    ) as pl_cross_sell_channel,
    nullif(property_business_development_manager__value, '')::varchar(
        2048
    ) as pl_business_development_manager_id,
    nullif(property_technical_program_manager__value, '')::varchar(
        2048
    ) as technical_program_manager_id,
    case
        when property_sales_priced__value = 'true' then true else false
    end as is_sales_priced,
    nullif(property_tracking_number__value, '')::varchar(124) as hubspot_tracking_number,
    nullif(property_tracking_link__value, '')::varchar(124) as hubspot_tracking_link,
    nullif(property_signed_customer_quote_pdf_link__value, '')::varchar(2048) as hubspot_signed_customer_quote_pdf_link,
    nullif(property_why_still_in_production__value, '')::varchar(2048) as why_still_in_production,
    nullif(property_last_page_seen__value, '')::varchar(65535) as last_page_seen,
    nullif(lower(property_latest_traffic_source_static__value), '')::varchar(124) as last_traffic_source
    
from deals as ehd
left join
    (
        select _sdc_source_key_dealid, value
        from
            (
                select
                    _sdc_source_key_dealid,
                    value,
                    row_number() over (
                        partition by _sdc_source_key_dealid order by value
                    ) as rn
                from {{ source("ext_hubspot", "deals__associations__associatedcompanyids") }}
            ) a
        where rn = 1
    ) as ehdacomp
    on ehd.dealid = ehdacomp._sdc_source_key_dealid
left join
    (
        select _sdc_source_key_dealid, value
        from
            (
                select
                    _sdc_source_key_dealid,
                    value,
                    row_number() over (
                        partition by _sdc_source_key_dealid order by value
                    ) as rn
                from {{ source("ext_hubspot", "deals__associations__associatedvids") }}
            ) a
        where rn = 1
    ) as ehdacont
    on ehd.dealid = ehdacont._sdc_source_key_dealid
where rown = 1
