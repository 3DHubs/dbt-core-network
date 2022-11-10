-- source from Hubspot Stitch setup initially by Nihad.
select dealid::bigint                                                                                         as deal_id,
       ehdacomp.value::bigint                                                                                 as hs_latest_associated_company_id,
       ehdacont.value::bigint                                                                                 as hs_latest_associated_contact_id,
       nullif(property_deal_category__value, '')::varchar(512)                                                as deal_category,
       nullif(property_closed_lost_reason__value, '')::varchar(124)                                           as closed_lost_reason,
       property_amount__value__double                                                                         as amount,
       property_closedate__value::timestamp without time zone                                                 as closedate,
       property_createdate__value::timestamp without time zone                                                as createdate,
       nullif(property_technologies__value, '')::varchar(128)                                                 as technologies,
       nullif(property_pipeline__value, '')::varchar(128)                                                     as pipeline,
       nullif(property_dealstage__value, '')::varchar(128)                                                    as dealstage,
       nullif(property_hubspot_owner_id__value, '')::bigint                                                   as hubspot_owner_id,
       (property_hubspot_owner_assigneddate__value)::timestamp without time zone                              as hubspot_owner_assigneddate,
       nullif(property_supply_owner__value, '')::varchar(124)                                                 as supply_owner,
       nullif(property_review_type__value, '')::varchar(124)                                                  as review_type,
       nullif(property_sales_engineer__value, '')::int                                                        as sales_engineer,
       nullif(property_bdr_assigned__value, '')::int                                                          as bdr_assigned,
       nullif(property_delay_liability__value, '')::varchar(124)                                              as delay_liability,
       nullif(property_in_review_reason__value, '')::varchar(124)                                             as in_review_reason,
       nullif(property_cancellation_reason__value, '')::varchar(124)                                          as cancellation_reason,
       (TIMESTAMP 'epoch' + property_first_time_quote_sent_date__value / 1000 *
                            INTERVAL '1 second')::timestamp without time zone                                 as first_time_quote_sent_date,
       nullif(property_dispute_liability__value, '')::varchar(124)                                            as dispute_liability,
       (TIMESTAMP 'epoch' + property_first_time_response_date__value / 1000 *
                            INTERVAL '1 second')::date                                                        as first_time_response_date,
-- trunc(property_first_time_response_date__value)::date as first_time_response_date,
       case
           when property_high_risk__value = 'Yes' then true
           when property_high_risk__value = ''
               then null end ::boolean                                                                        as high_risk,
       nullif(property_customer_success_manager__value, '')::bigint                                           as customer_success_manager,
       nullif(property_bdr_company_source__value, '')::varchar(65535)                                         as bdr_company_source,
       property_estimated_close_amount__value__double                                                         as estimated_close_amount,
       nullif(property_qc_inspection_result__value, '')::varchar(65535)                                       as qc_inspection_result,
       nullif(property_purchasing_manager__value, '')::bigint                                                 as purchasing_manager,
       nullif(property_delay_status__value, '')::varchar(65535)                                               as delay_status,
       nullif(property_review_owner__value, '')::varchar(65535)                                               as review_owner,
       nullif(property_sourcing_owner__value, '')::bigint                                                     as sourcing_owner,
       case
           when property_strategic__value = 'true' then true
           when property_strategic__value = 'false' then false
           when property_strategic__value = ''
               then null end ::boolean                                                                        as is_strategic,
       nullif(property_closing_probability__value, '')::varchar(2048)                                         as closing_probability,
       nullif(property_latest_qc_result__value, '')::varchar(2048)                                            as latest_qc_result,
       nullif(property_in_country_qc_status__value, '')::varchar(2048)                                        as in_country_qc_status,
       nullif(property_review_outcome__value, '')::varchar(2048)                                              as review_outcome,
       nullif(nullif(property_rfq_type__value, 'None'), '')::varchar(2048)                                    as rfq_type,
       nullif(property_target_price__value, '')::varchar(128)                                                 as target_price,
       nullif(property_match_lead_time__value, '')::varchar(128)                                              as match_lead_time,
       nullif(property_approved_by_services__value, '')::varchar(2048)                                        as approved_by_services,
       nullif(property_rejected_reason__value, '')::varchar(2048)                                             as rejected_reason,
       nullif(property_im_deal_type__value, '')::varchar(2048)                                                as im_deal_type,
       nullif(property_original_im_deal_s_order_number__value, '')::varchar(2048)                             as original_im_deal_s_order_number,
       nullif(property_critical_to_quality_check_complete__value, '')::varchar(2048)                          as critical_to_quality_check_complete,
       nullif(property_hs_priority__value, '')::varchar(64)                                                   as hs_priority,
       case
           when property_delayed_due_to_customs__value = 'true' then true
           when property_delayed_due_to_customs__value = 'false' then false
           when property_delayed_due_to_customs__value = ''
               then null end ::boolean                                                                        as is_delayed_due_to_customs,
       nullif(property_im_pm__value, '')::bigint                                                              as im_pm,
       nullif(property_me_team_review__value, '')::varchar(2048)                                              as me_team_review_results
from {{ source('ext_hubspot', 'deals') }}  as ehd
         left join {{ source('ext_hubspot', 'deals__associations__associatedcompanyids') }}  as ehdacomp
                   on ehd.dealid = ehdacomp._sdc_source_key_dealid
         left join (select _sdc_source_key_dealid,
                           value
                    from (select _sdc_source_key_dealid
                               , value
                               , ROW_NUMBER() OVER (PARTITION BY _sdc_source_key_dealid ORDER BY value) AS RN
                          from {{ source('ext_hubspot', 'deals__associations__associatedvids') }}  ) A
                    where RN = 1)
    as ehdacont on ehd.dealid = ehdacont._sdc_source_key_dealid
