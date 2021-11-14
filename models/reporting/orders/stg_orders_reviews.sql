----------------------------------------------------------------
-- TECHNICAL REVIEWS & RFQs
----------------------------------------------------------------

-- Combines:
-- 1. Supply Technical Reviews
-- 2. Reporting Hubspot Deal Reviews
-- 3. Fact Supplier RFQs

------------- SOURCE: SUPPLY -----------

with agg_supply_technical_review as (
       select orders.hubspot_deal_id,
              true                  as has_technical_review,
              count(*)              as number_of_technical_reviews,
              min(str.submitted_at) as supply_first_technical_review_submitted_at,
              max(str.completed_at) as supply_last_technical_review_completed_at
       from {{ ref('technical_reviews') }} as str
                inner join {{ ref('cnc_order_quotes') }} as soq on str.quote_uuid = soq.uuid
                left join {{ ref('cnc_orders') }} as orders on soq.order_uuid = orders.uuid
       group by 1
       order by 2 desc
),

     window_agg_hubspot_technical_review as (
       select deal_id,
              true                             as has_technical_review,
              min(first_review_ongoing_date)   as hubspot_first_technical_review_ongoing_at,
              min(review_completed_date)       as hubspot_first_technical_review_completed_at
       from {{ ref('fact_hubspot_deal_reviews') }}
       where review_outcome = 'completed'
       group by 1
     ),

     rfq_requests as (
       select order_uuid,
              true as has_rfq,
              bool_or(is_winning_bid) as is_rfq_automatically_sourced, -- An quote can be duplicated manually and not show as a winning bid
              count(distinct supplier_id) number_of_suppliers_rfq_requests,
              count(distinct case when supplier_rfq_responded_date is not null then supplier_id else null end) number_of_suppliers_rfq_responded,
              count(*) number_of_rfq_requests,
              sum(case when supplier_rfq_responded_date is not null then 1 else 0 end) as number_of_rfq_responded
       from {{ ref('fact_rfq_behaviour') }} as rfq
       group by 1  
     )

-- Final Query

select orders.uuid as order_uuid,
       coalesce(hubspot.has_technical_review, supply.has_technical_review) as has_technical_review,
       supply.number_of_technical_reviews,
       supply.supply_first_technical_review_submitted_at,
       hubspot_first_technical_review_ongoing_at,
       hubspot.hubspot_first_technical_review_completed_at,
       supply.supply_last_technical_review_completed_at,
       coalesce(rfq.has_rfq, false) as has_rfq,
       rfq.is_rfq_automatically_sourced,
       rfq.number_of_suppliers_rfq_requests,
       rfq.number_of_suppliers_rfq_responded, 
       rfq.number_of_rfq_requests,
       rfq.number_of_rfq_responded
from {{ ref('cnc_orders') }} as orders
left join agg_supply_technical_review as supply on orders.hubspot_deal_id = supply.hubspot_deal_id
left join window_agg_hubspot_technical_review as hubspot on supply.hubspot_deal_id = hubspot.deal_id
left join rfq_requests as rfq on orders.uuid = rfq.order_uuid
