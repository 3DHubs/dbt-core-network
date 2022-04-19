----------------------------------------------------------------
-- TECHNICAL REVIEWS AGGREGATES
----------------------------------------------------------------

-- Note: the current source for technical reviews seems to be hubspot,
-- because of this I deleted the aggregates from supply to keep this model simple.
-- The supply fields that were output from this models were not used downstream. 
-- Updated: Feb 2022 (Diego)


------------- SOURCE: HUBSPOT -----------

select orders.uuid as order_uuid,
       true                             as has_technical_review,
       min(first_review_ongoing_date)   as hubspot_first_technical_review_ongoing_at,
       min(review_completed_date)       as hubspot_first_technical_review_completed_at
from {{ ref('fact_hubspot_deal_reviews') }} as dr
left join {{ ref('prep_supply_orders') }} as orders on dr.deal_id = orders.hubspot_deal_id
group by 1
