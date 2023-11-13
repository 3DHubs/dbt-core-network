select review_id,
       deal_id,
       first_review_new_date as first_review_new_at ,
       first_review_ongoing_date as first_review_ongoing_at,
       review_completed_date as review_completed_at,
       total_time_in_review_stage_new_business_minutes,
       total_time_in_review_stage_ongoing_business_minutes,
       review_iteration
from {{ ref('fact_hubspot_deal_reviews') }}
where first_review_new_date >= date_add('years',-2,date_trunc('year',getdate()))
