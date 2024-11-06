select ticket_id,
       order_uuid,
       created_date as created_at,
       "group",
       tickets_team,
       resolved_date,
       survey_result_id,
       customer_satisfaction_survey_completed_at,
       customer_satisfaction,
       first_response_start_date as first_response_start_at,
       first_response_reply_date as first_response_reply_at,
       first_response_time_in_hours
from  {{ ref('fact_freshdesk_tickets') }}
where created_date >= date_add('years',-2,date_trunc('year',getdate()))
