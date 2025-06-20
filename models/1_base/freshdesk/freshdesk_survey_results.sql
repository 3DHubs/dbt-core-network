select id,
       survey_id,
       user_id,
       agent_id,
       feedback,
       group_id,
       ticket_id,
       created_at,
       updated_at,
       ratings_default_question,
       ratings_question_13000005574,
       ratings_question_13000005575,
       load_timestamp as _load_timestamp
from {{ ref('dbt_src_external', 'gold_ext_airbyte_freshdesk_survey_results')}}
