with survey_results as (
    select *,
           row_number() over (partition by id, survey_id, user_id order by load_timestamp desc) as rn

    from {{ source('landing', 'freshdesk_survey_results_landing') }}
)
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
from survey_results
where rn = 1