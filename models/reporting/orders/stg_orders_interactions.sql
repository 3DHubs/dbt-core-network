----------------------------------------------------------------
-- INTERACTIONS DATA at ORDER LEVEL
----------------------------------------------------------------

-- Sources: 
-- 1. Fact Interactions
-- 2. HS Engagements

-- The table "Fact Interactions" combines the sources of Freshdesk 
-- and Hubspot to contain a big table with all interactions. The 
-- HS Engagements table is joined to get some fields that are unique
-- to HS, like Task Subject (as Freshdesk don't have interactions of
-- task type.)


select distinct order_uuid,
                count(*) as order_number_of_interactions,
                count(case when interaction_type_mapped = 'Outgoing Email' then interaction_id end) as order_number_of_outgoing_emails,
                count(case when interaction_type_mapped = 'Incoming Email' then interaction_id end) as order_number_of_incoming_emails,
                bool_or(case when lower(task_subject) like ('%svp%') then true else false end) as order_has_svp_interaction,
                bool_or(case
                    when lower(task_subject) like ('%invoice extra%')
                        or lower(task_subject) like ('%extra charge%')
                        or lower(task_subject) like ('%extra cost%')
                        or lower(task_subject) like ('%underquote%')
                        or lower(note_body) like ('%underquote%') then true
                    else false end)                                                    as order_has_underquote_interaction
from {{ ref('fact_interactions') }} as fi
left join {{ ref('fact_hubspot_engagements') }} as fhe on fi.interaction_id = fhe.id
group by 1
