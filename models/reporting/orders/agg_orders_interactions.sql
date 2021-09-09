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

select distinct
    interactions.hubspot_deal_id,
    count(interactions.interaction_id) as number_of_interactions,
    count(
        case
            when
                interactions.interaction_type_mapped = 'Outgoing Email' then interaction_id
        end
    ) as number_of_outgoing_emails,
    count(
        case
            when
                interactions.interaction_type_mapped = 'Incoming Email' then interaction_id
        end
    ) as number_of_incoming_emails,
    bool_or(
        coalesce(lower(
             engagements.task_subject
        ) like ('%svp%'), false)
    ) as has_svp_interaction,
    bool_or(
        coalesce(lower(
            engagements.task_subject
        ) like ('%invoice extra%')
        or lower(engagements.task_subject) like ('%extra charge%')
        or lower(engagements.task_subject) like ('%extra cost%')
        or lower(engagements.task_subject) like ('%underquote%')
        or lower(engagements.note_body) like ('%underquote%'),
        false)
    ) as has_underquote_interaction

from {{ ref('fact_interactions') }} as interactions
left join {{ ref('fact_hubspot_engagements') }} as engagements
    on interactions.interaction_id = engagements.engagement_id
where interactions.hubspot_deal_id is not null
and interaction_type_mapped <> 'Portal'
group by 1
