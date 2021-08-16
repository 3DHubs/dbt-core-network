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
    fact_interactions.hubspot_deal_id,
    count(fact_interactions.interaction_id) as order_number_of_interactions,
    count(
        case
            when
                fact_interactions.interaction_type_mapped = 'Outgoing Email' then interaction_id
        end
    ) as order_number_of_outgoing_emails,
    count(
        case
            when
                fact_interactions.interaction_type_mapped = 'Incoming Email' then interaction_id
        end
    ) as order_number_of_incoming_emails,
    bool_or(
        coalesce(lower(
             fact_hubspot_engagements.task_subject
        ) like ('%svp%'), false)
    ) as order_has_svp_interaction,
    bool_or(
        coalesce(lower(
            fact_hubspot_engagements.task_subject
        ) like ('%invoice extra%')
        or lower(fact_hubspot_engagements.task_subject) like ('%extra charge%')
        or lower(fact_hubspot_engagements.task_subject) like ('%extra cost%')
        or lower(fact_hubspot_engagements.task_subject) like ('%underquote%')
        or lower(fact_hubspot_engagements.note_body) like ('%underquote%'),
        false)
    ) as order_has_underquote_interaction
from {{ ref('fact_interactions') }}
left join
    {{ ref('fact_hubspot_engagements') }} on
        fact_interactions.interaction_id = fact_hubspot_engagements.id
where fact_interactions.hubspot_deal_id is not null
group by 1
