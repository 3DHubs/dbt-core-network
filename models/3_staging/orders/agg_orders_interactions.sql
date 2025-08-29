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
    count(distinct interactions.interaction_id) as number_of_interactions,
    count(distinct case when interactions.source = 'Freshdesk' then interactions.interaction_id end) as number_of_interactions_fd,

    count(distinct
        case
            when
                interactions.interaction_type_mapped = 'Outgoing Email' then  interaction_id
        end
    ) as number_of_outgoing_emails,
    count(distinct
        case
            when interactions.source = 'Freshdesk' and
                interactions.interaction_type_mapped = 'Outgoing Email' then  interaction_id
        end
    ) as number_of_outgoing_emails_fd,

    count(distinct
        case
            when
                interactions.interaction_type_mapped = 'Incoming Email' then  interaction_id
        end
    ) as number_of_incoming_emails,
    count(distinct
        case
            when interactions.source = 'Freshdesk' and
                interactions.interaction_type_mapped = 'Incoming Email' then  interaction_id
        end
    ) as number_of_incoming_emails_fd,

    count(distinct
        case
            when interactions.source = 'Freshdesk' and
                interactions.interaction_type_mapped = 'Note' then  interaction_id
        end
    ) as number_of_notes_fd,

--todo-migration-test boolor_agg
    boolor_agg(
        coalesce(lower(
             engagements.task_subject
        ) like ('%svp%'), false)
    ) as has_svp_interaction
from {{ ref('fact_interactions') }} as interactions
left join {{ ref('fact_hubspot_engagements') }} as engagements
    on interactions.interaction_id = engagements.engagement_id
where interactions.hubspot_deal_id is not null
and interaction_type_mapped <> 'Portal'
group by 1
