
with frt_rn as (
        
        select *,
               row_number() over (partition by ticket_id order by created_date asc) as frt_idx
        
        from {{ ref('fact_freshdesk_interactions') }}
        
        where interaction_type <> 'note' --notes are considered internal communication and not an email reply so filter them out for FRT index
        
        order by ticket_id, created_date
    ),

     frt_transform as (
        select ticket_id,
               -- PO tickets are created at the time an order is sourced,
               -- so the conversation only begins at the time of the second interaction (frt_idx = 2)
               -- only need to measure FRT when the conversation is initiated by the supplier so we filter on type
               min(case
                       when source = 'portal' and ticket_tag = 'supplier' and frt_idx = 2
                           and
                            (interaction_type = 'customer reply' or from_email ~ 'notifier@(3d)?hubs.com')
                           then created_date
                       else null end) as first_response_time_start_supplier,
               -- customer tickets are created at the time the question is raised, so idx = 1.
               -- only need to calculate FRT when the customer initiated the conversation so add the filter
               min(case
                       when (ticket_tag = 'customer' or interaction_type like 'customer%') and frt_idx = 1
                           then created_date
                       else null end) as first_response_time_start_customer,
               --Here we want to capture the first agent reply for a ticket.
               --Filter out notifier because this is an automatic reply that is genertaed when the ticket is submitted
               min(case
                       when interaction_type = 'agent reply' and frt_idx > 1
                           and from_email !~ 'notifier@(3d)?hubs.com'
                           then created_date
                       else null end) as first_reply_time
        from frt_rn
             -- The "group by" here functions only to bring it all onto ticket level in a single row
             -- We always select only 1 row per condition because of frt_idx filter
        group by 1
     ),
     
         first_response_time as (
    
             select frt_transform.ticket_id,
                    coalesce(frt_transform.first_response_time_start_customer,
                             frt_transform.first_response_time_start_supplier)             as first_response_start_date,
                    case
                        when first_response_start_date is not null
                            then frt_transform.first_reply_time end                        as first_response_reply_date,
                    --we only want to expose first_response reply for tickets where it is relevant.
                    datediff('hour', first_response_start_date, first_response_reply_date) as first_response_time_in_hours
             from frt_transform
         )

select ft.ticket_id,
        ft.order_uuid,
        ft.subject,
        ft.created_date,
        ft."group",
        ft.ticket_agent_name,
        ft.customer_contact,
        ft.company,
        ft.category,
        ft.sub_category,
        ft.status,
        ft.priority,
        ft.source,
        ft.ticket_tag_3d_hubs,
        ft.resolved_date,
        ft.num_days_to_resolution,
        ft.survey_result_id,
        ft.survey_id,
        ft.customer_satisfaction_survey_agent_id,
        ft.customer_satisfaction_survey_agent_name,
        ft.customer_satisfaction_survey_id,
        ft.customer_satisfaction_survey_completed_at,
        ft.customer_satisfaction_score,
        ft.customer_satisfaction_score_technical_knowledge,
        ft.customer_satisfaction_score_friendliness,
        ft.customer_satisfaction,
        ft.customer_satisfaction_technical_knowledge,
        ft.customer_satisfaction_friendliness,
        ft.customer_satisfaction_feedback,
        ft.linked_ticket_id,
        ft.is_primary_ticket,
        -- Only populating first_response attributes for tickets that were not involved in a merge
        case when fme.ticket_id is null then frt.first_response_start_date end    first_response_start_date,
        case when fme.ticket_id is null then frt.first_response_reply_date end    first_response_reply_date,
        case when fme.ticket_id is null then frt.first_response_time_in_hours end first_response_time_in_hours,
        case when fme.ticket_id is not null then true else false end              is_involved_in_merge
from {{ ref('stg_fact_freshdesk_tickets') }} as ft
            left join first_response_time frt on ft.ticket_id = frt.ticket_id
            left join {{ ref('freshdesk_merge_events') }} as fme on ft.ticket_id = fme.ticket_id