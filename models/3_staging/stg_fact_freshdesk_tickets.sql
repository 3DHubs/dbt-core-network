{{
       config(
              sort = ["ticket_id", "order_uuid"],
              post_hook = "analyze {{ this }}",
              tags=["notmultipledayrefresh"]
       )
}}

with fdmapping as (select coalesce(hs.uuid, po.order_uuid, ho.uuid) as order_uuid, t.id
                   from {{ ref('freshdesk_tickets') }} t
             left outer join {{ ref('prep_supply_orders') }} as hs
                   on hs.hubspot_deal_id = t.hubspot_deal_id
                       left outer join {{ ref('prep_supply_orders') }} as ho on ho.number = t.derived_document_number
                       left outer join {{ ref('prep_supply_documents') }} as po on po.document_number = t.derived_po_number
                   where t._is_latest),
     tickets as (select * from {{ ref('freshdesk_tickets') }} where _is_latest),
     t1 as (
         select ft.id,
                ft.created_at,
                least(ft.created_at, min(ft_merged_into.created_at))               as first_created_at,
                ft.stats_resolved_at,
                least(ft.stats_resolved_at, min(ft_merged_into.stats_resolved_at)) as first_stats_resolved_at,
                ft.stats_first_responded_at,
                least(ft.stats_first_responded_at,
                      min(ft_merged_into.stats_first_responded_at))                as first_stats_first_responded_at,
                fme.linked_ticket_id,
                fme.raw_merge_event,
                fme.merged_at,
                case when is_merged then false else true end                          is_primary_ticket,
                row_number() over (partition by ft.id order by fme.merged_at desc) as merge_sequence
         from tickets ft
                  left outer join {{ ref('freshdesk_merge_events') }} fme
         on fme.is_merged_with_self = false and ft.id = fme.ticket_id
             left outer join tickets as ft_merged_into
             on fme.has_incoming_merge = true and fme.linked_ticket_id = ft_merged_into.id
         group by 1, 2, 4, 6, 8, 9, 10, 11),
     uniqs as (
         select tickets.*,
                t1.first_created_at,
                t1.first_stats_resolved_at,
                t1.first_stats_first_responded_at,
                t1.linked_ticket_id,
                t1.raw_merge_event,
                t1.is_primary_ticket
         from tickets
                  left outer join t1 on t1.id = tickets.id
         where merge_sequence = 1
     ),
     agents as (select id, contact_name from {{ ref('freshdesk_agents') }} where _is_latest),
     contacts as (select id, name, email from {{ ref('freshdesk_contacts') }} where _is_latest),
     groups as (select id::bigint as id, name from {{ ref('freshdesk_groups') }} where _is_latest),
     companies as (select id, name from {{ ref('freshdesk_companies') }} where _is_latest),
     freshdesk_survey_results as (
         select *,
                row_number() over (partition by ticket_id order by id) as rn --filters out duplicate survey responses on 1 ticket (2020-09 less than 1%)
         from {{ ref('freshdesk_survey_results') }}
         where survey_id = 13000000056)

select t.id                                                                  as ticket_id,
       fdmapping.order_uuid,
       t.subject,
       t.first_created_at                                                    as created_date,
       g.name                                                                as group,
       t.responder_id                                                        as agent_id,
       a.contact_name                                                        as ticket_agent_name,
       c.name                                                                as customer_contact,
       t.requester_id                                                        as requester_id,
       c.email                                                               as requester_email,
       com.name                                                              as company,
       t.custom_fields_cf_ticketsubcategorised                               as category,
       t.custom_fields_cf_categories                                         as sub_category,
       t.status_description                                                  as status,
       t.priority_description                                                as priority,
       t.source_description                                                  as source,
       custom_fields_cf_3d_hubs_tag                                          as ticket_tag_3d_hubs,
       t.first_stats_first_responded_at                                      as first_response_date,
       t.first_stats_resolved_at                                             as resolved_date,
       datediff('day', t.first_created_at, t.first_stats_resolved_at)        as num_days_to_resolution,
       datediff('day', t.first_created_at, t.first_stats_first_responded_at) as num_days_to_first_response,
       fsr.id                                                                as survey_result_id,
       fsr.survey_id                                                         as survey_id,
       fsr.agent_id                                                          as customer_satisfaction_survey_agent_id,
       afsr.contact_name                                                     as customer_satisfaction_survey_agent_name,
       fsr.survey_id                                                         as customer_satisfaction_survey_id,
       fsr.created_at                                                        as customer_satisfaction_survey_completed_at,
       fsr.ratings_default_question                                          as customer_satisfaction_score,
       fsr.ratings_question_13000005574                                      as customer_satisfaction_score_technical_knowledge,
       fsr.ratings_question_13000005575                                      as customer_satisfaction_score_friendliness,
       decode(fsr.ratings_default_question,
              103, 'extremely satisfied',
              102, 'satisfied',
              100, 'other',
              -102, 'dissatisfied',
              -103, 'extremely dissatisfied'
           )                                                                 as customer_satisfaction,
       decode(fsr.ratings_question_13000005574::decimal(15, 2)::int,
              103, 'extremely satisfied',
              102, 'satisfied',
              100, 'other',
              -102, 'dissatisfied',
              -103, 'extremely dissatisfied'
           )                                                                 as customer_satisfaction_technical_knowledge,
       decode(fsr.ratings_question_13000005575::decimal(15, 2)::int,
              103, 'extremely satisfied',
              102, 'satisfied',
              100, 'other',
              -102, 'dissatisfied',
              -103, 'extremely dissatisfied'
           )                                                                 as customer_satisfaction_friendliness,
       fsr.feedback                                                          as customer_satisfaction_feedback,
       t.linked_ticket_id,
       t.is_primary_ticket
from uniqs t
         left outer join agents a on t.responder_id = a.id
         left outer join contacts c on t.requester_id = c.id
         left outer join groups g on t.group_id = g.id
         left outer join companies com on t.company_id = com.id
         left outer join fdmapping fdmapping on t.id = fdmapping.id
         left outer join freshdesk_survey_results fsr on fsr.ticket_id = t.id and rn = 1
         left outer join agents afsr on fsr.agent_id = afsr.id