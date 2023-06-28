-- This model may need improvement, e.g. incremental model.
with ticket_conversations as (
    select *,
           row_number() over (partition by id order by updated_at desc, load_timestamp desc nulls last) as rn
    from {{ source('ext_freshdesk', 'freshdesk_ticket_conversations') }}
)
select tc.id,
       tc.incoming,
       tc.private,
       tc.user_id,
       tc.support_email,
       tc.source,
       fcsm.description                   as source_description,
       tc.category,
       case tc.category
           when 1 then 'Inbound reply'
           when 2 then 'Note'
           when 3 then 'Outbound reply'
           when 4 then 'Reply to a forwarded message (inbound)'
           when 5 then 'Freshdesk Automations'
           when 6 then 'Reply to a forwarded reply (category 4) (outbound)'
           when 7 then 'Customer Feedback'
           end                               derived_category_description,
       tc.ticket_id,
       tc.to_emails,
       tc.from_email,
       tc.cc_emails,
       tc.bcc_emails,
       tc.email_failure_count::float::int as email_failure_count,
       tc.outgoing_failures,
       tc.created_at,
       tc.updated_at,
       tc.attachments,
       tc.source_additional_info,
       tc.body,
       tc.body_text,
       decode(tc.rn, 1, True)             as _is_latest,
       tc.load_timestamp                  as _load_timestamp
from ticket_conversations as tc
         left outer join {{ ref('seed_freshdesk_conversation_source') }} as fcsm
                 on tc.source::int = fcsm.source_id