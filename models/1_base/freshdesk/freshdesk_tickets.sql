select 
        tickets.id,
        tickets.created_at,
        tickets.updated_at,
        tickets.subject,
        tickets.source,
        tickets.status,
        tickets.priority,
        tickets.requester_id,
        tickets.responder_id,
        tickets.company_id,
        tickets.custom_fields_cf_hubspot_id,
        tickets.tags, 
        tickets.custom_fields_cf_deal_owner,
        tickets.custom_fields_cf_ticketsubcategorised,
        tickets.custom_fields_cf_categories,
        tickets.custom_fields_cf_3d_hubs_tag, 
        tickets.custom_fields_cf_hubspot_deal_url,
        tickets.custom_fields_cf_value,
        tickets.stats_first_responded_at,
        tickets.stats_status_updated_at,
        tickets.stats_resolved_at,
        tickets.stats_closed_at,
        tickets._load_timestamp,
        nullif(regexp_substr(tickets.subject, 'C-[a-z0-9]{5,}', 1, 1, 'i'), '')      as derived_document_number, -- Kept regular expression to old format, newer tickets should be linked based on deal id or supply objects.
        nullif(trim(regexp_substr(tickets.subject, 'po-\\w+-?\\w?', 1, 1, 'i')), '') as derived_po_number,
        coalesce(
               tickets.custom_fields_cf_hubspot_id,
               nullif(reverse(regexp_replace(split_part(reverse(tickets.custom_fields_cf_hubspot_deal_url), '/', 1), '[^0-9]',
                                             '')), '')
           )                                                                as hubspot_deal_id,
       ftstam.description                                                   as status_description,
       coalesce(tickets.group_id, ftgb20200401.id)                          as group_id,
       ftpm.description                                                     as priority_description,
       ftsm.description                                                     as source_description,
       tickets._is_latest

from {{ ref('dbt_src_external', 'gold_airbyte_freshdesk_tickets') }} tickets
         left outer join {{ ref('seed_freshdesk_ticket_source') }} ftsm
                         on tickets.source = ftsm.source_id
         left outer join {{ ref('seed_freshdesk_ticket_priority') }} ftpm
                         on tickets.priority = ftpm.priority_id
         left outer join {{ ref('seed_freshdesk_ticket_status') }} ftstam
                         on tickets.status = ftstam.status_id
         left outer join {{ source('ext_freshdesk', 'freshdesk_tickets_groups_backup_20200401') }} ftgb20200401
                         on tickets.id = ftgb20200401.ticket_id