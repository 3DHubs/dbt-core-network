{{
    config(
        post_hook="analyze {{ this }}"
    )
}}

with hubspot_engagements as (
    select * from {{ source('ext_hubspot', 'hubspot_engagements') }}
),

stg_associations as (
    select id,
           replace(replace(he.contact_ids, '[', ''), ']', '') as engagement_contact_ids,
           replace(replace(he.company_ids, '[', ''), ']', '') as engagement_company_id,
           replace(replace(he.deal_ids, '[', ''), ']', '') as engagement_deal_ids

    from hubspot_engagements as he
),

ns as (
    {% for i in range(10) %}

        select {{ i }} + 1 as n {% if not loop.last %} union all {% endif %}

{% endfor %}
),

-- Gather contacts
engagements_gather1 as (
             select *,
                    trim(split_part(e.engagement_contact_ids, ',', ns.n)) as contact_id
             from ns
                      inner join stg_associations e on ns.n <= regexp_count(e.engagement_contact_ids, ',') + 1
         ),
         engagements_gather2 as (
             select *,
                    trim(split_part(c.engagement_deal_ids, ',', ns.n)) as deal_id
             from ns
                      inner join engagements_gather1 c on ns.n <= regexp_count(c.engagement_deal_ids, ',') + 1
         ),
         engagements_gather3 as (
             select *,
                    trim(split_part(c.engagement_company_id, ',', ns.n)) as company_id
             from ns
                      inner join engagements_gather2 c on ns.n <= regexp_count(c.engagement_company_id, ',') + 1
         )
select he.id::bigint                                                              as engagement_id,
       he.created_at,
       he.type,
       he.source,
       he.owner_id as engagement_owner_id,
       ho.name as engagement_owner_name,
       nullif(he_gather.contact_id, '')::bigint as contact_id,
       nullif(he_gather.company_id, '')::bigint as company_id,
       nullif(he_gather.deal_id, '')::bigint    as deal_id,
       case when he.type = 'NOTE' then he.body_preview end as note_body,
       --Email fields
       case when he.type = 'EMAIL' then he.status end as email_status,
       case when he.type = 'EMAIL' then he.subject end as email_subject,
       --Task Fields
       case when he.type = 'TASK' then he.subject end as task_subject,
       case when he.type = 'TASK' then he.status end as task_status,
       case when he.type = 'TASK' then he.for_object_type end as task_target,
       he.task_type,
       hedm.label,
       he.duration_milliseconds / 1000 as call_duration_seconds,
       case when he.type = 'MEETING' then he.title end as meeting_title,
       date_diff('minutes', timestamp 'epoch' + he.start_time / 1000 * interval '1 second',
                    timestamp 'epoch' + he.end_time / 1000 * interval '1 second') as meeting_duration_mins

from hubspot_engagements as he
         inner join engagements_gather3 as he_gather on he.id = he_gather.id
         left outer join {{ ref('hubspot_owners') }} ho on he.owner_id = ho.owner_id 
         left outer join {{ ref('seed_hubspot_engagement_dispositions') }} hedm on he.disposition = hedm.uuid
