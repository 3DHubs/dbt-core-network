
with freshdesk_interactions as (

select distinct {{ redshift.try_cast('con.id', 'bigint') }}                         as interaction_id,
                con.ticket_id,
                null::bigint                                                        as source_ticket_id,
                con.created_at                                                      as created_date,
                a.id                                                                as agent_id,
                a.contact_name                                                      as agent_name,
                support_email,
                nullif(regexp_substr(con.from_email, '\\w+@\\w+\\.\\w+', 1, 1), '') as from_email,
                case
                    when agent_name = 'Dev Ops' then 'Dev Ops'
                    when con.source = 2 and con.category = 6 then 'agent reply'/*  */
                    when con.private then 'note'
                    when con.category = 3 then 'agent reply'
                    else 'customer reply' end                                       as interaction_type,
                case when interaction_type = 'agent reply' then 1 else 0 end        as agent_interaction_count,
                case when con.private is true and agent_name != 'Dev Ops' then 1 
                    else 0 end                                                      as internal_interaction_count,
                con.incoming::int                                                   as customer_interaction_count,
                false                                                               as /*  */is_first_interaction,
                t.ticket_tag_3d_hubs                                                as ticket_tag,
                t.source                                                            as source,
                'dbt_prod_data_lake.freshdesk_ticket_conversations; not source 2 && 3; not source phone; is_primary_ticket = ' ||
                'True'                                                              as _data_source
from {{ ref('freshdesk_ticket_conversations') }} as con
          left outer join {{ ref('freshdesk_agents')}} as a on con.user_id = a.id
          left outer join {{ ref('stg_fact_freshdesk_tickets') }} as t on t.ticket_id = con.ticket_id
where not (con.source = 2 and con.category = 3) -- this is put in place to filter out all public notes (these should not be considered an interaction)
  and t.source <> 'phone'
  and exists(select 1 from {{ ref('stg_fact_freshdesk_tickets') }} as tix where tix.is_primary_ticket and tix.ticket_id = con.ticket_id)
union all
-- Non-primary tickets
select distinct {{ redshift.try_cast('con.id', 'bigint') }}                         as interaction_id,
                t.ticket_id                                                         as ticket_id,
                t.linked_ticket_id::bigint                                          as source_ticket_id, -- Conversations that were imported from this ticket id
                con.created_at                                                      as created_date,
                a.id                                                                as agent_id,
                a.contact_name                                                      as agent_name,
                support_email,
                nullif(regexp_substr(con.from_email, '\\w+@\\w+\\.\\w+', 1, 1), '') as from_email,
                case
                    when agent_name = 'Dev Ops' then 'Dev Ops'
                    when con.source = 2 and con.category = 6 then 'agent reply'
                    when con.private then 'note'
                    when con.category = 3 then 'agent reply'
                    else 'customer reply' end                                       as interaction_type,
                case when interaction_type = 'agent reply' then 1 else 0 end        as agent_interaction_count,
                case when con.private is true and agent_name != 'Dev Ops' then 1 
                    else 0 end                                                      as internal_interaction_count,
                con.incoming::int                                                   as customer_interaction_count,
                false                                                               as is_first_interaction,
                t.ticket_tag_3d_hubs                                                as ticket_tag,
                t.source                                                            as source,
                'dbt_prod_data_lake.freshdesk_ticket_conversations; not source 2 && 3; not source phone; is_primary_ticket = ' ||
                'False'                                                             as _data_source
from {{ ref('freshdesk_ticket_conversations') }} as con
          left outer join {{ ref ('freshdesk_agents') }} as a on con.user_id = a.id
          left outer join {{ ref('stg_fact_freshdesk_tickets') }} as t on t.linked_ticket_id = con.ticket_id
where not (con.source = 2 and con.category = 3) -- this is put in place to filter out all public notes (these should not be considered an interaction)
  and t.source <> 'phone'
  and exists(select 1 from {{ ref('stg_fact_freshdesk_tickets') }} as tix where not tix.is_primary_ticket and tix.ticket_id = con.ticket_id)
union all
select {{ redshift.try_cast('t.ticket_id', 'bigint') }}                               as interaction_id, -- TODO: better to populate w/ NULL?
       t.ticket_id,
       null::bigint                                                                   as source_ticket_id,
       t.created_date,
       t.agent_id                                                                     as agent_id,
       a.contact_name                                                                 as agent_name,
       null                                                                           as support_email,
       null                                                                           as from_email,
       case
           when t.source = 'portal' then 'portal'
           when (t.source = 'email' and requester_email ~ '@(3d)?hubs.com') or t.source = 'outbound_email' then 'agent initiation'
           when t.source = 'email' then 'customer initiation'
           when t.source = 'feedback_widget' then 'customer initiation' end           as interaction_type,
       case when interaction_type = 'agent initiation' then 1 else 0 end              as agent_interaction_count,
       0                                                                              as internal_interaction_count,
       case when interaction_type = 'customer initiation' then 1 else 0 end           as customer_interaction_count,
       true                                                                           as is_first_interaction,
       t.ticket_tag_3d_hubs                                                           as ticket_tag,
       t.source,
       'reporting.fact_freshdesk_tickets; not source phone'                           as _data_source
from {{ ref('stg_fact_freshdesk_tickets') }} as t
          left outer join {{ ref ('freshdesk_agents') }} as a on t.agent_id = a.id
where true
  and t.source <> 'phone'
),

/* Unique conversation ids only
       This is to take into account scenarios where ticket A got merged into B and B into A, resulting in some odd
       exceptions. In rare cases duplicates can occur for primary and non-primary tickets. For now we are only allowing
       unique interaction_ids. In this process we ignore interaction_id values that are in fact ticket_id values. */

freshdesk_interactions_rn as (
    select *,
        row_number() over (partition by interaction_id order by null) as rn
    from freshdesk_interactions
     )

select interaction_id,
       ticket_id,
       source_ticket_id,
       created_date,
       agent_id,
       agent_name,
       support_email,
       from_email,
       interaction_type,
       agent_interaction_count,
       internal_interaction_count,
       customer_interaction_count,
       is_first_interaction,
       ticket_tag,
       source,
       _data_source
from freshdesk_interactions_rn
where case
          when _data_source != 'reporting.fact_freshdesk_tickets; not source phone; is_primary_ticket = True' then
                  rn
                  = 1 end
   or _data_source = 'reporting.fact_freshdesk_tickets; not source phone; is_primary_ticket = True'
