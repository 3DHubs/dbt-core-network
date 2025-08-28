{{
    config(
        materialized='incremental'
    )
}}

with list_of_emails as (
    select distinct email as email
    from {{ ref('hubspot_owners') }}
    where is_current

    union all

    select distinct contact_email as email
    from {{ ref('freshdesk_agents') }}
),

     agents as (
         select emails.email,
                coalesce(ho.owner_name, fda.contact_name) as agent_name,
                ho.owner_id                               as hs_agent_id,
                ho.primary_team_name                      as subteam_hs,
                fda.id                                    as fd_agent_id

         from list_of_emails emails
                  left join (
             select owner_id, primary_team_name, email, name as owner_name
             from {{ ref('hubspot_owners') }}
             where is_current
         ) ho on emails.email = ho.email
                  left join (
             select contact_email, id, contact_name
             from {{ ref('freshdesk_agents') }}
         ) fda on emails.email = fda.contact_email
     ),

     hubspot_engagements as (
         select he.engagement_id                                            as interaction_id,
                'Hubspot'                                                   as source,
                he.created_at                                               as created_at,
                initcap(he.type)                                            as interaction_type,
                case
                    when initcap(he.type) = 'Email' then 'Outgoing Email'
                    else initcap(he.type) end                               as interaction_type_mapped,
                he.engagement_owner_id::varchar                             as agent_id,
                a.agent_name                                                as agent_name,
                a.subteam_hs                                                as subteam,
                case
                    when subteam_hs like 'Account%' or subteam_hs like 'BDR%' or subteam_hs like '%AM%' or
                         subteam_hs like 'SDR%' or
                         subteam_hs like 'Sales%' then 'Sales'
                    when subteam_hs like 'CSR%' or subteam_hs like 'Customer Succes%' or subteam_hs like 'PM%'
                        then 'Customer Team'
                    when subteam_hs like 'Fulfilment%' or subteam_hs like 'Partner%' then 'Partner Support'
                    when subteam_hs like 'Mech%' then 'Mechanical Engineering'
                    when subteam_hs = 'Logistics' then 'Logistics'
                    when subteam_hs = 'Finance' then 'Finance'
                    else null end                                           as team_mapped,
                he.deal_id                                                  as hubspot_deal_id,
                coalesce(he.contact_id, hd.hs_latest_associated_contact_id) as contact_id,
                coalesce(he.company_id, hd.hs_latest_associated_company_id) as company_id,
                null::float                                                 as freshdesk_ticket_id
         from {{ ref('fact_hubspot_engagements') }} as he
            left join agents a
         on he.engagement_owner_id = a.hs_agent_id
             left join {{ ref('hubspot_deals') }} as hd on he.deal_id = hd.deal_id

             {% if is_incremental() %}

         where he.created_at
             > (select max (created_at) from {{ this }} where source = 'Hubspot')

             {% endif %}
     ),

     freshdesk_engagements as (
         select fdi.interaction_id                                     as interaction_id,
                'Freshdesk'                                            as source,
                fdi.created_date                                       as created_at,
                fdi.interaction_type                                   as interaction_type,
                case
                    when fdi.interaction_type = 'agent reply' or fdi.interaction_type = 'agent initiation'
                        then 'Outgoing Email'
                    when fdi.interaction_type = 'customer reply' or fdi.interaction_type = 'customer initiation'
                        then 'Incoming Email'
                    when fdi.interaction_type = 'portal' then 'Portal'
                    when fdi.interaction_type = 'note' then 'Note' end as interaction_type_mapped,
                fdi.agent_id::varchar                                  as agent_id,
                a.agent_name                                           as agent_name,
                fdt.ticket_group                                            as subteam,
                case
                    when fdt.ticket_group like '%Partner%' or
                         fdt.ticket_group like '%Suppliers%' or
                         fdt.ticket_group = 'Legacy Order Fulfilment' then 'Partner Support'
                    when fdt.ticket_group like '%Customer%' or
                         fdt.ticket_group like 'Project Manager%' or
                         fdt.ticket_group in ('TrustPilot', 'Inbox', 'Legal') then 'Customer Team'
                    when fdt.ticket_group like '%Supply%' then 'Supply'
                    when fdt.ticket_group in ('Sales') then 'Sales'
                    when fdt.ticket_group in ('In Review - Supply RFQ', 'Technical Review')
                        then 'Mechanical Engineering'
                    when fdt.ticket_group in ('Broken tracking links', 'Logistics') then 'Logistics'
                    when fdt.ticket_group in ('Finance') then 'Finance'
                    else null end                                      as team_mapped, --todo-migration-test replaced "group" with "ticket_group" upstream due to reserved keyword
                orders.hubspot_deal_id                                 as hubspot_deal_id,
                hd.hs_latest_associated_contact_id                     as contact_id,
                hd.hs_latest_associated_company_id                     as company_id,
                fdi.ticket_id                                          as freshdesk_ticket_id
         from {{ ref('fact_freshdesk_interactions') }} as fdi
            left join {{ ref('fact_freshdesk_tickets') }} fdt
         on fdi.ticket_id = fdt.ticket_id
             left join {{ ref('prep_supply_orders') }} as orders on fdt.order_uuid = orders.uuid
             left join {{ ref('hubspot_deals') }}  as hd on orders.hubspot_deal_id = hd.deal_id
             left join agents a on fdi.agent_id = a.fd_agent_id

             {% if is_incremental() %}

         where fdi.created_date
             > (select max (created_at) from {{ this }} where source = 'Freshdesk')

             {% endif %}
     )
         select *
         from hubspot_engagements
         union all
         select *
         from freshdesk_engagements
