----------------------------------------------------------------
-- DISPUTE DATA
----------------------------------------------------------------

-- Combines Dispute Data with Dispute Resolution 
-- Data is Unique at the Order Level

-- Sources: 
-- Supply Disputes
-- Supply Dispute Line Items
-- Supply Order History Events 



with disputes as (

    with supply_order_history_events as (
        select order_uuid,
               min(created)                  as dispute_created_at,
               'supply_order_history_events' as _data_source
        from {{ ref('fact_order_events') }}
        where description ~ 'dispute' and created < '2022-01-01' --JG 270622, decided with Alison to have only disputes before 2022 measured this way. Since the description field caused issues and official process should be followed.
        group by 1
    ),
         line_item_disputes as (
             select order_uuid,
                    min(dli.created) as line_item_created
             from {{ ref('network_services', 'gold_disputes') }} dis
        left join {{ ref('network_services', 'gold_dispute_line_items_issues') }} dli on dli.dispute_uuid = dis.uuid
             group by 1
         ),
         disputes_temp as (select dis.order_uuid,
                                  dis.id                                                                                         as dispute_id,
                                  coalesce(lid.line_item_created, created)                                                       as dispute_created_at,
                                  dis.requested_outcome                                                                          as dispute_requested_outcome,
                                  dis.type                                                                                       as dispute_type,
                                  row_number() over ( partition by dis.order_uuid order by coalesce(lid.line_item_created, created) desc) as rn -- Prioritize 'new' over 'draft', most recent first
                           from {{ ref('network_services', 'gold_disputes') }} dis
                               left join line_item_disputes lid
                           on dis.order_uuid = lid.order_uuid
                           ),
         disputes as (
             select order_uuid,
                    dispute_id,
                    dispute_created_at,
                    dispute_requested_outcome,
                    dispute_type,
                    'supply_disputes' as _data_source
             from disputes_temp
             where rn = 1 -- Max. one dispute per order
         )
    select order_uuid,
           dispute_id,
           dispute_created_at,
           dispute_requested_outcome,
           dispute_type,
           _data_source
    from disputes
    union all
    select order_uuid,
           null as dispute_id,
           dispute_created_at,
           null as dispute_requested_outcome,
           null as dispute_type,
           _data_source
    from supply_order_history_events
    where not exists(select 1 from disputes where disputes.order_uuid = supply_order_history_events.order_uuid)
),

     dispute_resolution as (
         with freshdesk_ticket_remake_request as (
             select coalesce(hs.uuid, pso.uuid, ppo.order_uuid, dis.order_uuid, ho.uuid, po.order_uuid) as order_uuid,
                    min(con.created_at)                       as dispute_remake_resolution_date
             from {{ ref('freshdesk_tickets') }} t
                      left join {{ ref('prep_supply_orders') }} as hs on hs.hubspot_deal_id = t.hubspot_deal_id
                      left join {{ ref('prep_supply_orders') }} pso on pso.support_ticket_id = t.id
                      left join {{ ref('network_services', 'gold_disputes') }} dis on dis.customer_support_ticket_id = t.id
                      left join {{ ref('prep_purchase_orders') }} ppo on ppo.supplier_support_ticket_id = t.id
                      left outer join {{ ref('prep_supply_orders') }} as ho on ho.number = t.derived_document_number
                      left outer join {{ ref('prep_supply_documents') }} po on po.document_number = t.derived_po_number
                      left outer join {{ ref('freshdesk_ticket_conversations') }} con on con.ticket_id = t.id
             where t._is_latest
               and body_text like
                   'The Manufacturing Partner will resolve the dispute by reproducing the relevant parts. The new expected shipping date is%'
               and con.private --check that the interaction is a note
             group by 1
         ),
              dispute_refund_requests as (
                  select order_uuid,
                         min(created) as dispute_refund_resolution_date
                  from {{ source('int_service_supply', 'refund_requests') }}
                  group by 1
              )
         select so.uuid                                                               as order_uuid,
                dispute_refund_resolution_date,
                dispute_remake_resolution_date,
                least(dispute_refund_resolution_date, dispute_remake_resolution_date) as dispute_resolution_at,
                case
                    when least(dispute_refund_resolution_date, dispute_remake_resolution_date) =
                         dispute_refund_resolution_date then 'refund'
                    when least(dispute_refund_resolution_date, dispute_remake_resolution_date) =
                         dispute_remake_resolution_date then 'remake'
                    else null end                                                     as first_dispute_resolution_type
         from {{ ref('prep_supply_orders') }} so
                  left join dispute_refund_requests rr on rr.order_uuid = so.uuid
                  left join freshdesk_ticket_remake_request rer on rer.order_uuid = so.uuid
         where rr.order_uuid is not null
            or rer.order_uuid is not null
         group by 1, 2, 3
     )

     select disputes.order_uuid,
            disputes.dispute_id,
            disputes.dispute_created_at,
            true as is_quality_disputed,
            disputes.dispute_requested_outcome,
            disputes.dispute_type,
            disputes._data_source,
            disr.dispute_resolution_at,
            datediff('hour', dispute_created_at, dispute_resolution_at) as dispute_resolution_time_hours,
            disr.first_dispute_resolution_type
     from {{ ref('prep_supply_orders') }} as orders
     left join disputes on orders.uuid = disputes.order_uuid
     left join dispute_resolution as disr on orders.uuid = disr.order_uuid
     where dispute_created_at is not null
