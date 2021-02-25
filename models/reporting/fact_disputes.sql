with
    hubspot_events as (
        select order_uuid,
               min(created)                  as dispute_created_at,
               'new'                         as dispute_status, -- Regard all events as 'new'
               'supply_order_history_events' as _data_source
        from {{ ref('order_history_events') }}
        where description ~ 'dispute'
        group by 1, 3
    ),
    line_item_disputes as (
        select order_uuid,
               min(dli.created) as line_item_created
        from {{ ref('disputes') }} dis
        left join {{ ref ('dispute_line_items') }} dli on dli.dispute_uuid = dis.uuid
        group by 1
    ),
    disputes_temp as (select dis.order_uuid,
                             dis.id                                                                                         as dispute_id,
                             coalesce(lid.line_item_created, created)                                                       as dispute_created_at,
                             dis.requested_outcome                                                                          as dispute_requested_outcome,
                             dis.type                                                                                       as dispute_type,
                             dis.status                                                                                     as dispute_status,

                             row_number() over ( partition by dis.order_uuid order by case
                                                                                          when status = 'new' then
                                                                                              0
                                                                                          when status = 'draft'
                                                                                              then 1 end asc, created desc) as rn -- Prioritize 'new' over 'draft', most recent first
                      from {{ ref('disputes') }} dis
                               left join line_item_disputes lid on dis.order_uuid = lid.order_uuid
                      where deleted is null),
    disputes as (
        select order_uuid,
               dispute_id,
               dispute_created_at,
               dispute_requested_outcome,
               dispute_type,
               dispute_status,
               'supply_disputes' as _data_source
        from disputes_temp
        where rn = 1 -- Max. one dispute per order
    )
select order_uuid,
       dispute_id,
       dispute_created_at,
       dispute_requested_outcome,
       dispute_type,
       dispute_status,
       _data_source
from disputes
union all
select order_uuid,
       null as dispute_id,
       dispute_created_at,
       null as dispute_requested_outcome,
       null as dispute_type,
       dispute_status,
       _data_source
from hubspot_events
where not exists(select 1 from disputes where disputes.order_uuid = hubspot_events.order_uuid)