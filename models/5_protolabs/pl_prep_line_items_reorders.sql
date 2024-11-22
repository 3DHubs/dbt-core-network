with reordered_part as (
    select
        fq.material_id,
        fq.line_item_title,
        fq.line_item_total_volume_cm3,
        coalesce(fo.hubspot_company_id,fo.hubspot_contact_id)                                              as crm_id,                                                           
        count(distinct fq.order_uuid) as order_count
    from {{ ref("fact_quote_line_items") }} fq
        left join {{ ref("fact_orders") }} fo on fo.order_uuid = fq.order_uuid
    where
        fo.sourced_at >= '2021-01-01'
        and fq.line_item_type = 'part'
    group by
        fq.material_id, fq.line_item_title, fq.line_item_total_volume_cm3, crm_id
    having 
        count(distinct fq.order_uuid) > 1
),
ranked_data as (
    select
        fq.created_date,
        fq.order_uuid,
        fq.line_item_uuid,
        fq.line_item_title,
        fq.material_id,
        fq.line_item_total_volume_cm3,
        coalesce(fo.hubspot_company_id,fo.hubspot_contact_id)                                              as crm_id,
        rank() over (partition by fq.line_item_title, fq.material_id, fq.line_item_total_volume_cm3, coalesce(fo.hubspot_company_id,fo.hubspot_contact_id) order by fo.created_at asc)                                                                                    as rank
    from {{ ref("fact_quote_line_items") }} fq
    left join {{ ref("fact_orders") }} fo on fo.order_uuid = fq.order_uuid
    where
        fq.line_item_type = 'part'
)
select
    r.created_date,
    r.crm_id,
    r.order_uuid,
    r.line_item_uuid,
    r.line_item_title,
    r.material_id, 
    r.line_item_total_volume_cm3,
    r.rank,
    case
        when r.rank > 1 then true else false
    end                                                                                                   as is_line_reorder

from ranked_data r
inner join reordered_part rp
    on rp.material_id = r.material_id
    and rp.line_item_title = r.line_item_title
    and rp.crm_id = r.crm_id
    and rp.line_item_total_volume_cm3 = r.line_item_total_volume_cm3

order by
    r.order_uuid, r.rank