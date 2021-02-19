with stg_bids as
    (select md5(b.supplier_id || b.auction_uuid)                                                                     as supplier_auction_uuid,
            b.has_changed_prices,
            b.has_changed_shipping_date,
            b.has_design_modifications,
            b.created,
            b.updated,
            b.deleted,
            b.uuid,
            b.auction_uuid,
            b.response_type,
            b.placed_at,
            b.ship_by_date,
            b.rejection_text,
            b.is_active,
            b.supplier_id,
            b.rejection_reasons,
            b.accepted_ship_by_date,
            q.description                                                                                            as design_modification_text,
            round(((q.subtotal_price_amount / 100.00) / e.rate), 2)                                                     bid_amount_usd,
            row_number()
            over (partition by b.auction_uuid, b.supplier_id order by b.updated desc, b.placed_at desc)              as rn
    from {{ ref('bids') }} b
                left outer join {{ ref('cnc_order_quotes') }} q on b.uuid = q.uuid
                left outer join analytics.data_lake.exchange_rate_spot_daily e
                                on e.currency_code_to = q.currency_code and trunc(e.date) = trunc(q.created)),

    stg_auction_technology as (
    select a.order_quotes_uuid, -- public.auctions.uuid in Supply db
            a.status                                      as auction_status,
            oq.technology_id,
            dt.name                                       as technology_name,
            round((oq.subtotal_price_amount / 100.00), 2) as auction_amount_usd,
            oq.document_number
    from {{ ref('auctions') }} as a
                left join {{ ref('cnc_order_quotes') }} as oq on oq.uuid = a.order_quotes_uuid
                inner join {{ ref('technologies') }} as dt on oq.technology_id = dt.technology_id
                
                ),

  sa as ( select md5(supplier_id || auction_uuid) as supplier_auction_uuid, *
                from data_lake.supply_supplier_auctions ),

  b as ( select * from stg_bids where rn = 1 ),

  stg_supplier_auction_interactions as (

select sa.supplier_auction_uuid                                                          as supplier_auction_uuid,
        sa.supplier_id                                                                    as sa_supplier_id,
        sa.auction_uuid                                                                   as sa_auction_uuid,
        sa.assigned_at                                                                    as sa_assigned_at,
        sa.first_seen_at                                                                  as sa_first_seen_at,
        sa.last_seen_at                                                                   as sa_last_seen_at,
        sa.is_dismissed                                                                   as sa_has_dismissed_notification,
        sa.is_automated_shipping_available                                                as sa_is_automated_shipping_available,
        a.created                                                                         as auction_created_at,
        a.updated                                                                         as auction_updated_at,
        a.deleted                                                                         as auction_deleted_at,
        a.order_quotes_uuid                                                               as auction_order_quotes_uuid,
        a.winner_bid_uuid                                                                 as auction_winner_bid_uuid,
        a.status                                                                          as auction_status,
        a.started_at                                                                      as auction_started_at,
        a.finished_at                                                                     as auction_finished_at,
        a.ship_by_date                                                                    as auction_ship_by_at,
        a.internal_support_ticket_id                                                      as auction_support_ticket_id,
        a.last_processed_at                                                               as auction_last_processed_at,
        b.created                                                                         as bid_created_at,
        b.updated                                                                         as bid_updated_at,
        b.deleted                                                                         as bid_deleted_at,
        b.uuid                                                                            as bid_uuid,
        b.response_type                                                                   as bid_supplier_response,
        b.placed_at                                                                       as bid_placed_at,
        b.has_changed_prices                                                              as bid_has_changed_prices,
        b.has_design_modifications                                                        as bid_has_design_modifications,
        b.has_changed_shipping_date                                                       as bid_has_changed_shipping_date,
        b.ship_by_date                                                                    as bid_adjusted_ship_by_date,
        nullif(b.rejection_text, '')                                                      as bid_rejection_text,
        b.is_active                                                                       as bid_is_active,
        b.rejection_reasons                                                               as bid_rejection_reasons,
        b.accepted_ship_by_date                                                           as bid_accepted_ship_by_date,
        t.technology_id                                                                   as order_technology_id,
        t.technology_name                                                                 as order_technology_name,
        coalesce(round((sa.subtotal_price_amount_usd / 100.00), 2), t.auction_amount_usd) as auction_amount_usd,
        t.auction_amount_usd                                                              as auction_amount,
        t.document_number                                                                 as auction_document_number,
        b.bid_amount_usd                                                                  as bid_amount_usd,
        b.design_modification_text                                                        as design_modification_text,
        sa.margin                                                                         as sa_margin,
        sa.max_country_margin                                                             as sa_max_country_margin,
        a.base_margin                                                                     as auction_base_margin,
        case when b.uuid = a.winner_bid_uuid then true else false end                     as is_winning_bid,
        case when abs(sa.margin - a.base_margin) < 0.00001 then 'standard'
            when abs(sa.margin - sa.max_country_margin) < 0.00001 then 'country cap'
            else 'engagement' end                                                        as margin_type
from sa
            inner join {{ ref('auctions') }} a on a.order_quotes_uuid = sa.auction_uuid
            left outer join b on b.supplier_auction_uuid = sa.supplier_auction_uuid
            left outer join stg_auction_technology t on a.order_quotes_uuid = t.order_quotes_uuid),

    all_reasons as (

with ns as (
{% for i in range(1,11) %}

  select {{ i }} as n {% if not loop.last %} union all {% endif %}

{% endfor %} ),
        rej as ( select supplier_auction_uuid,
                        case when is_valid_json_array(bid_rejection_reasons) is false
                                then trim('[]' from translate(bid_rejection_reasons, '\'', '"'))
                            else trim('[]', bid_rejection_reasons) end rejection_reasons_fixed
                from stg_supplier_auction_interactions )
select supplier_auction_uuid,
        ns.n,
        translate(trim('"' from trim(split_part(rejection_reasons_fixed, ',', ns.n))), '"', '\'') as reason
from ns
            inner join rej on ns.n <= regexp_count(rejection_reasons_fixed, ',') + 1
where trim(reason) != ''),

    all_reasons_flattened as (

        select supplier_auction_uuid,
                min(case when reason in ('Other', '') then 1 else 0 end) as                             has_rejected_other,
                min(case when reason in ('One (or more) files are unprintable with the selected technology') then 1
                        else 0 end) as                                                                 has_rejected_unprintable,
                min(case when reason in ('Unable to machine design') then 1 else 0 end) as              has_rejected_design,
                min(case when reason in ('The dimensions of the part(s) are too large',
                                        'Part dimensions too large') then 1
                        else 0 end) as                                                                 has_rejected_dimensions,
                min(case when reason like 'Unable to machine at requested tolerance' then 1
                        else 0 end) as                                                                 has_rejected_tolerance,
                min(case when reason like 'Unable to make the requested deadline' then 1
                        else 0 end) as                                                                 has_rejected_deadline,
                min(case when reason in ('Do not have requested material',
                                        'I dont have the requested material') then 1
                        else 0 end) as                                                                 has_rejected_no_material,
                min(case when reason in ('In violation of Restricted Content policy in MP Agreement (e.g. weapon parts)',
                                        'In violation of Restricted Content policy in MP Agreement (e.g. gun parts)') then 1
                        else 0 end) as                                                                 has_rejected_violated_policy
        from all_reasons
        group by 1)

select sai.supplier_auction_uuid,
        sai.sa_supplier_id,
        sai.sa_auction_uuid,
        sai.sa_assigned_at,
        sai.sa_first_seen_at,
        sai.sa_last_seen_at,
        sai.sa_has_dismissed_notification,
        sai.sa_is_automated_shipping_available,
        sai.auction_document_number,
        sai.auction_created_at,
        sai.auction_updated_at,
        sai.auction_deleted_at,
        sai.auction_order_quotes_uuid,
        sai.auction_winner_bid_uuid,
        sai.auction_status,
        sai.auction_started_at,
        sai.auction_finished_at,
        sai.auction_ship_by_at,
        sai.auction_support_ticket_id,
        sai.auction_last_processed_at,
        sai.bid_created_at,
        sai.bid_updated_at,
        sai.bid_deleted_at,
        sai.bid_uuid,
        sai.is_winning_bid,
        sai.bid_supplier_response,
        sai.bid_placed_at,
        sai.bid_has_changed_prices,
        sai.bid_has_design_modifications,
        sai.bid_has_changed_shipping_date,
        sai.bid_adjusted_ship_by_date,
        sai.bid_rejection_text,
        sai.bid_is_active,
        sai.bid_rejection_reasons,
        sai.bid_accepted_ship_by_date,
        sai.auction_amount_usd,
        sai.auction_base_margin,
        sai.bid_amount_usd,
        sai.sa_margin,
        sai.sa_max_country_margin,
        sai.margin_type,
        sai.design_modification_text,
        r.has_rejected_other,
        r.has_rejected_unprintable,
        r.has_rejected_design,
        r.has_rejected_dimensions,
        r.has_rejected_tolerance,
        r.has_rejected_deadline,
        r.has_rejected_no_material,
        r.has_rejected_violated_policy,
        order_technology_id,
        order_technology_name
from stg_supplier_auction_interactions sai
            left outer join all_reasons_flattened r on r.supplier_auction_uuid = sai.supplier_auction_uuid