with agg as ( select
    sa_supplier_id,
    trunc(
         min(sa_last_seen_at)
    ) as first_seen_at,
    trunc(
         max(sa_last_seen_at)
    ) as last_seen_at,
    current_date - last_seen_at as last_seen_days_ago,
    sum(
         case
             when
                sa_assigned_at >= current_date - 7 and sa_last_seen_at is not null
                 then 1
         end) as number_auctions_seen_l7d,
    sum(
         case when sa_assigned_at >= current_date - 7 then 1 end
    ) as number_auctions_assigned_to_l7d,
    ((number_auctions_seen_l7d::float / number_auctions_assigned_to_l7d) *
       100
    )::decimal(15, 2) as pct_auctions_seen_l7d,
    coalesce(sum(
             case
                 when
                     sa_assigned_at >= current_date - 7 and response_type = 'accepted'
                     then 1 end),
         0
    ) as number_accepted_l7d,
    coalesce(
         (
             (number_accepted_l7d::float / number_auctions_seen_l7d) * 100
    )::decimal(15, 2),
    0
    ) as pct_accepted_of_seen_l7d,
    trunc(
         min(sa_assigned_at)
    ) as first_assigned_at,
    trunc(
         min(
             case
                when response_type = 'accepted' then sa_assigned_at
            end
    )
    ) as first_accepted_at,
    trunc(
         max(
             case
                when response_type = 'accepted' then sa_assigned_at
            end
    )
    ) as last_accepted_at
    from {{ ref('fact_auction_behaviour') }}
    where sa_supplier_id is not null and not is_rfq
    group by 1),

fst as ( select distinct
    fo.supplier_id,
    first_value(fo.technology_name)
    -- Was: first_sourced_technology
    over (
         partition by
             fo.supplier_id
         order by
        fo.sourced_at asc
         rows between unbounded preceding and unbounded following
    ) as first_technology,
    min(trunc(fo.sourced_at)) over (partition by fo.supplier_id) as first_sourced_order,
    max(trunc(fo.sourced_at)) over (partition by fo.supplier_id) as last_sourced_order

    from {{ ref('fact_orders') }} as fo ),

agg_fo as (
    select 
        fo.supplier_id,
        sum(case when fo.sourced_at is not null then 1 else 0 end) as orders_sourced_in_life_time,
        sum(case when fo.derived_delivered_at is not null then 1 else 0 end) as orders_delivered_in_life_time,
        sum(case when fo.mp_concerning_actions is not null then 1 else 0 end) as concerning_actions_in_life_time
    from {{ ref('fact_orders') }} as fo 
    group by 1



),


stg_deals as (
    select
        fst.supplier_id, 
        fst.first_technology,
        fst.first_sourced_order,
        fst.last_sourced_order,
        af.orders_sourced_in_life_time,
        af.orders_delivered_in_life_time,
        af.concerning_actions_in_life_time
    from fst
    left join agg_fo as af on fst.supplier_id = af.supplier_id
    where fst.supplier_id >= 1
)

select
    stg_dim_suppliers.supplier_id,
    stg_dim_suppliers.create_date,
    stg_dim_suppliers.address_id,
    stg_dim_suppliers.supplier_name,
    stg_dim_suppliers.full_name,
    stg_dim_suppliers.supplier_email,
    stg_dim_suppliers.is_suspended,
    stg_dim_suppliers.is_able_to_accept_auctions,
    stg_dim_suppliers.is_eligible_for_rfq,
    stg_dim_suppliers.is_eligible_for_vqc,
    stg_dim_suppliers.currency_code,
    stg_dim_suppliers.unit_preference,
    stg_dim_suppliers.last_sign_in_at,
    stg_dim_suppliers.last_sign_in_at_days_ago,
    stg_dim_suppliers.monthly_order_value_target,
    stg_dim_suppliers.country_id,
    initcap(stg_dim_suppliers.country_name) as country_name,
    stg_dim_suppliers.country_code as country_code,
    stg_dim_suppliers.continent as continent,
    stg_dim_suppliers.city,
    stg_dim_suppliers.state,
    stg_dim_suppliers.region,
    stg_dim_suppliers.postal_code,
    stg_dim_suppliers.address,
    stg_dim_suppliers.longitude,
    stg_dim_suppliers.latitude,
    stg_dim_suppliers.timezone,
    agg.sa_supplier_id,
    agg.first_seen_at,
    agg.last_seen_at,
    agg.last_seen_days_ago,
    agg.number_auctions_seen_l7d,
    agg.number_auctions_assigned_to_l7d,
    agg.pct_auctions_seen_l7d,
    agg.number_accepted_l7d,
    agg.pct_accepted_of_seen_l7d,
    agg.first_assigned_at,
    agg.first_accepted_at,
    agg.last_accepted_at,
    stg_deals.first_technology,
    stg_deals.orders_sourced_in_life_time,
    stg_deals.orders_delivered_in_life_time,
    stg_deals.concerning_actions_in_life_time,
    stg_deals.first_sourced_order,
    stg_deals.last_sourced_order
from {{ ref('stg_dim_suppliers') }}
left join agg on agg.sa_supplier_id = stg_dim_suppliers.supplier_id
left join stg_deals on stg_deals.supplier_id = stg_dim_suppliers.supplier_id