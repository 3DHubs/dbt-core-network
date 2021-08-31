with fat as ( select distinct
    sa_supplier_id,
    first_value(order_technology_name)
    over (
         partition by
             sa_supplier_id
         order by
             sa_assigned_at asc
         rows between unbounded preceding and unbounded following
    ) as first_assigned_technology
    from {{ ref('fact_supplier_auction_interactions') }}
    where sa_supplier_id is not null
),

agg as ( select
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
                     sa_assigned_at >= current_date - 7 and bid_supplier_response = 'accepted'
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
                when bid_supplier_response = 'accepted' then sa_assigned_at
            end
    )
    ) as first_accepted_at,
    trunc(
         max(
             case
                when bid_supplier_response = 'accepted' then sa_assigned_at
            end
    )
    ) as last_accepted_at
    from {{ ref('fact_supplier_auction_interactions') }}
    where sa_supplier_id is not null
    group by 1),

fst as ( select distinct
    supplier_id,
    first_value(technology_name)
    -- Was: first_sourced_technology
    over (
         partition by
             supplier_id
         order by
        sourced_date asc
         rows between unbounded preceding and unbounded following
    ) as first_technology
    from {{ source('reporting', 'cube_deals') }} ),

ord as ( select
    supplier_id,
    trunc(min(sourced_date)) as first_sourced_order,
    trunc(max(sourced_date)) as last_sourced_order
    from {{ source('reporting', 'cube_deals') }}
    group by 1 ),

stg_deals as (
    select

        fst.supplier_id, fst.first_technology,
        ord.first_sourced_order,
        ord.last_sourced_order
    from fst
    left outer join ord on fst.supplier_id = ord.supplier_id
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
    stg_dim_suppliers.currency_code,
    stg_dim_suppliers.unit_preference,
    stg_dim_suppliers.country_id,
    initcap(stg_dim_suppliers.country_name) as country_name,
    lower(stg_dim_suppliers.country_code) as country_code,
    lower(stg_dim_suppliers.continent) as continent,
    stg_dim_suppliers.city,
    stg_dim_suppliers.state,
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
    fat.first_assigned_technology,
    stg_deals.first_technology,
    stg_deals.first_sourced_order,
    stg_deals.last_sourced_order
from {{ ref('stg_dim_suppliers') }}
left join agg on agg.sa_supplier_id = stg_dim_suppliers.supplier_id
left join stg_deals on stg_deals.supplier_id = stg_dim_suppliers.supplier_id
left join fat on fat.sa_supplier_id = stg_dim_suppliers.supplier_id
