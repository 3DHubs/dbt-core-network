-- --------------------------------------------------------------
-- LOCATION DATA AT ORDER LEVEL
-- --------------------------------------------------------------
-- Geo/Location/Addresses data for our different actors:
-- Categories:
-- 1. Origin (Based on Contact Shipping Address in the Quote
-- 2. Destination (Based on Supplier Address)
-- 3. Company (Hubs) Entity
{{ config(tags=["multirefresh"]) }}

with
    stg_us_states as (
        select distinct
            lower(replace(sa.administrative_area, 'US-', '')) as administrative_area,
            sa.address_id as address_id,
            states.state as state
        from {{ ref("addresses") }} sa
        left join
            {{ ref("seed_states") }} states
            on lower(states.code) = lower(replace(sa.administrative_area, 'US-', ''))
        where sa.country_id = '237'
    ),
    us_states as (
        select address_id, coalesce(states.state, stg_us_states.state) as state
        from stg_us_states
        left join
            {{ ref("seed_states") }} states
            on lower(states.state)
            = lower(replace(stg_us_states.administrative_area, 'US-', ''))
    )

select distinct
    orders.uuid as order_uuid,

    -- Origin (Supplier)
    countries_origin.name as origin_country,
    addresses_origin.lat as origin_latitude,
    addresses_origin.lon as origin_longitude,
    addresses_origin.timezone as origin_timezone,
    countries_origin.market as origin_market,
    countries_origin.region as origin_region,

    -- Crossdock
    case
        when
            quotes.is_cross_docking is true
            and not coalesce(hs_deals.is_hubs_arranged_direct_shipping,false)
        then true
        else false
    end as is_cross_docking_ind,
    case
        when is_cross_docking_ind then cross_dock_geo.shipping_locality else null
    end as cross_dock_city,
    case
        when is_cross_docking_ind then cross_dock_geo.shipping_country else null
    end as cross_dock_country,
    case
        when is_cross_docking_ind then cross_dock_geo.shipping_latitude else null
    end as cross_dock_latitude,
    case
        when is_cross_docking_ind then cross_dock_geo.shipping_longitude else null
    end as cross_dock_longitude,

    hs_deals.is_hubs_arranged_direct_shipping as is_cross_dock_override,

    -- Destination (Based on Contact Shipping Address)
    coalesce(adr.company_name, quotes.shipping_company_name) as destination_company_name,
    coalesce(adr.locality, quotes.shipping_locality) as destination_city,
    coalesce(adr.postal_code, quotes.shipping_postal_code) as destination_postal_code,
    coalesce(adr.lat, quotes.shipping_latitude) as destination_latitude,
    coalesce(adr.lon, quotes.shipping_longitude) as destination_longitude,
    coalesce(adr.timezone, quotes.shipping_timezone) as destination_timezone,
    coalesce(adr.country_name, quotes.shipping_country) as destination_country,
    coalesce(adr.country_alpha2_code, quotes.shipping_country_alpha2_code) as destination_country_iso2,
    coalesce(adr.market, quotes.market) as destination_market,
    coalesce(adr.region, quotes.region) as destination_region,
    coalesce(adr.sub_region, quotes.sub_region) as destination_sub_region,
    states_destination.state as destination_us_state,

    -- Company Entity
    quotes.corporate_country as company_entity


from {{ ref("prep_supply_orders") }} as orders
left join
    {{ ref("prep_supply_documents") }} as quotes on orders.quote_uuid = quotes.uuid

-- Hubspot related
left join
    {{ ref("stg_orders_hubspot") }} as hs_deals
    on orders.hubspot_deal_id = hs_deals.hubspot_deal_id

-- Origin Related (Supplier)
left join {{ ref("stg_orders_documents") }} as docs on orders.uuid = docs.order_uuid
left join {{ ref("agg_orders_rda") }} as rda on orders.uuid = rda.order_uuid
left join {{ ref('suppliers') }} as s on s.id = coalesce(docs.po_active_supplier_id, rda.supplier_id)
left join
    {{ ref("addresses") }} as addresses_origin
    on addresses_origin.address_id = s.address_id
left join
    {{ ref("prep_countries") }} as countries_origin
    on countries_origin.country_id = addresses_origin.country_id

-- Purchase Order Related (CrossDock)
left join
    (
        select
            ppo.order_uuid,
            ppo.shipping_locality,
            ppo.shipping_latitude,
            ppo.shipping_longitude,
            ppo.shipping_country
        from {{ ref("prep_purchase_orders") }} as ppo
        where ppo.status = 'active'
    ) as cross_dock_geo
    on orders.uuid = cross_dock_geo.order_uuid

-- Destination Related Exception (Client)
left join {{ ref("seed_stg_orders_geo_exceptions") }} as exc 
    on quotes.uuid = exc.quote_uuid
left join {{ ref("prep_addresses") }} as adr
    on exc.shipping_address_id = adr.address_id

left join
    us_states as states_destination
    on quotes.shipping_address_id = states_destination.address_id