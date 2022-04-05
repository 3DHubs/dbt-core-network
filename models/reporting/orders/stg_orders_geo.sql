----------------------------------------------------------------
-- LOCATION DATA AT ORDER LEVEL
----------------------------------------------------------------

-- Geo/Location/Addresses data for our different actors:

-- Categories: 
-- 1. Origin (Based on Contact Shipping Address in the Quote
-- 2. Destination (Based on Supplier Address)
-- 3. Company (Hubs) Entity

with stg_us_states as (
    select distinct lower(replace(sa.administrative_area, 'US-', '')) as administrative_area,
                    sa.address_id                                     as address_id,
                    states.state                                      as state
    from {{ ref('addresses') }} sa
             left join {{ ref('states') }} states
                       on lower(states.code) = lower(replace(sa.administrative_area, 'US-', ''))
    where sa.country_id = '237'
),
     us_states as (
         select address_id,
                coalesce(states.state, stg_us_states.state) as state
         from stg_us_states
                  left join {{ ref('states') }} states
                            on lower(states.state) = lower(replace(stg_us_states.administrative_area, 'US-', ''))
     )

select distinct orders.uuid                    as order_uuid,

       -- Destination (Based on Contact Shipping Address)
       addresses_destination.locality    as destination_city,
       addresses_destination.lat         as destination_latitude,
       addresses_destination.lon         as destination_longitude,
       countries_destination.name        as destination_country,
       countries_destination.alpha2_code as destination_country_iso2,
       countries_destination.market      as destination_market,
       countries_destination.region      as destination_region,
       states_destination.state          as destination_us_state,
       case
           when addresses_destination.email ~ '@(3d)?hubs.com' and addresses_destination.email !~ 'anonymized' then true
           else false end             as contact_email_from_hubs,

       -- Company Entity
       countries_entity.name          as company_entity,

       -- Origin (Supplier)
       countries_origin.name        as origin_country,
       addresses_origin.lat         as origin_latitude,
       addresses_origin.lon         as origin_longitude

from {{ ref('supply_orders') }} as orders
         left join {{ ref('supply_documents') }} as quotes on orders.quote_uuid = quotes.uuid

-- Destination Related (Client)
         left join {{ ref('addresses') }} as addresses_destination
                   on quotes.shipping_address_id = addresses_destination.address_id
         left join {{ ref('countries') }} as countries_destination
                   on addresses_destination.country_id = countries_destination.country_id
         left join us_states as states_destination on quotes.shipping_address_id = states_destination.address_id

-- Company Entity Related (Hubs)
         left join {{ ref('company_entities') }} as entity on quotes.company_entity_id = entity.id
         left join {{ ref('countries') }} as countries_entity
                   on entity.corporate_country_id = countries_entity.country_id

-- Origin Related (Supplier)
         left join {{ ref('stg_orders_documents') }} as docs on orders.uuid = docs.order_uuid
         left join {{ ref('agg_orders_rda') }} as rda on orders.uuid = rda.order_uuid
         left join {{ ref('addresses') }} as addresses_origin
                   on addresses_origin.address_id =
                      coalesce(docs.po_active_supplier_address_id, rda.auction_supplier_address_id)
         left join {{ ref('countries') }} as countries_origin
                   on countries_origin.country_id = addresses_origin.country_id
