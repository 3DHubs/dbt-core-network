----------------------------------------------------------------
-- LOCATION DATA AT ORDER LEVEL
----------------------------------------------------------------

-- Geo/Location/Addresses data for our different actors:

-- Actors: 
-- 1. Customers
-- 2. Suppliers
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

       -- Customer
       addresses_customer.locality    as customer_city,
       addresses_customer.lat         as customer_latitude,
       addresses_customer.lon         as customer_longitude,
       countries_customer.name        as customer_country,
       countries_customer.alpha2_code as customer_country_iso2,
       countries_customer.market      as customer_market,
       countries_customer.region      as customer_region,
       case
           when addresses_customer.email ~ '@(3d)?hubs.com' and addresses_customer.email !~ 'anonymized' then true
           else false end             as customer_email_from_hubs,

       -- Company Entity
       countries_entity.name          as company_entity,

       --Supplier
       countries_supplier.name        as supplier_country,
       addresses_supplier.lat         as supplier_latitude,
       addresses_supplier.lon         as supplier_longitude

from {{ ref('cnc_orders') }} as orders
         left join {{ ref('cnc_order_quotes') }} as quotes on orders.quote_uuid = quotes.uuid

-- Customer Related
         left join {{ ref('addresses') }} as addresses_customer
                   on quotes.shipping_address_id = addresses_customer.address_id
         left join {{ ref('countries') }} as countries_customer
                   on addresses_customer.country_id = countries_customer.country_id
         left join us_states as states_customer on quotes.shipping_address_id = states_customer.address_id

-- Company Entity Related
         left join {{ ref('company_entities') }} as entity on quotes.company_entity_id = entity.id
         left join {{ ref('countries') }} as countries_entity
                   on entity.corporate_country_id = countries_entity.country_id

-- Supplier Related
         left join {{ ref('stg_orders_documents') }} as docs on orders.uuid = docs.order_uuid
         left join {{ ref('stg_orders_rda') }} as rda on orders.uuid = rda.order_uuid
         left join {{ ref('addresses') }} as addresses_supplier
                   on addresses_supplier.address_id =
                      coalesce(docs.po_active_supplier_address_id, rda.auction_supplier_address_id)
         left join {{ ref('countries') }} as countries_supplier
                   on countries_supplier.country_id = addresses_supplier.country_id
