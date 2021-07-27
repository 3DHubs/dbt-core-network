----------------------------------------------------------------
-- DOCUMENTS DEAL FIELDS & AGGREGATES
-- QUOTES & PURCHASE ORDERS
----------------------------------------------------------------

-- Main Sources: Supply Quotes (Type Quote & Purchase Order)

----------------------------------------------------------------
-- QUOTE FIELDS & AGGREGATES
----------------------------------------------------------------

-- FIRST QUOTE FIELDS

with first_quote as (
    with rn as (select order_uuid,
                       uuid                                                                        as quote_uuid,
                       is_admin                                                                    as quote_first_is_admin,

                       -- todo: if agreed, remove fields below

--                        finalized_at                                                                as quote_first_finalized_at,
--                        submitted_at                                                                as quote_first_submitted_at,
--                        created                                                                     as quote_first_created_at,

                       row_number() over (partition by order_uuid order by created asc nulls last) as rn
                from {{ ref('cnc_order_quotes') }} soq
                where type = 'quote'),
         agg_quote_li as (select quote_uuid,
                                 sum(((type = 'part' and upload_id is not null and
                                       auto_price_original_amount is null) or should_quote_manually)::int) >=
                                 1 as has_part_without_automatic_pricing
                          from {{ ref('line_items') }} li
                                   inner join rn using (quote_uuid)
                          where rn = 1
                          group by 1)
    select rn.order_uuid,
           agg_quote_li.has_part_without_automatic_pricing as quote_first_has_part_without_automatic_pricing,
           quote_first_is_admin                            as quote_first_created_by_admin

           -- todo: if agreed, remove fields below
--            quote_first_created_at,
--            quote_first_submitted_at,
--            quote_first_finalized_at
    from rn
             left join agg_quote_li using (quote_uuid)
    where rn = 1
),

-- ORDER QUOTE FIELDS
-- In the Orders table a quote uuid is always listed, if the order (a.k.a deal) is closed then the closed/won
-- quote will be used instead. So in many cases the order's quote will be the first quote as well but not always.

     --todo: investigate what the quote uuid in cnc orders leads to
     order_quote as (
         select orders.uuid                                                      as order_uuid,
                quotes.document_number                                           as order_quote_document_number,
                quotes.status                                                    as order_quote_status, -- Used to filter out carts
                quotes.created                                                   as order_quote_created_at,
                quotes.submitted_at                                              as order_quote_submitted_at,
                quotes.finalized_at                                              as order_quote_finalized_at,
                quotes.lead_time                                                 as order_quote_lead_time,
                lt_tiers.name                                                    as order_quote_lead_time_tier,
                quotes.is_cross_docking                                          as order_quote_is_cross_docking,
                quotes.requires_local_production                                 as order_quote_requires_local_production,
                round(((quotes.subtotal_price_amount / 100.00) / rates.rate), 2) as order_quote_amount_usd

                --todo: if agreed, remove:

                -- quotes.shipping_address_id, --Used to join addresses
                -- quotes.signed_quote_uuid, -- Used in financial fields, can be used in an appending table
                -- quotes.currency_code, -- Not leveraged
                -- quotes.price_multiplier    as order_quote_price_multiplier, -- Not leveraged
                -- quotes.type                as order_quote_type, -- Very unnecessary, is a type quote
                -- quotes.tax_category_id, -- Not leveraged
                -- quotes.shipping_speed, -- Not leveraged
                -- quotes.cross_docking_added_lead_time, -- Not leveraged

         from {{ ref('cnc_orders') }} as orders
         left join {{ ref('cnc_order_quotes') }} as quotes
         on orders.quote_uuid = quotes.uuid
             left join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
             on rates.currency_code_to = quotes.currency_code and
             trunc(coalesce (quotes.finalized_at, quotes.created)) = trunc(rates.date)
             left join {{ source('int_service_supply', 'lead_time_tiers') }} as lt_tiers on quotes.lead_time_tier_id = lt_tiers.id
         where true
           and quotes.type = 'quote'
     )
        ,

-- ALL QUOTE AGGREGATES

     agg_all_quotes as (
         select order_uuid                                        as                      order_uuid,
                sum(case when is_admin is true then 1 else 0 end) as                      number_of_quote_versions_by_admin,
                max(revision)                                     as                      number_of_quote_versions,
                case when number_of_quote_versions_by_admin >= 1 then true else false end has_admin_created_quote_request,
                sum(case
                        when submitted_at - finalized_at <> interval '0 seconds' then 1
                        else 0 end)                               as                      has_non_locked_quote_review,
                case
                    when has_admin_created_quote_request = true or has_non_locked_quote_review > 0 then true
                    else false end                                                        has_manual_quote_review

                -- has_non_locked_quote_review -- Not leveraged

         from {{ ref('cnc_order_quotes') }}
         where type = 'quote'
         group by 1
     ),


----------------------------------------------------------------
-- PURCHASE ORDERS AGGREGATES
----------------------------------------------------------------

-- FIRST PURCHASE ORDER FIELDS

     first_po as (
         with rn as (select order_uuid,
                            finalized_at                                                           as po_first_sourced_at,
                            spocl.supplier_id::int                                                 as po_first_supplier_id,
                            round(((subtotal_price_amount / 100.00) / rates.rate), 2)              as po_first_amount_usd,
                            sum(case
                                    when sqli.type = 'shipping'
                                        then round(((price_amount / 100.00) / rates.rate), 2) end) as po_first_shipping_usd,
                            row_number() over (partition by order_uuid order by finalized_at)      as rn

                            --todo: if agreed, remove:

--                             quote_uuid,
--                             document_number,
--                             (subtotal_price_amount / 100.00)                                       as po_first_amount_source_currency,
--                             currency_code                                                          as po_first_source_currency,
--                             is_created_manually                                                    as po_first_is_created_manually,

                     from {{ ref('cnc_order_quotes') }} as soq
    --todo: couldn't find this table in the models
    left join {{ source('data_lake', 'exchange_rate_spot_daily')}} as rates
                     on rates.currency_code_to = soq.currency_code and
                         rates.date = trunc(soq.finalized_at)
                         left join {{ ref('purchase_orders') }} as spocl on soq.uuid = spocl.uuid
                         left join {{ ref('line_items') }} as sqli on soq.uuid = sqli.quote_uuid
                     where true
                       and soq.type like 'purchase_order'
                       and soq.finalized_at is not null
                     group by 1, 2, 3, 4)
         select order_uuid,
                po_first_supplier_id, -- Used to define is_resourced
                po_first_sourced_at,  -- Used to define sourced_date
                po_first_amount_usd,  -- Used to defined sourced_cost_usd
                po_first_shipping_usd

                --todo: if agreed, remove:

--                 po_first_amount_source_currency,
--                 po_first_source_currency,
--                 po_first_is_created_manually,
--                 po_first_supplier_id

         from rn
         where rn = 1
     ),

-- ACTIVE PURCHASE ORDER FIELDS
-- This data is obtained by querying the quotes table (type PO) and filtering for status active

     active_po as (
         select quotes.order_uuid,
                round(((subtotal_price_amount / 100.00) / rates.rate), 2)                    as po_active_amount_usd,
                document_number                                                              as po_active_document_number,
                purchase_orders.supplier_id::int                                             as po_active_supplier_id,                    -- Used to define is_resourced field
                suppliers.name                                                               as po_active_supplier_name,
                suppliers.address_id                                                         as po_active_supplier_address_id,
                countries.name                                                               as po_active_company_entity,
                case
                    when quotes.shipping_date >= '2019-10-01'
                        then quotes.shipping_date end                                        as po_active_promised_shipping_at_by_supplier,
                sum(case
                        when sqli.type = 'shipping'
                            then round(((price_amount / 100.00) / rates.rate), 2) end)       as po_active_shipping_usd

                --todo: if agreed, remove:

--                 oqsl.uuid                                                              as order_active_po_uuid, --It is leveraged in fact deals but is it necessary there?
--                 is_created_manually                                                    as order_active_po_is_created_manually, -- Not leveraged
--                 case
--                     when oqsl.created >= '2020-08-22' then convert_timezone(sa.timezone, oqsl.shipping_date)
--                     when oqsl.created < '2020-08-22' and oqsl.shipping_date not like '%23:59:59%'
--                         then convert_timezone(sa.timezone, oqsl.shipping_date)
--                     else oqsl.shipping_date end                                        as po_shipping_render_date,
--                 (subtotal_price_amount / 100.00)                                       as order_active_po_amount_source_currency,
--                 oqsl.currency_code                                                     as order_active_po_source_currency,

         from {{ ref('cnc_order_quotes') }} as quotes
    inner join {{ ref('purchase_orders') }} as purchase_orders
         on quotes.uuid = purchase_orders.uuid
             left join {{ source('data_lake', 'exchange_rate_spot_daily')}} as rates
             on rates.currency_code_to = quotes.currency_code and rates.date = trunc(quotes.finalized_at)
             left join {{ ref('line_items') }} as sqli on quotes.uuid = sqli.quote_uuid
             left join {{ ref('company_entities') }} as ce on quotes.company_entity_id = ce.id
             left join {{ ref('countries') }} as countries on ce.corporate_country_id = countries.country_id
             left join {{ ref('suppliers') }} as suppliers on purchase_orders.supplier_id = suppliers.id
         where quotes.type = 'purchase_order'
           and purchase_orders.status = 'active'
         group by 1, 2, 3, 4, 5, 6, 7, 8
     ),

     -- ALL PURCHASE ORDER FIELDS

     agg_all_pos as (
         select osl.uuid as order_uuid,
                count(*) as number_of_purchase_orders -- Not leveraged? But seems key.

                --todo: if agreed, remove:

--                 sum(oqsl.subtotal_price_amount / 100.00)::decimal(15, 2)        as po_sum_of_price_sourced_amount,
--                 sum(oqsl.tax_price_amount / 100.00)::decimal(15, 2)             as po_sum_of_tax_amount,
--                 sum(
--                         round(((oqsl.subtotal_price_amount / 100.00)
--                             / rates.rate), 2))::decimal(15, 2)                  as po_sum_of_price_sourced_amount_usd,
--                 listagg(oqsl.status + ';') within group (order by oqsl.updated) as po_status_descriptions,
--                 listagg(oqsl.uuid + ': ' + oqsl.created + ';')
--                 within group (order by oqsl.updated)                            as po_create_dates,
--                 min(oqsl.created)                                               as po_first_issue_date,
--                 max(oqsl.created)                                               as po_last_issue_date

         from {{ ref('cnc_order_quotes') }} as oqsl
    inner join {{ ref('cnc_orders') }} as osl
         on oqsl.order_uuid = osl.uuid
             left join {{ source('data_lake', 'exchange_rate_spot_daily')}} as rates
             on rates.currency_code_to = oqsl.currency_code and
             trunc(osl.delivered_at) = trunc(rates.date)
         where oqsl.type = 'purchase_order'
           and oqsl.parent_uuid is not null
         group by 1
     )

----------------------------------------------------------------
-- COMBINE QUOTES & PURCHASE ORDERS
----------------------------------------------------------------

select -- First Quote
       fq.order_uuid,
       fq.quote_first_created_by_admin,
       fq.quote_first_has_part_without_automatic_pricing,

       --    fq.quote_first_created_at, -- Not leveraged
       --    fq.quote_first_submitted_at, -- Not leveraged
       --    fq.quote_first_finalized_at, -- Not leveraged

       -- Order Quote
       oq.order_quote_document_number,
       oq.order_quote_status,     -- Used to filter out carts
       oq.order_quote_created_at, -- This is actually not in fact deals?
       oq.order_quote_submitted_at,
       oq.order_quote_finalized_at,
       oq.order_quote_lead_time,
       oq.order_quote_lead_time_tier,
       oq.order_quote_is_cross_docking,
       oq.order_quote_requires_local_production,
       oq.order_quote_amount_usd,

       --    order_quote_shipping_address_id, -- Used for a join on addresses and states
       --    order_quote_type, -- Useless, type = quote
       --    order_currency_code_sold, -- Not leveraged
       --    order_quote_price_multiplier, -- Not leveraged
       --    order_quote_price_sold_amount, -- Only USD leveraged
       --    order_quote_tax_amount, -- Not leveraged
       --    order_quote_tax_amount_usd, -- Not leveraged
       --    order_quote_signed_quote_uuid, -- Used in financial fields, can be used in an appending table
       --    order_quote_tax_category_id, -- Not leveraged
       --    order_quote_shipping_speed, -- Not leveraged
       --    order_quote_cross_docking_added_lead_time,

       -- All Quotes
       aaq.number_of_quote_versions,
       aaq.number_of_quote_versions_by_admin,
       aaq.has_admin_created_quote_request,
       aaq.has_manual_quote_review,

       -- First PO

       fpo.po_first_sourced_at,
       fpo.po_first_sourced_at is not null as is_sourced,
       fpo.po_first_amount_usd,
       fpo.po_first_shipping_usd,
       fpo.po_first_supplier_id,

       --    fpo.po_first_amount_source_currency,
       --    fpo.po_first_source_currency,
       --    fpo.po_first_is_created_manually, -- Previously used for is manually sourced def

       -- Active PO
       apo.po_active_amount_usd,
       apo.po_active_document_number,
       apo.po_active_company_entity,
       apo.po_active_promised_shipping_at_by_supplier,
       apo.po_active_shipping_usd,
       apo.po_active_supplier_id,
       apo.po_active_supplier_name,
       apo.po_active_supplier_address_id,


       --    apo.order_active_po_quote_uuid,
       --    apo.order_active_po_amount_source_currency,
       --    apo.order_active_po_source_currency,

       -- All POs
       aapo.number_of_purchase_orders,

       -- pos.po_sum_of_tax_amount,
       -- pos.po_status_descriptions,
       -- pos.po_create_dates,
       -- pos.po_first_issue_date,
       -- pos.po_last_issue_date,

       -- Combined Fields

       case when fpo.po_first_supplier_id <> apo.po_active_supplier_id then true else false end as is_resourced

from first_quote as fq
         left join order_quote as oq on fq.order_uuid = oq.order_uuid
         left join agg_all_quotes as aaq on fq.order_uuid = aaq.order_uuid
         left join first_po as fpo on fq.order_uuid = fpo.order_uuid
         left join active_po as apo on fq.order_uuid = apo.order_uuid
         left join agg_all_pos as aapo on fq.order_uuid = aapo.order_uuid
