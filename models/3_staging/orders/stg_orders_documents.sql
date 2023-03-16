----------------------------------------------------------------
-- DOCUMENTS DEAL FIELDS & AGGREGATES
-- QUOTES & PURCHASE ORDERS
----------------------------------------------------------------

-- Main Sources: Supply Quotes (Type Quote & Purchase Order)

----------------------------------------------------------------
-- QUOTE FIELDS & AGGREGATES
----------------------------------------------------------------

{{ config(
    tags=["multirefresh"]
) }}

-- FIRST QUOTE FIELDS

with first_quote as (
    with rn as (select soq.order_uuid,
                       soq.uuid                                                                        as quote_uuid,
                       soq.is_admin                                                                    as quote_first_is_admin,
                       soq.split_off_from_quote_uuid is not null                                       as is_splitted_from_order,
                       sp.split_off_from_quote_uuid is not null                                        as is_splitted_order,
                       soq.split_off_from_quote_uuid                                                   as quote_first_splitted_from_quote_uuid,
                       row_number() over (partition by order_uuid order by created asc nulls last) as rn
                from {{ ref('prep_supply_documents') }} soq
                -- Competitiveness Feature that allows orders to be split to facilitate some suppliers to take some orders (Diego Jan, 2022)
                left join (select distinct split_off_from_quote_uuid from {{ ref('prep_supply_documents') }}) as sp on soq.uuid = sp.split_off_from_quote_uuid
                where type = 'quote')
    select rn.order_uuid,
           rn.is_splitted_from_order,
           rn.is_splitted_order,
           rn.quote_first_splitted_from_quote_uuid,
           quote_first_is_admin                            as quote_first_created_by_admin
    from rn
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
                quotes.finalized_at                                              as order_quote_finalised_at,
                quotes.lead_time                                                 as order_quote_lead_time,
                quotes.is_cross_docking                                          as order_quote_is_cross_docking,
                quotes.is_eligible_for_cross_docking                             as order_quote_is_eligible_for_cross_docking,
                quotes.is_eligible_for_local_sourcing                            as order_quote_is_eligible_for_local_sourcing,
                quotes.is_local_sourcing                                         as order_quote_is_local_sourcing,
                round(((quotes.subtotal_price_amount / 100.00) / rates.rate), 2) as order_quote_amount_usd,
                quotes.currency_code                                             as order_quote_source_currency,
                (1/rates.rate)                                                    as exchange_rate_at_closing,
                quotes.price_multiplier                                          as order_quote_price_multiplier,
                quotes.cross_docking_added_lead_time
         from {{ ref('prep_supply_orders') }} as orders
             left join {{ ref('prep_supply_documents') }} as quotes on orders.quote_uuid = quotes.uuid

             -- Joins for exchange rates
             left outer join {{ ref('stg_orders_dealstage') }} as order_deals on orders.uuid = order_deals.order_uuid
             left join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates 
                on rates.currency_code_to = quotes.currency_code
                -- From '2022-04-01' we started using the more appropriate closing date as exchange rate date for closing values instead of quote finalized_at, this has been changed but not retroactively.
                and trunc(coalesce(case when order_deals.closed_at >= '2022-04-01' then order_deals.closed_at else null end, quotes.finalized_at, quotes.created)) = trunc(rates.date)
         where true
           and quotes.type = 'quote'
     ),

-- ALL QUOTE AGGREGATES

     agg_all_quotes as (
         select order_uuid                                        as                      order_uuid,
                min(submitted_at)                                 as                      order_first_submitted_at,
                max(revision)                                     as                      number_of_quote_versions,
                bool_or(is_admin)                                 as                      has_admin_created_quote,
                sum(case
                        when submitted_at - finalized_at <> interval '0 seconds' then 1
                        else 0 end)                               as                      has_non_locked_quote_review,
                case
                    when has_admin_created_quote = true or has_non_locked_quote_review > 0 then true
                    else false end                                                        has_manual_quote_review
         from {{ ref('prep_supply_documents') }}
         where type = 'quote'
         group by 1
     ),


----------------------------------------------------------------
-- PURCHASE ORDERS AGGREGATES
----------------------------------------------------------------

-- FIRST PURCHASE ORDER FIELDS

     first_po as (
         with rn as (select soq.order_uuid,
                            soq.uuid                                                               as po_first_uuid,
                            finalized_at                                                           as sourced_at,
                            spocl.supplier_id::int                                                 as po_first_supplier_id,
                            supplier_support_ticket_id                                             as po_first_support_ticket_id,
                            round(((subtotal_price_amount / 100.00) / rates.rate), 2)              as subtotal_sourced_cost_usd,
                            (1/rates.rate)                                                         as exchange_rate_at_sourcing,
                            case when soq.shipping_date >= '2019-10-01' then soq.shipping_date end as po_first_promised_shipping_at_by_supplier,                                                                                                                      
                            row_number() over (partition by soq.order_uuid order by finalized_at)      as rn
                     from {{ ref('prep_supply_documents') }} as soq
                     left join {{ source('data_lake', 'exchange_rate_spot_daily')}} as rates
                        on rates.currency_code_to = soq.currency_code 
                        and rates.date = trunc(soq.finalized_at)
                     left join {{ ref('prep_purchase_orders') }} as spocl on soq.uuid = spocl.uuid
                     where true
                       and soq.type like 'purchase_order'
                       and soq.finalized_at is not null
                     group by 1, 2, 3, 4, 5, 6, 7, 8)
         select order_uuid,
                po_first_uuid,
                sourced_at,  -- Used to define sourced_date                
                po_first_supplier_id, -- Used to define is_resourced
                po_first_support_ticket_id,
                subtotal_sourced_cost_usd,
                exchange_rate_at_sourcing,
                po_first_promised_shipping_at_by_supplier
         from rn
         where rn = 1
     ),

-- ACTIVE PURCHASE ORDER FIELDS
-- This data is obtained by querying the quotes table (type PO) and filtering for status active

     active_po as (
         select quotes.order_uuid,
                quotes.uuid                                                                  as po_active_uuid,
                quotes.created                                                               as po_active_created_at, 
                round(((subtotal_price_amount / 100.00) / rates.rate), 2)                    as po_active_subtotal_cost_usd,
                document_number                                                              as po_active_document_number,
                purchase_orders.supplier_id::int                                             as po_active_supplier_id, -- Used to define is_resourced field
                purchase_orders.supplier_support_ticket_id                                   as po_active_support_ticket_id,
                suppliers.name                                                               as po_active_supplier_name,
                suppliers.address_id                                                         as po_active_supplier_address_id,
                countries.name                                                               as po_active_company_entity,
                case
                    when quotes.shipping_date >= '2019-10-01'
                        then quotes.shipping_date end                                        as po_active_promised_shipping_at_by_supplier,
                row_number() over (
                    partition by quotes.order_uuid order by quotes.created desc)             as rn -- Noticed a few orders with 2+ active POs, this helps us guarantee uniqueness
         from {{ ref('prep_supply_documents') }} as quotes
    inner join {{ ref('prep_purchase_orders') }} as purchase_orders
         on quotes.uuid = purchase_orders.uuid
             left join {{ source('data_lake', 'exchange_rate_spot_daily')}} as rates
                on rates.currency_code_to = quotes.currency_code 
                and rates.date = trunc(quotes.finalized_at)
             left join {{ ref('company_entities') }} as ce on quotes.company_entity_id = ce.id
             left join {{ ref('prep_countries') }} as countries on ce.corporate_country_id = countries.country_id
             left join {{ ref('suppliers') }} as suppliers on purchase_orders.supplier_id = suppliers.id
         where quotes.type = 'purchase_order'
           and purchase_orders.status = 'active'
         {{ dbt_utils.group_by(n=11) }}
     ),

     -- ALL PURCHASE ORDER FIELDS

     agg_all_pos as (
         select osl.uuid as order_uuid,
                count(*) as number_of_purchase_orders -- Not leveraged? But seems important.
         from {{ ref('prep_supply_documents') }} as oqsl
         inner join {{ ref('prep_supply_orders') }} as osl on oqsl.order_uuid = osl.uuid
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
       fq.is_splitted_from_order,
       fq.is_splitted_order,
       fq.quote_first_splitted_from_quote_uuid,

       -- Order Quote
       oq.order_quote_document_number,
       oq.order_quote_status,     -- Used to filter out carts
       oq.order_quote_created_at, -- This is actually not in fact deals?
       oq.order_quote_submitted_at,
       oq.order_quote_finalised_at,
       oq.order_quote_lead_time,
       case when oq.order_quote_lead_time <= 5 then 6
        when oq.order_quote_lead_time <= 10 then 15
        when oq.order_quote_lead_time <= 15 then 24
        when oq.order_quote_lead_time <= 20 then 32
        else 42
       end as sourcing_window,

       oq.cross_docking_added_lead_time,
       oq.order_quote_is_cross_docking,
       oq.order_quote_is_eligible_for_cross_docking,
       oq.order_quote_is_local_sourcing,
       oq.order_quote_is_eligible_for_local_sourcing,

       oq.order_quote_amount_usd,
       oq.order_quote_source_currency,
       oq.exchange_rate_at_closing,
       oq.order_quote_price_multiplier,

       -- All Quotes
       aaq.order_first_submitted_at,
       aaq.number_of_quote_versions,
       aaq.has_admin_created_quote,
       aaq.has_manual_quote_review,

       -- First PO
       fpo.po_first_uuid,
       fpo.subtotal_sourced_cost_usd,
       fpo.exchange_rate_at_sourcing,
       fpo.sourced_at,
       fpo.sourced_at is not null as is_sourced,
       fpo.po_first_supplier_id,
       fpo.po_first_support_ticket_id,
       fpo.po_first_promised_shipping_at_by_supplier,

       -- Active PO
       apo.po_active_uuid,
       apo.po_active_created_at,
       apo.po_active_subtotal_cost_usd,
       apo.po_active_document_number,
       apo.po_active_company_entity,
       apo.po_active_promised_shipping_at_by_supplier, -- This field has a different naming convention because is a key field
       apo.po_active_supplier_id,
       apo.po_active_supplier_name,
       apo.po_active_supplier_address_id,
       apo.po_active_support_ticket_id,

       -- All POs
       aapo.number_of_purchase_orders,

       -- Combined Fields

       case when fpo.po_first_supplier_id <> apo.po_active_supplier_id then true else false end as is_resourced

from first_quote as fq
         left join order_quote as oq on fq.order_uuid = oq.order_uuid
         left join agg_all_quotes as aaq on fq.order_uuid = aaq.order_uuid
         left join first_po as fpo on fq.order_uuid = fpo.order_uuid
         left join active_po as apo on fq.order_uuid = apo.order_uuid and apo.rn = 1
         left join agg_all_pos as aapo on fq.order_uuid = aapo.order_uuid