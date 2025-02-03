-- This model queries directly form int_service_supply.cnc_order_quotes and conveniently renames it
-- as supply_documents as the source model is not exclusive to CNC technology nor it is exclusive to
-- quotes, it also contains other documents such as auctions, bids, purchase orders and invoices.

{{ config(
            materialized='table',
            tags=["multirefresh"]
    )
    }}

select docs.created,
       docs.updated,
       docs.deleted,
       docs.uuid,
       docs.order_uuid,
       docs.revision,
       docs.status,
       docs.finalized_at,
       docs.document_number,
       docs.shipping_address_id,
       docs.billing_address_id,
       docs.currency_code,
       docs.price_multiplier,
       docs.subtotal_price_amount,
       docs.tax_price_amount,
       docs.parent_uuid,
       docs.payment_term,
       docs.type,
       docs.lead_time,
       docs.company_entity_id,
       docs.shipping_date,
       docs.submitted_at,
       docs.customer_purchase_order_uuid,
       docs.signed_quote_uuid,
       docs.technology_id,
       docs.cross_docking_added_lead_time,
       docs.split_off_from_quote_uuid,
       docs.chargeable_shipping_weight_estimate_kg,
       docs.is_last_version,
       -- Boolean Fields
       docs.is_eligible_for_local_sourcing,
       docs.is_local_sourcing,
       docs.is_eligible_for_cross_docking,
       docs.is_cross_docking_derived as is_cross_docking,
       docs.is_admin,
       docs.requires_local_production,
       docs.is_pl_pay_later_used,
       -- Geo Fields
       docs.shipping_company_name,
       docs.shipping_locality,
       docs.shipping_postal_code,
       docs.shipping_latitude,
       docs.shipping_longitude,
       docs.shipping_timezone,
       docs.shipping_country_id,
       docs.shipping_country,
       docs.shipping_country_alpha2_code,
       docs.corporate_country,
       adr.sub_region,
       adr.region,
       adr.market,

       -- Key Fields for Filtering
       orders.updated as order_updated_at, -- Necessary for incremental model settings in prep_line_items
       docs.uuid = orders.quote_uuid as is_order_quote,
       po.status = 'active' as is_active_po              

from {{ ref('documents') }} as docs
-- Filter: reduces the number of documents by filtering out empty orders, filter that takes place in the supply_orders model
inner join (select uuid, quote_uuid, updated from {{ ref('prep_supply_orders')}}) as orders on docs.order_uuid = orders.uuid
-- Useful to get the status of the purchase order, this allow us to filter on active POs on following models
left join (select uuid, status from {{ ref('network_services', 'gold_purchase_orders') }}) as po on docs.uuid = po.uuid
left join {{ref('prep_addresses')}} as adr on docs.shipping_address_id = adr.address_id

