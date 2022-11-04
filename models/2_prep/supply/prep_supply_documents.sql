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
       docs.id,
       docs.uuid,
       docs.order_uuid,
       docs.revision,
       docs.status,
       docs.finalized_at,
       docs.document_number,
       docs.contact_preference,
       docs.tax_number,
       docs.shipping_address_id,
       docs.billing_address_id,
       docs.payment_reference,
       docs.currency_code,
       docs.price_multiplier,
       docs.subtotal_price_amount,
       docs.tax_price_amount,
       docs.tax_rate,
       docs.turnaround_time,
       docs.description,
       docs.admin_description,
       docs.parent_uuid,
       docs.payment_term,
       docs.type,
       docs.lead_time,
       docs.lead_time_tier_id,
       docs.document_file_uuid,
       docs.company_entity_id,
       docs.payment_details_id,
       docs.shipping_date,
       docs.shipping_price_estimates,
       docs.terms_and_conditions_id,
       docs.quickbooks_upload_id,
       docs.submitted_at,
       docs.tax_number_2,
       docs.archive_file_uuid,
       docs.customer_purchase_order_reference,
       docs.customer_purchase_order_uuid,
       docs.signed_quote_uuid,
       docs.tax_category_id,
       docs.technology_id,
       docs.cross_docking_added_lead_time,
       docs.name,
       docs.customer_edited_at,
       docs.technical_drawing_anonymization_started_at,
       docs.split_off_from_quote_uuid,
       
       -- Key Fields for Filtering
       orders.updated as order_updated_at, -- Necessary for incremental model settings in prep_line_items
       docs.uuid = orders.quote_uuid as is_order_quote,
       po.status = 'active' as is_active_po,       
       rank() over (partition by docs.order_uuid, docs.type order by revision, docs.created desc) as revision_last_created_rank,

       -- Boolean Fields
       {{ varchar_to_boolean('is_eligible_for_local_sourcing') }},       
       {{ varchar_to_boolean('is_local_sourcing') }},       
       {{ varchar_to_boolean('is_eligible_for_cross_docking') }},       
       {{ varchar_to_boolean('is_cross_docking') }},
       {{ varchar_to_boolean('is_admin') }},       
       {{ varchar_to_boolean('tax_category_override') }},
       {{ varchar_to_boolean('is_admin_only') }},
       {{ varchar_to_boolean('is_instant_payment') }}                     

from {{ source('int_service_supply', 'cnc_order_quotes') }} as docs
-- Filter: reduces the number of documents by filtering out empty orders, filter that takes place in the supply_orders model
inner join (select uuid, quote_uuid, updated from {{ ref('prep_supply_orders')}}) as orders on docs.order_uuid = orders.uuid
-- Useful to get the status of the purchase order, this allow us to filter on active POs on following models
left join (select uuid, status from int_service_supply.purchase_orders) as po on docs.uuid = po.uuid