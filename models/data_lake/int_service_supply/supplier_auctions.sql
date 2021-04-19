select supplier_id,
       auction_uuid,
       assigned_at,
       last_seen_at,
       decode(is_dismissed, 'true', True, 'false', False) as is_dismissed,
       first_seen_at,
       decode(is_automated_shipping_available, 'true', True, 'false', False) as is_automated_shipping_available,
       currency_code,
       margin, -- Do not use for reporting, may include discount, which is not used in auction
       margin_without_discount, -- Margin unaffected by discount
       tax_rate,
       subtotal_price_amount,
       subtotal_price_amount_usd,
       tax_price_amount,
       tax_price_amount_usd,
       max_country_margin,
       company_entity_id,
       -- decode(is_reorder, 'true', True, 'false', False) as is_reorder, -- PS52: column `is_reorder` renamed to `is_detected_similar`. `is_reorder` will be brought back later.
       estimated_customs_price_amount,
       estimated_customs_price_amount_usd,
       estimated_customs_rate,
       ship_by_date,
       shipping_added_lead_time,
       decode(is_detected_similar, 'true', True, 'false', False) as is_detected_similar,
       decode(is_customer_requested_reorder, 'true', True, 'false', False) as is_customer_requested_reorder
from int_service_supply.supplier_auctions