{% set boolean_fields = [
    "is_dismissed",
    "is_automated_shipping_available",
    "is_detected_similar",
    "is_customer_requested_reorder",
    ]
%}

select supplier_id,
       auction_uuid,
       assigned_at,
       last_seen_at,
       first_seen_at,
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
       is_preferred_auction,
       {% for boolean_field in boolean_fields %}
           {{ varchar_to_boolean(boolean_field) }}
           {% if not loop.last %},{% endif %}
       {% endfor %}
from int_service_supply.supplier_auctions