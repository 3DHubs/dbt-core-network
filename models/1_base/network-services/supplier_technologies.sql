select
    uuid,
    supplier_id,
    supplier_name,
    technology_id,
    max_active_orders,
    min_order_amount_usd,
    max_order_amount_usd,
    allow_cosmetic_worthy_finishes,
    allow_orders_with_custom_finishes,
    num_parts_min,
    num_parts_max,
    num_units_min,
    num_units_max,
    technology_name

from {{ ref('sources_network', 'gold_supplier_technologies') }}