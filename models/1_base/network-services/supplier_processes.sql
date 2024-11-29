select
    uuid,
    supplier_id,
    supplier_name,
    process_id,
    depth_max,
    depth_min,
    height_max,
    height_min,
    width_max,
    width_min,
    process_name,
    technology_id

from {{ ref('network_services', 'gold_supplier_processes') }}