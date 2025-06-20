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

from {{ ref('sources_network', 'gold_supplier_processes') }}