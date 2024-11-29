select
    uuid,
    supplier_id,
    material_subset_id,
    supplier_name,
    material_subset_name,
    material_subset_slug,
    is_available_in_auctions,
    material_excluded_in_eu,
    material_excluded_in_us,
    technology_id,
    material_id,
    material_name,
    process_id,
    technology_name
from {{ ref('network_services', 'gold_supplier_material_subsets') }}