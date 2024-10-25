select 
material_subset_id,
material_id,
process_id,
name,
slug,
density,
is_available_in_auctions,
material_excluded_in_eu,
material_excluded_in_us
from {{ ref('network_services','gold_material_subsets') }}