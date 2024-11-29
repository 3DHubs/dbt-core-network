select
uuid,
name,
id,
material_id,
material_name,
technology_id
from {{ ref('network_services', 'gold_material_finishes') }}