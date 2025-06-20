select
uuid,
name,
id,
material_id,
material_name,
technology_id
from {{ ref('sources_network', 'gold_material_finishes') }}