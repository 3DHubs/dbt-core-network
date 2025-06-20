select
uuid,
supplier_id,
supplier_name,
surface_finish_id,
surface_finish_name

from {{ ref('sources_network', 'gold_supplier_finishes') }}