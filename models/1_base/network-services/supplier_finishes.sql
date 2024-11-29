select
uuid,
supplier_id,
supplier_name,
surface_finish_id,
surface_finish_name

from {{ ref('network_services', 'gold_supplier_finishes') }}