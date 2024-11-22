select li.uuid, 
    min(livqc.file_uuid) as livqc_file, 
    case when livqc_file is not null then true else false end as is_vqced
from {{ ref('line_items') }} as li
left join {{ source('int_service_supply', 'line_item_virtual_quality_control_part_photos') }} as livqc on li.uuid = livqc.line_item_uuid  
group by li.uuid
