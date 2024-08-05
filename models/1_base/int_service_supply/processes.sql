select created,
       updated,
       deleted,
       process_id,
       name,
       nullif(about, '-')                     as about,
       slug,
       slug_scope,
       benefits,
       limitations,
       price,
       tolerance,
       nullif(turnaround_time, '-')           as turnaround_time,
       nullif(wall_thickness, '-')            as wall_thickness,
       header_image_id,
       technology_id

from {{ source('int_service_supply', 'processes') }}