select
       material_id,
       name,
       technology_id

from {{ ref('gold_materials') }}