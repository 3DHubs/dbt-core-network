/* Industry mapping table depends on the industries.csv file. Be careful, if data is added to industries.csv it may
   change the `industry_id` and `industry_mapped_id` values. So, this may require feature changes in the way we 
   handle this data. E.g. manage the IDs in the csv directly. */

{{
    config(
        materialized='table'
    )
}}

with unique_industries as (
    select distinct trim(lower(industry_mapped)) as industry_mapped
    from {{ ref('industries') }}
),
     mapped_industries as (
         select industry_mapped,
                row_number() over (order by industry_mapped) as industry_mapped_id
         from unique_industries
     ),
     industries as (
         select lower(trim(industry))                     as industry,
                row_number() over (order by industry asc) as industry_id,
                lower(trim(industry_mapped))              as industry_mapped
         from {{ ref('industries') }}
     )
select tmp.industry,
       tmp.industry_id,
       tmp.industry_mapped,
       mi.industry_mapped_id
from industries as tmp
         join mapped_industries as mi on tmp.industry_mapped = mi.industry_mapped