select *,
{{ dbt_utils.get_url_parameter(field='pages', url_parameter='abt') }}                    as test_name,
{{ dbt_utils.get_url_parameter(field='pages', url_parameter='abv') }}                    as test_variant
from {{ source('data_lake', 'hubspot_pages') }}
