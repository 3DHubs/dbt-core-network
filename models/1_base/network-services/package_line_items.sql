select
id,
package_uuid,
quantity
from {{ ref('sources_network', 'gold_package_line_items') }}