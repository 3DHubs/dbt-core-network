select
id,
package_uuid,
quantity
from {{ ref('network_services', 'gold_package_line_items') }}