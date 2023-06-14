select cr.*, srs.reason_mapped
from {{ source("int_service_supply", "cancellation_reasons") }} cr
left join {{ ref("seed_cancellation_reasons") }} srs on lower(srs.reason) = lower(cr.title)
