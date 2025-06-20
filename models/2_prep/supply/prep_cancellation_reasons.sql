select
    cr.id             as cancellation_reason_id,
    cr.title          as cancellation_reason_title,
    scr.reason_mapped as cancellation_reason_mapped

from {{ ref('sources_network', 'gold_cancellation_reasons') }} cr

    left join {{ ref("seed_cancellation_reasons") }} scr
        on lower(scr.reason) = lower(cr.title)
