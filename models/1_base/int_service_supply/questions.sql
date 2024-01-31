-- Base model for questions table
-- Only columns leveraged downstream included

select
uuid,
order_uuid,
line_item_uuid,
status,
submitted_at,
author_id,
answered_at,
answered_by_id,
title
from {{ source('fed_fulfilment', 'questions') }}