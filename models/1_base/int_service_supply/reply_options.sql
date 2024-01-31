-- Base model for reply_options
-- Only leveraged columns are included

select
id,
description
from {{ source('fed_fulfilment', 'reply_options') }}