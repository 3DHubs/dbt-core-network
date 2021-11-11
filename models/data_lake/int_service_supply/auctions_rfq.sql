-- CONTAINS ONLY RFQ AUCTIONS 

/* 
The Auctions data coming from Int Service Supply can hold rows from both "Real RDA Auctions" and 
RFQ Auctions (Request for Quotation). We decided to split the Auctions data into both categories for simplicity.  
 */

{% set boolean_fields = [
       "is_internal_support_ticket_opened",
       "china_throttled",
       "is_resourcing",
       "is_rfq"
    ]
%}

with stg as (
       select oqs.created,
              oqs.updated,
              oqs.deleted,
              oqs.order_uuid                                                 as order_uuid,
              oqs.document_number                                            as auction_document_number,
              auctions.uuid                                                  as order_quotes_uuid,
              auctions.winner_bid_uuid,
              auctions.status,                       -- If auction gets status 'resourced' it means it has been brought back to the auction
              auctions.started_at,
              auctions.finished_at,
              auctions.ship_by_date,
              auctions.last_processed_at,
              auctions.internal_support_ticket_id,
              auctions.base_margin,                  -- For debugging purposes only, do not use for reporting
              auctions.base_margin_without_discount, -- This field will be used in auctions
              {% for boolean_field in boolean_fields %}
                     {{ varchar_to_boolean(boolean_field) }}
                     {% if not loop.last %},{% endif %}
              {% endfor %},
              row_number() over (partition by oqs.order_uuid order by auctions.started_at desc nulls last)
                                                                             as recency_idx
       from {{ source('int_service_supply', 'auctions') }} as auctions
              inner join {{ ref('cnc_order_quotes') }} as oqs
                            on auctions.uuid = oqs.uuid
)
select *
from stg
where is_rfq