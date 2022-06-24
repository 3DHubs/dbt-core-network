----------------------------------------------------------------
-- REQUEST FOR QUOTATION (RFQ) AGGREGATES
----------------------------------------------------------------
       
-- Data comes from the model fact_rfq_behaviour, this model primarily depends on
-- the table supplier-rfqs coming from supply DB where auctions are of type RFQ.

{{ config(
    tags=["multirefresh"]
) }}

select order_uuid,
       true                                                                                              as has_rfq,
       -- An quote can be duplicated manually and not show as a winning bid
       bool_or(is_automatically_allocated_rfq)                                                           as has_automatically_allocated_rfq,
       bool_or(is_winning_bid)                                                                           as is_rfq_automatically_sourced,  
       count(distinct supplier_id)                                                                       as number_of_suppliers_rfq_requests,
       count(distinct case when supplier_rfq_responded_date is not null then supplier_id else null end)  as number_of_suppliers_rfq_responded,
       count (distinct auction_uuid)                                                                     as number_of_rfqs,
       count(*)                                                                                          as number_of_rfq_requests,
       sum(case when supplier_rfq_responded_date is not null then 1 else 0 end)                          as number_of_rfq_responded
from {{ ref('fact_rfq_behaviour') }} as rfq
group by 1