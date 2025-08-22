-- --------------------------------------------------------------
-- REQUEST FOR QUOTATION (RFQ) AGGREGATES
-- --------------------------------------------------------------
-- Data comes from the model fact_rfq_behaviour, this model primarily depends on
-- the table supplier-rfqs coming from supply DB where auctions are of type RFQ.
{{ config(tags=["multirefresh"]) }}

select
    order_uuid,
    true as has_rfq,
    -- An quote can be duplicated manually and not show as a winning bid
    bool_or(is_automatically_allocated_rfq) as has_automatically_allocated_rfq,
    bool_or(is_winning_bid) as is_rfq_automatically_sourced,
    count(distinct sa_supplier_id) as number_of_suppliers_rfq_requests,
    count(
        distinct case
            when response_placed_at <> null then sa_supplier_id else null --todo-migration-test
        end
    ) as number_of_suppliers_rfq_responded,
    count(distinct auction_uuid) as number_of_rfqs,
    count(*) as number_of_rfq_requests,
    sum(
        case when response_placed_at <> null then 1 else 0 end --todo-migration-test
    ) as number_of_rfq_responded,
    max(
        case when is_winning_bid then bid_estimated_first_leg_customs_amount_usd end
    ) as winning_bid_estimated_first_leg_customs_amount_usd,
    max(
        case
            when is_winning_bid then bid_estimated_second_leg_customs_amount_usd
        end
    ) as winning_bid_estimated_second_leg_customs_amount_usd
from {{ ref("fact_auction_behaviour") }}
where is_rfq
group by 1
