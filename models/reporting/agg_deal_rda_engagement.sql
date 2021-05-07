/*
 * Author: Diego
 */
select sai.order_uuid,
       count(distinct sai.sa_supplier_id)                                                        as number_of_suppliers_assigned,
       --General Bid Aggregates
       count(distinct sai.bid_uuid)                                                              as number_of_bids,
       count(distinct (case when sai.bid_supplier_response = 'countered' then sai.bid_uuid end)) as
                                                                                                    number_of_counterbids,
       count(distinct (case when sai.bid_supplier_response = 'rejected' then sai.bid_uuid end))  as
                                                                                                    number_of_rejected_bids,
       --Counter Bids Count by Type
       count(distinct (case when sai.bid_has_design_modifications then sai.bid_uuid end))        as
                                                                                                    number_of_design_counterbids,
       --todo:replace data type at source
       count(distinct (case
                           when sai.bid_has_changed_shipping_date = 'true'
                               then sai.bid_uuid end))                                           as number_of_lead_time_counterbids,
       count(distinct
             (case when sai.bid_has_changed_prices then sai.bid_uuid end))                       as number_of_price_counterbids,
       --Winning Bid Results
       bool_or(sai.is_winning_bid)                                                               as order_has_winning_bid,
       bool_or(sai.is_winning_bid and sai.bid_supplier_response = 'accepted')                    as order_has_accepted_winning_bid,
       bool_or(sai.bid_has_changed_prices and sai.is_winning_bid)                                as order_has_winning_bid_countered_on_price,
       bool_or(
               sai.bid_has_changed_shipping_date = 'true' and sai.is_winning_bid)                as order_has_winning_bid_countered_on_lead_time,
       bool_or(sai.bid_has_design_modifications and sai.is_winning_bid)                          as order_has_winning_bid_countered_on_design
from reporting.fact_supplier_auction_interactions as sai
group by 1