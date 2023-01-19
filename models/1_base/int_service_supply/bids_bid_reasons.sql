    -- Description: The bids bid reasons table is a mapping table from bids to bid_reasons.
    -- Reasons are predefined categories selected in the UI by suppliers when counterbidding or rejecting.

    -- Right now bid reasons are single select in the front-end but it is possible that this change in the feature,
    -- the model was hence create to anticipate this scenario although right now there are no (see note) one-to-many relationships.
    -- Updated: Feb 2022 (Diego)

    with bids_with_multiple_reasons as (
        select bid_uuid, count(*) as n_reasons
        from {{ source('int_service_supply', 'new_bids_bid_reasons') }}
        group by 1
        having n_reasons > 1
    ) 
    
    select * from {{ source('int_service_supply', 'new_bids_bid_reasons') }}
    where bid_uuid not in (select bid_uuid from bids_with_multiple_reasons)
    
    --- Note: there are no one-to-many relationships on the current implementation but the table have data 
    -- before mid 2021 with bids with multiple reasons hence I am filtering out those.
    -- See query on comments of PR #224 (Diego).