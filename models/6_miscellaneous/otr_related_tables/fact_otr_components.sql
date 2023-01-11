-----------------------------------
-- Fact Order Component Aggregation
-----------------------------------
-- Created by: Daniel Salazar Soplapuco
-- Maintained by: Daniel Salazar Soplapuco
-- Last updated: December 2022

-- Use case:
-- This table aggregates the data originating from the stg_otr tables.
-- The output should provide an overview of time spent for each otr component and compare that to the allocated time.


-- sourcing time
select *
from {{ ref ('stg_otr_sourcing') }} as foc

union

-- Supplier Production
select *
from {{ ref ('stg_otr_production') }} as foc

union

-- Impact of logistics
select *
from {{ ref ('stg_otr_logistics') }} as foc

union

-- Impact of cross docking creation
select *
from {{ ref ('stg_otr_cross_dock') }} as foc