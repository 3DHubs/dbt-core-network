----------------------------------------------------------------
-- Overall Integrity Check
----------------------------------------------------------------

-- Use case:
-- This test pulls the result from table: overall_integrity_test, which performs the actual test.
-- If there are any results in the table overall_integrity_test the test will fail as it will indicate that there are integrity failures.
-- The tables are split so that the integrity failures are stored and can be viewed in looker.

-- Last updated: July 6, 2022
-- Maintained by: Daniel Salazar Soplapuco

select oit.test_description,
       count(*) as failure_count
from {{ ref('overall_integrity_test') }}  as oit
group by 1
having failure_count != 0