-- Test that SCD2 behavior works correctly
-- This test should pass when there are exactly 3 current records after initial load
-- (one current version for each of the 3 unique customer_ids)

select count(*) as current_record_count
from {{ ref('test_scd2_basic') }}
where _IS_CURRENT = true
having count(*) != 3