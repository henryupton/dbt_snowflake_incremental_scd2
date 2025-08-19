-- Test that scd2_join produces the expected number of rows
-- Based on our test data, we expect specific temporal periods for each customer

with expected_counts as (
    select 
        -- Customer 1: Should have periods for each unique timestamp across both tables
        -- Timestamps: 09:00, 10:00, 14:00, 15:00, 16:00 = 5 periods
        -- Customer 2: Timestamps: 10:00, 10:30, 12:00, 13:00, 18:00 = 5 periods  
        -- Customer 3: Timestamps: 11:00, 11:30, 17:00 = 3 periods
        -- Total expected: 13 periods
        13 as expected_row_count
),

actual_counts as (
    select count(*) as actual_row_count
    from {{ ref('test_scd2_join') }}
    where customer_id is not null
        and _valid_from < _valid_to  -- Only valid periods
)

select 
    abs(expected_row_count - actual_row_count) as row_count_difference
from expected_counts, actual_counts  
having abs(expected_row_count - actual_row_count) > 0