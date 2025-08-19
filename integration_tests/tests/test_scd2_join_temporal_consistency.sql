-- Test that scd2_join maintains temporal consistency
-- This test verifies that the temporal spine correctly captures all time periods
-- and that joined data is temporally consistent (no gaps or overlaps)

with spine_periods as (
    select 
        customer_id,
        _valid_from,
        _valid_to,
        lag(_valid_to) over (partition by customer_id order by _valid_from) as prev_valid_to
    from {{ ref('test_scd2_join') }}
    where customer_id is not null
),

-- Check for gaps in the temporal spine (where prev_valid_to != _valid_from)
gaps as (
    select count(*) as gap_count
    from spine_periods 
    where prev_valid_to is not null 
        and prev_valid_to != _valid_from
),

-- Check for overlapping periods (where _valid_from >= _valid_to)
overlaps as (
    select count(*) as overlap_count
    from {{ ref('test_scd2_join') }}
    where _valid_from >= _valid_to
)

select 
    gap_count + overlap_count as total_inconsistencies
from gaps, overlaps
having total_inconsistencies > 0