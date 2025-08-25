-- Test that scd2_join produces exactly the expected output
-- This test compares the actual join result with a pre-calculated expected output

with actual_output as (
    select 
        customer_id,
        customer_name,
        email,
        street,
        city,
        state,
        _is_current,
        _valid_from::timestamp as _valid_from,
        case 
            when _valid_to::varchar = '2999-12-31 23:59:59+0000' then '2999-12-31 23:59:59'::timestamp
            else _valid_to::timestamp 
        end as _valid_to
    from {{ ref('test_scd2_join') }}
),

expected_output as (
    select 
        customer_id,
        customer_name,
        email,
        street,
        city,
        state,
        _is_current,
        _valid_from::timestamp as _valid_from,
        _valid_to::timestamp as _valid_to
    from {{ ref('expected_scd2_join_output') }}
),

-- Find records that are in actual but not in expected
missing_from_expected as (
    select 'missing_from_expected' as issue_type, customer_id, _valid_from, _valid_to
    from actual_output
    except
    select 'missing_from_expected' as issue_type, customer_id, _valid_from, _valid_to
    from expected_output
),

-- Find records that are in expected but not in actual
missing_from_actual as (
    select 'missing_from_actual' as issue_type, customer_id, _valid_from, _valid_to
    from expected_output
    except
    select 'missing_from_actual' as issue_type, customer_id, _valid_from, _valid_to
    from actual_output
),

all_differences as (
    select * from missing_from_expected
    union all
    select * from missing_from_actual
)

select count(*) as total_differences
from all_differences
having count(*) > 0