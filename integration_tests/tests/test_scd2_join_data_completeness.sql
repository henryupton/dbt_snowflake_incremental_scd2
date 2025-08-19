-- Test that scd2_join includes all expected records
-- This test verifies that the join doesn't lose any customer periods
-- and properly handles customers that exist in one table but not the other

with customer_periods_from_customers as (
    select distinct customer_id, _valid_from::timestamp_tz as _valid_from
    from {{ ref('customers_scd2') }}
    where customer_id is not null
),

customer_periods_from_addresses as (
    select distinct customer_id, _valid_from::timestamp_tz as _valid_from
    from {{ ref('addresses_scd2') }}
    where customer_id is not null
),

-- All unique customer-period combinations that should exist in the result
expected_periods as (
    select customer_id, _valid_from from customer_periods_from_customers
    union
    select customer_id, _valid_from from customer_periods_from_addresses
),

-- Customer-period combinations that actually exist in the joined result
actual_periods as (
    select distinct customer_id, _valid_from
    from {{ ref('test_scd2_join') }}
    where customer_id is not null
),

-- Find any missing periods
missing_periods as (
    select customer_id, _valid_from
    from expected_periods
    except
    select customer_id, _valid_from
    from actual_periods
)

select count(*) as missing_period_count
from missing_periods
having count(*) > 0