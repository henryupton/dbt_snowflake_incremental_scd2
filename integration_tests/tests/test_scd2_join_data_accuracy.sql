-- Test that scd2_join accurately reflects the state of both tables at each point in time
-- This test validates that the correct versions of customer and address data
-- are joined together for each temporal period

with test_case as (
    -- Test specific scenario: Customer 1's state at 2024-01-01 14:30:00
    -- At this time:
    -- - Customer should have email 'john@example.com' (changed to john.updated@example.com at 15:00)
    -- - Address should be '456 Oak Ave' (changed from 123 Main St at 14:00)
    select 
        customer_id,
        customer_name,
        email,
        street,
        city,
        state
    from {{ ref('test_scd2_join') }}
    where customer_id = 1
        and _valid_from <= '2024-01-01 14:30:00'::timestamp
        and _valid_to > '2024-01-01 14:30:00'::timestamp
),

validation as (
    select 
        customer_id,
        -- At 14:30, customer email should still be john@example.com
        case when email = 'john@example.com' then 0 else 1 end as email_incorrect,
        -- At 14:30, address should be 456 Oak Ave (changed at 14:00)
        case when street = '456 Oak Ave' then 0 else 1 end as address_incorrect,
        case when city = 'New York' then 0 else 1 end as city_incorrect,
        case when state = 'NY' then 0 else 1 end as state_incorrect
    from test_case
)

select 
    email_incorrect + address_incorrect + city_incorrect + state_incorrect as total_errors
from validation
having total_errors > 0