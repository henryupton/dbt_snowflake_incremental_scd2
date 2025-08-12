-- Test that valid_from is always before or equal to valid_to

select 
    customer_id,
    _VALID_FROM,
    _VALID_TO,
    _UPDATED_AT
from {{ ref('test_scd2_basic') }}
where _VALID_FROM > _VALID_TO