-- Test that each customer_id has exactly one current record

select 
    customer_id,
    count(*) as current_count
from {{ ref('test_scd2_basic') }}
where _IS_CURRENT = true
group by customer_id
having count(*) != 1