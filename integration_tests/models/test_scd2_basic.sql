{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        scd_check_columns=['customer_name', 'email', 'status']
    )
}}

select 
    customer_id,
    customer_name,
    email,
    status,
    _updated_at
from {{ ref('customers_raw') }}