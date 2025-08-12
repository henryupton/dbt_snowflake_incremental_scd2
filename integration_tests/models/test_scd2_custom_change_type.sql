{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        scd_check_columns=['customer_name', 'email', 'status'],
        change_type_expr="CASE WHEN status = 'INACTIVE' THEN 'D' ELSE 'U' END"
    )
}}

select 
    customer_id,
    customer_name,
    email,
    status,
    _updated_at
from {{ ref('customers_raw') }}