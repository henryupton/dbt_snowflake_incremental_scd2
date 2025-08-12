{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        scd_check_columns=['customer_name', 'email', 'status'],
        is_current_column='IS_CURRENT_RECORD',
        valid_from_column='EFFECTIVE_FROM',
        valid_to_column='EFFECTIVE_TO',
        updated_at_column='LAST_MODIFIED',
        change_type_column='RECORD_CHANGE_TYPE'
    )
}}

select 
    customer_id,
    customer_name,
    email,
    status,
    _updated_at as LAST_MODIFIED
from {{ ref('customers_raw') }}