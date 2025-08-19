{{
    config(
        materialized='incremental_scd2',
        unique_key=['customer_id'],
        scd_check_columns=['street', 'city', 'state']
    )
}}

select 
    customer_id,
    street,
    city,
    state,
    _updated_at
from {{ ref('addresses_for_join') }}