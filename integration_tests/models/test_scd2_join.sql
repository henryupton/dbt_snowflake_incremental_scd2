{{
    config(
        materialized='table'
    )
}}

{{
    dbt_snowflake_incremental_scd2.scd2_join([
        ref('customers_scd2'),
        ref('addresses_scd2')
    ], 'customer_id')
}}