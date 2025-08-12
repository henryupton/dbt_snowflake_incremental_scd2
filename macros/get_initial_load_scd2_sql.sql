{% macro get_initial_load_scd2_sql(arg_dict) %}
    {% set temp_relation = arg_dict["temp_relation"] %}
    {% set unique_key = arg_dict["unique_key"] %}
    {% set scd_check_columns = arg_dict["scd_check_columns"] %}
    {% set audit_columns = arg_dict["audit_columns"] %}
    
    {# Define our audit columns #}
    {%- set is_current_col = arg_dict.get('is_current_column', var('is_current_column', '_IS_CURRENT')) -%}
    {%- set valid_from_col = arg_dict.get('valid_from_column', var('valid_from_column', '_VALID_FROM')) -%}
    {%- set valid_to_col = arg_dict.get('valid_to_column', var('valid_to_column', '_VALID_TO')) -%}
    {%- set updated_at_col = arg_dict.get('updated_at_column', var('updated_at_column', '_UPDATED_AT')) -%}
    {%- set change_type_col = arg_dict.get('change_type_column', var('change_type_column', '_CHANGE_TYPE')) -%}
    {%- set change_type_expr = arg_dict.get('change_type_expr', none) -%}
    {%- set default_valid_to = arg_dict.get('default_valid_to', var('default_valid_to', '2999-12-31 23:59:59+0000')) -%}

    {# Prepare unique key CSV for window functions #}
    {%- set unique_keys_csv = dbt_snowflake_incremental_scd2.get_quoted_csv(unique_key) -%}

    {# Process change_type_expr - defaults to ROW_NUMBER logic if not provided #}
    {%- if change_type_expr -%}
        {%- set change_type_sql = change_type_expr -%}
    {%- else -%}
        {# Default ROW_NUMBER logic #}
        {%- set change_type_sql = "CASE WHEN ROW_NUMBER() OVER (PARTITION BY " + unique_keys_csv + " ORDER BY " + updated_at_col + ") = 1 THEN 'I' ELSE 'U' END" -%}
    {%- endif -%}

with source_data as (
  select * from {{ temp_relation }}
)
select 
  {# Select all original columns except the updated_at column since we'll add it as an audit column #}
  {% for col in adapter.get_columns_in_relation(temp_relation) %}
    {% if col.name != updated_at_col %}
      {{ col.name }},
    {% endif %}
  {% endfor %}
  
  {# Add SCD2 audit columns using reusable macros #}
  {{ dbt_snowflake_incremental_scd2.get_is_current_sql(unique_keys_csv, updated_at_col) }} as {{ is_current_col }},
  {{ dbt_snowflake_incremental_scd2.get_valid_from_sql(updated_at_col) }} as {{ valid_from_col }},
  {{ dbt_snowflake_incremental_scd2.get_valid_to_sql(unique_keys_csv, updated_at_col, default_valid_to) }} as {{ valid_to_col }},
  {{ updated_at_col }} as {{ updated_at_col }},
  {{ change_type_sql }} as {{ change_type_col }}
from source_data

{% endmacro %}