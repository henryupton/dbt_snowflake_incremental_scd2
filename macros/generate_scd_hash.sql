{% macro generate_scd_hash(temp_relation, scd_check_columns, audit_columns) %}
  {%- if scd_check_columns is none -%}
    {# Use all columns except audit columns if no specific columns specified #}
    {%- set all_columns = adapter.get_columns_in_relation(temp_relation) -%}
    {%- set columns_to_hash = all_columns | map(attribute='name') | reject('in', audit_columns) | list -%}
  {%- else -%}
    {# Use specified columns #}
    {%- set columns_to_hash = scd_check_columns -%}
  {%- endif -%}
  
  {%- if columns_to_hash | length == 0 -%}
    {{ exceptions.raise_compiler_error("No columns available for SCD hash generation") }}
  {%- endif -%}
  
  {# Use dbt_utils.surrogate_key for hash generation #}
  {{ dbt_utils.generate_surrogate_key(columns_to_hash) }}
{% endmacro %}
