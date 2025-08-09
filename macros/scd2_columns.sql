{% macro get_is_current_sql(unique_keys_csv, updated_at_col) %}
  row_number() over(
    partition by {{ unique_keys_csv }} 
    order by {{ updated_at_col }} desc
  ) = 1
{% endmacro %}

{% macro get_valid_to_sql(unique_keys_csv, updated_at_col, default_valid_to) %}
  coalesce(
    lead({{ updated_at_col }}) over(
      partition by {{ unique_keys_csv }} 
      order by {{ updated_at_col }}
    ), 
    {{ parse_timestamp_literal(default_valid_to) }}
  )
{% endmacro %}

{% macro get_valid_from_sql(updated_at_col) %}
  {{ updated_at_col }}
{% endmacro %}