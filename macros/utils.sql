{% macro cast_to_timestamp(date_string) %}
  cast('{{ date_string }}' as timestamp_tz)
{% endmacro %}

{% macro parse_timestamp_literal(timestamp_string) %}
  '{{ timestamp_string }}'::timestamp_tz
{% endmacro %}

{% macro current_timestamp_func() %}
  current_timestamp()
{% endmacro %}

