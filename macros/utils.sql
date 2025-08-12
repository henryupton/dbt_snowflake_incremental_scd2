{% macro cast_to_timestamp(date_string) %}
  cast('{{ date_string }}' as timestamp_tz)
{% endmacro %}

{% macro parse_timestamp_literal(timestamp_string) %}
  '{{ timestamp_string }}'::timestamp_tz
{% endmacro %}

{% macro current_timestamp_func() %}
  current_timestamp()
{% endmacro %}

{% macro get_quoted_csv(column_names) %}
  {% if column_names is string %}
    {{ column_names }}
  {% else %}
    {% for column in column_names %}
      {{ column }}{{ ", " if not loop.last }}
    {% endfor %}
  {% endif %}
{% endmacro %}

