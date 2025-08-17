{%- docs get_quoted_csv -%}
Converts column names (string or array) into a comma-separated list for SQL queries.

**Args:**
- `column_names` (string|array): Either a single column name string or an array of column names

**Returns:**
- String: Comma-separated list of column names suitable for SQL queries

**Example:**
- `get_quoted_csv("col1")` returns `"col1"`
- `get_quoted_csv(["col1", "col2"])` returns `"col1, col2"`
{%- enddocs -%}

{% macro get_quoted_csv(column_names) %}
  {% if column_names is string %}
    {{ column_names }}
  {% else %}
    {% for column in column_names %}
      {{ column }}{{ ", " if not loop.last }}
    {% endfor %}
  {% endif %}
{% endmacro %}