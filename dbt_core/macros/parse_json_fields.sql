{% macro parse_json_fields_with_cast(json_column, field_map) %}
  {% for field, type in field_map.items() %}
    {% if type == "string" %}
      JSON_EXTRACT_SCALAR({{ json_column }}, '$.{{ field }}') as {{ field }}
    {% elif type == "int" %}
      CAST(JSON_EXTRACT_SCALAR({{ json_column }}, '$.{{ field }}') AS INT64) as {{ field }}
    {% elif type == "float" %}
      CAST(JSON_EXTRACT_SCALAR({{ json_column }}, '$.{{ field }}') AS FLOAT64) as {{ field }}
    {% elif type == "bool" %}
      CAST(JSON_EXTRACT_SCALAR({{ json_column }}, '$.{{ field }}') AS BOOL) as {{ field }}
    {% else %}
      -- Default fallback to string
      JSON_EXTRACT_SCALAR({{ json_column }}, '$.{{ field }}') as {{ field }}
    {% endif %}
    {% if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}
