{% macro null_prices(column_name, default_value) %}
    coalesce({{column_name}}, {{ default_value}})
{% endmacro %}

