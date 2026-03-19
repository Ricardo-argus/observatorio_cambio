{% macro validate_variation(column_name, threshold=0.2) %}
    (
        abs({{column_name}} - lag({{ column_name }}) over (order by datahoracotacao))
         / lag({{ column_name}}) over (order by datahoracotacao)
    ) > {{ threshold}}
{% endmacro %}