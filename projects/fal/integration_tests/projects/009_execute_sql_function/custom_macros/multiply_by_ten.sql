{% macro multiply_by_ten(column_name) %}
    cast({{ column_name }} * 10 as INT)
{% endmacro %}
