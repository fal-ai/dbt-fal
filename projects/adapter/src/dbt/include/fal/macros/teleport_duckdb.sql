{% macro duckdb__copy_to(relation, url) -%}
    COPY {{ relation }} TO '{{ url }}'
{%- endmacro %}

{% macro duckdb__copy_from_parquet(relation, url) -%}
    CREATE OR REPLACE TABLE {{ relation }} AS
        SELECT * FROM read_parquet('{{ url }}');
{%- endmacro %}
