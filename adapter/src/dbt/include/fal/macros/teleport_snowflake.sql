{% macro snowflake__create_table_from_parquet(relation, location) -%}
    CREATE OR REPLACE TABLE {{ relation }} USING template (
      SELECT array_agg(object_construct(*))
	  	FROM table(
  			infer_schema(
	  			LOCATION=>'{{ location }}',
		  		FILE_FORMAT=>'falparquet',
		  		IGNORE_CASE=>TRUE
			  )
  		)
    );
{%- endmacro %}

{% macro snowflake__copy_from_parquet(relation, location) -%}
    COPY INTO {{ relation }} FROM (
      SELECT
        {% for col in adapter.get_columns_in_relation(relation) %}
          $1:{{col.column}}
          {%- if not loop.last -%},{%- endif -%}
        {% endfor %}
      FROM {{ location }})
    FILE_FORMAT = (FORMAT_NAME = 'falparquet');
{%- endmacro %}

{% macro snowflake__copy_to_parquet(relation, location) -%}
    COPY INTO {{ location }} FROM {{ relation }}
    FILE_FORMAT = (TYPE=parquet)
    OVERWRITE = TRUE
    SINGLE = TRUE
    HEADER = TRUE;
{%- endmacro %}
