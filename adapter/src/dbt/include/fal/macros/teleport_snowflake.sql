{% macro snowflake__copy_from_parquet(relation, location, columns) -%}
    CREATE OR REPLACE TABLE {{ relation }} USING template (
      SELECT array_agg(object_construct(*))
	  	FROM table(
  			infer_schema(
	  			LOCATION=>'{{ location }}',
		  		file_format=>'falparquet'
			  )
  		)
    );
    COPY INTO {{ relation }} FROM (SELECT {{ columns }} FROM {{ location }})
    FILE_FORMAT = (FORMAT_NAME = 'falparquet');
{%- endmacro %}

{% macro snowflake__copy_to_parquet(relation, location) -%}
    COPY INTO {{ location }} FROM {{ relation }}
    FILE_FORMAT = (TYPE=parquet)
    OVERWRITE = TRUE
    SINGLE = TRUE
    HEADER = TRUE;
{%- endmacro %}
