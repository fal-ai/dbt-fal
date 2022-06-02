
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED 4ece7938c7f764bd94f3955f868ab5c5

Script dependencies:

{{ ref('model_c') }}

*/

SELECT * FROM {{ target.schema }}.{{ model.alias }}
