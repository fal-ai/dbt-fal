
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED 2d1b785862f633481ecaa4b410a5a8a6

Script dependencies:

{{ ref('model_a') }}
{{ ref('model_b') }}

*/

SELECT * FROM {{ this }}
