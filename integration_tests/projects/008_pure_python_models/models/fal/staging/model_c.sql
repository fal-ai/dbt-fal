
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED ed955dd5ab415f33c13a1efa0de2fb89

Script dependencies:

{{ ref("model_b") }}
{{ ref('model_a') }}

*/

SELECT * FROM {{ target.schema }}.{{ model.name }}
