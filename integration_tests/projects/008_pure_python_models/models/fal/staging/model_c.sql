
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED 8575017c3d1726fc82796a665c9a60a9

Script dependencies:

{{ ref("model_b") }}
{{ ref('model_a') }}

*/

SELECT * FROM {{ target.schema }}.{{ model.alias }}
