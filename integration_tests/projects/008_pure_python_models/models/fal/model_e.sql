
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED f3d686c040e94a5b33aa082f0ddcd6d3

Script dependencies:

{{ ref('model_c') }}

*/

SELECT * FROM {{ target.schema }}.{{ model.name }}
