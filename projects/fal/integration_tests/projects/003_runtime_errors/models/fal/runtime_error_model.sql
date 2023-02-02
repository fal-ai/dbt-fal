
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED 35d7f6579989a8628c2b64b496c8e3d9

Script dependencies:

{{ ref('working_model') }}

*/

SELECT * FROM {{ this }}
