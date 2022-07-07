
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED 5703f4afba785d3cf956a51bcb4dc564

Script dependencies:

{{ ref("model_b") }}
{{ ref('model_a') }}

*/

SELECT * FROM {{ this }}
