
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED d5d59e7be72d81f154140338f730e38c

Script dependencies:

{{ ref('model_c') }}

*/

SELECT * FROM {{ this }}
