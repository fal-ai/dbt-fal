
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED 8cbbef9c1ac3aacaddb5c94d55d2ce6c

Script dependencies:

{{ ref('model_a1') }}

*/

SELECT * FROM {{ target.schema }}.{{ model.alias }}
