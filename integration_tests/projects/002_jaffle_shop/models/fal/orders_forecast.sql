
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED ad0f56029e8f6dc8ac7c39911c276624

Script dependencies:

{{ ref('orders_daily') }}

*/

SELECT * FROM {{ this }}
