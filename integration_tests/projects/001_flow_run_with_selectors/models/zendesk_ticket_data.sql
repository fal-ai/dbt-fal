{{ config(materialized='table') }}

with source_data as (

    select id,_fivetran_synced,allow_channelback,assignee_id,brand_id from {{ ref('raw_zendesk_ticket_data') }}
)

select *
from source_data
