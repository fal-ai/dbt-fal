{{ config(materialized='table') }}

with source_data as (

    select id,_fivetran_synced,allow_channelback,assignee_id,brand_id
    {% if env_var('extra_col', False) %}
        , 'yes' as extra_col
    {% endif %}
    from {{ ref('raw_zendesk_ticket_data') }}
)

select *
from source_data
