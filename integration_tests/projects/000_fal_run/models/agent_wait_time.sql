{{ config(materialized='table', alias='wait_time') }}

with source_data as (

    select * from {{ ref('time_series') }}
)

select *
from source_data
