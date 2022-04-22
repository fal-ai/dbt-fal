{{ config(materialized='table') }}

with source_data as (

    select * from {{ ref('time_series') }}
)

select *
from source_data
