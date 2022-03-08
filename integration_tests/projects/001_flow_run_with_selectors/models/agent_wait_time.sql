{{ config(materialized='table') }}

with source_data as (

    select y, ds from {{ ref('time_series') }}
)

select *
from source_data
