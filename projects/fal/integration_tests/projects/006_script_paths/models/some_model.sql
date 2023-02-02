{{ config(materialized='table') }}

with data as (

    SELECT
        cast(1 AS integer) as my_int,
        'some text' as my_text,
        cast(0.1 AS numeric) as my_float
)

select *
from data
