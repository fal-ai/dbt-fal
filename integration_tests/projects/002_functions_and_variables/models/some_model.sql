{{ config(materialized='table') }}

with data as (

    SELECT 1::INTEGER as my_int, 'some text'::text as my_text, 0.1::NUMERIC as my_float
)

select *
from data
