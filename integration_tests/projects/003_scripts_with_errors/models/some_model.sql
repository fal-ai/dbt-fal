{{ config(materialized='table') }}

select 1/0 as my_int
