{{ config(materialized='view') }}

select *
from {{ ref('dim_agent') }}
where is_current
