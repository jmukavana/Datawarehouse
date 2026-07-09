{{ config(materialized='view') }}

-- Convenience view: analysts who don't need history get one row per customer.
select *
from {{ ref('dim_customer') }}
where is_current
