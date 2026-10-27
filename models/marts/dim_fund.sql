{{ config(materialized='table', contract={'enforced': true}) }}
select fund_id::text as fund_id, fund_name::text as fund_name,
       base_currency::text as base_currency, vintage_year::integer as vintage_year,
       fund_nav::numeric(18,2) as fund_nav
from {{ ref('stg_funds') }}
