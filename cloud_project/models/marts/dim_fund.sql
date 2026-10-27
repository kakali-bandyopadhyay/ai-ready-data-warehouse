{{ config(materialized='table') }}
select fund_id, fund_name, base_currency, vintage_year, fund_nav
from {{ ref('stg_funds') }}
