{{ config(materialized='table', contract={'enforced': true}) }}
select dbt_scd_id::text as company_sk, company_id::text as company_id,
       company_name::text as company_name, sector::text as sector,
       country_code::text as country_code, parent_group_id::text as parent_group_id,
       employee_count::integer as employee_count, annual_revenue::numeric(18,2) as annual_revenue,
       dbt_valid_from::timestamptz as valid_from, dbt_valid_to::timestamptz as valid_to,
       (dbt_valid_to is null)::boolean as is_current
from {{ ref('company_history') }}
