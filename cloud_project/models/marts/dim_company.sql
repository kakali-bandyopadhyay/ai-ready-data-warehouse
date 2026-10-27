{{ config(materialized='table') }}
select cast(dbt_scd_id as string) as company_sk, company_id, company_name, sector,
       country_code, parent_group_id, employee_count, annual_revenue,
       cast(dbt_valid_from as timestamp) as valid_from,
       cast(dbt_valid_to as timestamp) as valid_to,
       case when dbt_valid_to is null then true else false end as is_current
from {{ ref('company_history') }}
