with typed as (
  select cast(company_id as string) as company_id, cast(company_name as string) as company_name,
         cast(sector as string) as sector, cast(country_code as string) as country_code,
         nullif(cast(parent_group_id as string), '') as parent_group_id,
         cast(employee_count as integer) as employee_count,
         cast(annual_revenue as decimal(18,2)) as annual_revenue,
         cast(classification as string) as classification, cast(owner as string) as owner,
         cast(ingested_at as timestamp) as ingested_at,
         cast(loaded_at as timestamp) as loaded_at
  from {{ source('raw', 'companies') }}
  where cast(ingested_at as timestamp_ntz) <= {{ current_utc_timestamp() }}
), ranked as (
  select *, row_number() over (
    partition by company_id order by ingested_at desc, loaded_at desc
  ) as row_num
  from typed
)
select company_id, company_name, sector, country_code, parent_group_id,
       employee_count, annual_revenue, classification, owner, ingested_at
from ranked where row_num = 1
