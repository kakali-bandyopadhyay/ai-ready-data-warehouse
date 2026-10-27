with typed as (
  select company_id::text as company_id, company_name::text as company_name,
         sector::text as sector, country_code::text as country_code,
         nullif(parent_group_id::text, '') as parent_group_id,
         employee_count::integer as employee_count,
         annual_revenue::numeric(18,2) as annual_revenue,
         classification::text as classification, owner::text as owner,
         ingested_at::timestamptz as ingested_at, loaded_at::timestamptz as loaded_at
  from {{ source('raw', 'companies') }}
  where ingested_at::timestamptz <= {{ current_utc_timestamp() }}
), ranked as (
  select *, row_number() over (
    partition by company_id order by ingested_at desc, loaded_at desc
  ) as row_num
  from typed
)
select company_id, company_name, sector, country_code, parent_group_id,
       employee_count, annual_revenue, classification, owner, ingested_at
from ranked where row_num = 1
