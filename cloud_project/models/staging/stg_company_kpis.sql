{{ config(materialized='table') }}
with ranked as (
  select cast(kpi_id as string) as kpi_id, cast(company_id as string) as company_id,
         cast(as_of_date as date) as as_of_date, cast(revenue_growth_pct as decimal(8,4)) as revenue_growth_pct,
         cast(ebitda_margin_pct as decimal(8,4)) as ebitda_margin_pct,
         cast(ingested_at as timestamp) as ingested_at,
         row_number() over (
           partition by company_id
           order by cast(as_of_date as date) desc, cast(ingested_at as timestamp) desc, cast(kpi_id as string) desc
         ) as row_num
  from {{ source('raw', 'company_kpis') }}
)
select kpi_id, company_id, as_of_date, revenue_growth_pct, ebitda_margin_pct, ingested_at
from ranked
where row_num = 1
