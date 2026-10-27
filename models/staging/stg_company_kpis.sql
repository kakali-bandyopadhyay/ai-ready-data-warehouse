with typed as (
  select kpi_id::text as kpi_id, company_id::text as company_id, as_of_date::date as as_of_date,
         revenue_growth_pct::numeric(8,4) as revenue_growth_pct,
         ebitda_margin_pct::numeric(8,4) as ebitda_margin_pct,
         ingested_at::timestamptz as ingested_at,
         row_number() over (partition by kpi_id order by ingested_at desc) as row_num
  from {{ source('raw', 'company_kpis') }}
)
select t.kpi_id, t.company_id, t.as_of_date, t.revenue_growth_pct,
       t.ebitda_margin_pct, t.ingested_at
from typed t inner join {{ ref('stg_companies') }} c using (company_id)
where t.row_num = 1
