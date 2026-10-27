with typed as (
  select valuation_id::text as valuation_id, company_id::text as company_id,
         valuation_date::date as valuation_date, enterprise_value::numeric(20,2) as enterprise_value,
         ingested_at::timestamptz as ingested_at,
         row_number() over (partition by valuation_id order by ingested_at desc) as row_num
  from {{ source('raw', 'valuations') }}
)
select t.valuation_id, t.company_id, t.valuation_date, t.enterprise_value, t.ingested_at
from typed t inner join {{ ref('stg_companies') }} c using (company_id)
where t.row_num = 1
