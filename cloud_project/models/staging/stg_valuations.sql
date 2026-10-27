select cast(valuation_id as string) as valuation_id, cast(company_id as string) as company_id,
       cast(valuation_date as date) as valuation_date,
       cast(enterprise_value as decimal(20,2)) as enterprise_value,
       cast(ingested_at as timestamp) as ingested_at
from {{ source('raw', 'valuations') }}
where company_id in (select company_id from {{ ref('stg_companies') }})
