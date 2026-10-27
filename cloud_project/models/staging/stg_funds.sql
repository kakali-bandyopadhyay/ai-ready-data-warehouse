select cast(fund_id as string) as fund_id, cast(fund_name as string) as fund_name,
       cast(base_currency as string) as base_currency, cast(vintage_year as integer) as vintage_year,
       cast(fund_nav as decimal(18,2)) as fund_nav, cast(ingested_at as timestamp) as ingested_at
from {{ source('raw', 'funds') }}
