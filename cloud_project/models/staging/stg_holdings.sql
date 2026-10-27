with ranked as (
  select cast(holding_id as string) as holding_id, cast(fund_id as string) as fund_id,
         cast(company_id as string) as company_id, cast(as_of_date as date) as as_of_date,
         cast(units as decimal(20,4)) as units, cast(price as decimal(18,4)) as price,
         cast(market_value as decimal(20,2)) as market_value, cast(ingested_at as timestamp) as ingested_at,
         row_number() over (partition by holding_id order by cast(ingested_at as timestamp) desc) as row_num
  from {{ source('raw', 'holdings') }}
)
select h.holding_id, h.fund_id, h.company_id, h.as_of_date, h.units, h.price, h.market_value, h.ingested_at
from ranked h
join {{ ref('stg_funds') }} f on h.fund_id = f.fund_id
join {{ ref('stg_companies') }} c on h.company_id = c.company_id
where h.row_num = 1
