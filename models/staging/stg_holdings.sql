with typed as (
  select holding_id::text as holding_id, fund_id::text as fund_id,
         company_id::text as company_id, as_of_date::date as as_of_date,
         units::numeric(20,4) as units, price::numeric(18,4) as price,
         market_value::numeric(20,2) as market_value, ingested_at::timestamptz as ingested_at,
         row_number() over (partition by holding_id order by ingested_at desc) as row_num
  from {{ source('raw', 'holdings') }}
)
select h.holding_id, h.fund_id, h.company_id, h.as_of_date, h.units, h.price,
       h.market_value, h.ingested_at
from typed h
inner join {{ ref('stg_funds') }} f using (fund_id)
inner join {{ ref('stg_companies') }} c using (company_id)
where h.row_num = 1
