select h.holding_id, h.fund_id, h.company_id, h.as_of_date, h.units, h.price,
       h.market_value, h.ingested_at, f.base_currency, c.sector, c.parent_group_id
from {{ ref('stg_holdings') }} h
join {{ ref('stg_funds') }} f using (fund_id)
join {{ ref('stg_companies') }} c using (company_id)
