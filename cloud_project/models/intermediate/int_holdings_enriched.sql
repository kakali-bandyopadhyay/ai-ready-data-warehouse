select h.*, f.base_currency, c.sector, c.parent_group_id
from {{ ref('stg_holdings') }} h
join {{ ref('stg_funds') }} f on h.fund_id = f.fund_id
join {{ ref('stg_companies') }} c on h.company_id = c.company_id
