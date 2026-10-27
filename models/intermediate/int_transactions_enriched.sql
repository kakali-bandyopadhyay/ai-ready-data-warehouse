select t.transaction_id, t.fund_id, t.company_id, t.transaction_date,
       t.transaction_type, t.amount, t.currency, t.ingested_at, t.batch_id,
       c.sector, c.parent_group_id
from {{ ref('stg_transactions') }} t
join {{ ref('stg_companies') }} c using (company_id)
