select t.*, c.sector, c.parent_group_id
from {{ ref('stg_transactions') }} t
join {{ ref('stg_companies') }} c on t.company_id = c.company_id
