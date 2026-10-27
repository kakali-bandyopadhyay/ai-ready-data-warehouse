with typed as (
  select transaction_id::text as transaction_id, fund_id::text as fund_id,
         company_id::text as company_id, transaction_date::date as transaction_date,
         transaction_type::text as transaction_type, amount::numeric(20,2) as amount,
         currency::text as currency, ingested_at::timestamptz as ingested_at,
         batch_id::text as batch_id,
         row_number() over (partition by transaction_id order by ingested_at desc) as row_num
  from {{ source('raw', 'transactions') }}
)
select t.transaction_id, t.fund_id, t.company_id, t.transaction_date,
       t.transaction_type, t.amount, t.currency, t.ingested_at, t.batch_id
from typed t
inner join {{ ref('stg_funds') }} f using (fund_id)
inner join {{ ref('stg_companies') }} c using (company_id)
where t.row_num = 1
