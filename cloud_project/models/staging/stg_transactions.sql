with ranked as (
  select cast(transaction_id as string) as transaction_id, cast(fund_id as string) as fund_id,
         cast(company_id as string) as company_id, cast(transaction_date as date) as transaction_date,
         cast(transaction_type as string) as transaction_type, cast(amount as decimal(20,2)) as amount,
         cast(currency as string) as currency, cast(ingested_at as timestamp) as ingested_at,
         cast(batch_id as string) as batch_id, cast(loaded_at as timestamp) as loaded_at,
         row_number() over (
           partition by transaction_id order by cast(ingested_at as timestamp) desc,
             cast(loaded_at as timestamp) desc
         ) as row_num
  from {{ source('raw', 'transactions') }}
)
select t.transaction_id, t.fund_id, t.company_id, t.transaction_date, t.transaction_type,
       t.amount, t.currency, t.ingested_at, t.batch_id
from ranked t
join {{ ref('stg_funds') }} f on t.fund_id = f.fund_id
join {{ ref('stg_companies') }} c on t.company_id = c.company_id
where t.row_num = 1
