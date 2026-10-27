{{ config(materialized='incremental', incremental_strategy='merge', unique_key='transaction_id',
          on_schema_change='fail', contract={'enforced': true}) }}
select t.transaction_id::text as transaction_id, t.fund_id::text as fund_id,
       c.company_sk::text as company_sk,
       to_char(t.transaction_date, 'YYYYMMDD')::integer as date_key,
       t.transaction_type::text as transaction_type,
       t.amount::numeric(20,2) as amount, t.currency::text as currency,
       t.ingested_at::timestamptz as ingested_at, t.batch_id::text as batch_id
from {{ ref('int_transactions_enriched') }} t
join {{ ref('dim_company') }} c on t.company_id = c.company_id and c.is_current
{% if is_incremental() %}
where t.ingested_at >= coalesce((select max(ingested_at) from {{ this }}), '1900-01-01'::timestamptz) - interval '90 days'
{% endif %}
