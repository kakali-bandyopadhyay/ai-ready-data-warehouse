{% if target.type == 'databricks' %}
  {{ config(materialized='incremental', incremental_strategy='merge', unique_key='transaction_id', file_format='delta') }}
{% else %}
  {{ config(materialized='incremental', incremental_strategy='merge', unique_key='transaction_id') }}
{% endif %}
select t.transaction_id, t.fund_id, c.company_sk,
       {% if target.type == 'snowflake' %}
         to_number(to_char(t.transaction_date, 'YYYYMMDD'))
       {% else %}
         cast(date_format(t.transaction_date, 'yyyyMMdd') as int)
       {% endif %} as date_key,
       t.transaction_type, t.amount, t.currency, t.ingested_at, t.batch_id
from {{ ref('int_transactions_enriched') }} t
join {{ ref('dim_company') }} c on t.company_id = c.company_id and c.is_current
