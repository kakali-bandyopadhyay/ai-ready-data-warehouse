{% if target.type == 'snowflake' %}
  {{ config(materialized='incremental', incremental_strategy='merge', unique_key='holding_id',
            cluster_by=['date_key', 'company_sk']) }}
{% else %}
  {{ config(materialized='incremental', incremental_strategy='merge', unique_key='holding_id',
            file_format='delta', liquid_clustered_by=['date_key', 'company_sk'],
            databricks_tags={
              'owner': 'data_platform',
              'classification': 'synthetic_public',
              'ai_readiness': 'false'
            } if env_var('DATABRICKS_ENABLE_TAGS', 'false') | lower == 'true' else {}) }}
{% endif %}
select h.holding_id, h.fund_id, c.company_sk,
       {% if target.type == 'snowflake' %}
         to_number(to_char(h.as_of_date, 'YYYYMMDD'))
       {% else %}
         cast(date_format(h.as_of_date, 'yyyyMMdd') as int)
       {% endif %} as date_key,
       h.units, h.price, h.market_value, h.ingested_at
from {{ ref('int_holdings_enriched') }} h
join {{ ref('dim_company') }} c on h.company_id = c.company_id and c.is_current
