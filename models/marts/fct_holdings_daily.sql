{{ config(materialized='incremental', incremental_strategy='merge', unique_key='holding_id',
          on_schema_change='fail', contract={'enforced': true}) }}
select h.holding_id::text as holding_id, h.fund_id::text as fund_id,
       c.company_sk::text as company_sk,
       to_char(h.as_of_date, 'YYYYMMDD')::integer as date_key,
       h.units::numeric(20,4) as units, h.price::numeric(18,4) as price,
       h.market_value::numeric(20,2) as market_value, h.ingested_at::timestamptz as ingested_at
from {{ ref('int_holdings_enriched') }} h
join {{ ref('dim_company') }} c on h.company_id = c.company_id and c.is_current
{% if is_incremental() %}
where h.as_of_date >= coalesce((select max(as_of_date) from {{ ref('int_holdings_enriched') }}), h.as_of_date) - interval '30 days'
{% endif %}
