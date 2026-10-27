{% if target.type == 'databricks' %}
  {{ config(materialized='table', file_format='delta', databricks_tags={
    'owner': 'data_platform', 'classification': 'synthetic_public', 'ai_readiness': 'true'
  } if env_var('DATABRICKS_ENABLE_TAGS', 'false') | lower == 'true' else {}) }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}
select c.company_id, c.company_name, c.sector, c.country_code, c.parent_group_id,
       c.employee_count, c.annual_revenue, k.revenue_growth_pct, k.ebitda_margin_pct,
       k.as_of_date as kpi_as_of_date, c.classification, c.owner,
       c.ingested_at as source_ingested_at,
       'company_history + stg_company_kpis' as lineage
from {{ ref('company_history') }} c
left join {{ ref('stg_company_kpis') }} k on c.company_id = k.company_id
where c.dbt_valid_to is null
