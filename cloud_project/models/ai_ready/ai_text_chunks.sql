{% if target.type == 'databricks' %}
  {{ config(materialized='table', file_format='delta', databricks_tags={
    'owner': 'data_platform', 'classification': 'synthetic_public', 'ai_readiness': 'true'
  } if env_var('DATABRICKS_ENABLE_TAGS', 'false') | lower == 'true' else {}) }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}
select n.note_id as chunk_id, n.company_id, n.note_text as chunk_text, n.source_name,
       n.as_of_date, n.owner, n.classification,
       {% if target.type == 'snowflake' %}
         cast(null as vector(float, 8))
       {% else %}
         cast(null as array<float>)
       {% endif %} as embedding
from {{ ref('stg_company_notes') }} n
