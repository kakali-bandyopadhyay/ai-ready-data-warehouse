{% snapshot company_history %}
{{
  config(
    target_schema='snapshots', unique_key='company_id', strategy='timestamp',
    updated_at='ingested_at', invalidate_hard_deletes=True
  )
}}
select company_id, company_name, sector, country_code, parent_group_id,
       employee_count, annual_revenue, classification, owner,
       ingested_at::timestamp without time zone as ingested_at
from {{ ref('stg_companies') }}
{% endsnapshot %}
