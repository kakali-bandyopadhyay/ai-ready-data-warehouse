-- Raw company versions in the initial load are all eligible; compare to the true raw maximum.
with raw_latest as (
    select company_id, max(cast(ingested_at as timestamp)) as max_ingested_at
    from {{ source('raw', 'companies') }}
    group by company_id
), staged as (
    select company_id, cast(ingested_at as timestamp) as staged_ingested_at
    from {{ ref('stg_companies') }}
)
select r.company_id, r.max_ingested_at, s.staged_ingested_at
from raw_latest r
left join staged s on r.company_id = s.company_id
where s.company_id is null or s.staged_ingested_at <> r.max_ingested_at
