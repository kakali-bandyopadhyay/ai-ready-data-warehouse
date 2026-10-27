select cast(note_id as string) as note_id, cast(company_id as string) as company_id,
       cast(note_text as string) as note_text, cast(source_name as string) as source_name,
       cast(as_of_date as date) as as_of_date, cast(owner as string) as owner,
       cast(classification as string) as classification, cast(ingested_at as timestamp) as ingested_at
from {{ source('raw', 'company_notes') }}
where company_id in (select company_id from {{ ref('stg_companies') }})
