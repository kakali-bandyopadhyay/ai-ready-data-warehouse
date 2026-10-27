with typed as (
  select note_id::text as note_id, company_id::text as company_id, note_text::text as note_text,
         source_name::text as source_name, as_of_date::date as as_of_date,
         owner::text as owner, classification::text as classification,
         ingested_at::timestamptz as ingested_at,
         row_number() over (partition by note_id order by ingested_at desc) as row_num
  from {{ source('raw', 'company_notes') }}
)
select t.note_id, t.company_id, t.note_text, t.source_name, t.as_of_date,
       t.owner, t.classification, t.ingested_at
from typed t inner join {{ ref('stg_companies') }} c using (company_id)
where t.row_num = 1
