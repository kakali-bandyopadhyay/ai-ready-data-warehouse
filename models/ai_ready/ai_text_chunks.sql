{{ config(materialized='table', post_hook=[
  "create index if not exists {{ this.identifier }}_embedding_hnsw on {{ this }} using hnsw (embedding vector_cosine_ops) where embedding is not null"
]) }}
select n.note_id::text as chunk_id, n.company_id::text as company_id,
       n.note_text::text as chunk_text, n.source_name::text as source_name,
       n.as_of_date::date as as_of_date, n.owner::text as owner,
       n.classification::text as classification,
       null::vector(8) as embedding
from {{ ref('stg_company_notes') }} n
