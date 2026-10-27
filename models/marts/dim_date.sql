{{ config(materialized='table', contract={'enforced': true}) }}
with bounds as (
  select min(d)::date as min_date, max(d)::date as max_date from (
    select as_of_date as d from {{ ref('stg_holdings') }}
    union all select transaction_date from {{ ref('stg_transactions') }}
    union all select valuation_date from {{ ref('stg_valuations') }}
  ) dates
)
select to_char(day::date, 'YYYYMMDD')::integer as date_key,
       day::date as full_date, extract(year from day)::smallint as year_number,
       extract(quarter from day)::smallint as quarter_number,
       extract(month from day)::smallint as month_number,
       to_char(day, 'Mon')::text as month_name,
       extract(day from day)::smallint as day_of_month,
       extract(isodow from day)::smallint as iso_day_of_week,
       (extract(isodow from day) in (6, 7))::boolean as is_weekend
from bounds cross join generate_series(min_date, max_date, interval '1 day') as days(day)
