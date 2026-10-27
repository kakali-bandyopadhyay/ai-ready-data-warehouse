{{ config(materialized='table') }}
with date_values as (
  select as_of_date as full_date from {{ ref('stg_holdings') }}
  union select transaction_date from {{ ref('stg_transactions') }}
  union select valuation_date from {{ ref('stg_valuations') }}
), distinct_dates as (
  select distinct cast(full_date as date) as full_date from date_values
)
select
  {% if target.type == 'snowflake' %}
    to_number(to_char(full_date, 'YYYYMMDD'))
  {% else %}
    cast(date_format(full_date, 'yyyyMMdd') as int)
  {% endif %} as date_key,
  full_date, year(full_date) as year_number, quarter(full_date) as quarter_number,
  month(full_date) as month_number,
  {% if target.type == 'snowflake' %}
    to_char(full_date, 'Mon')
  {% else %}
    date_format(full_date, 'MMM')
  {% endif %} as month_name,
  dayofmonth(full_date) as day_of_month,
  {% if target.type == 'snowflake' %}dayofweekiso(full_date){% else %}dayofweek(full_date){% endif %} as day_of_week,
  {% if target.type == 'snowflake' %}
    dayofweekiso(full_date) in (6, 7)
  {% else %}
    dayofweek(full_date) in (1, 7)
  {% endif %} as is_weekend
from distinct_dates
