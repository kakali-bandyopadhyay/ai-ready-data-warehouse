{% test holdings_nav_reconcile(model, tolerance=0.25) %}
with totals as (
  select h.fund_id, sum(h.market_value) as holdings_value
  from {{ model }} h
  join (select fund_id, max(date_key) as latest_date from {{ model }} group by fund_id) latest
    on h.fund_id = latest.fund_id and h.date_key = latest.latest_date
  group by h.fund_id
)
select t.fund_id
from totals t join {{ ref('dim_fund') }} f using (fund_id)
where abs(t.holdings_value - f.fund_nav) > f.fund_nav * {{ tolerance }}
{% endtest %}
