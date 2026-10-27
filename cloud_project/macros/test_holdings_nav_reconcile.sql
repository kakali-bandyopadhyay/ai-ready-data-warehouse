{% test holdings_nav_reconcile(model, tolerance=0.25) %}
with latest as (
  select fund_id, max(date_key) as date_key from {{ model }} group by fund_id
), totals as (
  select h.fund_id, sum(h.market_value) as holdings_value
  from {{ model }} h join latest l on h.fund_id = l.fund_id and h.date_key = l.date_key
  group by h.fund_id
)
select t.fund_id from totals t join {{ ref('dim_fund') }} f on t.fund_id = f.fund_id
where abs(t.holdings_value - f.fund_nav) > f.fund_nav * {{ tolerance }}
{% endtest %}
