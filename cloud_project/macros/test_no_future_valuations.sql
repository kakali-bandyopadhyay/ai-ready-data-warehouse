{% test no_future_valuations(model) %}
{% if target.type == 'postgres' %}
{% set utc_date %}({{ current_utc_timestamp() }} at time zone 'UTC')::date{% endset %}
{% else %}
{% set utc_date %}to_date({{ current_utc_timestamp() }}){% endset %}
{% endif %}
select valuation_id from {{ model }} where valuation_date > {{ utc_date }}
{% endtest %}
