{% test no_future_valuations(model) %}
select valuation_id from {{ model }} where valuation_date > current_date
{% endtest %}
