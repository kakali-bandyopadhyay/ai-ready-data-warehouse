{% macro current_utc_timestamp() %}
{% if target.type == 'postgres' %}
((current_timestamp at time zone 'UTC') at time zone 'UTC')
{% elif target.type == 'snowflake' %}
sysdate()
{% elif target.type == 'databricks' %}
convert_timezone(current_timezone(), 'UTC', cast(current_timestamp() as timestamp_ntz))
{% else %}
current_timestamp
{% endif %}
{% endmacro %}
