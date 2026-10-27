{% macro set_session_timezone_utc() %}
{% if target.type == 'snowflake' %}
alter session set timezone = 'UTC'
{% else %}
select 1
{% endif %}
{% endmacro %}
