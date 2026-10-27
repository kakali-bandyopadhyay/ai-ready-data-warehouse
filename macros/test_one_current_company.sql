{% test one_current_company(model) %}
select company_id
from {{ model }}
group by company_id
having count(*) filter (where is_current) <> 1
{% endtest %}
