{% test one_current_company(model) %}
select company_id
from {{ model }}
group by company_id
having sum(case when is_current then 1 else 0 end) <> 1
{% endtest %}
