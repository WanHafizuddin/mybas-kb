{% test value_between(model, column_name, min_value, max_value) %}
-- Fails for every row whose value is outside [min_value, max_value].
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})
{% endtest %}
