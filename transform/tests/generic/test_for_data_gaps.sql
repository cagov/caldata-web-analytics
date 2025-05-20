{% test data_gaps(model, column_name) %}

with date_range as (
    {{ dbt_utils.date_spine(
    datepart="day",
    start_date= var('ga_data_start_date'),
    end_date="current_date - 2"
   )
}}
)

select 
to_date(date_day) as date_day
from date_range
where date_day not in (select {{ column_name }} from {{ model }})

{% endtest %}
