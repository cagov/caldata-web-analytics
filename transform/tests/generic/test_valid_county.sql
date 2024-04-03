{% test valid_county(model, column_name) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} NOT IN (SELECT name FROM {{ ref('stg_tiger_counties') }})

{% endtest %}
