WITH counties AS (SELECT * FROM {{ source('tiger_2022', 'COUNTIES') }})

SELECT
    countyns AS gnis_code,
    name,
    "geometry" AS geometry
FROM counties
