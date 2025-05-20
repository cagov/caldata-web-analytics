WITH places AS (SELECT * FROM {{ source('tiger_2022', 'PLACES') }})

SELECT
    placens AS gnis_code,
    name,
    "geometry" AS geometry
FROM places
