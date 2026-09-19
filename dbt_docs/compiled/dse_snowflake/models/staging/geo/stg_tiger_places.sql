WITH places AS (SELECT * FROM RAW_PRD.tiger_2022.PLACES)

SELECT
    placens AS gnis_code,
    name,
    "geometry" AS geometry
FROM places