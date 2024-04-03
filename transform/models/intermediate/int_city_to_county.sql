WITH base_mapping AS (
    SELECT
        p.name AS city,
        p.gnis_code AS city_gnis_code,
        c.name AS county,
        c.gnis_code AS county_gnis_code
    FROM {{ ref('stg_tiger_places') }} AS p
    LEFT JOIN {{ ref('stg_tiger_counties') }} AS c
        ON ST_INTERSECTS(p.geometry, c.geometry)

    -- Filter intersecting counties for the one with the most intersection
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY p.gnis_code
        ORDER BY ST_AREA(ST_INTERSECTION(p.geometry, c.geometry)) DESC NULLS LAST
    ) = 1
),

extras AS (
    SELECT
        e.city,
        NULL AS city_gnis_code,
        e.county,
        c.gnis_code AS county_gnis_code
    FROM {{ ref('city_county_extras') }} AS e
    LEFT JOIN {{ ref('stg_tiger_counties') }} AS c
        ON e.county = c.name
),

combined AS (
    SELECT * FROM base_mapping
    UNION ALL
    SELECT * FROM extras
)

SELECT
    city,
    city_gnis_code,
    county,
    county_gnis_code,
    IFF(
        city IN
        (
            SELECT name FROM {{ ref('stg_tiger_places') }}
            GROUP BY name HAVING COUNT(*) > 1
        ),
        1, 0
    ) AS is_dupe_city
FROM combined
