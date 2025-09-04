

SELECT county
FROM ANALYTICS_PRD.analytics.city_county_extras
WHERE county NOT IN (SELECT name FROM ANALYTICS_PRD.geo.stg_tiger_counties)

