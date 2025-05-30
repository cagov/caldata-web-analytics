with source_data as (
    select * from {{ ref('ga__total_users_and_page_views') }}
    where
        event_date >= '2025-02-21' -- soft launch date
        and STARTSWITH(page_location, 'https://engaged.ca.gov')

),

cities_by_page_views as (
    select
        event_date,
        geo_city,
        SUM(total_page_views) as total_page_views
    from source_data
    where
        geo_region = 'California'
        and LENGTH(TRIM(geo_city)) > 0
        and geo_city != '(not set)'
    group by event_date, geo_city

)

select * from cities_by_page_views
