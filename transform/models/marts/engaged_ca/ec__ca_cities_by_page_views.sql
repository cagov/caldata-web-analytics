with source_data as (
    select * from {{ ref('ga__total_users_and_page_views') }}
    where
        event_date >= '2025-02-21' -- soft launch date
        and STARTSWITH(page_location, 'https://engaged.ca.gov')

),

cities_by_page_views as (
    select
        MAX(event_date) as max_event_date,
        geo_city,
        SUM(total_page_views) as total_page_views
    from source_data
    where
        geo_region = 'California'
        and LENGTH(TRIM(geo_city)) > 0
        and geo_city != '(not set)'
    group by geo_city

),

final as (
    select
        max_event_date,
        geo_city,
        total_page_views,
        total_page_views / (select SUM(total_page_views) from cities_by_page_views) as percent_of_total_page_views
    from cities_by_page_views
)

select * from final
