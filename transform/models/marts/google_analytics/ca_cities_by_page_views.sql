with source_data as (
    select
        event_date,
        geo_city,
        geo_region,
        event_name,
        page_location
    from {{ ref('stg_ga_statewide') }}
),

cities_by_page_views as (
    select
        event_date,
        geo_city,
        page_location,
        count_if(event_name = 'page_view') as total_page_views
    from source_data
    where
        geo_region = 'California'
        and length(trim(geo_city)) > 0
        and geo_city != '(not set)'
    group by event_date, geo_city, page_location
)

select * from cities_by_page_views
