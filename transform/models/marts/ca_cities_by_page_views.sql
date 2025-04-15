with source_data as (
    select
        ga.geo_city,
        ga.geo_region,
        ga.event_name,
        ga.page_location
    from {{ ref('stg_ga_statewide_with_event_params_flattened') }} as ga
),

cities_by_page_views as (
    select
        geo_city,
        page_location,
        count(case when event_name = 'page_view' then 1 end) as page_views
    from source_data
    where
        geo_region = 'California'
        and length(trim(geo_city)) > 0
        and geo_city != '(not set)'
    group by geo_city, page_location
)

select * from cities_by_page_views
