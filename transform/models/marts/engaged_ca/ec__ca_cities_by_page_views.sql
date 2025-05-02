with source_data as (
    select * from {{ ref('ga__total_users_and_page_views') }}
    where event_date > '2025-02-10'
    and page_location ilike '%engaged.ca.gov%'

),

cities_by_page_views as (
    select
        geo_city,
        total_page_views
    from source_data
    where
        geo_region = 'California'
        and length(trim(geo_city)) > 0
        and geo_city != '(not set)'

)

select * from cities_by_page_views
