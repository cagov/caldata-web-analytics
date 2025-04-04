with source_data as (
    select
        ga.geo_city,
        ga.geo_region,
        ga.event_name,
        ga.device_web_info_hostname
    from {{ ref('stg_ga_statewide') }} as ga
),

cities_by_page_views as (
    select
        geo_city,
        COUNT(case when event_name = 'page_view' then 1 end) as page_views
    from source_data
    where
        device_web_info_hostname = 'engaged.ca.gov'
        and geo_region = 'California'
        and LENGTH(TRIM(geo_city)) > 0
        and geo_city != '(not set)'
    group by 1
)

select * from cities_by_page_views
order by 2 desc
