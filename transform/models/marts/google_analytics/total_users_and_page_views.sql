with source_data as (
    select
        event_date,
        user_pseudo_id,
        event_name,
        page_location,
        geo_region,
        geo_city
    from {{ ref('stg_ga_statewide') }}
),


key_metrics as (
    select
        event_date,
        any_value(page_location) as page_location,
        any_value(geo_region) as geo_region,
        any_value(geo_city) as geo_city,
        count_if(event_name = 'page_view') as total_page_views,
        count(distinct user_pseudo_id) as total_users
    from source_data
    group by event_date
)

select * from key_metrics
