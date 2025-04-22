with source_data as (
    select
        event_date,
        user_pseudo_id,
        event_name,
        page_location,
        geo_city
    from {{ ref('stg_ga_statewide') }}
),


key_metrics as (
    select
        event_date,
        geo_city,
        page_location,
        count_if(event_name = 'page_view') as total_page_views,
        count(distinct user_pseudo_id) as total_users
    from source_data
    group by event_date, geo_city, page_location
)

select * from key_metrics
