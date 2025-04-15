with source_data as (
    select
        ga.user_pseudo_id,
        ga.event_name,
        ga.page_location,
        ga.geo_city
    from {{ ref('stg_ga_statewide_with_event_params_flattened') }} as ga
),


key_metrics as (
    select
        geo_city,
        page_location,
        count_if(event_name = 'page_view') as total_page_views,
        count(distinct user_pseudo_id) as total_users
    from source_data
    group by geo_city, page_location
)

select * from key_metrics
