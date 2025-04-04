with source_data as (
    select
        ga.user_pseudo_id,
        ga.event_name,
        ga.device_web_info_hostname,
        ga.geo_city
    from {{ ref('stg_ga_statewide') }} as ga
),

key_metrics_la as (
    select
        COUNT(case when event_name = 'page_view' then 1 end) as total_page_views,
        COUNT(distinct user_pseudo_id) as total_users
    from source_data
    where
        device_web_info_hostname = 'engaged.ca.gov'
        and geo_city = 'Los Angeles'
),

key_metrics as (
    select
        COUNT(case when event_name = 'page_view' then 1 end) as total_page_views,
        COUNT(distinct user_pseudo_id) as total_users
    from source_data
    where device_web_info_hostname = 'engaged.ca.gov'
)

select * from key_metrics
