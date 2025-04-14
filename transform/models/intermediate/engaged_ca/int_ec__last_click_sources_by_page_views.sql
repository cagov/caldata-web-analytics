with source_data as (
    select
        ga.session_traffic_source_last_click_manual_campaign_source,
        ga.event_name,
        ga.device_web_info_hostname
    from {{ ref('stg_ga_statewide') }} as ga
),

last_click_sources_by_page_views as (
    select
        session_traffic_source_last_click_manual_campaign_source as last_click_sources,
        COUNT(case when event_name = 'page_view' then 1 end) as page_views
    from source_data
    where device_web_info_hostname = 'engaged.ca.gov'
    group by 1
)

select * from last_click_sources_by_page_views
order by 2 desc
