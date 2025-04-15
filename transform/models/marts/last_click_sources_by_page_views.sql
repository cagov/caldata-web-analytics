with source_data as (
    select
        ga.session_traffic_source_last_click_manual_campaign_source,
        ga.event_name,
        ga.page_location
    from {{ ref('stg_ga_statewide_with_event_params_flattened') }} as ga
),

last_click_sources_by_page_views as (
    select
        page_location,
        session_traffic_source_last_click_manual_campaign_source as last_click_sources,
        COUNT(case when event_name = 'page_view' then 1 end) as page_views
    from source_data
    group by page_location, last_click_sources
)

select * from last_click_sources_by_page_views
order by 2 desc
