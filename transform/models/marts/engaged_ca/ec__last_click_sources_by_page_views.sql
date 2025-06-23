with source_data as (
    select * from {{ ref('ga__last_click_sources_by_page_views') }}
    where
        event_date >= '2025-02-21' -- soft launch date
        and STARTSWITH(page_location, 'https://engaged.ca.gov')
),

last_click_sources_by_page_views as (
    select
        MAX(event_date) as max_event_date,
        last_click_sources as sources,
        SUM(total_page_views) as total_page_views
    from source_data
    group by sources

)

select * from last_click_sources_by_page_views
