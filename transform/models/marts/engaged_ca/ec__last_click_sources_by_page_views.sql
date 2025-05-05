with source_data as (
    select * from {{ ref('ga__last_click_sources_by_page_views') }}
    where
        event_date > '2025-02-21' -- soft launch date
        and page_location ilike '%engaged.ca.gov%'
),

last_click_sources_by_page_views as (
    select
        last_click_sources as sources,
        total_page_views
    from source_data

)

select * from last_click_sources_by_page_views
