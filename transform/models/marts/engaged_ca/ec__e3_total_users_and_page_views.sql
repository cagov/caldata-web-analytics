with source_data as (
    select * from {{ ref('ga__total_users_and_page_views') }}
    where
        event_date >= '2025-08-01' -- soft launch date
        and startswith(page_location, 'https://engaged.ca.gov/stateemployees/')
        or startswith(page_location, 'https://engaged.ca.gov/es/stateemployees/')
        or startswith(page_location, 'https://engaged.ca.gov/ko/stateemployees/')
        or startswith(page_location, 'https://engaged.ca.gov/tl/stateemployees/')
        or startswith(page_location, 'https://engaged.ca.gov/vi/stateemployees/')
        or startswith(page_location, 'https://engaged.ca.gov/zh-hans/stateemployees/')
        or startswith(page_location, 'https://engaged.ca.gov/zh-hant/stateemployees/')
        or startswith(page_location, 'https://engaged.ca.gov/fa/stateemployees/')
        or startswith(page_location, 'https://engaged.ca.gov/hy/stateemployees/')
),

totals as (
    select
        max(event_date) as max_event_date,
        sum(total_page_views) as total_page_views,
        sum(total_users) as total_users
    from source_data

)

select * from totals
