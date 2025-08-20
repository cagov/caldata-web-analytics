with source_data as (
    select * from {{ ref('ga__total_users_and_page_views') }}
    where
        event_date >= '2025-08-01' -- soft launch date
        and page_location rlike $$^https://engaged.ca.gov(/[\w-]+)?/stateemployees/.*$$
),

totals as (
    select
        max(event_date) as max_event_date,
        sum(total_page_views) as total_page_views,
        sum(total_users) as total_users
    from source_data

)

select * from totals
