with source_data as (
    select * from {{ ref('ga__total_users_and_page_views') }}
    where
        event_date >= '2025-02-21' -- soft launch date
        and STARTSWITH(page_location, 'https://engaged.ca.gov')
),

totals as (
    select
        SUM(total_page_views) as total_page_views,
        SUM(total_users) as total_users
    from source_data

),

la_totals as (
    select SUM(total_users) as total_users_la
    from source_data
    where geo_city = 'Los Angeles'

)

select * from totals, la_totals -- noqa: RF02
