with source_data as (
    select * from ANALYTICS_PRD.google_analytics.int_ga_statewide__total_users_and_page_views
    where
        event_date >= '2025-02-21' -- soft launch date
        and STARTSWITH(page_location, 'https://engaged.ca.gov')
),

totals as (
    select
        MAX(event_date) as max_event_date,
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