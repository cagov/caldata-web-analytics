with source as (
    select * from {{ ref('stg_ga_engaged_ca') }}
),

events_and_counts as (
    select
        event_date,
        event_name,
        page_location,
        session_traffic_source_last_click_manual_campaign_source as referral_source,
        count(event_name) as total_event_count
    from source
    where page_location not like 'http://localhost/%' and page_location not like '%jbum%'
    group by event_date, event_name, page_location, referral_source
)

select * from events_and_counts
