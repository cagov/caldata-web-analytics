{% set dt = (
    var('ga_statewide_beg_date')
    if target.name == 'prd'
    else (modules.datetime.datetime.now() - modules.datetime.timedelta(days=7)).isoformat()
) %}

{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='EVENT_DATE',
    begin=dt,
    batch_size='day',
    snowflake_warehouse = get_snowflake_refresh_warehouse(big="XL", small="XS")
) }}

with source_data as (
    select
        event_date,
        session_traffic_source_last_click_manual_campaign_source,
        event_name,
        page_location
    from {{ ref('stg_ga_statewide') }}
),

last_click_sources_by_page_views as (
    select
        event_date,
        page_location,
        session_traffic_source_last_click_manual_campaign_source as last_click_sources,
        count_if(event_name = 'page_view') as total_page_views
    from source_data
    group by event_date, page_location, last_click_sources
)

select * from last_click_sources_by_page_views
