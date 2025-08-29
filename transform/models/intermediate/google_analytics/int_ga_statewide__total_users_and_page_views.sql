{% set dt = var('ga_statewide_beg_date') if target.name == 'prd'
else (modules.datetime.datetime.now() - modules.datetime.timedelta(days=7)).isoformat() %}

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
        user_pseudo_id,
        event_name,
        page_location,
        geo_region,
        geo_city
    from {{ ref('stg_ga_statewide') }}
),

key_metrics as (
    select
        event_date,
        page_location,
        geo_region,
        geo_city,
        count_if(event_name = 'page_view') as total_page_views,
        count(distinct user_pseudo_id) as total_users
    from source_data
    group by event_date, page_location, geo_region, geo_city
)

select * from key_metrics
