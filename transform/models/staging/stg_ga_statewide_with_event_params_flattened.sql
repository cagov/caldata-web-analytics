with ga_statewide as (
    select * from {{ ref('stg_ga_statewide') }}
),

ga_statewide_event_params_flattened as (
    select
        ga.*,
        ep.value:key as param_key,
        ep.value:value as param_value
    from ga_statewide as ga, lateral flatten(input => ga.event_params) as ep -- noqa: AL01
),

create_page_location as (
    select 
        g.*,
        case when param_key = 'page_location' then param_value:string_value::text else NULL end as page_location
    from ga_statewide_event_params_flattened g
)

select * from create_page_location
