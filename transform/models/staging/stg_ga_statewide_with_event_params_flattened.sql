with ga_statewide as (
    select * from {{ ref('stg_ga_statewide') }}
),

ga_statewide_event_params_flattened as (
    select
        ga.*,
        ep.value:key as param_key,
        ep.value:value as param_value
    from ga_statewide as ga, lateral flatten(input => ga.event_params) as ep -- noqa: AL01
)

select * from ga_statewide_event_params_flattened
