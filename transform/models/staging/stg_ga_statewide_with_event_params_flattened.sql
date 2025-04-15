with ga_statewide as (
    select * from {{ ref('stg_ga_statewide') }}
 ),

ga_statewide_event_params_flattened as (
    select 
        *, 
        ep.value:key as param_key, 
        ep.value:value as param_value 
    from ga_statewide, lateral FLATTEN(input => EVENT_PARAMS) ep
)
select * from ga_statewide_event_params_flattened
