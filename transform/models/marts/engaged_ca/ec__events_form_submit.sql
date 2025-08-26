with source as (
    select * from {{ ref('int_ga_engaged_ca__events') }}
),

form_submits as (
    select
        case
            when page_location like 'https://engaged.ca.gov%'
                then
                    case
                        -- handle root and short urls
                        when len(page_location) <= 23 then '/'
                        -- handle known programs
                        when contains(page_location, '/stateemployees/') then '/stateemployees'
                        when contains(page_location, '/lafires-recovery/') then 'lafires-recovery'
                        -- handle facebook click ids
                        when contains(page_location, 'fbclid=') then '/'
                        else page_location
                    end
            else page_location
        end as page_location_of_event,
        referral_source,
        total_event_count
    from source
    where
        event_date >= '2025-02-21' -- engaged.ca.gov soft launch date
        and event_name = 'form_submit'
    group by all

)

select * from form_submits
