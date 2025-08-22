with source as (
    select * from {{ ref('int_ga_engaged_ca__events') }}
),

form_submits as (
    select
        case
            when page_location rlike $$^https://engaged.ca.gov(/[\w-]+)?/stateemployees/.*$$ then '/stateemployees'
            when page_location rlike $$^https://engaged.ca.gov(/[\w-]+)?/|^https://engaged.ca.gov(/[\w-]+)?/.*fbclid=.*$$ then '/'
            when page_location rlike $$^https://engaged.ca.gov(/[\w-]+)?/lafires-recovery/.*$$ then 'lafires-recovery'
            else page_location
        end
            as page_location_of_event,
        referral_source,
        total_event_count
    from source
    where
        event_date >= '2025-02-21' -- engaged.ca.gov soft launch date
        and event_name = 'form_submit'
    group by all

)

select * from form_submits
