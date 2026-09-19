
    
    

select
    gnis_code as unique_field,
    count(*) as n_records

from ANALYTICS_PRD.geo.stg_tiger_counties
where gnis_code is not null
group by gnis_code
having count(*) > 1


