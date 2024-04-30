SELECT *
FROM {{ ref('int_br_ga_statewide') }}
WHERE collected_traffic_source_manual_campaign_name = 'odibr'
