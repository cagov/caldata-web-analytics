WITH widget_data AS (
    SELECT * FROM {{ ref('int_br_widget') }}
    WHERE event_timestamp_pst BETWEEN '2023-11-16' AND '2024-02-08'
),

ga_acp_data AS (
    SELECT * FROM {{ ref('int_br_ga_statewide') }}
    WHERE
        event_date BETWEEN '2023-11-16' AND '2024-02-08'
        AND page_location LIKE '%apply-for-acp%'
        AND (
            page_location LIKE '%odibr%'
            OR page_referrer LIKE '%odibr%'
            OR collected_traffic_source_manual_campaign_name = 'odibr'
        )
),

br_views AS (
    SELECT COUNT(*) AS total
    FROM widget_data
    WHERE
        experiment_variation LIKE '%acp%'
        AND event = 'render'
),

br_clicks AS (
    SELECT COUNT(*) AS total
    FROM widget_data
    WHERE link LIKE '%broadband%'
),

web_views AS (
    SELECT COUNT(*) AS total
    FROM ga_acp_data
    WHERE event_name = 'page_view'
),

apply_acp_clicks AS (
    SELECT COUNT(*) AS total
    FROM ga_acp_data
    WHERE link_url LIKE '%getinternet.gov/apply%'
)

SELECT
    'BR Views' AS source,
    'BR Clicks' AS destination,
    total
FROM br_clicks
UNION
SELECT
    'BR Views' AS source,
    'No Action' AS destination,
    total - (SELECT total FROM br_clicks) AS total
FROM br_views
UNION
SELECT
    'BR Clicks' AS source,
    'Web Views' AS destination,
    total
FROM web_views
UNION
SELECT
    'BR Clicks' AS source,
    'No Records' AS destination,
    total - (SELECT total FROM web_views) AS total
FROM br_clicks
UNION
SELECT
    'Web Views' AS source,
    'Apply for ACP' AS destination,
    total
FROM apply_acp_clicks
UNION
SELECT
    'Web Views' AS source,
    'No Application' AS destination,
    total - (SELECT total FROM apply_acp_clicks) AS total
FROM web_views
