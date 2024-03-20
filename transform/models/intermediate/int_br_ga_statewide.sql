SELECT
    ga.event_date,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', ga.event_timestamp)
        AS event_timestamp_pst,
    ga.event_name,
    -- Flatten event_parameters, an unordered, variable size array of json objects
    MAX(CASE WHEN ep.value:key = 'page_location' THEN ep.value:value:string_value END)
        AS page_location,
    MAX(CASE WHEN ep.value:key = 'page_title' THEN ep.value:value:string_value END)
        AS page_title,
    MAX(CASE WHEN ep.value:key = 'page_referrer' THEN ep.value:value:string_value END)
        AS page_referrer,
    MAX(CASE WHEN ep.value:key = 'link_url' THEN ep.value:value:string_value END)
        AS link_url,
    MAX(CASE WHEN ep.value:key = 'link_domain' THEN ep.value:value:string_value END)
        AS link_domain,
    CASE
        -- Classifications below taken from Looker Studio report
        WHEN page_location IS NULL THEN NULL
        WHEN page_location LIKE '%csd.ca.gov%' THEN 'CSD'
        WHEN page_location LIKE '%broadband%' THEN 'ACP'
        WHEN page_location LIKE '%myfamily%' THEN 'WIC'
        WHEN page_location LIKE '%caleitc%' THEN 'CalEITC'
        WHEN page_location LIKE '%calfile%' THEN 'CalFile'
        ELSE 'Other'
    END AS page_location_alias,
    --ga.EVENT_PREVIOUS_TIMESTAMP,
    --ga.EVENT_VALUE_IN_USD,
    --ga.EVENT_BUNDLE_SEQUENCE_ID,
    --ga.EVENT_SERVER_TIMESTAMP_OFFSET,
    --ga.USER_ID,
    ga.user_pseudo_id,
    --ga.PRIVACY_INFO_ANALYTICS_STORAGE,
    --ga.PRIVACY_INFO_ADS_STORAGE,
    --ga.PRIVACY_INFO_USES_TRANSIENT_TOKEN,
    --ga.USER_PROPERTIES,
    --ga.USER_FIRST_TOUCH_TIMESTAMP,
    --ga.USER_LTV_REVENUE,
    --ga.USER_LTV_CURRENCY,
    ga.device_category,
    --ga.DEVICE_MOBILE_BRAND_NAME,
    --ga.DEVICE_MOBILE_MODEL_NAME,
    --ga.DEVICE_MOBILE_MARKETING_NAME,
    --ga.DEVICE_MOBILE_OS_HARDWARE_MODEL,
    --ga.DEVICE_OPERATING_SYSTEM,
    --ga.DEVICE_OPERATING_SYSTEM_VERSION,
    --ga.DEVICE_VENDOR_ID,
    --ga.DEVICE_ADVERTISING_ID,
    ga.device_language,
    --ga.DEVICE_IS_LIMITED_AD_TRACKING,
    --ga.DEVICE_TIME_ZONE_OFFSET_SECONDS,
    --ga.DEVICE_BROWSER,
    --ga.DEVICE_BROWSER_VERSION,
    --ga.DEVICE_WEB_INFO_BROWSER,
    --ga.DEVICE_WEB_INFO_BROWSER_VERSION,
    ga.device_web_info_hostname,
    --ga.geo_continent,
    ga.geo_country,
    ga.geo_region,
    ga.geo_city,
    --ga.geo_sub_continent,
    ga.geo_metro,
    --ga.APP_INFO_ID,
    --ga.APP_INFO_VERSION,
    --ga.APP_INFO_INSTALL_STORE,
    --ga.APP_INFO_FIREBASE_APP_ID,
    --ga.APP_INFO_INSTALL_SOURCE,
    ga.traffic_source_name,
    --ga.TRAFFIC_SOURCE_MEDIUM,
    ga.traffic_source_source,
    --ga.STREAM_ID,
    --ga.PLATFORM,
    --ga.EVENT_DIMENSIONS_HOSTNAME,
    --ga.ECOMMERCE_TOTAL_ITEM_QUANTITY,
    --ga.ECOMMERCE_PURCHASE_REVENUE_IN_USD,
    --ga.ECOMMERCE_PURCHASE_REVENUE,
    --ga.ECOMMERCE_REFUND_VALUE_IN_USD,
    --ga.ECOMMERCE_REFUND_VALUE,
    --ga.ECOMMERCE_SHIPPING_VALUE_IN_USD,
    --ga.ECOMMERCE_SHIPPING_VALUE,
    --ga.ECOMMERCE_TAX_VALUE_IN_USD,
    --ga.ECOMMERCE_TAX_VALUE,
    --ga.ECOMMERCE_UNIQUE_ITEMS,
    --ga.ECOMMERCE_TRANSACTION_ID,
    --ga.ITEMS,
    --ga.COLLECTED_TRAFFIC_SOURCE_MANUAL_CAMPAIGN_ID,
    ga.collected_traffic_source_manual_campaign_name,
    ga.collected_traffic_source_manual_source,
    ga.collected_traffic_source_manual_medium,
    ga.collected_traffic_source_manual_term,
    ga.collected_traffic_source_manual_content
    --ga.COLLECTED_TRAFFIC_SOURCE_GCLID,
    --ga.COLLECTED_TRAFFIC_SOURCE_DCLID,
    --ga.COLLECTED_TRAFFIC_SOURCE_SRSLTID
FROM {{ ref('stg_ga_statewide') }} AS ga,
    LATERAL FLATTEN(input => event_params) AS ep
WHERE ga.collected_traffic_source_manual_campaign_name = 'odibr'
GROUP BY ALL
