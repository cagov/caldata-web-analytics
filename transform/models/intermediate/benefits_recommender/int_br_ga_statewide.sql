WITH ga_base_data AS (
    SELECT
        ga.event_date,
        CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', ga.event_timestamp)
            AS event_timestamp_pst,
        ga.event_name,
        -- Flatten event_params, an unordered, variable size array of json objects
        MAX(
            CASE
                WHEN ep.value:key = 'page_location' THEN ep.value:value:string_value
            END
        )
            AS page_location,
        MAX(CASE WHEN ep.value:key = 'page_title' THEN ep.value:value:string_value END)
            AS page_title,
        MAX(
            CASE
                WHEN ep.value:key = 'page_referrer' THEN ep.value:value:string_value
            END
        )
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
        LATERAL FLATTEN(input => ga.event_params) AS ep

    -- Limit to data relevant to the BR project
    WHERE
        ga.collected_traffic_source_manual_campaign_name = 'odibr'
        OR ga.device_web_info_hostname LIKE '%broadbandforall.cdt.ca.gov'
        OR ga.device_web_info_hostname LIKE '%csd.ca.gov'
        OR ga.device_web_info_hostname LIKE '%ftb.ca.gov'
        OR ga.device_web_info_hostname LIKE '%myfamily.wic.ca.gov'
    GROUP BY ALL
),

/*
    Add unique id for downstream partitioning. This could be done in the above CTE but
    the "GROUP BY" would have to call out columns explicitly so that "id" isn't included
    in the "ALL".

    Rather than create a new column, it also appears possible to partition unique
    records using the following columns:
        - event_timestamp_pst
        - event_name
        - user_pseudo_id
        - event_params
    However, this comes with the drawback of needing to include event_params in the
    final query, which would increase the overall data size more than is desireable.
*/
ga_data AS (
    SELECT
        SEQ8() AS id,
        *
    FROM ga_base_data
),

/*
   Finally get county name using the city name from GA. Some city names have multiple
   occurrences in California (and other states!) and will appear as duplicates after the
   join. Some of these can be resolved with the geo_metro column but for others, we set
   the county value to NULL.
*/
ga_data_with_county AS (
    SELECT
        ga.*,
        (
            ga.geo_region = 'California'
            AND (NOT ccm.is_dupe_city OR ga.geo_metro LIKE '%' || ccm.county || '%')
        ) AS logic,
        IFF(logic, ccm.city_gnis_code, NULL) AS geo_city_gnis_code,
        IFF(logic, ccm.county, NULL) AS geo_county,
        IFF(logic, ccm.county_gnis_code, NULL) AS geo_county_gnis_code
    FROM ga_data AS ga
    LEFT JOIN {{ ref('int_city_to_county') }} AS ccm ON ga.geo_city = ccm.city
    QUALIFY (
        ROW_NUMBER() OVER (PARTITION BY ga.id ORDER BY geo_county DESC NULLS LAST)
    ) = 1
)

SELECT * EXCLUDE logic
FROM ga_data_with_county
