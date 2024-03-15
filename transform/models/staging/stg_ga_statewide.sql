SELECT
    TO_DATE(RAW:"event_date"::string, 'YYYYMMDD') AS EVENT_DATE,
    TO_TIMESTAMP(RAW:"event_timestamp"::int, 6) AS EVENT_TIMESTAMP,
    RAW:"event_name"::string AS EVENT_NAME,
    RAW:"event_params" AS EVENT_PARAMS,
    --to_timestamp(RAW:"event_previous_timestamp"::int, 6) AS event_previous_timestamp,
    --RAW:"event_value_in_usd"::float AS event_value_in_usd,
    --RAW:"event_bundle_sequence_id"::int AS event_bundle_sequence_id,
    --RAW:"event_server_timestamp_offset"::int AS event_server_timestamp_offset,
    --RAW:"user_id"::string AS user_id,
    RAW:"user_pseudo_id"::string AS USER_PSEUDO_ID,
    --RAW:"privacy_info"."analytics_storage"::string AS privacy_info_analytics_storage,
    --RAW:"privacy_info"."ads_storage"::string AS privacy_info_ads_storage,
    --RAW:"privacy_info"."uses_transient_token"::string
    --    AS privacy_info_uses_transient_token,
    --RAW:"user_properties" AS user_properties,
    --to_timestamp(RAW:"user_first_touch_timestamp"::int, 6)
    --    AS user_first_touch_timestamp,
    --RAW:"user_ltv"."revenue"::float AS user_ltv_revenue,
    --RAW:"user_ltv"."currency"::string AS user_ltv_currency,
    RAW:"device"."category"::string AS DEVICE_CATEGORY,
    --RAW:"device"."mobile_brand_name"::string AS device_mobile_brand_name,
    --RAW:"device"."mobile_model_name"::string AS device_mobile_model_name,
    --RAW:"device"."mobile_marketing_name"::string AS device_mobile_marketing_name,
    --RAW:"device"."mobile_os_hardware_model"::string
    --    AS device_mobile_os_hardware_model,
    --RAW:"device"."operating_system"::string AS device_operating_system,
    --RAW:"device"."operating_system_version"::string
    --    AS device_operating_system_version,
    --RAW:"device"."vendor_id"::string AS device_vendor_id,
    --RAW:"device"."advertising_id"::string AS device_advertising_id,
    RAW:"device"."language"::string AS DEVICE_LANGUAGE,
    --RAW:"device"."is_limited_ad_tracking"::boolean AS device_is_limited_ad_tracking,
    --RAW:"device"."time_zone_offset_seconds"::int AS device_time_zone_offset_seconds,
    --RAW:"device"."browser"::string AS device_browser,
    --RAW:"device"."browser_version"::string AS device_browser_version,
    --RAW:"device"."web_info"."browser"::string AS device_web_info_browser,
    --RAW:"device"."web_info"."browser_version"::string
    --    AS device_web_info_browser_version,
    RAW:"device"."web_info"."hostname"::string AS DEVICE_WEB_INFO_HOSTNAME,
    RAW:"geo"."continent"::string AS GEO_CONTINENT,
    RAW:"geo"."country"::string AS GEO_COUNTRY,
    RAW:"geo"."region"::string AS GEO_REGION,
    RAW:"geo"."city"::string AS GEO_CITY,
    RAW:"geo"."sub_continent"::string AS GEO_SUB_CONTINENT,
    RAW:"geo"."metro"::string AS GEO_METRO,
    --RAW:"app_info"."id"::string AS app_info_id,
    --RAW:"app_info"."version"::string AS app_info_version,
    --RAW:"app_info"."install_store"::string AS app_info_install_store,
    --RAW:"app_info"."firebase_app_id"::string AS app_info_firebase_app_id,
    --RAW:"app_info"."install_source"::string AS app_info_install_source,
    RAW:"traffic_source"."name"::string AS TRAFFIC_SOURCE_NAME,
    --RAW:"traffic_source"."medium"::string AS traffic_source_medium,
    RAW:"traffic_source"."source"::string AS TRAFFIC_SOURCE_SOURCE,
    --RAW:"stream_id"::string AS stream_id,
    --RAW:"platform"::string AS platform,
    --RAW:"event_dimensions"."hostname"::string AS event_dimensions_hostname,
    --RAW:"ecommerce"."total_item_quantity"::int AS ecommerce_total_item_quantity,
    --RAW:"ecommerce"."purchase_revenue_in_usd"::float
    --    AS ecommerce_purchase_revenue_in_usd,
    --RAW:"ecommerce"."purchase_revenue"::float AS ecommerce_purchase_revenue,
    --RAW:"ecommerce"."refund_value_in_usd"::float AS ecommerce_refund_value_in_usd,
    --RAW:"ecommerce"."refund_value"::float AS ecommerce_refund_value,
    --RAW:"ecommerce"."shipping_value_in_usd"::float AS ecommerce_shipping_value_in_usd,
    --RAW:"ecommerce"."shipping_value"::float AS ecommerce_shipping_value,
    --RAW:"ecommerce"."tax_value_in_usd"::float AS ecommerce_tax_value_in_usd,
    --RAW:"ecommerce"."tax_value"::float AS ecommerce_tax_value,
    --RAW:"ecommerce"."unique_items"::int AS ecommerce_unique_items,
    --RAW:"ecommerce"."transaction_id"::string AS ecommerce_transaction_id,
    --RAW:"items" AS items,
    --RAW:"collected_traffic_source"."manual_campaign_id"::string
    --    AS collected_traffic_source_manual_campaign_id,
    RAW:"collected_traffic_source"."manual_campaign_name"::string
        AS COLLECTED_TRAFFIC_SOURCE_MANUAL_CAMPAIGN_NAME,
    RAW:"collected_traffic_source"."manual_source"::string
        AS COLLECTED_TRAFFIC_SOURCE_MANUAL_SOURCE,
    RAW:"collected_traffic_source"."manual_medium"::string
        AS COLLECTED_TRAFFIC_SOURCE_MANUAL_MEDIUM,
    RAW:"collected_traffic_source"."manual_term"::string
        AS COLLECTED_TRAFFIC_SOURCE_MANUAL_TERM,
    RAW:"collected_traffic_source"."manual_content"::string
        AS COLLECTED_TRAFFIC_SOURCE_MANUAL_CONTENT
--RAW:"collected_traffic_source"."gclid"::string AS collected_traffic_source_gclid,
--RAW:"collected_traffic_source"."dclid"::string AS collected_traffic_source_dclid,
--RAW:"collected_traffic_source"."srsltid"::string AS collected_traffic_source_srsltid
FROM {{ source('ca_google_analytics', 'ANALYTICS_314711183') }}
WHERE INGESTION_COMPLETE = TRUE
-- Filter out benefits recommender pilot program data
AND EVENT_DATE >= '2023-11-16'
