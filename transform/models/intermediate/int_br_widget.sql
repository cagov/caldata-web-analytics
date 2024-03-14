SELECT
    convert_timezone('UTC', 'America/Los_Angeles', to_timestamp(timestamp/1000)) AS event_timestamp_pst,
    display_url,
    event,
    page_url,
    CASE
        WHEN page_url LIKE '%uio%edd.ca.gov%Pages/ExternalUser/Certification/FormCCA4581RegularDUACertificationQuestionsConfirmation.aspx%' THEN 'UI'
        WHEN page_url LIKE '%sdio%edd.ca.gov%Pages/ExternalUser/InitialClaim/Confirmation.aspx%' THEN 'SDI'
        WHEN page_url LIKE '%sdio%edd.ca.gov%Pages/ExternalUser/PFLForm/ConfirmationForForm.aspx%' THEN 'PFL'
        WHEN page_url IS NULL THEN NULL
        ELSE 'NA'
    END AS page_url_alias,
    user_agent,
    language,
    link,
    ca.alias AS link_alias,
    link_text,
    experiment_name,
    experiment_variation
FROM {{ source('calinnovate_dynamodb', 'BENEFITS_RECOMMENDATION_API_PRODUCTION_EVENTS_TABLE_PIGRTV_2_DYWTM') }} widget
LEFT JOIN {{ ref('click_aliases') }} ca ON widget.link = ca.url

-- Filter out pilot program data
WHERE TO_DATE(event_timestamp_pst) >= '2023-11-16'
