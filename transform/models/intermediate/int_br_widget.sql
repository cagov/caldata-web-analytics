SELECT
    convert_timezone('UTC', 'America/Los_Angeles', to_timestamp(w.timestamp / 1000))
        AS event_timestamp_pst,
    w.display_url,
    w.event,
    w.page_url,
    CASE
        WHEN
            w.page_url LIKE
            '%uio%edd.ca.gov%Pages/ExternalUser/Certification/'
            || 'FormCCA4581RegularDUACertificationQuestionsConfirmation.aspx%'
            THEN 'UI'
        WHEN
            w.page_url LIKE
            '%sdio%edd.ca.gov%Pages/ExternalUser/InitialClaim/Confirmation.aspx%'
            THEN 'SDI'
        WHEN
            w.page_url LIKE
            '%sdio%edd.ca.gov%Pages/ExternalUser/PFLForm/ConfirmationForForm.aspx%'
            THEN 'PFL'
        WHEN w.page_url IS NULL THEN NULL
        ELSE 'NA'
    END AS page_url_alias,
    w.user_agent,
    w.language,
    w.link,
    ca.alias AS link_alias,
    w.link_text,
    w.experiment_name,
    w.experiment_variation
FROM
    {{
        source('calinnovate_dynamodb',
               'BENEFITS_RECOMMENDATION_API_PRODUCTION_EVENTS_TABLE_PIGRTV_2_DYWTM')
    }} AS w
LEFT JOIN {{ ref('click_aliases') }} AS ca ON w.link = ca.url

-- Filter out pilot program data
WHERE to_date(event_timestamp_pst) >= '2023-11-16'
