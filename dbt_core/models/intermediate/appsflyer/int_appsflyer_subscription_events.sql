WITH source AS (

  SELECT *
  FROM {{ ref('stg_appsflyer__event_data') }}
  WHERE event_name IN (
    'af_ars_subscription_xgraded',
    'af_ars_subscription_renewed',
    'af_ars_subscription_canceled',
    'af_purchase_canceled',
    'af_purchase_refund'
  )

)

SELECT
  DATE(event_time) AS event_date,
  event_name,
  app_name,
  CASE
    WHEN app_id IN ('com.ft.news', 'id1200842933') THEN 'Main App'
    WHEN app_id = 'id1574510369' THEN 'FT Edit'
    WHEN app_id IN ('com.ft.ftepaper.android', 'id6449040684') THEN 'Digital Edition'
    ELSE 'Other'
  END AS app_group,
  app_id,
  platform,
  region,
  country_code,
  campaign,
  campaign_type,

  CASE
    WHEN media_source = 'QR_code' THEN 'QR_code'
    WHEN media_source = 'Email' THEN 'Email_onboarding'
    WHEN media_source IN ('Web-to-app', 'web_to_app') THEN 'Onsite'
    WHEN campaign_type = 'organic' AND platform = 'ios' THEN 'Apple_store_organic'
    WHEN campaign_type = 'organic' AND platform = 'android' THEN 'Google_play_organic'
    WHEN media_source = 'googleadwords_int' THEN 'ACi/Ace_paid'
    WHEN media_source = 'Apple Search Ads' THEN 'ASA_paid'
    ELSE 'Other'
  END AS channel_category,

  appsflyer_id,
  customer_user_id AS user_id,
  install_time,

  -- Flatten the nested event_value JSON
  JSON_EXTRACT_SCALAR(event_value, '$.af_content_type') AS af_content_type,
  JSON_EXTRACT_SCALAR(event_value, '$.af_order_id') AS af_order_id,
  SAFE_CAST(JSON_EXTRACT_SCALAR(event_value, '$.af_revenue') AS FLOAT64) AS revenue,
  JSON_EXTRACT_SCALAR(event_value, '$.payment_method_type') AS payment_method_type,
  JSON_EXTRACT_SCALAR(event_value, '$.af_currency') AS currency

FROM source
