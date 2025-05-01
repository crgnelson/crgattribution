WITH source AS (

  SELECT *
  FROM {{ ref('stg_appsflyer__event_data') }}
  WHERE event_name = 'af_purchase'

)

SELECT
  DATE(event_time) AS event_date,
  app_name,
  CASE
    WHEN app_id IN ('com.ft.news', 'id1200842933') THEN 'Main App'
    WHEN app_id = 'id1574510369' THEN 'FT Edit'
    WHEN app_id IN ('com.ft.ftepaper.android', 'id6449040684') THEN 'FT Digital Edition'
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

  -- 🔥 Flattened fields from event_value JSON
  SAFE_CAST(JSON_EXTRACT_SCALAR(event_value, '$.AFEventParamRevenue') AS FLOAT64) AS revenue,
  JSON_EXTRACT_SCALAR(event_value, '$.AFEventParamCurrency') AS currency,
  JSON_EXTRACT_SCALAR(event_value, '$.payment_method_type') AS payment_method_type,
  JSON_EXTRACT_SCALAR(event_value, '$.AFEventParamOrderId') AS order_id,
  JSON_EXTRACT_SCALAR(event_value, '$.AFEventParamContentType') AS product_id,

  -- 🔥 Derived purchase_type based on product_id patterns
  CASE 
    WHEN JSON_EXTRACT_SCALAR(event_value, '$.AFEventParamContentType') LIKE '%_M_T%' THEN 'monthly_trial'
    WHEN JSON_EXTRACT_SCALAR(event_value, '$.AFEventParamContentType') LIKE '%713f1e28_0bc5_8261_f1e6_eebab6f7600e_M%' THEN 'ios monthly premium'
    WHEN JSON_EXTRACT_SCALAR(event_value, '$.AFEventParamContentType') LIKE '%c8ad55e6_ba74_fea0_f9da_a4546ae2ee23_M%' THEN 'ios monthly standard'
    WHEN JSON_EXTRACT_SCALAR(event_value, '$.AFEventParamContentType') LIKE '%713f1e28_0bc5_8261_f1e6_eebab6f7600e_Annual%' THEN 'ios annual premium'
    WHEN JSON_EXTRACT_SCALAR(event_value, '$.AFEventParamContentType') LIKE '%c8ad55e6_ba74_fea0_f9da_a4546ae2ee23_Annual%' THEN 'ios annual standard'
    WHEN JSON_EXTRACT_SCALAR(event_value, '$.AFEventParamContentType') LIKE '%p1m_premium%' THEN 'monthly premium'
    WHEN JSON_EXTRACT_SCALAR(event_value, '$.AFEventParamContentType') LIKE '%p1m_standard%' THEN 'monthly standard'
    WHEN JSON_EXTRACT_SCALAR(event_value, '$.AFEventParamContentType') LIKE '%p1y_premium%' THEN 'annual premium'
    WHEN JSON_EXTRACT_SCALAR(event_value, '$.AFEventParamContentType') LIKE '%p1y_standard%' THEN 'annual standard'
    ELSE 'not identified'
  END AS purchase_type

FROM source
