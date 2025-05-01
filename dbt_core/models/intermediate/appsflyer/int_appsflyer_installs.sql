WITH source AS (

  SELECT *
  FROM {{ ref('stg_appsflyer__event_data') }}
  WHERE event_name = 'install'

)

SELECT
  DATE(event_time) AS event_date,
  app_name,
  app_id,
  platform,
  region,
  country_code,
  campaign,
  campaign_type,
  appsflyer_id,
  customer_user_id AS user_id,
  install_time,

  -- 1. New Channel Categories
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

  -- 2. New App Group Column
  CASE
    WHEN app_id IN ('com.ft.news', 'id1200842933') THEN 'Main App'
    WHEN app_id = 'id1574510369' THEN 'FT Edit'
    WHEN app_id IN ('com.ft.ftepaper.android', 'id6449040684') THEN 'Digital Edition'
    ELSE 'Other'
  END AS app_group

FROM source
