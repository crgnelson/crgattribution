WITH source AS (

  SELECT *
  FROM {{ ref('stg_appsflyer__event_data') }}
  WHERE event_name IN (
    'af_content_view',
    'af_first_open',
    'af_share',
    'af_login',
    'select_package'
    'af_barrier_view'
  )

)

SELECT
  DATE(event_time) AS event_date,
  event_name,
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
  install_time

FROM source
