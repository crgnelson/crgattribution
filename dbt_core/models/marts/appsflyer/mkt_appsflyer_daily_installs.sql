{{ config(
    materialized='table',
    partition_by={
      "field": "event_date",
      "data_type": "date"
    },
    cluster_by=["app_group", "platform", "channel_category"]
) }}

WITH installs AS (

  SELECT *
  FROM {{ ref('int_appsflyer_installs') }}

)

SELECT
  event_date,
  app_name,
  app_group,
  platform,
  app_id,
  region,
  country_code,
  campaign,
  campaign_type,
  channel_category,

  COUNT(*) AS installs,               -- Total install events
  COUNT(DISTINCT user_id) AS unique_user_installs  -- Unique installs per user_id

FROM installs
GROUP BY
  event_date,
  app_name,
  app_group,
  platform,
  app_id,
  region,
  country_code,
  campaign,
  campaign_type,
  channel_category
