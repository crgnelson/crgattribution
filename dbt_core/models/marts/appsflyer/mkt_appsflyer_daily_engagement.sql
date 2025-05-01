{{ config(
    materialized='table',
    partition_by={
      "field": "event_date",
      "data_type": "date"
    },
    cluster_by=["event_name", "app_group", "platform", "country_code"]
) }}

WITH engagement_events AS (

  SELECT *
  FROM {{ ref('int_appsflyer_engagement_events') }}

)

SELECT
  event_date,
  event_name,
  app_name,
  app_group,
  app_id,
  platform,
  region,
  country_code,
  campaign,
  campaign_type,
  channel_category,
  
  COUNT(*) AS total_events,
  COUNT(DISTINCT user_id) AS unique_users

FROM engagement_events
GROUP BY
  event_date,
  event_name,
  app_name,
  app_group,
  app_id,
  platform,
  region,
  country_code,
  campaign,
  campaign_type,
  channel_category
