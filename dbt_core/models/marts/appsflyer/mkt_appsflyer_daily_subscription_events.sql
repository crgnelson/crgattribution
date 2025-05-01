{{ config(
    materialized='table',
    partition_by={
      "field": "event_date",
      "data_type": "date"
    },
    cluster_by=["event_name", "app_group", "platform"]
) }}

WITH subscription_events AS (

  SELECT *
  FROM {{ ref('int_appsflyer_subscription_events') }}

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
  payment_method_type,
  
  COUNT(*) AS total_events,            -- Raw event count
  COUNT(DISTINCT user_id) AS unique_users  -- Unique users per event per day

FROM subscription_events
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
  channel_category,
  payment_method_type
