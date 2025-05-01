{{ config(
    materialized='table',
    partition_by={
      "field": "event_date",
      "data_type": "date"
    },
    cluster_by=["app_group", "channel_category", "platform"]
) }}

WITH purchases AS (

  SELECT *
  FROM {{ ref('int_appsflyer_purchases') }}

)

SELECT
  event_date,
  app_name,
  app_group,
  app_id,
  platform,
  region,
  country_code,
  campaign,
  campaign_type,
  channel_category,
  purchase_type,
  currency,
  payment_method_type,

  COUNT(*) AS total_purchases,              -- Number of purchase events
  COUNT(DISTINCT user_id) AS unique_purchasers,  -- Unique users who purchased
  SUM(revenue) AS total_revenue              -- Total revenue in FLOAT64
FROM purchases
GROUP BY
  event_date,
  app_name,
  app_group,
  app_id,
  platform,
  region,
  country_code,
  campaign,
  campaign_type,
  channel_category,
  purchase_type,
  currency,
  payment_method_type
