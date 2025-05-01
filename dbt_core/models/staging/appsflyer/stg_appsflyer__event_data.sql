WITH source AS (

  SELECT *
  FROM {{ source('appsflyer', 'installs') }}
  WHERE _PARTITIONDATE >= '2023-01-01'  -- ✅ ensure BigQuery allows the query

),

renamed AS (

  SELECT
    event_name,
    event_value,
    media_source,
    platform,
    event_time,
    install_time,
    campaign,
    campaign_type,
    region,
    country_code,
    appsflyer_id,
    customer_user_id,
    app_id,
    app_name
  FROM source

)

SELECT * FROM renamed
