{{ config(
    materialized='table', 
    description="Staging model that classifies media_type, channel, and platform from visit-level data"
) }}

WITH temp AS (
    SELECT *
  FROM (
      SELECT
          *,
          COUNT(*) OVER (PARTITION BY visit_traffic_source_type) AS source_frequency
      FROM {{ source('bi_layer_integration', 'conv_visit_attribution') }}
      WHERE
          b2b_b2c = "B2C"
  ) AS filtered_data
  WHERE source_frequency >= 10 --NEW RULE from 50 to 10
  #AND (visit_traffic_source_type IS NOT NULL AND visit_traffic_source_name IS NOT NULL) --NEW RULE
  #AND (visit_traffic_source_type <> "" AND visit_traffic_source_name <> "") --NEW RULE
  ), 

/*WITH
   TEMP AS (
   SELECT
     DISTINCT visit_traffic_source_type,
     visit_traffic_source_name,
     segment_name,
     segment_campaign_name,
     segment_campaign_grouping,
     segment_channel,
     segment_audience,
     is_campaign_landing,
     b2c_abstraction_datasource
   FROM
     `ft-bi-team.BI_Layer_Integration.conv_visit_attribution`
   WHERE
     b2b_b2c = "B2C" 
  AND (visit_traffic_source_type IS NOT NULL AND visit_traffic_source_name IS NOT NULL) --NEW RULE
  AND (visit_traffic_source_type <> "" AND visit_traffic_source_name <> "")), --NEW RULE */

processed AS (
  SELECT
    *,
    -- Classify media type
    CASE
    --organic rules using traffic source only
      WHEN LOWER(visit_traffic_source_name) LIKE 'organic%' OR LOWER(visit_traffic_source_name) LIKE '%organic%' OR LOWER(visit_traffic_source_name) LIKE "%organic%" THEN 'organic'
      WHEN LOWER(visit_traffic_source_type) LIKE "%social_free%" THEN "organic"
    --organic rules rules using segment info
      WHEN is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name
    AND (LOWER(segment_name) LIKE 'organic%'
      OR LOWER(segment_campaign_name) LIKE '%organic%'
      OR LOWER(segment_campaign_grouping) LIKE "%organic%") THEN 'organic'
    --organic rules rules when segment_name != visit_traffic_source_name
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND (LOWER(visit_traffic_source_type) LIKE '%search%' OR LOWER(visit_traffic_source_type) LIKE "%social free%" OR LOWER(visit_traffic_source_type) LIKE "%social media%") THEN 'organic'
    --paid media rules using visit traffic source only 
      WHEN LOWER(visit_traffic_source_name) LIKE '%paid media%' OR LOWER(visit_traffic_source_name) = 'sem' OR LOWER(visit_traffic_source_type) = 'sem' OR LOWER(visit_traffic_source_name) LIKE '%paid campaigns%' OR LOWER(visit_traffic_source_name) LIKE '%- sem -%' OR LOWER(visit_traffic_source_name) LIKE '%- sem-%' OR LOWER(visit_traffic_source_name) LIKE '%paid social%'
      OR LOWER(visit_traffic_source_name) LIKE '%paid search%'
      OR (LOWER(visit_traffic_source_name) LIKE '%paid%' AND LOWER(visit_traffic_source_name) NOT LIKE "paid.outbrain.com") --NEW RULE
      OR LOWER(visit_traffic_source_name) LIKE '%pmax%'
      OR visit_traffic_source_name = "Core Subs_Other_Newsletter_UK_H2 Sale 2024_9-2024_Mill Media Sheffield" -- NEW RULE
       THEN 'paid media'
      --WHEN LOWER(visit_traffic_source_type) LIKE '%unclassified campaign%' THEN "unclassified"
      WHEN REGEXP_CONTAINS(LOWER(visit_traffic_source_type), r'(pmax|performance max|pmax_[^ ]*|[^ ]*_pmax)') THEN "paid media"
      WHEN LOWER(visit_traffic_source_type) LIKE '%capi%' THEN "capi"
      WHEN LOWER(visit_traffic_source_type) LIKE "%barrier%" THEN LOWER(visit_traffic_source_type)
      WHEN LOWER(visit_traffic_source_type) LIKE "%external video%" THEN LOWER(visit_traffic_source_type)
      WHEN LOWER(visit_traffic_source_type) LIKE "%ft_print%" THEN LOWER(visit_traffic_source_type)
      --WHEN LOWER(visit_traffic_source_type) LIKE "%unclassified referrer%" THEN "unclassified"
    --paid media rules using segment info
      WHEN is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name
    AND (LOWER(segment_name) LIKE 'paid media%'OR LOWER(segment_channel) = 'sem'
      OR LOWER(visit_traffic_source_type) = 'sem'
      OR LOWER(segment_campaign_name) LIKE "%paid campaigns%"
      OR LOWER(segment_name) LIKE "%- sem -%"
      OR LOWER(segment_name) LIKE "%- sem-%"
      OR LOWER(segment_name) LIKE "%paid social%"
      OR LOWER(segment_campaign_name) LIKE "%paid media%"
    OR LOWER(segment_name) LIKE "%paid media%") THEN 'paid media'
    --unclassified referrer rules when segment_name != visit_traffic_source_name
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE 'unclassified referrer%' THEN visit_traffic_source_type
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE 'push notification' THEN 'ft app'
    --other rule using visit traffic source only
      --WHEN visit_traffic_source_name = "" THEN 'other'
      WHEN visit_traffic_source_type = "Facebook" AND visit_traffic_source_name = "Social" THEN "unclassified social" --NEW RULE
    --other rules using using segment info
      --WHEN is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name AND segment_name = "" THEN 'other'
      ELSE LOWER(visit_traffic_source_type)
  END
    AS media_type,
    -- Extract channel  --  --  --  --  --  --  --  --  --  --  --
    CASE
    --rules using traffic source only
      WHEN (LOWER(visit_traffic_source_name) LIKE "%pmax%"
      OR LOWER(visit_traffic_source_name) LIKE "%performance max%") THEN "pmax"
      WHEN LOWER(visit_traffic_source_name) = 'sem' OR LOWER(visit_traffic_source_name) LIKE "%- sem -%" OR LOWER(visit_traffic_source_name) LIKE "%- sem-%" OR LOWER(visit_traffic_source_name) LIKE '%paid search%' OR visit_traffic_source_name LIKE 'Paid Media_H2 2023 Sale_Search_%' OR visit_traffic_source_name LIKE "Paid Media_BAU_Search_%" OR LOWER(visit_traffic_source_type) = 'sem' THEN 'ppc'
      WHEN LOWER(visit_traffic_source_name) LIKE "%search%" AND LOWER(visit_traffic_source_type) LIKE "%external display%" THEN "ppc"
      WHEN LOWER(visit_traffic_source_type) LIKE '%capi%' THEN LOWER(visit_traffic_source_type) 
      WHEN LOWER(visit_traffic_source_name) = 'paid media - social'
    OR LOWER(visit_traffic_source_name) LIKE "%paid social%"
    OR visit_traffic_source_name LIKE 'Paid Media_H2 2023 Sale_LinkedIn_%'
    OR visit_traffic_source_name LIKE 'Paid Media_H2 2023 Sale_Facebook_%'
    OR visit_traffic_source_name LIKE 'Paid Media_H2 2023 Sale_Twitter_%'
    OR (LOWER(visit_traffic_source_name) LIKE "%paid media%"
      AND LOWER(visit_traffic_source_name) LIKE '%fb%') THEN 'paid social'
      WHEN visit_traffic_source_name LIKE 'Paid Media_H2 2023 Sale_Landing Page_%' AND visit_traffic_source_name LIKE "% PMAX" THEN 'pmax'
      WHEN visit_traffic_source_name LIKE 'Paid Media_H2 2023 Sale_Display_%' THEN 'display'
      WHEN (LOWER(visit_traffic_source_name) LIKE 'paid media_%' OR LOWER(visit_traffic_source_name) LIKE 'organic_%' OR  LOWER(visit_traffic_source_name) LIKE 'print_%') AND (LENGTH(visit_traffic_source_name) - LENGTH(REPLACE(visit_traffic_source_name, '_', ''))) >= 5 THEN LOWER(SPLIT(visit_traffic_source_name, '_')[SAFE_OFFSET(1)])
      WHEN LOWER(visit_traffic_source_type) LIKE "%email%" THEN 'email'
      WHEN LOWER(visit_traffic_source_type) LIKE "%barrier%" THEN LOWER(visit_traffic_source_type)
      WHEN LOWER(visit_traffic_source_type) LIKE "%external video%" THEN LOWER(visit_traffic_source_type)
      WHEN LOWER(visit_traffic_source_type) LIKE "%ft_print%" THEN LOWER(visit_traffic_source_type)
      WHEN LOWER(visit_traffic_source_type) LIKE "%social_free%" THEN LOWER(visit_traffic_source_type)
      WHEN LOWER(visit_traffic_source_type)="ft product" THEN LOWER(visit_traffic_source_type) -- New rule
    --rules using segment info
      WHEN is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name AND (LOWER(segment_channel) = 'sem' OR LOWER(segment_name) LIKE "%- sem -%" OR LOWER(segment_name) LIKE "%- sem-%" OR LOWER(segment_name) LIKE '%paid search%' OR segment_name LIKE 'Paid Media_H2 2023 Sale_Search_%' OR segment_name LIKE "Paid Media_BAU_Search_%" OR segment_campaign_name LIKE "Paid Media - SEM" ) THEN 'ppc'
      WHEN is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name
    AND (LOWER(segment_campaign_name) = 'paid media - social'
      OR LOWER(segment_name) LIKE "%paid social%"
      OR segment_name LIKE 'Paid Media_H2 2023 Sale_LinkedIn_%'
      OR segment_name LIKE 'Paid Media_H2 2023 Sale_Facebook_%'OR segment_name LIKE 'Paid Media_H2 2023 Sale_Twitter_%') THEN 'paid social'
      WHEN is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name AND (segment_name LIKE 'Paid Media_H2 2023 Sale_Landing Page_%' AND segment_name LIKE "% PMAX") THEN 'pmax'
      WHEN is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name
    AND segment_name LIKE 'Paid Media_H2 2023 Sale_Display_%' THEN 'display'
      WHEN is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name AND (LOWER(segment_name) LIKE 'paid media_%' OR LOWER(segment_name) LIKE 'organic_%' OR LOWER(segment_name) LIKE 'print_%') AND (LENGTH(segment_name) - LENGTH(REPLACE(segment_name, '_', ''))) >= 5 THEN LOWER(SPLIT(segment_name, '_')[SAFE_OFFSET(1)])
      WHEN is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name
    AND LOWER(segment_channel) LIKE "%marketing email%" THEN 'marketing email'
      WHEN is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name AND LOWER(segment_channel) LIKE "%editorial email%" THEN 'editorial email'
      WHEN is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name
    AND LOWER(segment_channel) LIKE "%direct email%" THEN 'direct email'
      WHEN is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%email%" THEN 'email'
      WHEN is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%affiliates%" THEN 'affiliates'
    --rules when visit_traffic_source_name != segment_name and is_campaign_landing = false
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%search%" THEN "search"
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%social media%" THEN "social free"
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%push notification%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%internal%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%aggregator%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%unclassified referrer%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%direct%" THEN visit_traffic_source_type
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%news%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%partner%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%sister%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%wiki%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%blog%" THEN LOWER(visit_traffic_source_type)
    WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%external print ad%" THEN LOWER(visit_traffic_source_type) --NEW RULE
    WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%internal print ad%" THEN LOWER(visit_traffic_source_type) --NEW RULE
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%university%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%external%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%email%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%affiliates%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%barrier%" THEN LOWER(visit_traffic_source_type)
    --rules when visit_traffic_source_name = segment_name and is_campaign_landing = false
      WHEN is_campaign_landing = FALSE
    AND segment_name = visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%affiliates%" THEN LOWER(visit_traffic_source_type)
      ELSE NULL
  END
    AS processed_channel,
    -- Extract platform  --  --  --  --  --  --  --  --  --  --  --  --  --
    CASE
    -- rules using traffic source only
      WHEN LOWER(visit_traffic_source_name) = 'sem' OR LOWER(visit_traffic_source_name) LIKE "%- sem -%" OR LOWER(visit_traffic_source_name) LIKE "%- sem-%" OR LOWER(visit_traffic_source_name) LIKE '%paid search%' OR visit_traffic_source_name LIKE 'Paid Media_H2 2023 Sale_Search_%' OR visit_traffic_source_name LIKE "Paid Media_BAU_Search_%" OR LOWER(visit_traffic_source_type) = 'sem' THEN 'search'
      WHEN LOWER(visit_traffic_source_type) LIKE '%capi%' THEN "capi"
      WHEN (LOWER(visit_traffic_source_name) LIKE 'paid media_%' OR LOWER(visit_traffic_source_name) LIKE 'organic_%') AND (LENGTH(visit_traffic_source_name) - LENGTH(REPLACE(visit_traffic_source_name, '_', ''))) >= 5 THEN LOWER(SPLIT(visit_traffic_source_name, '_')[SAFE_OFFSET(2)])
      WHEN LOWER(visit_traffic_source_name) = 'sem'
    OR LOWER(visit_traffic_source_name) LIKE "%- sem -%"
    OR LOWER(visit_traffic_source_name) LIKE "%- sem-%"
    OR LOWER(visit_traffic_source_type) = 'sem' THEN 'sem'
      WHEN is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%affiliates%" THEN 'affiliates'
      WHEN LOWER(visit_traffic_source_type) LIKE "%ft print%" THEN LOWER(visit_traffic_source_type)
      WHEN (LOWER(visit_traffic_source_name) LIKE "%pmax"
      OR LOWER(visit_traffic_source_name) LIKE "%performance max%")
    AND LOWER(visit_traffic_source_name) LIKE "%paid search%"
    OR LOWER(visit_traffic_source_name) LIKE "% sem %" THEN "search" --Updated from "%sem%" 
      WHEN LOWER(visit_traffic_source_type) LIKE "%barrier%" THEN LOWER(visit_traffic_source_type)
      WHEN LOWER(visit_traffic_source_type) LIKE "%external video%" THEN LOWER(visit_traffic_source_type)
      WHEN LOWER(visit_traffic_source_type) LIKE "%outbrain%" THEN "outbrain" --NEW RULE
    --rules using segment info
      WHEN is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name
    AND (LOWER(segment_channel) = 'sem'
      OR LOWER(segment_name) LIKE "%- sem -%"
      OR LOWER(segment_name) LIKE "%- sem-%"
      OR LOWER(segment_name) LIKE '%paid search%'
      OR segment_name LIKE 'Paid Media_H2 2023 Sale_Search_%'
      OR segment_name LIKE "Paid Media_BAU_Search_%"
      OR segment_campaign_name LIKE "Paid Media - SEM") THEN 'search' --NEW RULE
      WHEN is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name AND (visit_traffic_source_name LIKE 'Paid Media_H2 2023 Sale_Landing Page_%' AND visit_traffic_source_name LIKE "% PMAX") THEN 'search'
      WHEN is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name
    AND (LOWER(segment_name) LIKE 'paid media_%'
      OR LOWER(segment_name) LIKE 'organic_%')
    AND (LENGTH(segment_name) - LENGTH(REPLACE(segment_name, '_', ''))) >= 5 THEN LOWER(SPLIT(segment_name, '_')[SAFE_OFFSET(2)])
      WHEN is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name AND (LOWER(segment_channel) = 'sem' OR LOWER(segment_name) LIKE "%- sem -%" OR LOWER(segment_name) LIKE "%- sem-%" OR LOWER(visit_traffic_source_type) = 'sem') THEN 'search'
    --rules when visit_traffic_source_name != segment_name and is_campaign_landing = false
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%partnership%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%push notification%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%aggregator%" THEN LOWER(visit_traffic_source_name)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%internal%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%search%" THEN LOWER(visit_traffic_source_name)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%social media%" THEN LOWER(visit_traffic_source_name)
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%direct%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%internal%" THEN visit_traffic_source_type
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%unclassified referrer%" AND LOWER(visit_traffic_source_name) NOT LIKE "%outbrain%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%news%" THEN LOWER(visit_traffic_source_name)
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%partner%" THEN LOWER(visit_traffic_source_name)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%sister%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%wiki%" THEN LOWER(visit_traffic_source_name)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%blog%" THEN LOWER(visit_traffic_source_name)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%external print ad%" THEN "external print ad" -- NEW RULE
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%internal print ad%" THEN "internal print ad" -- NEW RULE
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%university%" THEN visit_traffic_source_name
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%external%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%email%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%affiliates%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE
    AND segment_name != visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%barrier%" THEN LOWER(visit_traffic_source_type)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_name) LIKE "%microsoft teams%" THEN LOWER(visit_traffic_source_name)
      WHEN is_campaign_landing = FALSE AND segment_name != visit_traffic_source_name AND LOWER(visit_traffic_source_type) LIKE "%ft_print%" THEN LOWER(visit_traffic_source_type)
    --rules when visit_traffic_source_name = segment_name and is_campaign_landing = false
      WHEN is_campaign_landing = FALSE
    AND segment_name = visit_traffic_source_name
    AND LOWER(visit_traffic_source_type) LIKE "%affiliates%" THEN LOWER(visit_traffic_source_type)
      ELSE NULL
  END
    AS processed_platform
  FROM
    TEMP),
  reference_table AS (
  SELECT
    DISTINCT *,
    -- Assign static channel values
    CASE
    --rules using traffic source only
      WHEN visit_traffic_source_type = "Facebook" AND visit_traffic_source_name = "Social" THEN "unclassified social" --NEW RULE
      WHEN LOWER(visit_traffic_source_name) LIKE '%onsite%' AND processed_channel IS NULL THEN 'onsite'
      WHEN LOWER(visit_traffic_source_name) LIKE '%ppc%'
    AND processed_channel IS NULL THEN 'ppc'
      WHEN LOWER(visit_traffic_source_name) LIKE '%pmax%'AND processed_channel IS NULL THEN 'pmax'
      WHEN LOWER(visit_traffic_source_name) LIKE '%influencer%'AND processed_channel IS NULL THEN 'influencer'
      WHEN LOWER(visit_traffic_source_name) LIKE '%article%' AND processed_channel IS NULL THEN 'article'
      WHEN LOWER(visit_traffic_source_name) LIKE '%organic social%'
    AND processed_channel IS NULL THEN 'organic social'
      WHEN LOWER(visit_traffic_source_name) LIKE '%paid social%'AND processed_channel IS NULL THEN 'paid social'
      WHEN LOWER(visit_traffic_source_name) LIKE '%video%'
    AND processed_channel IS NULL THEN 'video'
      WHEN LOWER(visit_traffic_source_name) LIKE '%affiliates%' AND processed_channel IS NULL THEN 'affiliates'
      WHEN LOWER(visit_traffic_source_type) LIKE '%external print ad%' AND processed_channel IS NULL THEN 'external print ad' --NEW RULE
      WHEN LOWER(visit_traffic_source_type) LIKE '%internal print ad%' AND processed_channel IS NULL THEN 'internal print ad' --NEW RULE
      WHEN LOWER(visit_traffic_source_name) LIKE '%newsletter%'AND processed_channel IS NULL THEN 'newsletter'
      WHEN LOWER(visit_traffic_source_name) LIKE '%display%' AND processed_channel IS NULL THEN 'display'
      WHEN LOWER(visit_traffic_source_name) LIKE '%audio%'
    AND processed_channel IS NULL THEN 'audio'
      #WHEN LOWER(visit_traffic_source_name) LIKE '%app%'AND processed_channel IS NULL AND LOWER(visit_traffic_source_name) NOT LIKE "%whatsapp%" THEN 'app'
      WHEN LOWER(visit_traffic_source_name) LIKE '%print%'
    AND processed_channel IS NULL AND visit_traffic_source_name NOT LIKE "Print Sale" THEN 'print'
      WHEN LOWER(visit_traffic_source_name) LIKE '%marketing email%' AND processed_channel IS NULL THEN 'marketing email'
      WHEN LOWER(visit_traffic_source_name) LIKE '%editorial email%'
    AND processed_channel IS NULL THEN 'editorial email'
      WHEN LOWER(visit_traffic_source_name) LIKE '%direct email%'AND processed_channel IS NULL THEN 'direct email'
      --WHEN LOWER(visit_traffic_source_name) LIKE '%email%'AND processed_channel IS NULL THEN 'email'
      WHEN LOWER(visit_traffic_source_name) LIKE '%podcast%'AND processed_channel IS NULL THEN 'podcast'
      WHEN LOWER(visit_traffic_source_type) LIKE '%external display%'AND processed_platform IS NULL THEN 'external display'
      WHEN LOWER(visit_traffic_source_type) LIKE '%social_free%' THEN 'social free' -- NEW RULE
    -- rules using segment info (i.e., we can only use segment_campaign_name when we have the conditions below)
      WHEN LOWER(segment_campaign_name) LIKE '%onsite%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'onsite'
      WHEN LOWER(segment_campaign_name) LIKE '%ppc%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'ppc'
      WHEN LOWER(segment_campaign_name) LIKE '%pmax%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'pmax'
      WHEN LOWER(segment_campaign_name) LIKE '%influencer%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'influencer'
      WHEN LOWER(segment_campaign_name) LIKE '%article%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'article'
      WHEN LOWER(segment_campaign_name) LIKE '%organic social%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'organic social'
      WHEN LOWER(segment_campaign_name) LIKE '%paid social%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'paid social'
      WHEN LOWER(segment_campaign_name) LIKE '%video%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'video'
      WHEN LOWER(segment_campaign_name) LIKE '%affiliates%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'affiliates'
      WHEN LOWER(segment_campaign_name) LIKE '%newsletter%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'newsletter'
      WHEN LOWER(segment_campaign_name) LIKE '%display%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'display'
      WHEN LOWER(segment_campaign_name) LIKE '%audio%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'audio'
     WHEN LOWER(visit_traffic_source_type) LIKE "%ft app" AND LOWER(segment_campaign_name) LIKE '%app%'
     AND processed_channel IS NULL
     AND is_campaign_landing = TRUE
     AND segment_name = visit_traffic_source_name
     AND LOWER(visit_traffic_source_name) NOT LIKE "%whatsapp%" THEN 'ft app' --NEW RULE changed from 'app'
      WHEN LOWER(segment_campaign_name) LIKE '%print%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'print'
      WHEN LOWER(segment_campaign_name) LIKE '%marketing email%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'marketing email'
      WHEN LOWER(segment_campaign_name) LIKE '%editorial email%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'editorial email'
      WHEN LOWER(segment_campaign_name) LIKE '%direct email%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'direct email'
      WHEN LOWER(segment_campaign_name) LIKE '%email%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'other email'
      WHEN LOWER(segment_campaign_name) LIKE '%podcast%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'podcast'
      ELSE NULL
  END
    AS static_channel,
    -- Assign static platform values
    CASE
    --rules using traffic source only
      WHEN LOWER(visit_traffic_source_name) LIKE '%unidays%'AND processed_platform IS NULL THEN 'unidays'
      WHEN LOWER(visit_traffic_source_name) LIKE '%search%' AND processed_platform IS NULL THEN 'search'
      WHEN LOWER(visit_traffic_source_name) LIKE '% sem %' --updated from '%sem%
    AND processed_platform IS NULL THEN 'sem'
    WHEN LOWER(visit_traffic_source_name) LIKE '%newsletter%'
    AND processed_platform IS NULL THEN 'newsletter'
     WHEN LOWER(visit_traffic_source_name) LIKE '%email%'
    AND processed_platform IS NULL THEN 'email'
      WHEN LOWER(visit_traffic_source_name) LIKE '%dv360%' AND processed_platform IS NULL THEN 'dv360'
      WHEN LOWER(visit_traffic_source_name) LIKE '%instagram%'AND processed_platform IS NULL THEN 'instagram'
      WHEN (LOWER(visit_traffic_source_name) LIKE '%facebook%' OR LOWER(visit_traffic_source_name) LIKE '%fb%') AND processed_platform IS NULL THEN 'facebook'
      WHEN LOWER(visit_traffic_source_name) LIKE '%linkedin%'AND processed_platform IS NULL THEN 'linkedin'
      WHEN LOWER(visit_traffic_source_name) LIKE '%acast%' AND processed_platform IS NULL THEN 'acast'
      WHEN LOWER(visit_traffic_source_name) LIKE '%twitter%'
    AND processed_platform IS NULL THEN 'twitter'
      WHEN (LOWER(visit_traffic_source_name) LIKE '%youtube%' OR LOWER(visit_traffic_source_name) LIKE '%- yt -%') AND processed_platform IS NULL THEN 'youtube' --NEW RULE
      WHEN LOWER(visit_traffic_source_name) LIKE '%outbrain%'AND processed_platform IS NULL THEN 'outbrain'
      WHEN LOWER(visit_traffic_source_name) LIKE '%display%'
    AND processed_platform IS NULL THEN 'display'
      WHEN LOWER(visit_traffic_source_name) LIKE '%affiliates%'AND processed_platform IS NULL THEN 'affiliates'
      WHEN LOWER(visit_traffic_source_type) LIKE "%ft app" AND LOWER(visit_traffic_source_name) LIKE '%app%'
    AND processed_platform IS NULL
    AND LOWER(visit_traffic_source_name) NOT LIKE "%whatsapp%" THEN 'ft app' --NEW RULE changed from 'app'
      WHEN LOWER(visit_traffic_source_name) LIKE '%house ads%' AND processed_platform IS NULL THEN 'house ads'
      WHEN LOWER(visit_traffic_source_name) LIKE '%ft_print%'
    AND processed_platform IS NULL THEN 'ft print'
      WHEN LOWER(visit_traffic_source_name) LIKE '%google dem gen%' AND processed_platform IS NULL THEN 'google dem gen'
      WHEN LOWER(visit_traffic_source_name) LIKE '%keywee%'AND processed_platform IS NULL THEN 'keywee'
      WHEN LOWER(visit_traffic_source_name) LIKE '%reddit%'AND processed_platform IS NULL THEN 'reddit'
      WHEN LOWER(visit_traffic_source_name) LIKE '%blis%'AND processed_platform IS NULL THEN 'blis'
      WHEN LOWER(visit_traffic_source_name) LIKE '%direct email%'AND processed_channel IS NULL THEN 'direct email'
      WHEN LOWER(visit_traffic_source_name) LIKE '%meta%'AND processed_channel IS NULL THEN 'meta'
      WHEN LOWER(visit_traffic_source_name) LIKE '%google%'AND processed_channel IS NULL THEN 'google'
      WHEN LOWER(visit_traffic_source_name) LIKE '%sony%'AND processed_platform IS NULL THEN 'sony'
      WHEN LOWER(visit_traffic_source_name) LIKE '%siriusxm_%'AND processed_platform IS NULL THEN 'siriusxm'
      WHEN LOWER(visit_traffic_source_type) = "facebook" AND processed_platform IS NULL THEN 'facebook' --NEW RULE
      WHEN LOWER(visit_traffic_source_name) LIKE '%whatsapp%'AND processed_platform IS NULL THEN 'whatsapp' --NEW RULE
      WHEN LOWER(visit_traffic_source_name) LIKE '%tiktok%'AND processed_platform IS NULL THEN 'tiktok' --NEW RULE
      
    
    -- rules using segment info (i.e., we can only use segment_campaign_name when we have the conditions below)
      WHEN LOWER(segment_campaign_name) LIKE '%search%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'search'
      WHEN (LOWER(segment_campaign_name) = 'sem' OR LOWER(segment_campaign_name) LIKE '% sem %') AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'sem'--Updated rule
      WHEN LOWER(segment_campaign_name) LIKE '%google%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'google'
      WHEN LOWER(segment_campaign_name) LIKE '%newsletter%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'newsletter'
      WHEN LOWER(segment_campaign_name) LIKE '%dv360%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'dv360'
      WHEN LOWER(segment_campaign_name) LIKE '%instagram%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'instagram'
      WHEN (LOWER(segment_campaign_name) LIKE '%facebook%' OR LOWER(segment_campaign_name) LIKE '%fb%') AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'facebook'
      WHEN LOWER(segment_campaign_name) LIKE '%linkedin%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'linkedin'
      WHEN LOWER(segment_campaign_name) LIKE '%acast%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'acast'
      WHEN LOWER(segment_campaign_name) LIKE '%twitter%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'twitter'
      WHEN (LOWER(segment_campaign_name) LIKE '%youtube%' OR LOWER(segment_campaign_name) LIKE '%- yt -%') AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'youtube' -- NEW RULE
      WHEN LOWER(segment_campaign_name) LIKE '%display%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'display'
      WHEN LOWER(segment_campaign_name) LIKE '%affiliates%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'affiliates'
    WHEN LOWER(visit_traffic_source_type) LIKE "%ft app" AND LOWER(segment_campaign_name) LIKE '%app%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name
    AND LOWER(visit_traffic_source_name) NOT LIKE "%whatsapp%" THEN 'ft app' --NEW RULE changed from 'app'
      WHEN LOWER(segment_campaign_name) LIKE '%house ads%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'house ads'
      WHEN LOWER(segment_campaign_name) LIKE '%ft print%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'ft print'
      WHEN LOWER(segment_campaign_name) LIKE '%google dem gen%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'google dem gen'
      WHEN LOWER(segment_campaign_name) LIKE '%keywee%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'keywee'
      WHEN LOWER(segment_campaign_name) LIKE '%reddit%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'reddit'
      WHEN LOWER(segment_campaign_name) LIKE '%blis%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'blis'
      WHEN LOWER(segment_campaign_name) LIKE '%meta%' AND processed_channel IS NULL AND is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN 'meta'
      WHEN LOWER(segment_campaign_name) LIKE '%whatsapp%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'whatsapp' --NEW RULE
    WHEN LOWER(segment_campaign_name) LIKE '%tiktok%'
    AND processed_channel IS NULL
    AND is_campaign_landing = TRUE
    AND segment_name = visit_traffic_source_name THEN 'tiktok' --NEW RULE
      ELSE NULL
  END
    AS static_platform
  FROM
    processed ),
  final_filled AS (
  SELECT
    *,
    -- Final fallback rules for channel
    CASE
    --rules using traffic source only and  rules using segment info
      WHEN is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN COALESCE(processed_channel, static_channel, LOWER(segment_channel), LOWER(visit_traffic_source_type)) --NEW RULE changed from 'other'
      ELSE COALESCE(processed_channel, static_channel, LOWER(visit_traffic_source_type)) --NEW RULE changed from 'other'
  END
    AS channel,
    -- Final fallback rules for platform
    CASE
    --rules using traffic source only and  rules using segment info
      WHEN is_campaign_landing = TRUE AND segment_name = visit_traffic_source_name THEN COALESCE(processed_platform, static_platform, processed_channel, static_channel, LOWER(visit_traffic_source_type)) --NEW RULE changed from 'other'
      ELSE COALESCE(processed_platform, static_platform, processed_channel, static_channel, LOWER(visit_traffic_source_type)) --NEW RULE changed from 'other'
  END
    AS platform
  FROM
    reference_table )
SELECT
  *
FROM
  final_filled
