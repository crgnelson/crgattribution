{{ config(materialized='table') }}

with raw as (
  select * from {{ source('attribution_pipeline', 'test_attribution_channel_level') }}
),

transformed as (
  select
    channels,
    attribution_last_click_heuristic,
    attribution_markov_algorithmic,
    run_date,
    conversion_window,
    conversion_type,

    -- Parsing 'channels' string inline
    case when trim(channels) = '-' then '-' else trim(split(channels, '|')[safe_offset(0)]) end as media_type,
    case when trim(channels) = '-' then '-' else trim(split(channels, '|')[safe_offset(1)]) end as media_channel,
    case when trim(channels) = '-' then '-' else trim(split(channels, '|')[safe_offset(2)]) end as platform,
    case when trim(channels) = '-' then '-' else trim(
      regexp_replace(
        regexp_replace(channels, r'^([^|]+\|){3}', ''),
        r'(\|[^|]+){4}$', ''
      )
    ) end as channel_name,
    case when trim(channels) = '-' then '-' else trim(split(channels, '|')[safe_offset(array_length(split(channels, '|')) - 4)]) end as channel_device_type,
    case when trim(channels) = '-' then '-' else trim(split(channels, '|')[safe_offset(array_length(split(channels, '|')) - 3)]) end as geo_country_name,
    case when trim(channels) = '-' then '-' else trim(split(channels, '|')[safe_offset(array_length(split(channels, '|')) - 2)]) end as user_cohort_primary,
    case when trim(channels) = '-' then '-' else trim(split(channels, '|')[safe_offset(array_length(split(channels, '|')) - 1)]) end as product_type
  from raw
),

final as (
  select
    *,
    -- Business logic
    case 
      when media_channel in ('social_free', 'social free') then 'organic social'
      when media_channel = 'ppc' then 'paid search'
      when media_type = 'organic' and media_channel = 'search' then 'organic search'
      else media_channel
    end as cleaned_media_channel,

    case 
      when media_type in ('email', 'editorial email') then 'organic'
      else media_type
    end as cleaned_media_type,

    case 
      when media_type = 'email' and platform = 'newsletter' then 'email'
      else platform
    end as cleaned_platform,

    case 
      when conversion_type = 'Trial' then 'Trial'
      when product_type in ('Premium Monthly', 'Premium Annual', 'Standard Monthly', 'Standard Annual') then 'Directs'
      else 'Other'
    end as trial_direct
  from transformed
)

select * from final
