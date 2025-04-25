{{ config(materialized='table') }}

with removal_effects as (
  select
    channel,
    run_date,
    conversion_type,
    conversion_window,
    average_ltv,
    removal_effect
  from {{ source('attribution_pipeline', 'test_attribution_removal_effects') }}
),

channel_meta as (
  select *
  from {{ ref('mkt_channel_level_agg') }}
),

final as (
  select
    cm.*,
    re.removal_effect,
    re.average_ltv
  from channel_meta cm
  left join removal_effects re
    on cm.channels = re.channel
   and cm.run_date = re.run_date
   and cm.conversion_window = re.conversion_window
   and cm.conversion_type = re.conversion_type
)

select * from final
