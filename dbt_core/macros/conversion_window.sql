-- macros/conversion_window.sql

{% macro generate_conversion_window_model(lookback_days) %}

with converting_users as (
    select distinct
        user_guid,
        conversion_visit_timestamp
    from {{ ref('stg_channel_classification') }}
    where b2b_b2c = 'B2C'
      and converting_visit = 1
      and date(conversion_visit_timestamp) >= date_sub(current_date(), interval 15 day)
),

selected_visits as (
    select
        a.user_guid,
        a.attribution_visit_start_time,
        a.conversion_visit_timestamp,
        a.visit_traffic_source_type,
        a.visit_traffic_source_name,
        a.visit_device_type,
        a.converting_visit,
        a.conversion_type,
        a.product_arrangement_id,
        a.geo_country_name,
        a.product_name,
        a.user_cohort_primary,
        a.media_type,
        a.channel,
        a.platform,
        a.b2c_product_type,
        a.b2c_product_name_and_term,
        concat(
            media_type, ' | ',
            channel, ' | ',
            platform, ' | ',
            visit_traffic_source_name, ' | ',
            visit_device_type, ' | ',
            geo_country_name, ' | ',
            user_cohort_primary, ' | ',
            case
                when lower(product_name) like "%e-paper%" then "Digital Edition"
                when lower(product_name) like "%edit%" then "FT Edit"
                when lower(product_name) = "newspaper - 5 weekdays" then "Newspaper"
                when lower(product_name) = "newspaper - 6 days a week" then "Newspaper"
                when lower(product_name) = "ft weekend" then "FT Weekend"
                when lower(product_name) = "newspaper - weekend only" then "Newspaper"
                when lower(b2c_product_name_and_term) = "standard monthly" then "Standard Monthly"
                when lower(b2c_product_name_and_term) = "premium monthly" then "Premium Monthly"
                when lower(b2c_product_name_and_term) = "standard annual" then "Standard Annual"
                when lower(b2c_product_name_and_term) = "premium annual" then "Premium Annual"
                else "Others"
            end
        ) as touchpoint
    from {{ ref('stg_channel_classification') }} a
    join converting_users b
        on a.user_guid = b.user_guid
       and a.conversion_visit_timestamp = b.conversion_visit_timestamp
    where date(a.attribution_visit_start_time) >= date_sub(date(b.conversion_visit_timestamp), interval {{ lookback_days }} day)
       and date(a.attribution_visit_start_time) <= date(b.conversion_visit_timestamp)

)

select * from selected_visits

{% endmacro %}
