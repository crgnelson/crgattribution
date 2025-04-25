-- models/staging/stg_conv_visits.sql

with source as (

    select * from `ft-bi-team.BI_Layer_Integration.conv_visit_attribution`

),

renamed as (

    select
        user_guid,
        attribution_visit_start_time,
        visit_traffic_source_type,
        visit_traffic_source_name,
        visit_device_type,
        is_campaign_landing,
        segment_name,
        segment_campaign_name,
        segment_campaign_grouping,
        segment_channel,
        segment_audience,
        b2c_abstraction_datasource,
        b2b_b2c,
        product_name,
        b2c_product_name_and_term,
        geo_country_name,
        conversion_visit_timestamp,
        converting_visit
    from source

)

select * from renamed
